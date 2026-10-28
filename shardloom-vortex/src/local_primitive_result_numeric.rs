//! Owned numeric result copying after native take/cast. Preserve validation
//! order, initialized NULL slots and allocator lifetime without reconstructing
//! every selected value through scalar keys and generic result values.

use super::{
    ArrayRef, DType, HostAllocatorRef, PType, PrimitiveArray, Result, Validity, allocate, failed,
    finish_column, finish_validity, multiply, set_bit, width,
};
use vortex::{array::arrays::primitive::PrimitiveArrayExt as _, buffer::ByteBuffer, mask::Mask};

#[cfg(test)]
#[path = "local_primitive_result_numeric_tests.rs"]
mod tests;

pub(super) fn copy_column(
    dtype: &DType,
    values: &PrimitiveArray,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    check_cancelled: impl FnMut() -> Result<()>,
) -> Result<ArrayRef> {
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
    super::super::native_relational_observation::record(|counts| {
        counts.numeric_output_copy_batches += 1;
        counts.numeric_output_copy_rows += values.len() as u64;
    });
    macro_rules! copy {
        ($validate:expr) => {
            copy_validated(dtype, source, valid, allocator, $validate, check_cancelled)
        };
    }
    match ptype {
        PType::F32 => {
            let values = values.as_slice::<f32>();
            copy!(|row| finite(values[row].is_finite()))
        }
        PType::F64 => {
            let values = values.as_slice::<f64>();
            copy!(|row| finite(values[row].is_finite()))
        }
        PType::I32 if temporal.is_some() => {
            let values = values.as_slice::<i32>();
            copy!(|row| crate::native_temporal::validate_ticks(dtype, i64::from(values[row])))
        }
        PType::I64 if temporal.is_some() => {
            let values = values.as_slice::<i64>();
            copy!(|row| crate::native_temporal::validate_ticks(dtype, values[row]))
        }
        _ => copy!(|_| Ok(())),
    }
}

fn finite(valid: bool) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(failed(
            "numeric value differs from its finite declared dtype",
        ))
    }
}

fn copy_validated(
    dtype: &DType,
    source: &ByteBuffer,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    mut validate: impl FnMut(usize) -> Result<()>,
    mut check_cancelled: impl FnMut() -> Result<()>,
) -> Result<ArrayRef> {
    let rows = valid.len();
    let stride = width(dtype)?;
    let bytes = multiply(rows, stride)?;
    if source.len() != bytes {
        return Err(failed("selected numeric buffer changed byte count"));
    }
    // Keep the existing allocation and refusal order: validity, then values.
    let mut validity = dtype
        .is_nullable()
        .then(|| allocate(allocator, rows.div_ceil(8), 1))
        .transpose()?;
    let mut data = allocate(allocator, bytes, stride)?;
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
            data.as_mut_slice()[start * stride..end * stride]
                .copy_from_slice(&source[start * stride..end * stride]);
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
                let start = row * stride;
                data.as_mut_slice()[start..start + stride]
                    .copy_from_slice(&source[start..start + stride]);
            } else if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable output column"));
            }
        }
    }
    let validity = validity
        .map(|buffer| finish_validity(buffer.freeze(), rows, allocator))
        .transpose()?
        .unwrap_or(Validity::NonNullable);
    finish_column(dtype, rows, data.freeze(), validity, None, allocator)
}
