//! Evaluate variable-width expression scalars once into independent native
//! buffers. Growth admits the complete replacement while the old buffer lives.

use super::{
    ArrayRef, DType, HostAllocatorRef, Result, Validity, Value, WritableHostBuffer, add, allocate,
    copy_bytes, failed, finish_column, finish_validity, multiply, set_bit, vortex_error,
};

#[cfg(test)]
#[path = "local_primitive_result_expression_variable_tests.rs"]
mod tests;

pub(super) fn build_once<'a>(
    dtype: &DType,
    rows: usize,
    allocator: &HostAllocatorRef,
    mut value: impl FnMut(usize) -> Result<Value<'a>>,
) -> Result<ArrayRef> {
    let mut validity = dtype
        .is_nullable()
        .then(|| allocate(allocator, rows.div_ceil(8), 1))
        .transpose()?;
    let mut offsets = allocate(allocator, multiply(add(rows, 1)?, 8)?, 8)?;
    let mut payload = allocate(allocator, 0, 1)?;
    let mut end = 0;
    let mut validation_error = None;
    for row in 0..rows {
        let value = value(row)?;
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.expression_variable_values += 1;
        });
        // The prior sizing pass evaluated all callbacks before output type and
        // null validation. Keep that semantic failure precedence without
        // retaining the scalars or evaluating their callbacks a second time.
        let error = match (dtype, &value) {
            (_, Value::Null) if !dtype.is_nullable() => Some("null in a nonnullable output column"),
            (_, Value::Null)
            | (DType::Utf8(_), Value::Text(_) | Value::SharedText(_))
            | (DType::Binary(_), Value::Binary(_) | Value::SharedBinary(_)) => None,
            _ => Some("value differs from the declared output dtype"),
        };
        validation_error = validation_error.or(error);
        if !matches!(value, Value::Null)
            && let Some(validity) = validity.as_mut()
        {
            set_bit(validity, row);
        }
        if let Some(bytes) = value.variable_bytes() {
            let needed = add(end, bytes.len())?;
            grow(&mut payload, end, needed, allocator)?;
            end = copy_bytes(&mut payload, end, bytes)?;
        }
        offsets.as_mut_slice()[(row + 1) * 8..(row + 2) * 8]
            .copy_from_slice(&u64::try_from(end).map_err(vortex_error)?.to_ne_bytes());
    }
    if let Some(message) = validation_error {
        return Err(failed(message));
    }
    let validity = validity
        .map(|buffer| finish_validity(buffer.freeze(), rows, allocator))
        .transpose()?
        .unwrap_or(Validity::NonNullable);
    finish_column(
        dtype,
        rows,
        offsets.freeze(),
        validity,
        Some(payload.freeze().slice(0..end)),
        allocator,
    )
}

fn grow(
    payload: &mut WritableHostBuffer,
    used: usize,
    needed: usize,
    allocator: &HostAllocatorRef,
) -> Result<()> {
    if needed <= payload.len() {
        return Ok(());
    }
    let capacity = needed.max(payload.len().saturating_mul(2).max(256));
    let mut replacement = allocate(allocator, capacity, 1)?;
    replacement.as_mut_slice()[..used].copy_from_slice(&payload.as_mut_slice()[..used]);
    *payload = replacement;
    Ok(())
}
