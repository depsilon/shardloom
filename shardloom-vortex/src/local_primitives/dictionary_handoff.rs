//! Keep a filtered dictionary's unused domain out of owned execution/evidence state.

use super::{Result, ShardLoomError, vortex_error};
use vortex::array::{ArrayRef, IntoArray as _, arrays::PrimitiveArray};

/// Remap only sparse domains. Scratch is bounded by the surviving code count,
/// never by the potentially much larger source dictionary. Dense dictionaries
/// retain their existing representation and do not pay for sorting/remapping.
pub(super) fn referenced_values(
    values: &ArrayRef,
    codes: &mut [u32],
    row_nulls: Option<&[bool]>,
) -> Result<ArrayRef> {
    let invalid = |message: &str| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex dictionary handoff: {message}; no fallback execution was attempted"
        ))
    };
    if row_nulls.is_some_and(|nulls| nulls.len() != codes.len()) {
        return Err(invalid("code validity length mismatch"));
    }
    let is_null = |row: usize| row_nulls.is_some_and(|nulls| nulls[row]);
    for (row, &code) in codes.iter().enumerate() {
        if !is_null(row) && code as usize >= values.len() {
            return Err(invalid("valid code exceeds dictionary value count"));
        }
    }
    // A fourfold bound ensures sorting scratch remains small relative to the
    // domain that would otherwise be materialized. This is an admission policy,
    // not a measured universal break-even claim.
    if values.len() <= codes.len().saturating_mul(4) {
        return Ok(values.clone());
    }
    let mut referenced = codes
        .iter()
        .enumerate()
        .filter_map(|(row, &code)| (!is_null(row)).then_some(code))
        .collect::<Vec<_>>();
    referenced.sort_unstable();
    referenced.dedup();
    if referenced.is_empty() {
        codes.fill(0);
        return values.slice(0..0).map_err(vortex_error);
    }
    for (row, code) in codes.iter_mut().enumerate() {
        *code = if is_null(row) {
            0
        } else {
            let index = referenced
                .binary_search(code)
                .map_err(|_| invalid("referenced code missing during remap"))?;
            u32::try_from(index).map_err(|_| invalid("compact code exceeds u32"))?
        };
    }
    // Vortex chooses the native take implementation (including encoded FSST).
    // Selection precedes canonicalization and preserves dictionary value nulls.
    values
        .take(
            referenced
                .into_iter()
                .collect::<PrimitiveArray>()
                .into_array(),
        )
        .map_err(vortex_error)
}

#[cfg(test)]
#[path = "dictionary_handoff_tests.rs"]
mod tests;
