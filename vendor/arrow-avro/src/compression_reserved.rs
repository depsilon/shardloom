// SPDX-License-Identifier: Apache-2.0
//! Raw Deflate with stack-owned state and credited output as complete history.

use crate::{
    errors::AvroError,
    reader::{BlockMemoryReservation, block_memory::reserve},
};
use miniz_oxide::inflate::{TINFLStatus, core};

#[cfg(test)]
#[path = "compression_reserved_tests.rs"]
mod tests;

pub(super) fn deflate(
    block: &[u8],
    encoded_capacity: usize,
    owner: &dyn BlockMemoryReservation,
) -> Result<Vec<u8>, AvroError> {
    let invalid = || AvroError::ParseError("Invalid or truncated Avro Deflate block".to_string());
    let mut state = core::DecompressorOxide::new();
    let mut output = Vec::new();
    let mut input_position = 0_usize;
    let mut output_position = 0_usize;
    loop {
        let (status, consumed, written) = core::decompress(
            &mut state,
            &block[input_position..],
            &mut output,
            output_position,
            core::inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF,
        );
        input_position = input_position
            .checked_add(consumed)
            .filter(|position| *position <= block.len())
            .ok_or_else(invalid)?;
        output_position = output_position
            .checked_add(written)
            .filter(|position| *position <= output.len())
            .ok_or_else(invalid)?;
        match status {
            TINFLStatus::Done if input_position == block.len() => {
                output.truncate(output_position);
                return Ok(output);
            }
            TINFLStatus::HasMoreOutput => {
                let capacity = if output.capacity() == 0 {
                    block.len().max(1)
                } else {
                    output.capacity().checked_mul(2).ok_or_else(invalid)?
                };
                // Retain all prior output for codec back-references. No private
                // heap window is created, and old/new growth overlap is admitted.
                reserve(&mut output, capacity, encoded_capacity, owner)?;
                output.resize(capacity, 0);
            }
            _ => return Err(invalid()),
        }
    }
}
