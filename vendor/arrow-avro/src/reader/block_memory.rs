// SPDX-License-Identifier: Apache-2.0
// Original ShardLoom continuation and block-storage admission integration.

use crate::errors::AvroError;
use arrow_schema::ArrowError;
use std::fmt::Debug;

/// Caller-owned accounting for OCF block storage.
///
/// The reader calls this before growing encoded or decompressed block buffers,
/// including replacement overlap, and retains the owner while a block is live.
/// A rejection is fatal: consumed input must not be retried. This scope excludes
/// input buffering, schema/record builders and codec-private workspaces; callers
/// must admit those separately. It is not an allocator or process-RSS bound.
pub trait BlockMemoryReservation: Debug + Send + Sync {
    /// Replace the current block-storage reservation with `bytes`.
    fn resize(&self, bytes: usize) -> Result<(), ArrowError>;
}

pub(crate) fn reserve(
    buffer: &mut Vec<u8>,
    required: usize,
    other: usize,
    owner: &dyn BlockMemoryReservation,
) -> Result<(), AvroError> {
    let add = |left: usize, right: usize| {
        left.checked_add(right)
            .ok_or_else(|| AvroError::ParseError("Avro block capacity overflow".to_string()))
    };
    if required > buffer.capacity() {
        // Charge both old and replacement storage before requesting growth.
        owner.resize(add(add(other, buffer.capacity())?, required)?)?;
        buffer
            .try_reserve_exact(required - buffer.len())
            .map_err(|error| AvroError::External(Box::new(error)))?;
        if buffer.capacity() > required {
            return Err(AvroError::ParseError(
                "Avro allocator exceeded admitted block capacity".to_string(),
            ));
        }
    }
    owner.resize(add(other, buffer.capacity())?)?;
    Ok(())
}
