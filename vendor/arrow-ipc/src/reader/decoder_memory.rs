// SPDX-License-Identifier: Apache-2.0
//! Independent allocation owners for the controlled IPC decoder.

use arrow_buffer::{Buffer, MutableBuffer};
use arrow_schema::ArrowError;
use bytes::Bytes;
use std::{fmt::Debug, sync::Arc};

/// An independently owned reservation, released with its allocation.
pub trait DecoderMemoryReservation: Debug + Send + Sync {}

/// Caller-owned admission for controlled decoder buffers and codec workspaces.
///
/// Returned payload buffers retain credits across clones and slices. Input
/// messages and schema/array metadata require their own lifetime owners. These
/// credits describe named capacities, not a process allocator or RSS bound.
pub trait DecoderMemory: Debug + Send + Sync {
    /// Admit an independent capacity before allocation.
    fn reserve(&self, bytes: usize) -> Result<Box<dyn DecoderMemoryReservation>, ArrowError>;

    /// Decode Zstandard using the caller's admitted workspace implementation.
    ///
    /// The destination already has an independent allocation owner. Implementors
    /// must account for codec state separately and return the number of bytes
    /// written. Controlled decoding never falls back to an unowned codec context.
    fn decompress_zstd(&self, _input: &[u8], _output: &mut [u8]) -> Result<usize, ArrowError> {
        Err(invalid(
            "controlled Zstandard decoder workspace is unavailable",
        ))
    }
}

pub(crate) type Memory = Option<Arc<dyn DecoderMemory>>;

pub(crate) fn invalid(message: &str) -> ArrowError {
    ArrowError::IpcError(message.to_owned())
}

/// `MutableBuffer` rounds capacity to a multiple of 64 before allocating.
pub(crate) fn aligned_capacity(length: usize) -> Result<usize, ArrowError> {
    length
        .checked_add(63)
        .map(|length| length & !63)
        .filter(|capacity| *capacity <= isize::MAX as usize)
        .ok_or_else(|| invalid("IPC buffer capacity overflow"))
}

pub(crate) struct AdmittedBuffer {
    buffer: MutableBuffer,
    credit: Box<dyn DecoderMemoryReservation>,
}

impl AdmittedBuffer {
    pub(crate) fn new(length: usize, memory: &dyn DecoderMemory) -> Result<Self, ArrowError> {
        let capacity = aligned_capacity(length)?;
        let credit = memory.reserve(capacity)?;
        let mut buffer = MutableBuffer::new(capacity);
        buffer.resize(length, 0);
        Ok(Self { buffer, credit })
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_slice_mut()
    }

    pub(crate) fn finish(self) -> Buffer {
        Buffer::from(Bytes::from_owner(OwnedBuffer {
            buffer: self.buffer.into(),
            _credit: self.credit,
        }))
    }
}

struct OwnedBuffer {
    // Drop the allocation before refunding its credit.
    buffer: Buffer,
    _credit: Box<dyn DecoderMemoryReservation>,
}

impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
