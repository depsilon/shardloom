// SPDX-License-Identifier: Apache-2.0
//! Optional caller-owned credits for decoder buffers. These account for the
//! named buffers, not allocator overhead, codec-private storage or process RSS.

use std::{fmt::Debug, io, sync::Arc};

#[path = "decoded_values.rs"]
pub(crate) mod decoded;

#[cfg(test)]
#[path = "memory_tests.rs"]
pub(crate) mod tests;

/// One independently owned credit. Dropping it releases its reservation.
pub trait DecoderMemoryReservation: Debug + Send + Sync {
    /// Admit the new total before growth. Failure must preserve the old credit.
    fn resize(&mut self, bytes: usize) -> io::Result<()>;
}

/// The caller's existing shared budget, used by each live decoder buffer.
pub trait DecoderMemory: Debug + Send + Sync {
    /// Reserve a new independently owned credit from the same budget.
    fn reserve(&self, bytes: usize) -> io::Result<Box<dyn DecoderMemoryReservation>>;

    /// Retain actual Zstandard workspace before choosing output rows when a
    /// prepared stream may enter a later compressed block. This is idempotent.
    fn prepare_zstd(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "controlled Zstandard requires a caller-owned decoder workspace",
        ))
    }

    /// Decode Zstandard using actual workspace owned by the caller's budget.
    /// The destination is already credited. Controlled readers require this
    /// capability; they never substitute an unaccounted streaming decoder.
    fn decode_zstd(&self, _input: &[u8], _output: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "controlled Zstandard requires a caller-owned decoder workspace",
        ))
    }
}

/// A payload and its credit move/drop together. Capacity growth admits both
/// old and replacement allocations, then refunds the old allocation after drop.
pub(crate) struct DecoderBuffer {
    pub(crate) bytes: Vec<u8>,
    credit: Option<Box<dyn DecoderMemoryReservation>>,
    memory: Option<Arc<dyn DecoderMemory>>,
}

impl DecoderBuffer {
    pub(crate) fn new(bytes: Vec<u8>, memory: Option<Arc<dyn DecoderMemory>>) -> Self {
        Self {
            bytes,
            credit: None,
            memory,
        }
    }

    pub(crate) fn controlled(&self) -> bool {
        self.memory.is_some()
    }

    pub(crate) fn memory(&self) -> Option<Arc<dyn DecoderMemory>> {
        self.memory.clone()
    }

    fn admit(&mut self, bytes: usize) -> io::Result<()> {
        if let Some(credit) = &mut self.credit {
            credit.resize(bytes)?;
        } else if let Some(memory) = &self.memory {
            self.credit = Some(memory.reserve(bytes)?);
        }
        Ok(())
    }

    pub(crate) fn check(&mut self) -> io::Result<()> {
        self.admit(self.bytes.capacity())
    }

    pub(crate) fn clear_storage(&mut self) {
        self.bytes = Vec::new();
        // Drop the bytes before returning the credit to the shared owner.
        self.credit = None;
    }

    pub(crate) fn grow(&mut self, capacity: usize, maximum: usize) -> io::Result<()> {
        if capacity > maximum {
            return Err(invalid("decompressed block exceeds its declared maximum"));
        }
        let old_capacity = self.bytes.capacity();
        if capacity <= old_capacity {
            return self.admit(old_capacity);
        }
        let overlap = old_capacity
            .checked_add(capacity)
            .ok_or_else(|| invalid("decoder buffer overlap overflow"))?;
        self.admit(overlap)?;
        let mut replacement = Vec::new();
        replacement
            .try_reserve_exact(capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        if replacement.capacity() > capacity {
            return Err(invalid("decoder allocation exceeds its reservation"));
        }
        replacement.extend_from_slice(&self.bytes);
        self.bytes = replacement;
        self.admit(capacity)
    }

    pub(crate) fn resize(&mut self, length: usize, maximum: usize) -> io::Result<()> {
        self.grow(length, maximum)?;
        self.bytes.resize(length, 0);
        Ok(())
    }

    pub(crate) fn read_block(
        &mut self,
        mut reader: impl io::Read,
        maximum: usize,
    ) -> io::Result<()> {
        self.bytes.clear();
        let mut part = [0; 8192];
        loop {
            let remaining = maximum - self.bytes.len();
            // One extra byte diagnoses expansion beyond the format's declared
            // block bound without growing another payload allocation.
            let take = part.len().min(remaining.saturating_add(1));
            let count = reader.read(&mut part[..take])?;
            if count == 0 {
                return Ok(());
            }
            let length = self
                .bytes
                .len()
                .checked_add(count)
                .ok_or_else(|| invalid("decompressed block length overflow"))?;
            if length > maximum {
                return Err(invalid("decompressed block exceeds its declared maximum"));
            }
            let capacity = length.max(self.bytes.capacity().saturating_mul(2).min(maximum));
            self.grow(capacity, maximum)?;
            self.bytes.extend_from_slice(&part[..count]);
        }
    }
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
