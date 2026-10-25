// SPDX-License-Identifier: Apache-2.0
// Modified by ShardLoom on 2026-10-24: preserve fallible read admission while
// keeping the upstream buffer-allocator entry points available.

use vortex_array::memory::{HostAllocatorRef, HostBufferMut, WritableHostBuffer};
use vortex_buffer::{Alignment, BufferAllocatorRef, ByteBuffer, ByteBufferMut};
use vortex_error::VortexResult;
use vortex_error::vortex_err;

pub(crate) fn checked_read_end(offset: u64, length: usize) -> VortexResult<u64> {
    u64::try_from(length)
        .ok()
        .and_then(|length| offset.checked_add(length))
        .ok_or_else(|| vortex_err!("Read range end overflow: offset {offset}, length {length}"))
}

#[derive(Clone)]
pub(crate) enum ReadBufferAllocator {
    Buffer(BufferAllocatorRef),
    Host(HostAllocatorRef),
}

impl ReadBufferAllocator {
    pub(crate) fn allocate(
        &self,
        length: usize,
        alignment: Alignment,
    ) -> VortexResult<WritableHostBuffer> {
        match self {
            Self::Host(allocator) => allocator.allocate(length, alignment),
            Self::Buffer(allocator) => Ok(WritableHostBuffer::new(Box::new(ReadBuffer {
                buffer: ByteBufferMut::zeroed_aligned_in(length, alignment, allocator.clone()),
                alignment,
            }))),
        }
    }
}

struct ReadBuffer {
    buffer: ByteBufferMut,
    alignment: Alignment,
}

impl HostBufferMut for ReadBuffer {
    fn len(&self) -> usize {
        self.buffer.len()
    }
    fn alignment(&self) -> Alignment {
        self.alignment
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }
    fn freeze(self: Box<Self>) -> ByteBuffer {
        self.buffer.freeze()
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "fallible_read_tests.rs"]
mod tests;
