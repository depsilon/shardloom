//! Reservation ownership at the native Vortex host-allocation boundary.
//!
//! Only allocations made through this provider are charged. This does not claim
//! to cover arbitrary upstream allocations, imported arrays, or process RSS.

use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use vortex::{
    array::memory::{DefaultHostAllocator, HostAllocator, HostBufferMut, WritableHostBuffer},
    buffer::{Alignment, ByteBuffer},
    error::{VortexResult, vortex_err},
};

#[derive(Debug, Clone)]
pub struct ReservedHostAllocator {
    memory: LiveMemoryPool,
}

impl ReservedHostAllocator {
    #[must_use]
    pub const fn new(memory: LiveMemoryPool) -> Self {
        Self { memory }
    }
}

impl HostAllocator for ReservedHostAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        // Pinned Vortex 0.85 DefaultHostAllocator requests len + preferred
        // alignment bytes. Charge that capacity, not just the logical slice.
        let capacity = len
            .checked_add(*alignment.max(Alignment::DEFAULT_ALIGNMENT))
            .and_then(|size| u64::try_from(size).ok())
            .ok_or_else(|| vortex_err!("native host allocation size overflow"))?;
        let lease = self
            .memory
            .reserve(capacity)
            .map_err(|error| vortex_err!("{error}"))?;
        let buffer = DefaultHostAllocator.allocate(len, alignment)?;
        Ok(WritableHostBuffer::new(Box::new(ReservedWritableBuffer {
            buffer,
            lease,
        })))
    }
}

struct ReservedWritableBuffer {
    buffer: WritableHostBuffer,
    lease: MemoryLease,
}

impl HostBufferMut for ReservedWritableBuffer {
    fn len(&self) -> usize {
        self.buffer.len()
    }

    fn alignment(&self) -> Alignment {
        self.buffer.alignment()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buffer.as_mut_slice()
    }

    fn freeze(self: Box<Self>) -> ByteBuffer {
        let Self { buffer, lease } = *self;
        let alignment = buffer.alignment();
        let owner = ReservedBufferOwner {
            buffer: buffer.freeze(),
            _lease: lease,
        };
        ByteBuffer::from_bytes_aligned(bytes::Bytes::from_owner(owner), alignment)
    }
}

// Field order ensures the last buffer owner releases memory before its credit.
struct ReservedBufferOwner {
    buffer: ByteBuffer,
    _lease: MemoryLease,
}

impl AsRef<[u8]> for ReservedBufferOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_and_clones_retain_full_allocation_credit_without_copying() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let allocator = ReservedHostAllocator::new(memory.clone());
        let mut buffer = allocator.allocate(1024, Alignment::new(64)).unwrap();
        buffer.as_mut_slice().fill(37);
        let pointer = buffer.as_mut_slice().as_ptr();
        let frozen = buffer.freeze();
        assert_eq!(pointer, frozen.as_ptr());
        let slice = frozen.slice(64..128);
        let clone = slice.clone();
        assert_eq!(memory.snapshot().reserved_bytes, 1280);
        drop(frozen);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 1280);
        assert_eq!(clone.as_slice(), &[37; 64]);
        drop(clone);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn rejected_allocations_and_unfrozen_buffers_leave_no_credit_leak() {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let allocator = ReservedHostAllocator::new(memory.clone());
        assert!(allocator.allocate(1024, Alignment::none()).is_err());
        assert!(allocator.allocate(usize::MAX, Alignment::none()).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let buffer = allocator.allocate(128, Alignment::new(512)).unwrap();
        assert_eq!(memory.snapshot().reserved_bytes, 640);
        drop(buffer);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
