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
        let lease = reserve(&self.memory, capacity)?;
        let buffer = DefaultHostAllocator.allocate(len, alignment)?;
        Ok(WritableHostBuffer::new(Box::new(ReservedWritableBuffer {
            buffer,
            lease,
        })))
    }
}

#[derive(Debug)]
struct OwnedReservationDenied(shardloom_core::ShardLoomError);

pub(crate) fn reserve(memory: &LiveMemoryPool, bytes: u64) -> VortexResult<MemoryLease> {
    memory
        .reserve(bytes)
        .map_err(|error| vortex_err!(External: OwnedReservationDenied(error)))
}

impl std::fmt::Display for OwnedReservationDenied {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, formatter)
    }
}

impl std::error::Error for OwnedReservationDenied {}

#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn is_owned_reservation_denial(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error.is::<OwnedReservationDenied>() {
            return true;
        }
        let Some(source) = error.source() else {
            return false;
        };
        error = source;
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
struct ReservedBufferOwner<L = MemoryLease> {
    buffer: ByteBuffer,
    _lease: L,
}

/// Keep a pre-reserved native metadata owner with an existing required buffer.
/// No payload copy is needed; clones of this buffer retain both reservations.
#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn retain_credit(buffer: ByteBuffer, lease: MemoryLease) -> ByteBuffer {
    let alignment = buffer.alignment();
    ByteBuffer::from_bytes_aligned(
        bytes::Bytes::from_owner(ReservedBufferOwner {
            buffer,
            _lease: lease,
        }),
        alignment,
    )
}

/// Retain one admitted provider allocation across all of its native buffers.
#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn retain_shared_credit(
    buffer: ByteBuffer,
    lease: std::sync::Arc<MemoryLease>,
) -> ByteBuffer {
    let alignment = buffer.alignment();
    ByteBuffer::from_bytes_aligned(
        bytes::Bytes::from_owner(ReservedBufferOwner {
            buffer,
            _lease: lease,
        }),
        alignment,
    )
}

impl<L> AsRef<[u8]> for ReservedBufferOwner<L> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

/// Attach structural metadata credit to every buffer in a native result tree.
/// A surviving child, slice or clone keeps the shared credit alive independently
/// of the producer and of the allocator used while constructing the tree.
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn with_credit(
    allocator: vortex::array::memory::HostAllocatorRef,
    lease: MemoryLease,
) -> vortex::array::memory::HostAllocatorRef {
    std::sync::Arc::new(CreditAllocator {
        allocator,
        lease: std::sync::Arc::new(lease),
    })
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[derive(Debug)]
struct CreditAllocator {
    allocator: vortex::array::memory::HostAllocatorRef,
    lease: std::sync::Arc<MemoryLease>,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
impl HostAllocator for CreditAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        Ok(WritableHostBuffer::new(Box::new(CreditBuffer {
            buffer: self.allocator.allocate(len, alignment)?,
            lease: std::sync::Arc::clone(&self.lease),
        })))
    }
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
struct CreditBuffer {
    buffer: WritableHostBuffer,
    lease: std::sync::Arc<MemoryLease>,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
impl HostBufferMut for CreditBuffer {
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
        ByteBuffer::from_bytes_aligned(
            bytes::Bytes::from_owner(ReservedBufferOwner {
                buffer: buffer.freeze(),
                _lease: lease,
            }),
            alignment,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "vortex-local-primitives")]
    fn owned_denial_remains_typed_through_native_context_and_shared_wrappers() {
        let memory = LiveMemoryPool::new(16).unwrap();
        let allocator = ReservedHostAllocator::new(memory);
        let denied = allocator
            .allocate(128, Alignment::DEFAULT_ALIGNMENT)
            .err()
            .unwrap();
        assert!(is_owned_reservation_denial(&denied));
        let contextual = denied.with_context("native filter allocation");
        let shared = vortex::error::VortexError::Shared(std::sync::Arc::new(contextual));
        assert!(is_owned_reservation_denial(&shared));
        assert!(!is_owned_reservation_denial(
            &vortex_err!(InvalidArgument: "corrupt source")
        ));
        assert!(!is_owned_reservation_denial(&vortex_err!(
            "cancelled; memory reservation denied:"
        )));
    }

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
