//! Share structural credits without nesting allocator calls or writable buffers.

use std::{any::Any, fmt, sync::Arc};

use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::memory::{
        HostAllocator, HostAllocatorRef, HostBufferMut, HostMemoryReservation, WritableHostBuffer,
    },
    buffer::{Alignment, ByteBuffer},
    error::VortexResult,
};

pub(super) fn allocator(base: HostAllocatorRef, lease: Arc<MemoryLease>) -> HostAllocatorRef {
    let (base, parent) =
        if let Some(credited) = (base.as_ref() as &dyn Any).downcast_ref::<CreditAllocator>() {
            (Arc::clone(&credited.base), credited.credits.clone())
        } else {
            (base, Credits::default())
        };
    Arc::new(CreditAllocator {
        base,
        credits: Credits(Some(Arc::new(CreditNode { parent, lease }))),
    })
}

#[derive(Clone, Default)]
struct Credits(Option<Arc<CreditNode>>);

struct CreditNode {
    parent: Credits,
    lease: Arc<MemoryLease>,
}

impl fmt::Debug for Credits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credits").finish_non_exhaustive()
    }
}

impl Drop for Credits {
    fn drop(&mut self) {
        let mut current = self.0.take();
        let mut pending = Vec::new();
        while let Some(node) = current {
            let Some(CreditNode { mut parent, lease }) = Arc::into_inner(node) else {
                break;
            };
            current = parent.0.take();
            pending.push(lease);
        }
        // MemoryLease destruction is infallible. Preserve the old nested field
        // order, so older credits are refunded before newer credits.
        while let Some(lease) = pending.pop() {
            drop(lease);
        }
    }
}

#[derive(Debug)]
struct CreditAllocator {
    // Field order keeps the base allocator alive until its storage is gone,
    // and refunds metadata only after releasing the base allocator handle.
    base: HostAllocatorRef,
    credits: Credits,
}

impl HostAllocator for CreditAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        Ok(WritableHostBuffer::new(Box::new(CreditBuffer {
            buffer: self.base.allocate(len, alignment)?,
            credits: self.credits.clone(),
        })))
    }

    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        Ok(HostMemoryReservation::new((
            self.base.reserve_external(len)?,
            self.credits.clone(),
        )))
    }
}

struct CreditBuffer {
    buffer: WritableHostBuffer,
    credits: Credits,
}

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
        let Self { buffer, credits } = *self;
        buffer.freeze().with_retained_owner(Arc::new(credits))
    }
}

#[cfg(test)]
#[path = "owned_buffers_credit_tests.rs"]
mod tests;
