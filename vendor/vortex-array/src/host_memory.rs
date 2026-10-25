// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-24: retain the reviewed fallible host
// allocation and foreign-owner contract beside the 0.90 buffer allocator.
// Original API source: Vortex 0.85.0 src/memory.rs; see upstream-provenance.json.

//! Fallible exact-buffer and external-owner admission retained from Vortex 0.85.
//! The 0.90 `BufferAllocator` API remains separate: allocation denial there aborts.
//! This boundary preserves typed errors without an unsafe allocator implementation.
//! Typed access is limited to reviewed plain-data types and checks the actual
//! pointer, independently of a safe allocator's advertised alignment.

use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of;
use std::sync::Arc;

use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_session::SessionExt;
use vortex_session::SessionGuard;
use vortex_session::SessionVar;
use vortex_session::VortexSession;

/// Plain-data types admitted by the fallible host buffer's typed accessors.
///
/// Every bit pattern is valid, values contain no references or padding, and
/// writing a value leaves every byte initialized. This sealed set excludes
/// booleans, characters, nonzero integers, references and owning Rust objects.
pub trait HostBufferElement: host_element::Sealed + Copy + Send + Sync + 'static {}

mod host_element {
    use crate::arrays::varbinview::BinaryView;
    use crate::dtype::i256;

    pub trait Sealed {}

    macro_rules! elements {
        ($($type:ty),+ $(,)?) => {$(
            impl Sealed for $type {}
            impl super::HostBufferElement for $type {}
        )+};
    }

    elements!(
        u8,
        u16,
        u32,
        u64,
        u128,
        usize,
        i8,
        i16,
        i32,
        i64,
        i128,
        isize,
        f32,
        f64,
        half::f16,
        i256,
        BinaryView,
    );
}

/// Mutable host buffer contract used by [`WritableHostBuffer`].
pub trait HostBufferMut: Send + 'static {
    /// Returns the logical byte length of the buffer.
    fn len(&self) -> usize;

    /// Whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the alignment of the buffer.
    fn alignment(&self) -> Alignment;

    /// Returns mutable access to the writable byte range.
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Freeze the buffer into an immutable [`ByteBuffer`].
    fn freeze(self: Box<Self>) -> ByteBuffer;
}

/// Exact-size writable host buffer returned by a [`HostAllocator`].
pub struct WritableHostBuffer {
    inner: Box<dyn HostBufferMut>,
}

impl WritableHostBuffer {
    /// Create a writable host buffer from an implementation of [`HostBufferMut`].
    pub fn new(inner: Box<dyn HostBufferMut>) -> Self {
        Self { inner }
    }

    /// Returns the logical byte length of the buffer.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true when the buffer has zero bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the alignment of the buffer.
    pub fn alignment(&self) -> Alignment {
        self.inner.alignment()
    }

    /// Returns mutable access to the writable byte range.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.inner.as_mut_slice()
    }

    /// Returns mutable access to the buffer as a typed slice.
    pub fn as_mut_slice_typed<T: HostBufferElement>(&mut self) -> VortexResult<&mut [T]> {
        vortex_ensure!(
            size_of::<T>() != 0,
            InvalidArgument: "Cannot create typed mutable slice for zero-sized type {}",
            std::any::type_name::<T>()
        );
        vortex_ensure!(
            self.alignment().is_aligned_to(Alignment::of::<T>()),
            InvalidArgument: "Buffer is not sufficiently aligned for type {}",
            std::any::type_name::<T>()
        );

        let bytes = self.as_mut_slice();
        let byte_len = bytes.len();
        let ptr = bytes.as_mut_ptr();
        let type_size = size_of::<T>();

        vortex_ensure!(
            Alignment::of::<T>().is_ptr_aligned(ptr),
            InvalidArgument: "Mutable buffer pointer is not aligned for type {}",
            std::any::type_name::<T>()
        );

        vortex_ensure!(
            byte_len.is_multiple_of(type_size),
            InvalidArgument: "Buffer length {byte_len} is not a multiple of {} for {}",
            type_size,
            std::any::type_name::<T>()
        );

        // SAFETY: We checked size divisibility and actual pointer alignment,
        // and exclusively borrow initialized bytes. The sealed element set
        // accepts every bit pattern and cannot expose uninitialized padding.
        Ok(unsafe { std::slice::from_raw_parts_mut(ptr.cast::<T>(), byte_len / type_size) })
    }

    /// Freeze the writable buffer into an immutable [`ByteBuffer`].
    pub fn freeze(self) -> ByteBuffer {
        self.inner.freeze()
    }

    /// Freeze the writable buffer into a typed immutable [`Buffer<T>`].
    pub fn freeze_typed<T: HostBufferElement>(self) -> VortexResult<Buffer<T>> {
        vortex_ensure!(
            size_of::<T>() != 0,
            InvalidArgument: "Cannot freeze typed buffer for zero-sized type {}",
            std::any::type_name::<T>()
        );

        let buffer = self.freeze();
        let byte_len = buffer.len();
        let type_size = size_of::<T>();
        let type_align = Alignment::of::<T>();

        vortex_ensure!(
            byte_len.is_multiple_of(type_size),
            InvalidArgument: "Buffer length {byte_len} is not a multiple of {} for {}",
            type_size,
            std::any::type_name::<T>()
        );
        vortex_ensure!(
            buffer.is_aligned(type_align),
            InvalidArgument: "Buffer pointer is not aligned to {} for {}",
            type_align,
            std::any::type_name::<T>()
        );

        Ok(Buffer::from_byte_buffer(buffer))
    }
}

impl Debug for WritableHostBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WritableHostBuffer")
            .field("len", &self.len())
            .field("alignment", &self.alignment())
            .finish()
    }
}

/// Allocator for exact-size writable host buffers.
pub trait HostAllocator: Debug + Send + Sync + 'static {
    /// Allocate a writable host buffer with the requested byte length and alignment.
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer>;

    /// Reserve `len` bytes for a bounded provider that owns its heap allocations.
    ///
    /// The caller must establish a provider-specific upper bound before allocating
    /// and keep the returned owner until all covered storage has been freed.
    /// This does not allocate a second payload buffer. Custom allocators must
    /// explicitly implement admission; the default refuses an unsupported policy.
    fn reserve_external(&self, _len: usize) -> VortexResult<HostMemoryReservation> {
        Err(vortex_err!(
            "Host allocator does not support external memory reservations"
        ))
    }
}

/// Lifetime owner for an allocator-admitted foreign heap reservation.
///
/// The provider's allocations must be released before this owner is dropped.
pub struct HostMemoryReservation {
    _owner: Box<dyn Send + Sync>,
}

impl HostMemoryReservation {
    /// Retain an allocator's reservation owner without exposing its concrete type.
    pub fn new(owner: impl Send + Sync + 'static) -> Self {
        Self {
            _owner: Box::new(owner),
        }
    }
}

impl Debug for HostMemoryReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostMemoryReservation")
            .finish_non_exhaustive()
    }
}

/// Shared allocator reference used throughout session-scoped memory APIs.
pub type HostAllocatorRef = Arc<dyn HostAllocator>;

/// Extension methods for [`HostAllocator`]s.
pub trait HostAllocatorExt: HostAllocator {
    /// Allocate host memory for `len` elements of `T` using `Alignment::of::<T>()`.
    fn allocate_typed<T>(&self, len: usize) -> VortexResult<WritableHostBuffer> {
        let bytes = len.checked_mul(size_of::<T>()).ok_or_else(|| {
            vortex_err!(
                "Typed host allocation overflow for type {} and len {}",
                std::any::type_name::<T>(),
                len
            )
        })?;
        self.allocate(bytes, Alignment::of::<T>())
    }
}

impl<A: HostAllocator + ?Sized> HostAllocatorExt for A {}

/// Session-scoped memory configuration for Vortex arrays.
#[derive(Clone, Debug)]
pub struct HostMemorySession {
    allocator: HostAllocatorRef,
}

impl HostMemorySession {
    /// Creates a new session memory configuration using the provided allocator.
    pub fn new(allocator: HostAllocatorRef) -> Self {
        Self { allocator }
    }

    /// Returns the configured allocator.
    pub fn host_allocator(&self) -> HostAllocatorRef {
        Arc::clone(&self.allocator)
    }

    /// Updates the configured allocator.
    pub fn set_host_allocator(&mut self, allocator: HostAllocatorRef) {
        self.allocator = allocator;
    }
}

impl Default for HostMemorySession {
    fn default() -> Self {
        Self::new(Arc::new(DefaultHostAllocator))
    }
}

impl SessionVar for HostMemorySession {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Extension trait for accessing session-scoped memory configuration.
pub trait HostMemorySessionExt: SessionExt {
    /// Returns the memory session for this execution/session context.
    fn host_memory(&self) -> SessionGuard<'_, HostMemorySession> {
        self.get::<HostMemorySession>()
    }

    /// Returns the configured host allocator for this execution/session context.
    fn host_allocator(&self) -> HostAllocatorRef {
        self.host_memory().host_allocator()
    }

    /// Configures the session to use `allocator` as its host allocator, mutating it in place and
    /// returning it for chaining.
    fn with_host_allocator(self, allocator: HostAllocatorRef) -> VortexSession {
        let session = self.session();
        session
            .get_mut::<HostMemorySession>()
            .set_host_allocator(allocator);
        session
    }
}

impl<S: SessionExt> HostMemorySessionExt for S {}

/// Default host allocator.
#[derive(Debug, Default)]
pub struct DefaultHostAllocator;

impl HostAllocator for DefaultHostAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        // The safe HostBufferMut contract exposes initialized bytes immediately.
        // Never create a mutable byte slice from uninitialized capacity.
        let buffer = ByteBufferMut::zeroed_aligned(len, alignment);
        Ok(WritableHostBuffer::new(Box::new(
            DefaultWritableHostBuffer { buffer, alignment },
        )))
    }

    fn reserve_external(&self, _len: usize) -> VortexResult<HostMemoryReservation> {
        // This provider is explicitly unbounded, including its foreign storage.
        Ok(HostMemoryReservation::new(()))
    }
}

#[derive(Debug)]
struct DefaultWritableHostBuffer {
    buffer: ByteBufferMut,
    alignment: Alignment,
}

impl HostBufferMut for DefaultWritableHostBuffer {
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
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    static_assertions::assert_not_impl_any!(bool: HostBufferElement);
    static_assertions::assert_not_impl_any!(char: HostBufferElement);
    static_assertions::assert_not_impl_any!(std::num::NonZeroU32: HostBufferElement);
    static_assertions::assert_not_impl_any!(String: HostBufferElement);
    static_assertions::assert_not_impl_any!(&'static u64: HostBufferElement);

    #[test]
    fn typed_access_checks_the_pointer_instead_of_trusting_allocator_alignment() {
        struct Misaligned([u8; 16]);

        impl HostBufferMut for Misaligned {
            fn len(&self) -> usize {
                8
            }

            fn alignment(&self) -> Alignment {
                Alignment::of::<u64>()
            }

            fn as_mut_slice(&mut self) -> &mut [u8] {
                let start = usize::from(Alignment::of::<u64>().is_ptr_aligned(self.0.as_ptr()));
                &mut self.0[start..start + 8]
            }

            fn freeze(self: Box<Self>) -> ByteBuffer {
                unreachable!("this test only requests mutable typed access")
            }
        }

        let mut buffer = WritableHostBuffer::new(Box::new(Misaligned([0; 16])));
        let error = buffer.as_mut_slice_typed::<u64>().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Mutable buffer pointer is not aligned")
        );
    }

    #[derive(Debug)]
    struct CountingAllocator {
        allocations: Arc<AtomicUsize>,
    }

    impl HostAllocator for CountingAllocator {
        fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            DefaultHostAllocator.allocate(len, alignment)
        }
    }

    #[test]
    fn writable_host_buffer_freeze_round_trip() {
        let allocator = DefaultHostAllocator;
        let mut writable = allocator.allocate(16, Alignment::new(8)).unwrap();
        for (idx, byte) in writable.as_mut_slice().iter_mut().enumerate() {
            *byte = u8::try_from(idx).unwrap();
        }

        let host = writable.freeze();
        assert_eq!(host.len(), 16);
        assert!(host.is_aligned(Alignment::new(8)));
        assert_eq!(host.as_slice(), (0u8..16).collect::<Vec<_>>().as_slice());
    }

    #[test]
    fn memory_session_replaces_allocator() {
        let allocations = Arc::new(AtomicUsize::new(0));
        let allocator = Arc::new(CountingAllocator {
            allocations: Arc::clone(&allocations),
        });
        let mut session = HostMemorySession::default();
        session.set_host_allocator(allocator);
        drop(
            session
                .host_allocator()
                .allocate(4, Alignment::none())
                .unwrap(),
        );
        assert_eq!(allocations.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn typed_allocation_uses_type_alignment() {
        let allocator = DefaultHostAllocator;
        let writable = allocator.allocate_typed::<u64>(4).unwrap();
        assert_eq!(writable.len(), 4 * size_of::<u64>());
        assert_eq!(writable.alignment(), Alignment::of::<u64>());
    }

    #[test]
    fn typed_mut_slice_round_trip() {
        let allocator = DefaultHostAllocator;
        let mut writable = allocator.allocate_typed::<u64>(4).unwrap();
        writable
            .as_mut_slice_typed::<u64>()
            .unwrap()
            .copy_from_slice(&[10, 20, 30, 40]);

        let frozen = writable.freeze();
        let values = unsafe {
            std::slice::from_raw_parts(
                frozen.as_slice().as_ptr().cast::<u64>(),
                frozen.len() / size_of::<u64>(),
            )
        };
        assert_eq!(values, [10, 20, 30, 40]);
    }

    #[test]
    fn typed_mut_slice_rejects_length_mismatch() {
        let allocator = DefaultHostAllocator;
        let mut writable = allocator.allocate(7, Alignment::none()).unwrap();
        assert!(writable.as_mut_slice_typed::<u32>().is_err());
    }

    #[test]
    fn freeze_typed_round_trip() {
        let allocator = DefaultHostAllocator;
        let mut writable = allocator.allocate_typed::<u64>(4).unwrap();
        writable
            .as_mut_slice_typed::<u64>()
            .unwrap()
            .copy_from_slice(&[1, 3, 5, 7]);

        let frozen = writable.freeze_typed::<u64>().unwrap();
        assert_eq!(frozen.as_slice(), [1, 3, 5, 7]);
    }

    #[test]
    fn freeze_typed_rejects_length_mismatch() {
        let allocator = DefaultHostAllocator;
        let writable = allocator.allocate(7, Alignment::none()).unwrap();
        let err = writable.freeze_typed::<u32>().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("not a multiple of"));
    }
}
