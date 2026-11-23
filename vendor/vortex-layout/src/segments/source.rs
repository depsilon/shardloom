// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use vortex_array::buffer::BufferHandle;
use vortex_array::memory::HostAllocatorRef;
use vortex_error::VortexResult;

use crate::segments::SegmentId;
/// Static future resolving to a segment byte buffer.
pub type SegmentFuture = BoxFuture<'static, VortexResult<BufferHandle>>;

/// Provides segment data to a [`crate::LayoutReader`].
///
/// Implementations may issue asynchronous file reads, object-store requests, cache lookups, or
/// in-memory buffer slices. Returned futures must be independent and safe to poll concurrently.
pub trait SegmentSource: 'static + Send + Sync {
    /// Request a segment, returning a future that will eventually resolve to the segment data.
    fn request(&self, id: SegmentId) -> SegmentFuture;
}

/// Relocates host segment buffers into an explicit reader storage allocator.
pub(crate) struct StorageAllocatedSegmentSource {
    inner: Arc<dyn SegmentSource>,
    allocator: HostAllocatorRef,
}

impl StorageAllocatedSegmentSource {
    pub(crate) fn wrap(
        inner: Arc<dyn SegmentSource>,
        allocator: HostAllocatorRef,
    ) -> Arc<dyn SegmentSource> {
        Arc::new(Self { inner, allocator })
    }
}

impl SegmentSource for StorageAllocatedSegmentSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let future = self.inner.request(id);
        let allocator = Arc::clone(&self.allocator);
        async move {
            let handle = future.await?;
            if !handle.is_on_host() {
                return Ok(handle);
            }
            let alignment = handle.alignment();
            let source = handle.unwrap_host();
            let mut dst = allocator.allocate(source.len(), alignment)?;
            dst.as_mut_slice().copy_from_slice(source.as_ref());
            drop(source);
            Ok(BufferHandle::new_host(dst.freeze()))
        }
        .boxed()
    }
}
