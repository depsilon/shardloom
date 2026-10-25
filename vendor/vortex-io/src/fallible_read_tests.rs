// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors.
// ShardLoom regression tests for admission, initialized reads and last-owner release.
//! Regression evidence for the retained fallible I/O allocation boundary.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use vortex_array::memory::DefaultHostAllocator;
use vortex_array::memory::HostAllocator;
use vortex_array::memory::HostAllocatorRef;
use vortex_array::memory::HostBufferMut;
use vortex_array::memory::WritableHostBuffer;
use vortex_buffer::Alignment;
use vortex_buffer::BufferAllocatorRef;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use super::ReadBufferAllocator;
use super::checked_read_end;
use crate::VortexReadAt;
use crate::runtime::BlockingRuntime;
use crate::runtime::current::CurrentThreadRuntime;
use crate::std_file::FileReadAt;

const DATA: &[u8] = b"0123456789abcdef";

#[derive(Debug, Default)]
struct TrackingAllocator {
    live_bytes: Arc<AtomicUsize>,
    requests: AtomicUsize,
    deny: AtomicBool,
}

impl TrackingAllocator {
    fn live(&self) -> usize {
        self.live_bytes.load(Ordering::SeqCst)
    }
}

impl HostAllocator for TrackingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if self.deny.load(Ordering::SeqCst) {
            vortex_bail!("test read allocation denied");
        }
        self.live_bytes.fetch_add(len, Ordering::SeqCst);
        let lease = Lease {
            live_bytes: Arc::clone(&self.live_bytes),
            bytes: len,
        };
        Ok(WritableHostBuffer::new(Box::new(TrackedBuffer {
            buffer: DefaultHostAllocator.allocate(len, alignment)?,
            lease,
        })))
    }
}

struct Lease {
    live_bytes: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.live_bytes.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

struct TrackedBuffer {
    buffer: WritableHostBuffer,
    lease: Lease,
}

impl HostBufferMut for TrackedBuffer {
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
            Bytes::from_owner(OwnedBytes {
                buffer: buffer.freeze(),
                _lease: lease,
            }),
            alignment,
        )
    }
}

struct OwnedBytes {
    buffer: ByteBuffer,
    _lease: Lease,
}

impl AsRef<[u8]> for OwnedBytes {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

#[test]
fn read_buffers_expose_initialized_bytes_for_both_allocator_paths() -> anyhow::Result<()> {
    for allocator in [
        ReadBufferAllocator::Buffer(BufferAllocatorRef::statically_allocated()),
        ReadBufferAllocator::Host(Arc::new(DefaultHostAllocator)),
    ] {
        for length in [0, 1, 257] {
            let mut buffer = allocator.allocate(length, Alignment::new(64))?;
            assert_eq!(buffer.len(), length);
            assert_eq!(buffer.as_mut_slice(), vec![0; length]);
            buffer.as_mut_slice().fill(0x5a);
            let frozen = buffer.freeze();
            assert!(frozen.is_aligned(Alignment::new(64)));
            assert_eq!(frozen.as_slice(), vec![0x5a; length]);
        }
    }
    assert_eq!(checked_read_end(u64::MAX, 0)?, u64::MAX);
    assert!(checked_read_end(u64::MAX, 1).is_err());
    Ok(())
}

#[test]
fn file_read_admits_before_io_and_releases_failed_or_last_buffer_owners() -> anyhow::Result<()> {
    let mut file = tempfile::NamedTempFile::new()?;
    file.write_all(DATA)?;
    let runtime = CurrentThreadRuntime::new();
    let allocator = Arc::new(TrackingAllocator::default());
    let reader = FileReadAt::open_with_host_allocator(
        file.path(),
        runtime.handle(),
        Arc::clone(&allocator) as HostAllocatorRef,
    )?;

    allocator.deny.store(true, Ordering::SeqCst);
    let error = runtime.block_on(reader.read_at(0, DATA.len() + 1, Alignment::new(64)));
    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("test read allocation denied")
    );
    assert_eq!(allocator.live(), 0);
    assert_eq!(allocator.requests.load(Ordering::SeqCst), 1);
    let error = runtime.block_on(reader.read_at(u64::MAX, 1, Alignment::none()));
    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("Read range end overflow")
    );
    assert_eq!(allocator.requests.load(Ordering::SeqCst), 1);

    allocator.deny.store(false, Ordering::SeqCst);
    assert!(
        runtime
            .block_on(reader.read_at(0, DATA.len() + 1, Alignment::none()))
            .is_err()
    );
    assert_eq!(
        allocator.live(),
        0,
        "EOF must release the admitted writable buffer"
    );
    let output = runtime.block_on(reader.read_at(3, 8, Alignment::new(64)))?;
    let host = runtime.block_on(output.to_host());
    assert_eq!(host.as_slice(), &DATA[3..11]);
    assert!(host.is_aligned(Alignment::new(64)));
    let clone = host.clone();
    let slice = host.slice_unaligned(1..5);
    drop((host, output, reader));
    assert_eq!(allocator.live(), 8);
    assert_eq!(slice.as_slice(), &DATA[4..8]);
    drop(clone);
    assert_eq!(allocator.live(), 8);
    drop(slice);
    assert_eq!(allocator.live(), 0);

    let default = FileReadAt::open(file.path(), runtime.handle())?;
    let output = runtime.block_on(default.read_at(3, 8, Alignment::new(64)))?;
    assert_eq!(runtime.block_on(output.to_host()).as_slice(), &DATA[3..11]);
    Ok(())
}

#[cfg(feature = "object_store")]
#[test]
fn object_reads_admit_before_get_and_keep_independent_range_owners() -> anyhow::Result<()> {
    use futures::StreamExt;
    use object_store::ObjectStore;
    use object_store::ObjectStoreExt;
    use object_store::PutPayload;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use crate::ReadAtRequest;
    use crate::object_store::ObjectStoreReadAt;

    let runtime = CurrentThreadRuntime::new();
    let allocator = Arc::new(TrackingAllocator::default());
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("ranges.bin");
    let reader = ObjectStoreReadAt::new_with_host_allocator(
        Arc::clone(&store),
        path.clone(),
        runtime.handle(),
        Arc::clone(&allocator) as HostAllocatorRef,
    )
    .with_concurrency(2);
    allocator.deny.store(true, Ordering::SeqCst);
    let error = runtime.block_on(reader.read_at(0, 8, Alignment::new(64)));
    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("test read allocation denied"),
        "admission must precede GET of a missing object"
    );
    assert_eq!(allocator.live(), 0);
    let requests = allocator.requests.load(Ordering::SeqCst);
    let error = runtime.block_on(reader.read_at(u64::MAX, 1, Alignment::none()));
    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("Read range end overflow")
    );
    assert_eq!(allocator.requests.load(Ordering::SeqCst), requests);
    allocator.deny.store(false, Ordering::SeqCst);
    assert!(
        runtime
            .block_on(reader.read_at(0, 8, Alignment::none()))
            .is_err()
    );
    assert_eq!(allocator.live(), 0, "failed GET must refund admission");
    runtime.block_on(store.put(&path, PutPayload::from_static(DATA)))?;
    let requests: Arc<[ReadAtRequest]> = Arc::from([
        ReadAtRequest::new(0, 3, Alignment::new(64)),
        ReadAtRequest::new(4, 5, Alignment::new(64)),
        ReadAtRequest::new(10, 6, Alignment::new(64)),
    ]);
    let results = runtime.block_on(reader.read_ranges(requests).collect::<Vec<_>>());
    assert_eq!(results.len(), 3);
    assert_eq!(allocator.live(), 14);
    for (request, result) in results {
        let output = result?;
        let host = runtime.block_on(output.to_host());
        let start = usize::try_from(request.offset)?;
        assert_eq!(host.as_slice(), &DATA[start..start + request.length]);
        assert!(host.is_aligned(request.alignment));
    }
    assert_eq!(allocator.live(), 0);
    // A truncated stream must release its buffer, just like a failed GET.
    assert!(
        runtime
            .block_on(reader.read_at(12, 8, Alignment::none()))
            .is_err()
    );
    assert_eq!(allocator.live(), 0);
    Ok(())
}

#[cfg(feature = "object_store")]
#[test]
fn object_file_payload_retains_credit_and_refunds_eof() -> anyhow::Result<()> {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use crate::object_store::ObjectStoreReadAt;

    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("data.bin"), DATA)?;
    let runtime = CurrentThreadRuntime::new();
    let allocator = Arc::new(TrackingAllocator::default());
    let reader = ObjectStoreReadAt::new_with_host_allocator(
        Arc::new(LocalFileSystem::new_with_prefix(directory.path())?),
        Path::from("data.bin"),
        runtime.handle(),
        Arc::clone(&allocator) as HostAllocatorRef,
    );
    let output = runtime.block_on(reader.read_at(3, 8, Alignment::new(64)))?;
    assert_eq!(runtime.block_on(output.to_host()).as_slice(), &DATA[3..11]);
    assert_eq!(allocator.live(), 8);
    drop(output);
    assert_eq!(allocator.live(), 0);
    assert!(
        runtime
            .block_on(reader.read_at(12, 8, Alignment::none()))
            .is_err()
    );
    assert_eq!(allocator.live(), 0);
    Ok(())
}
