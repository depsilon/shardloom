use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use vortex::array::memory::HostBufferMut;
use vortex::buffer::ByteBuffer;

#[derive(Clone, Copy, Debug)]
enum Fault {
    ShortMutable,
    LongMutable,
    ShortFrozen,
    MisalignedFrozen,
}

#[derive(Debug)]
struct MalformedAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    calls: AtomicUsize,
    target: usize,
    fault: Fault,
}

impl HostAllocator for MalformedAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != self.target {
            return self.inner.allocate(bytes, alignment);
        }
        assert!(bytes > 0);
        Ok(WritableHostBuffer::new(Box::new(MalformedBuffer {
            inner: self.inner.allocate(bytes + 1, alignment)?,
            requested: bytes,
            fault: self.fault,
        })))
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.inner.reserve_external(bytes)
    }
}

struct MalformedBuffer {
    inner: WritableHostBuffer,
    requested: usize,
    fault: Fault,
}

impl HostBufferMut for MalformedBuffer {
    fn len(&self) -> usize {
        self.requested
    }

    fn alignment(&self) -> Alignment {
        self.inner.alignment()
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        let len = match self.fault {
            Fault::ShortMutable => self.requested - 1,
            Fault::LongMutable => self.requested + 1,
            Fault::ShortFrozen | Fault::MisalignedFrozen => self.requested,
        };
        &mut self.inner.as_mut_slice()[..len]
    }

    fn freeze(self: Box<Self>) -> ByteBuffer {
        let buffer = self.inner.freeze();
        match self.fault {
            Fault::ShortFrozen => buffer.slice(0..self.requested - 1),
            Fault::MisalignedFrozen => ByteBuffer::from_bytes_aligned(
                buffer.into_inner().slice(1..=self.requested),
                Alignment::none(),
            ),
            Fault::ShortMutable | Fault::LongMutable => {
                panic!("incorrect mutable extent must not be frozen")
            }
        }
    }
}

fn malformed_compression(input: &VarBinViewArray, target: usize, fault: Fault) {
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let allocator = Arc::new(MalformedAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        calls: AtomicUsize::new(0),
        target,
        fault,
    });
    let session = VortexSession::default().with_allocator(allocator);
    install(&session, memory.clone());
    let error =
        ZstdData::from_var_bin_view_without_dict(input, 0, 1, &mut session.create_execution_ctx())
            .unwrap_err();
    let text = error.to_string();
    assert!(
        text.contains("length") || text.contains("aligned") || text.contains("not a multiple"),
        "{text}"
    );
    assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_validates_every_actual_mutable_extent() {
    let input = VarBinViewArray::from_iter_str(["a long native Unicode λ sample", "second sample"]);
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let (session, recorder) = admission_session(&memory, None);
    drop(
        ZstdData::from_var_bin_view_without_dict(&input, 0, 1, &mut session.create_execution_ctx())
            .unwrap(),
    );
    let count = recorder
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| matches!(request, Request::Buffer { .. }))
        .count();
    assert_eq!(
        count, 7,
        "payload, offsets, starts, samples, scratch and two retained frames"
    );
    for target in 0..count {
        for fault in [Fault::ShortMutable, Fault::LongMutable] {
            malformed_compression(&input, target, fault);
        }
    }
}

#[test]
fn native_zstd_compression_validates_frozen_extents_and_actual_offset_alignment() {
    let input = VarBinViewArray::from_iter_str(["a long native Unicode λ sample", "second sample"]);
    // The reusable output scratch is never frozen; all other payloads are.
    for target in [0, 1, 2, 3, 5, 6] {
        malformed_compression(&input, target, Fault::ShortFrozen);
    }
    // These are typed value offsets, frame starts and sample sizes. Their
    // actual frozen pointers must be aligned even if the allocator says so.
    for target in [1, 2, 3] {
        malformed_compression(&input, target, Fault::MisalignedFrozen);
    }
}

#[derive(Debug)]
struct HoldingForeignAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    calls: AtomicUsize,
    ready: mpsc::Sender<usize>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl HostAllocator for HoldingForeignAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.inner.allocate(bytes, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        let reservation = self.inner.reserve_external(bytes)?;
        // The first reservation covers the Rust frame table. Hold the second,
        // foreign compressor reservation to preserve this contention test.
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.ready
                .send(bytes)
                .map_err(|error| vortex_err!("ready: {error}"))?;
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .map_err(|error| vortex_err!("resume: {error}"))?;
        }
        Ok(reservation)
    }
}

#[test]
fn native_zstd_compression_overlapping_contexts_share_one_grant() {
    let input = PrimitiveArray::from_iter([17i64]);
    let probe = LiveMemoryPool::new(64 << 20).unwrap();
    drop(
        ZstdData::from_primitive_without_dict(
            &input,
            0,
            0,
            &mut session(&probe).create_execution_ctx(),
        )
        .unwrap(),
    );
    let one_call_peak = probe.snapshot().peak_reserved_bytes;
    let memory = LiveMemoryPool::new(one_call_peak).unwrap();
    let (ready_send, ready_receive) = mpsc::channel();
    let (resume_send, resume_receive) = mpsc::channel();
    let allocator = Arc::new(HoldingForeignAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        calls: AtomicUsize::new(0),
        ready: ready_send,
        resume: Mutex::new(resume_receive),
    });
    let first_input = input.clone();
    let first_memory = memory.clone();
    let first = std::thread::spawn(move || {
        let session = VortexSession::default().with_allocator(allocator);
        install(&session, first_memory);
        ZstdData::from_primitive_without_dict(
            &first_input,
            0,
            0,
            &mut session.create_execution_ctx(),
        )
    });
    let foreign = ready_receive.recv_timeout(Duration::from_secs(30)).unwrap();
    let held = memory.snapshot().reserved_bytes;
    let second = ZstdData::from_primitive_without_dict(
        &input,
        0,
        0,
        &mut session(&memory).create_execution_ctx(),
    );
    let held_after_second = memory.snapshot().reserved_bytes;
    resume_send.send(()).unwrap();
    let data = first.join().unwrap().unwrap();
    assert!(foreign > 4096);
    assert!(crate::owned_buffers::is_owned_reservation_denial(
        &second.unwrap_err()
    ));
    assert_eq!(held_after_second, held);
    assert!(memory.snapshot().peak_reserved_bytes <= one_call_peak);
    let retained = memory.snapshot().reserved_bytes;
    assert!(retained > 0 && retained < held);
    drop(data);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    drop(
        ZstdData::from_primitive_without_dict(
            &input,
            0,
            0,
            &mut session(&memory).create_execution_ctx(),
        )
        .unwrap(),
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
