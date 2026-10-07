use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;
use vortex::array::memory::HostBufferMut;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Short,
    Misaligned,
}

#[derive(Debug)]
struct MalformedAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    calls: AtomicUsize,
    target: usize,
    fault: Fault,
}

impl HostAllocator for MalformedAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != self.target {
            return self.inner.allocate(len, alignment);
        }
        Ok(WritableHostBuffer::new(Box::new(MalformedBuffer {
            buffer: self.inner.allocate(len + 8, alignment)?,
            advertised_len: len,
            fault: self.fault,
        })))
    }
}

struct MalformedBuffer {
    buffer: WritableHostBuffer,
    advertised_len: usize,
    fault: Fault,
}

impl HostBufferMut for MalformedBuffer {
    fn len(&self) -> usize {
        self.advertised_len
    }
    fn alignment(&self) -> Alignment {
        Alignment::new(8)
    }
    fn as_mut_slice(&mut self) -> &mut [u8] {
        let bytes = self.buffer.as_mut_slice();
        match self.fault {
            Fault::Short => &mut bytes[..self.advertised_len - 1],
            Fault::Misaligned => &mut bytes[1..=self.advertised_len],
        }
    }
    fn freeze(self: Box<Self>) -> ByteBuffer {
        panic!("invalid workspace must never be frozen")
    }
}

#[test]
fn native_zstd_checks_actual_workspace_slices_before_ffi() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    for target in [1, 2] {
        for fault in [Fault::Short, Fault::Misaligned] {
            let input = if target == 2 {
                with_dictionary(&base, vec![b'x'; 16])
            } else {
                base.clone()
            };
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let allocator = Arc::new(MalformedAllocator {
                inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
                calls: AtomicUsize::new(0),
                target,
                fault,
            });
            let session = VortexSession::default().with_allocator(allocator);
            install(&session, memory.clone());
            let error = input
                .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
                .unwrap_err();
            let message = match fault {
                Fault::Short => "workspace has incorrect length",
                Fault::Misaligned => "workspace must be eight-byte aligned",
            };
            assert!(error.to_string().contains(message), "{error}");
            assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[derive(Debug)]
struct BlockingAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    calls: AtomicUsize,
    ready: mpsc::Sender<usize>,
    resume: Mutex<mpsc::Receiver<()>>,
}

impl HostAllocator for BlockingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        let buffer = self.inner.allocate(len, alignment)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.ready
                .send(len)
                .map_err(|error| vortex_err!("test ready channel: {error}"))?;
            self.resume
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(30))
                .map_err(|error| vortex_err!("test resume channel: {error}"))?;
        }
        Ok(buffer)
    }
}

#[derive(Debug)]
struct RejectingAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    calls: AtomicUsize,
    target: usize,
}

impl HostAllocator for RejectingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == self.target {
            // Every test grants 1 MiB: this exercises the real typed denial
            // before allocation, at exactly one chosen provider boundary.
            return self.inner.allocate(2 << 20, alignment);
        }
        self.inner.allocate(len, alignment)
    }
}

#[test]
fn native_zstd_every_nullable_allocation_failure_releases_previous_owners() {
    let inputs = [
        primitive(&PrimitiveArray::from_option_iter([
            Some(17i64),
            None,
            Some(42),
        ])),
        strings(&VarBinViewArray::from_iter_nullable_str([
            Some("native nullable Unicode λ value"),
            None,
            Some(""),
        ])),
    ];
    for base in inputs {
        let input = with_dictionary(&base, vec![b'x'; 16]);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let (session, recorder) = recorded_session(&memory);
        drop(
            input
                .clone()
                .execute::<vortex::array::Canonical>(&mut session.create_execution_ctx())
                .unwrap(),
        );
        let allocations = recorder.requests.lock().unwrap().len();
        assert!(
            allocations >= 4,
            "include nullable output allocation after decoder cleanup"
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        for target in 0..allocations {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let allocator = Arc::new(RejectingAllocator {
                inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
                calls: AtomicUsize::new(0),
                target,
            });
            let session = VortexSession::default().with_allocator(allocator.clone());
            install(&session, memory.clone());
            let error = input
                .clone()
                .execute::<vortex::array::Canonical>(&mut session.create_execution_ctx())
                .unwrap_err();
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(allocator.calls.load(Ordering::SeqCst), target + 1);
            assert_eq!(memory.snapshot().denied_reservations, 1);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_zstd_overlapping_calls_share_one_grant_and_release_independent_contexts() {
    let input = primitive(&PrimitiveArray::from_iter([17i64]));
    let probe = LiveMemoryPool::new(1 << 20).unwrap();
    drop(decode(&input, &probe).unwrap());
    let one_call_peak = probe.snapshot().peak_reserved_bytes;
    assert!(one_call_peak > 4096);
    assert_eq!(probe.snapshot().reserved_bytes, 0);
    // Both payloads fit; only one context can be live in the shared grant.
    let memory = LiveMemoryPool::new(one_call_peak + 8 + 256).unwrap();
    let (ready_send, ready_receive) = mpsc::channel();
    let (resume_send, resume_receive) = mpsc::channel();
    let allocator = Arc::new(BlockingAllocator {
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
        first_input.execute::<PrimitiveArray>(&mut session.create_execution_ctx())
    });
    let workspace = ready_receive.recv_timeout(Duration::from_secs(30)).unwrap();
    let second = decode(&input, &memory);
    let held_after_second = memory.snapshot().reserved_bytes;
    resume_send.send(()).unwrap();
    let output = first.join().unwrap().unwrap();
    let error = second.unwrap_err();
    assert!(workspace > 4096);
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(held_after_second, one_call_peak);
    assert_eq!(
        memory.snapshot().peak_reserved_bytes,
        one_call_peak + 8 + 256
    );
    assert_eq!(memory.snapshot().reserved_bytes, 8 + 256);
    assert_eq!(output.as_slice::<i64>(), &[17]);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let retried = decode(&input, &memory).unwrap();
    assert_eq!(retried.as_slice::<i64>(), &[17]);
    drop(retried);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
