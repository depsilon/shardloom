//! Complete credit ownership through real native allocation and alias boundaries.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    process::Command,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::memory::{
        HostAllocator, HostAllocatorRef, HostBufferMut, HostMemoryReservation, WritableHostBuffer,
    },
    buffer::{Alignment, Buffer, ByteBuffer},
    error::VortexResult,
};

use super::super::{
    ReservedHostAllocator, is_owned_reservation_denial, retain_credit, retain_shared_credit,
    with_shared_credit,
};

const DEPTH: usize = 8192;
const STACK: usize = 128 * 1024;

fn chain(
    memory: &LiveMemoryPool,
    depth: usize,
) -> (
    HostAllocatorRef,
    Vec<std::sync::Weak<shardloom_exec::live_memory::MemoryLease>>,
) {
    let mut allocator: HostAllocatorRef = Arc::new(ReservedHostAllocator::new(memory.clone()));
    let mut weak = Vec::new();
    for _ in 0..depth {
        let lease = Arc::new(memory.reserve(1).unwrap());
        weak.push(Arc::downgrade(&lease));
        allocator = with_shared_credit(allocator, lease);
    }
    (allocator, weak)
}

fn deep_buffer_lifetimes() {
    for len in [0, 64] {
        let memory = LiveMemoryPool::new(1 << 24).unwrap();
        let (allocator, weak) = chain(&memory, DEPTH);
        let mut buffer = allocator.allocate(len, Alignment::new(64)).unwrap();
        assert_eq!(buffer.len(), len);
        buffer.as_mut_slice().fill(29);
        let pointer = buffer.as_mut_slice().as_ptr();
        let frozen = buffer.freeze();
        assert_eq!(frozen.as_ptr(), pointer);
        let typed = Buffer::<u64>::from_byte_buffer(frozen.clone());
        let bytes = typed.into_byte_buffer().into_bytes();
        drop((allocator, frozen));
        assert_eq!(
            memory.snapshot().reserved_bytes,
            DEPTH as u64 + len as u64 + 256
        );
        assert!(weak.iter().all(|owner| owner.upgrade().is_some()));
        assert_eq!(bytes.as_ref(), vec![29; len]);
        drop(bytes);
        assert!(weak.iter().all(|owner| owner.upgrade().is_none()));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    for freeze in [false, true] {
        let memory = LiveMemoryPool::new(1 << 24).unwrap();
        let (allocator, weak) = chain(&memory, DEPTH);
        let buffer = allocator.allocate(32, Alignment::none()).unwrap();
        let external = allocator.reserve_external(511).unwrap();
        drop(allocator);
        if freeze {
            drop(buffer.freeze());
        } else {
            drop(buffer);
        }
        assert_eq!(memory.snapshot().reserved_bytes, DEPTH as u64 + 511);
        assert!(weak.iter().all(|owner| owner.upgrade().is_some()));
        drop(external);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn deep_retained_lifetimes() {
    // Direct retention and repeated shared retention use the same provider
    // backing instead of hiding a ByteBuffer in an opaque Bytes owner.
    for shared in [false, true] {
        let memory = LiveMemoryPool::new(1 << 24).unwrap();
        let mut buffer = ReservedHostAllocator::new(memory.clone())
            .allocate(8, Alignment::none())
            .unwrap()
            .freeze();
        let mut middle = None;
        for id in 0..DEPTH {
            let lease = memory.reserve(1).unwrap();
            buffer = if shared {
                retain_shared_credit(buffer, Arc::new(lease))
            } else {
                retain_credit(buffer, lease)
            };
            if id == DEPTH / 2 - 1 {
                middle = Some(buffer.clone());
            }
        }
        drop(buffer);
        assert_eq!(memory.snapshot().reserved_bytes, DEPTH as u64 / 2 + 264);
        drop(middle);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    // A retained earlier allocator owns only its original prefix of credits.
    let memory = LiveMemoryPool::new(1 << 24).unwrap();
    let (middle, weak) = chain(&memory, DEPTH / 2);
    let mut allocator = Arc::clone(&middle);
    for _ in DEPTH / 2..DEPTH {
        allocator = with_shared_credit(allocator, Arc::new(memory.reserve(1).unwrap()));
    }
    drop(allocator);
    assert_eq!(memory.snapshot().reserved_bytes, DEPTH as u64 / 2);
    assert!(weak.iter().all(|owner| owner.upgrade().is_some()));
    drop(middle);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn deep_concurrent_and_failure() {
    for _ in 0..16 {
        let memory = LiveMemoryPool::new(1 << 24).unwrap();
        let (allocator, weak) = chain(&memory, DEPTH);
        let barrier = Arc::new(Barrier::new(2));
        let workers = [Arc::clone(&allocator), allocator].map(|allocator| {
            let barrier = Arc::clone(&barrier);
            std::thread::Builder::new()
                .stack_size(STACK)
                .spawn(move || {
                    barrier.wait();
                    drop(allocator);
                })
                .unwrap()
        });
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(weak.iter().all(|owner| owner.upgrade().is_none()));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    // Partial construction refusal and panic unwind keep every prefix balanced.
    let memory = LiveMemoryPool::new(DEPTH as u64).unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let (allocator, _) = chain(&memory, DEPTH);
            assert!(is_owned_reservation_denial(
                &allocator.allocate(1, Alignment::none()).unwrap_err()
            ));
            assert!(is_owned_reservation_denial(
                &allocator.reserve_external(1).unwrap_err()
            ));
            assert!(memory.reserve(1).is_err());
            panic!("partial credit construction unwind");
        }))
        .is_err()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn deep_child() {
    deep_buffer_lifetimes();
    deep_retained_lifetimes();
    deep_concurrent_and_failure();
    println!("NATIVE_CREDIT_DEEP_COMPLETE");
}

#[test]
fn credit_chains_complete_native_lifetimes_on_small_stacks() {
    const ENV: &str = "SHARDLOOM_NATIVE_CREDIT_DEEP_CHILD";
    if std::env::var_os(ENV).is_some() {
        std::thread::Builder::new()
            .stack_size(STACK)
            .spawn(deep_child)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "owned_buffers::credit::tests::credit_chains_complete_native_lifetimes_on_small_stacks",
            "--nocapture",
        ])
        .env(ENV, "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("NATIVE_CREDIT_DEEP_COMPLETE"));
}

#[derive(Debug)]
struct Observer {
    memory: LiveMemoryPool,
    credits: u64,
    drops: Arc<AtomicUsize>,
}

impl Drop for Observer {
    fn drop(&mut self) {
        assert!(self.memory.snapshot().reserved_bytes >= self.credits);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

struct ObservedBuffer {
    buffer: WritableHostBuffer,
    observer: Observer,
    panic_freeze: bool,
}

impl HostBufferMut for ObservedBuffer {
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
        assert!(!self.panic_freeze, "base freeze panic");
        let Self {
            buffer, observer, ..
        } = *self;
        buffer.freeze().with_retained_owner(Arc::new(observer))
    }
}

#[derive(Debug)]
struct ObservedAllocator {
    memory: LiveMemoryPool,
    calls: Arc<Mutex<Vec<(usize, usize)>>>,
    drops: Arc<AtomicUsize>,
    panic_freeze: bool,
}

impl ObservedAllocator {
    fn observer(&self) -> Observer {
        Observer {
            memory: self.memory.clone(),
            credits: 16,
            drops: Arc::clone(&self.drops),
        }
    }
}

impl HostAllocator for ObservedAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.calls.lock().unwrap().push((len, alignment.as_usize()));
        Ok(WritableHostBuffer::new(Box::new(ObservedBuffer {
            buffer: ReservedHostAllocator::new(self.memory.clone()).allocate(len, alignment)?,
            observer: self.observer(),
            panic_freeze: self.panic_freeze,
        })))
    }
    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.calls.lock().unwrap().push((len, 0));
        Ok(HostMemoryReservation::new((
            ReservedHostAllocator::new(self.memory.clone()).reserve_external(len)?,
            self.observer(),
        )))
    }
}

#[test]
fn credit_chains_preserve_base_calls_storage_first_and_freeze_panic_refunds() {
    for panic_freeze in [false, true] {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut allocator: HostAllocatorRef = Arc::new(ObservedAllocator {
            memory: memory.clone(),
            calls: Arc::clone(&calls),
            drops: Arc::clone(&drops),
            panic_freeze,
        });
        for _ in 0..16 {
            allocator = with_shared_credit(allocator, Arc::new(memory.reserve(1).unwrap()));
        }
        let external = allocator.reserve_external(17).unwrap();
        let buffer = allocator.allocate(8, Alignment::new(512)).unwrap();
        assert_eq!(*calls.lock().unwrap(), [(17, 0), (8, 512)]);
        drop(allocator);
        let frozen = catch_unwind(AssertUnwindSafe(|| buffer.freeze()));
        assert_eq!(frozen.is_err(), panic_freeze);
        drop(frozen);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(memory.snapshot().reserved_bytes, 33);
        drop(external);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
