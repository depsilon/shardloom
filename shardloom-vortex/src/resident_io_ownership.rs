//! Native I/O permits survive cancellation of the waiting provider future.

use super::{native_error, resident_error};
use futures::task::AtomicWaker;
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    sync::{Arc, Mutex},
    task::Poll,
};
use vortex::io::runtime::{BlockingRuntime, current::CurrentThreadRuntime};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentIoSnapshot {
    pub active_requests: usize,
    pub active_bytes: u64,
    pub peak_requests: usize,
    pub peak_bytes: u64,
    pub rejected_requests: u64,
}

pub(super) struct IoBudget {
    state: Mutex<ResidentIoSnapshot>,
    max_requests: usize,
    max_bytes: u64,
}

impl IoBudget {
    pub(super) fn new(max_requests: usize, max_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ResidentIoSnapshot::default()),
            max_requests,
            max_bytes,
        })
    }

    pub(super) fn snapshot(&self) -> ResidentIoSnapshot {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct ScopeState {
    closed: bool,
    pending: usize,
    readers: usize,
}

// Completion signals must outlive the reservation-owning scope: a waiting
// caller can release its grant as soon as it observes both counters at zero.
struct IoCompletion {
    state: Mutex<ScopeState>,
    waker: AtomicWaker,
}

pub(super) struct IoScope {
    completion: Arc<IoCompletion>,
    budget: Arc<IoBudget>,
    cancellation: CancellationToken,
    metadata: Mutex<MemoryLease>,
}

impl IoScope {
    const METADATA_BYTES: u64 = (size_of::<Self>() + size_of::<IoCompletion>()) as u64;

    pub(super) fn new(
        budget: Arc<IoBudget>,
        memory: &LiveMemoryPool,
        cancellation: CancellationToken,
    ) -> Result<Arc<Self>> {
        let metadata = memory.reserve(Self::METADATA_BYTES)?;
        Ok(Self::with_metadata(budget, cancellation, metadata))
    }

    /// An ordinary operation may finish from its retained footer. Reserve I/O
    /// bookkeeping only when admitting a reader or read; no payload may bypass it.
    pub(super) fn deferred(
        budget: Arc<IoBudget>,
        memory: &LiveMemoryPool,
        cancellation: CancellationToken,
    ) -> Result<Arc<Self>> {
        Ok(Self::with_metadata(
            budget,
            cancellation,
            memory.reserve(0)?,
        ))
    }

    fn with_metadata(
        budget: Arc<IoBudget>,
        cancellation: CancellationToken,
        metadata: MemoryLease,
    ) -> Arc<Self> {
        Arc::new(Self {
            completion: Arc::new(IoCompletion {
                state: Mutex::new(ScopeState {
                    closed: false,
                    pending: 0,
                    readers: 0,
                }),
                waker: AtomicWaker::new(),
            }),
            budget,
            cancellation,
            metadata: Mutex::new(metadata),
        })
    }

    pub(super) fn admit(self: &Arc<Self>, length: usize) -> Result<ReadJob> {
        self.cancellation.check()?;
        let bytes = u64::try_from(length).map_err(native_error)?;
        let mut scope = self
            .completion
            .state
            .lock()
            .map_err(|_| resident_error("native I/O scope poisoned"))?;
        let mut total = self
            .budget
            .state
            .lock()
            .map_err(|_| resident_error("native I/O budget poisoned"))?;
        if scope.closed
            || total.active_requests == self.budget.max_requests
            || bytes > self.budget.max_bytes.saturating_sub(total.active_bytes)
        {
            total.rejected_requests += 1;
            return Err(resident_error(
                "native I/O is closed or exceeds its shared request/byte envelope",
            ));
        }
        self.reserve_metadata()?;
        scope.pending += 1;
        total.active_requests += 1;
        total.active_bytes += bytes;
        total.peak_requests = total.peak_requests.max(total.active_requests);
        total.peak_bytes = total.peak_bytes.max(total.active_bytes);
        Ok(ReadJob {
            scope: Some(Arc::clone(self)),
            bytes,
        })
    }

    /// Track the operation's provider reader as well as its blocking reads.
    /// Dropping a native scan schedules asynchronous provider destruction; a
    /// zero read count alone does not mean that cancelled driver has drained.
    pub(super) fn retain_reader(
        self: &Arc<Self>,
        memory: &LiveMemoryPool,
    ) -> Result<Arc<ReaderOwner>> {
        let metadata = memory.reserve(size_of::<ReaderOwner>() as u64)?;
        let mut state = self
            .completion
            .state
            .lock()
            .map_err(|_| resident_error("native I/O scope poisoned"))?;
        if state.closed {
            return Err(resident_error("native I/O scope is closed"));
        }
        self.reserve_metadata()?;
        state.readers += 1;
        Ok(Arc::new(ReaderOwner {
            scope: Some(Arc::clone(self)),
            metadata: Some(metadata),
        }))
    }

    fn reserve_metadata(&self) -> Result<()> {
        self.metadata
            .lock()
            .map_err(|_| resident_error("native I/O metadata poisoned"))?
            .resize(Self::METADATA_BYTES)
    }

    /// Closing prevents late provider work from registering reads. The caller
    /// drives provider cleanup until actual blocking completions release their
    /// guards. Running OS reads are cooperative and cannot be interrupted.
    pub(super) fn close_and_drain(&self, runtime: &CurrentThreadRuntime) {
        self.completion
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        runtime.block_on(futures::future::poll_fn(|cx| {
            self.completion.waker.register(cx.waker());
            let state = self
                .completion
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.pending == 0 && state.readers == 0 {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }));
    }
}

pub(super) struct ReaderOwner {
    scope: Option<Arc<IoScope>>,
    metadata: Option<MemoryLease>,
}

impl Drop for ReaderOwner {
    fn drop(&mut self) {
        let Some(scope) = self.scope.take() else {
            return;
        };
        let completion = Arc::clone(&scope.completion);
        let mut state = completion
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Refund both owners before publishing the zero count. Keeping the
        // counter lock through release also protects a concurrently polling
        // waiter, not only one awakened by the notification below.
        drop(self.metadata.take());
        drop(scope);
        state.readers -= 1;
        drop(state);
        completion.waker.wake();
    }
}

pub(super) struct ReadJob {
    scope: Option<Arc<IoScope>>,
    bytes: u64,
}

impl ReadJob {
    pub(super) fn check_cancelled(&self) -> Result<()> {
        self.scope
            .as_ref()
            .expect("a live native I/O job owns its scope")
            .cancellation
            .check()
    }
}

impl Drop for ReadJob {
    fn drop(&mut self) {
        let Some(scope) = self.scope.take() else {
            return;
        };
        let completion = Arc::clone(&scope.completion);
        let budget = Arc::clone(&scope.budget);
        let mut state = completion
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut total = budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(scope);
        state.pending -= 1;
        total.active_requests -= 1;
        total.active_bytes -= self.bytes;
        drop(total);
        drop(state);
        completion.waker.wake();
    }
}

pub(super) struct ReadCompletion<T> {
    // Release a discarded buffer before declaring its blocking job drained.
    pub(super) result: T,
    pub(super) _job: Option<ReadJob>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        task::{Wake, Waker},
    };

    struct DrainAtWake {
        scope: Mutex<Option<Arc<IoScope>>>,
        memory: LiveMemoryPool,
        observed_bytes: AtomicU64,
    }

    impl Wake for DrainAtWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // Model a caller that observes drained work and releases its grant
            // immediately, before the notifying destructor resumes. No sleeps
            // or thread scheduling are needed to expose late credit release.
            let scope = self.scope.lock().unwrap().take();
            drop(scope);
            self.observed_bytes
                .store(self.memory.snapshot().reserved_bytes, Ordering::Release);
        }
    }

    fn assert_release_before_wake<T>(create: impl FnOnce(&Arc<IoScope>, &LiveMemoryPool) -> T) {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let scope = IoScope::new(
            IoBudget::new(1, 4096),
            &memory,
            CancellationToken::default(),
        )
        .unwrap();
        let owner = create(&scope, &memory);
        assert!(memory.snapshot().reserved_bytes > 0);
        let probe = Arc::new(DrainAtWake {
            scope: Mutex::new(Some(Arc::clone(&scope))),
            memory: memory.clone(),
            observed_bytes: AtomicU64::new(u64::MAX),
        });
        scope
            .completion
            .waker
            .register(&Waker::from(Arc::clone(&probe)));
        drop(scope);
        drop(owner);
        assert_eq!(probe.observed_bytes.load(Ordering::Acquire), 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn read_job_releases_scope_metadata_before_notifying_drain() {
        assert_release_before_wake(|scope, _| scope.admit(1).unwrap());
    }

    #[test]
    fn reader_releases_all_metadata_before_notifying_drain() {
        assert_release_before_wake(|scope, memory| scope.retain_reader(memory).unwrap());
    }
}
