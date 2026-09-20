//! Serving I/O permits survive cancellation of the waiting native future.

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
}

pub(super) struct IoScope {
    state: Mutex<ScopeState>,
    waker: AtomicWaker,
    budget: Arc<IoBudget>,
    cancellation: CancellationToken,
    _metadata: MemoryLease,
}

impl IoScope {
    pub(super) fn new(
        budget: Arc<IoBudget>,
        memory: &LiveMemoryPool,
        cancellation: CancellationToken,
    ) -> Result<Arc<Self>> {
        let metadata = memory.reserve(size_of::<Self>() as u64)?;
        Ok(Arc::new(Self {
            state: Mutex::new(ScopeState {
                closed: false,
                pending: 0,
            }),
            waker: AtomicWaker::new(),
            budget,
            cancellation,
            _metadata: metadata,
        }))
    }

    pub(super) fn admit(self: &Arc<Self>, length: usize) -> Result<ReadJob> {
        self.cancellation.check()?;
        let bytes = u64::try_from(length).map_err(native_error)?;
        let mut scope = self
            .state
            .lock()
            .map_err(|_| resident_error("serving I/O scope poisoned"))?;
        let mut total = self
            .budget
            .state
            .lock()
            .map_err(|_| resident_error("serving I/O budget poisoned"))?;
        if scope.closed
            || total.active_requests == self.budget.max_requests
            || bytes > self.budget.max_bytes.saturating_sub(total.active_bytes)
        {
            total.rejected_requests += 1;
            return Err(resident_error(
                "serving I/O is closed or exceeds its shared request/byte envelope",
            ));
        }
        scope.pending += 1;
        total.active_requests += 1;
        total.active_bytes += bytes;
        total.peak_requests = total.peak_requests.max(total.active_requests);
        total.peak_bytes = total.peak_bytes.max(total.active_bytes);
        Ok(ReadJob {
            scope: Arc::clone(self),
            bytes,
        })
    }

    /// Closing prevents late provider work from registering reads. The caller
    /// drives provider cleanup until actual blocking completions release their
    /// guards. Running OS reads are cooperative and cannot be interrupted.
    pub(super) fn close_and_drain(&self, runtime: &CurrentThreadRuntime) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        runtime.block_on(futures::future::poll_fn(|cx| {
            self.waker.register(cx.waker());
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                == 0
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }));
    }
}

pub(super) struct ReadJob {
    scope: Arc<IoScope>,
    bytes: u64,
}

impl ReadJob {
    pub(super) fn check_cancelled(&self) -> Result<()> {
        self.scope.cancellation.check()
    }
}

impl Drop for ReadJob {
    fn drop(&mut self) {
        let mut scope = self
            .scope
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut total = self
            .scope
            .budget
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        scope.pending -= 1;
        total.active_requests -= 1;
        total.active_bytes -= self.bytes;
        drop(total);
        drop(scope);
        self.scope.waker.wake();
    }
}

pub(super) struct ReadCompletion<T> {
    // Release a discarded buffer before declaring its blocking job drained.
    pub(super) result: T,
    pub(super) _job: Option<ReadJob>,
}
