//! Bounded source-chunk work on caller-owned, persistent compute workers.
//!
//! A window permit covers queued, active, completed, and caller-merged partials.
//! Payload leases remain attached through the merge callback. The shared pool
//! charges explicitly reserved capacity, not arbitrary provider allocations/RSS.

use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{
    compute_pool::{
        CancellationToken, ComputePool, ComputePoolSnapshot, ComputeTask, WorkerContext,
    },
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

pub(super) enum ChunkWorkerContext {
    Pool(WorkerContext),
    Inline(CancellationToken),
}

impl ChunkWorkerContext {
    pub(super) fn check_cancelled(&self) -> Result<()> {
        match self {
            Self::Pool(context) => context.check_cancelled(),
            Self::Inline(token) => token.check(),
        }
    }
}

#[derive(Default)]
struct Window {
    outstanding: AtomicUsize,
    peak: AtomicUsize,
}

struct WindowPermit(Arc<Window>);

impl Drop for WindowPermit {
    fn drop(&mut self) {
        self.0.outstanding.fetch_sub(1, Ordering::AcqRel);
    }
}

enum Pending<T> {
    Worker(ComputeTask<T>),
    Inline(Budgeted<T>),
}

/// An initial capacity denial never runs the job or advances its ordinal. All
/// failures after admission remain errors, including a provider error observed
/// alongside an unrelated reservation denial.
pub(super) enum SubmitOutcome {
    Submitted(u64),
    InitialCapacityDenied(ShardLoomError),
}

pub(super) struct CompletedChunk<T> {
    ordinal: u64,
    result: Budgeted<T>,
    cancellation: CancellationToken,
    // Drop payload and its reservation before opening the next window slot.
    _permit: WindowPermit,
}

impl<T> CompletedChunk<T> {
    pub(super) const fn ordinal(&self) -> u64 {
        self.ordinal
    }

    #[cfg(test)]
    pub(super) fn value(&self) -> &T {
        self.result.value()
    }

    #[cfg(test)]
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.result.reserved_bytes()
    }

    /// Keep all task ownership and its window permit until ordered merging ends.
    pub(super) fn consume<R>(self, merge: impl FnOnce(&T) -> Result<R>) -> Result<R> {
        let result = merge(self.result.value());
        if result.is_err() {
            self.cancellation.cancel();
        }
        result
    }
}

pub(super) struct AggregateChunkJobs<T> {
    pending: VecDeque<(u64, Pending<T>, WindowPermit)>,
    pool: Option<ComputePool>,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    failure: Arc<Mutex<Option<ShardLoomError>>>,
    window: Arc<Window>,
    window_limit: usize,
    input_byte_limit: u64,
    next_ordinal: u64,
    joined: u64,
    inline_busy_nanos: u128,
    worker_busy_nanos: Arc<AtomicU64>,
    join_wait_nanos: u128,
}

impl<T: Send + 'static> AggregateChunkJobs<T> {
    pub(super) fn new(
        max_parallelism: usize,
        window_limit: usize,
        queue_byte_limit: u64,
        memory: LiveMemoryPool,
    ) -> Result<Self> {
        if max_parallelism == 0 || window_limit == 0 || queue_byte_limit == 0 {
            return Err(failed(
                "worker, window and queue byte limits must be positive",
            ));
        }
        if queue_byte_limit > memory.snapshot().limit_bytes {
            return Err(failed("queue byte limit exceeds shared capacity"));
        }
        // The source/progress/merge caller occupies one CPU lane. The runtime
        // integrating this helper must not retain a second background CPU pool.
        let pool = (max_parallelism > 1)
            .then(|| {
                ComputePool::new(
                    max_parallelism - 1,
                    window_limit,
                    queue_byte_limit,
                    memory.clone(),
                )
            })
            .transpose()?;
        Ok(Self {
            pending: VecDeque::new(),
            pool,
            memory,
            cancellation: CancellationToken::default(),
            failure: Arc::new(Mutex::new(None)),
            window: Arc::new(Window::default()),
            window_limit,
            input_byte_limit: queue_byte_limit,
            next_ordinal: 0,
            joined: 0,
            inline_busy_nanos: 0,
            worker_busy_nanos: Arc::new(AtomicU64::new(0)),
            join_wait_nanos: 0,
        })
    }

    pub(super) fn memory(&self) -> &LiveMemoryPool {
        &self.memory
    }

    pub(super) fn is_full(&self) -> bool {
        self.window.outstanding.load(Ordering::Acquire) >= self.window_limit
    }

    pub(super) fn outstanding(&self) -> usize {
        self.window.outstanding.load(Ordering::Acquire)
    }

    pub(super) fn peak_outstanding(&self) -> usize {
        self.window.peak.load(Ordering::Acquire)
    }

    pub(super) const fn submitted(&self) -> u64 {
        self.next_ordinal
    }

    pub(super) const fn joined(&self) -> u64 {
        self.joined
    }

    pub(super) const fn join_wait_nanos(&self) -> u128 {
        self.join_wait_nanos
    }

    pub(super) const fn inline_busy_nanos(&self) -> u128 {
        self.inline_busy_nanos
    }

    pub(super) fn worker_busy_nanos(&self) -> u64 {
        self.worker_busy_nanos.load(Ordering::Relaxed)
    }

    pub(super) fn pool_snapshot(&self) -> Option<ComputePoolSnapshot> {
        self.pool.as_ref().map(ComputePool::snapshot)
    }

    pub(super) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(super) fn submit<F>(&mut self, initial_bytes: u64, job: F) -> Result<u64>
    where
        F: FnOnce(&ChunkWorkerContext, &mut MemoryLease) -> Result<T> + Send + 'static,
    {
        match self.try_submit(initial_bytes, job)? {
            SubmitOutcome::Submitted(ordinal) => Ok(ordinal),
            SubmitOutcome::InitialCapacityDenied(error) => Err(error),
        }
    }

    pub(super) fn try_submit<F>(&mut self, initial_bytes: u64, job: F) -> Result<SubmitOutcome>
    where
        F: FnOnce(&ChunkWorkerContext, &mut MemoryLease) -> Result<T> + Send + 'static,
    {
        self.cancellation
            .check()
            .map_err(|error| self.failure_or(error))?;
        if self.is_full() {
            return Err(failed(
                "outstanding window is full; merge its oldest chunk before reading more input",
            ));
        }
        if initial_bytes > self.input_byte_limit {
            return Err(failed("initial input exceeds the admitted task byte limit"));
        }
        let next = self
            .next_ordinal
            .checked_add(1)
            .ok_or_else(|| failed("chunk ordinal overflowed"))?;
        let mut lease = match self.memory.reserve(initial_bytes) {
            Ok(lease) => lease,
            Err(error) => return Ok(SubmitOutcome::InitialCapacityDenied(error)),
        };
        let count = self.window.outstanding.fetch_add(1, Ordering::AcqRel) + 1;
        self.window.peak.fetch_max(count, Ordering::AcqRel);
        let permit = WindowPermit(Arc::clone(&self.window));
        let pending = if let Some(pool) = self.pool.as_ref() {
            let failure = Arc::clone(&self.failure);
            let busy = Arc::clone(&self.worker_busy_nanos);
            Pending::Worker(pool.submit(
                Budgeted::new(
                    move |context: &WorkerContext, lease: &mut MemoryLease| {
                        let started = Instant::now();
                        let result = catch_unwind(AssertUnwindSafe(|| {
                            job(&ChunkWorkerContext::Pool(context.clone()), lease)
                        }))
                        .unwrap_or_else(|_| Err(failed("worker task panicked")));
                        if let Err(error) = &result {
                            record_first_failure(&failure, error);
                        }
                        // Publish timing before publishing the completed payload.
                        // The outer pool's post-send accounting can lag join().
                        busy.fetch_add(
                            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                        result
                    },
                    lease,
                ),
                self.cancellation.clone(),
            )?)
        } else {
            let started = Instant::now();
            let value = catch_unwind(AssertUnwindSafe(|| {
                let value = job(
                    &ChunkWorkerContext::Inline(self.cancellation.clone()),
                    &mut lease,
                )?;
                self.cancellation.check()?;
                Ok(value)
            }))
            .unwrap_or_else(|_| Err(failed("inline worker task panicked")));
            self.inline_busy_nanos += started.elapsed().as_nanos();
            match value {
                Ok(value) => Pending::Inline(Budgeted::new(value, lease)),
                Err(error) => {
                    record_first_failure(&self.failure, &error);
                    self.cancel();
                    return Err(error);
                }
            }
        };
        let ordinal = self.next_ordinal;
        self.next_ordinal = next;
        self.pending.push_back((ordinal, pending, permit));
        Ok(SubmitOutcome::Submitted(ordinal))
    }

    pub(super) fn join_next(&mut self) -> Result<Option<CompletedChunk<T>>> {
        let Some((ordinal, pending, permit)) = self.pending.pop_front() else {
            return Ok(None);
        };
        let started = Instant::now();
        let result = match pending {
            Pending::Worker(task) => task.join(),
            Pending::Inline(result) => Ok(result),
        };
        self.join_wait_nanos += started.elapsed().as_nanos();
        match result {
            Ok(result) => {
                self.joined += 1;
                Ok(Some(CompletedChunk {
                    ordinal,
                    result,
                    cancellation: self.cancellation.clone(),
                    _permit: permit,
                }))
            }
            Err(error) => {
                self.cancel();
                Err(self.failure_or(error))
            }
        }
    }

    fn failure_or(&self, error: ShardLoomError) -> ShardLoomError {
        self.failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .unwrap_or(error)
    }
}

fn record_first_failure(failure: &Mutex<Option<ShardLoomError>>, error: &ShardLoomError) {
    let mut first = failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if first.is_none() {
        *first = Some(error.clone());
    }
}

impl<T> Drop for AggregateChunkJobs<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        for (_, pending, _) in self.pending.drain(..) {
            if let Pending::Worker(task) = pending {
                let _ = task.join();
            }
        }
    }
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex aggregate chunk jobs {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "aggregate_chunk_jobs_tests.rs"]
mod tests;
