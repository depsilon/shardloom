//! Caller-owned persistent workers with bounded input queues and retained-result credits.

use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::mpsc;

use shardloom_core::{Result, ShardLoomError};

use crate::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};

/// Select local CPU lanes within the supplied maximum and process capacity.
///
/// Call once when admitting an operation owner, before sizing batches or
/// creating workers. The count includes the caller; task and memory admission
/// may narrow it further. This does not start workers or change a live owner.
/// Public entrypoints validate positive maxima; internal zero means one caller.
#[must_use]
pub fn bounded_cpu_parallelism(requested: usize) -> usize {
    cpu_parallelism_for_capacity(
        requested,
        thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get),
    )
}

fn cpu_parallelism_for_capacity(requested: usize, available: usize) -> usize {
    requested.max(1).min(available.max(1))
}

#[test]
fn cpu_selection_handles_arbitrary_allocations_without_fixed_tiers() {
    let sizes = [1, 2, 3, 4, 6, 8, 17, 32, 64, 128, 257, 4096, usize::MAX];
    for requested in sizes {
        for available in sizes {
            let selected = cpu_parallelism_for_capacity(requested, available);
            assert!((1..=requested).contains(&selected));
            assert!(selected <= available);
            assert!(selected == requested || selected == available);
        }
    }
    assert_eq!(cpu_parallelism_for_capacity(128, 96), 96);
    assert_eq!(cpu_parallelism_for_capacity(1, 96), 1);
    assert_eq!(cpu_parallelism_for_capacity(96, 0), 1);
    assert_eq!(cpu_parallelism_for_capacity(0, 96), 1);
}

#[derive(Debug, Clone, Default)]
pub struct CancellationToken {
    flag: Arc<AtomicBool>,
    parent: Option<Arc<Self>>,
    deadline: Option<Instant>,
}

impl CancellationToken {
    /// Shares cancellation with an existing operation owner. Clones keep the
    /// same flag alive; neither construction nor normal destruction cancels it.
    #[must_use]
    pub fn from_shared_flag(flag: Arc<AtomicBool>) -> Self {
        Self {
            flag,
            parent: None,
            deadline: None,
        }
    }

    /// Observe both an existing owner flag and its enclosing operation. Cancelling
    /// this token writes only the supplied flag; observing parent cancellation
    /// does not set that flag. Use distinct flags for independent owners.
    #[must_use]
    pub fn from_shared_flag_with_parent(flag: Arc<AtomicBool>, parent: &Self) -> Self {
        Self {
            flag,
            parent: Some(Arc::new(parent.clone())),
            deadline: None,
        }
    }

    /// Give a child operation a cooperative deadline. Expiration and explicit
    /// child cancellation never cancel the parent or its other operations.
    /// Existing kernel/admission checkpoints observe the deadline without a
    /// timer thread; an uncooperative foreign callback cannot be preempted.
    #[must_use]
    pub fn with_deadline(&self, deadline: Instant) -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            parent: Some(Arc::new(self.clone())),
            deadline: Some(deadline),
        }
    }

    pub fn cancel(&self) {
        self.flag.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        let mut current = self;
        loop {
            if current.flag.load(Ordering::Acquire)
                || current
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return true;
            }
            match &current.parent {
                Some(parent) => current = parent,
                None => return false,
            }
        }
    }

    /// # Errors
    /// Returns a deterministic cancellation error.
    pub fn check(&self) -> Result<()> {
        let mut current = self;
        loop {
            if current
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(pool_error("execution deadline exceeded"));
            }
            if current.flag.load(Ordering::Acquire) {
                return Err(pool_error("execution cancelled"));
            }
            match &current.parent {
                Some(parent) => current = parent,
                None => return Ok(()),
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerContext {
    pub worker_index: usize,
    pub memory: LiveMemoryPool,
    cancellation: CancellationToken,
    shutdown: CancellationToken,
}

impl WorkerContext {
    /// # Errors
    /// Rejects cancelled work and work whose owning runtime is shutting down.
    pub fn check_cancelled(&self) -> Result<()> {
        self.shutdown.check()?;
        self.cancellation.check()
    }
}

type Job = Box<dyn FnOnce(usize, &Shared) + Send>;

struct QueuedJob {
    bytes: u64,
    run: Job,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<QueuedJob>,
    bytes: u64,
    peak_bytes: u64,
    peak_jobs: usize,
    active: usize,
    peak_active: usize,
    active_worker_limit: usize,
    idle_workers: usize,
    waiting_producers: usize,
    closed: bool,
}

struct Shared {
    queue: Mutex<Queue>,
    worker_ready: Condvar,
    producer_ready: Condvar,
    memory: LiveMemoryPool,
    worker_count: usize,
    queue_limit: usize,
    queue_byte_limit: u64,
    shutdown: CancellationToken,
    completed: AtomicU64,
    worker_busy_nanos: AtomicU64,
    enqueue_wait_nanos: AtomicU64,
    waits: Option<Box<WaitStats>>,
}

struct OneshotInner<T> {
    result: Option<Result<Budgeted<T>>>,
    disconnected: bool,
}

struct OneshotSlot<T> {
    state: Mutex<OneshotInner<T>>,
    ready: Condvar,
}

struct OneshotSender<T> {
    slot: Arc<OneshotSlot<T>>,
    sent: bool,
}

impl<T> OneshotSender<T> {
    fn send(mut self, value: Result<Budgeted<T>>) {
        self.sent = true;
        {
            let mut state = self
                .slot
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.result = Some(value);
        }
        self.slot.ready.notify_one();
    }
}

impl<T> Drop for OneshotSender<T> {
    fn drop(&mut self) {
        if !self.sent {
            {
                let mut state = self
                    .slot
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.disconnected = true;
            }
            self.slot.ready.notify_one();
        }
    }
}

struct WaitStats {
    empty_queue_waits: AtomicU64,
    empty_queue_wait_nanos: AtomicU64,
    capacity_waits: AtomicU64,
    capacity_wait_nanos: AtomicU64,
    waiting_workers: AtomicUsize,
    _lease: MemoryLease,
}

impl WaitStats {
    fn new(memory: &LiveMemoryPool) -> Result<Box<Self>> {
        let lease = memory.reserve(size_of::<Self>() as u64)?;
        Ok(Box::new(Self {
            empty_queue_waits: AtomicU64::new(0),
            empty_queue_wait_nanos: AtomicU64::new(0),
            capacity_waits: AtomicU64::new(0),
            capacity_wait_nanos: AtomicU64::new(0),
            waiting_workers: AtomicUsize::new(0),
            _lease: lease,
        }))
    }

    fn snapshot(&self) -> ComputePoolWaitObservation {
        ComputePoolWaitObservation {
            completed_empty_queue_waits: self.empty_queue_waits.load(Ordering::Relaxed),
            empty_queue_wait_nanos: self.empty_queue_wait_nanos.load(Ordering::Relaxed),
            completed_capacity_waits: self.capacity_waits.load(Ordering::Relaxed),
            capacity_wait_nanos: self.capacity_wait_nanos.load(Ordering::Relaxed),
            waiting_workers: self.waiting_workers.load(Ordering::Relaxed),
        }
    }
}

/// Opt-in condition-variable observations, separate from submission overhead.
/// Counts include spurious wakeups. Open waits contribute elapsed time only
/// after returning; worker elapsed times overlap and are not CPU or engine time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputePoolWaitObservation {
    pub completed_empty_queue_waits: u64,
    pub empty_queue_wait_nanos: u64,
    pub completed_capacity_waits: u64,
    pub capacity_wait_nanos: u64,
    pub waiting_workers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComputePoolSnapshot {
    pub workers_created: usize,
    pub active_workers: usize,
    pub peak_active_workers: usize,
    pub queued_jobs: usize,
    pub peak_queued_jobs: usize,
    pub queued_bytes: u64,
    pub peak_queued_bytes: u64,
    pub completed_jobs: u64,
    pub worker_busy_nanos: u64,
    pub enqueue_wait_nanos: u64,
    pub waiting_producers: usize,
    pub queue_closed: bool,
    /// None means observation was disabled, not that no waiting occurred.
    pub waits: Option<ComputePoolWaitObservation>,
}

/// Dropping the owner cancels and drains jobs, then joins its workers.
pub struct ComputePool {
    shared: Arc<Shared>,
    workers: Vec<JoinHandle<()>>,
}

impl ComputePool {
    /// # Errors
    /// Rejects zero limits, impossible queue limits, and worker startup failures.
    pub fn new(
        workers: usize,
        queue_limit: usize,
        queue_byte_limit: u64,
        memory: LiveMemoryPool,
    ) -> Result<Self> {
        Self::with_wait_observation(workers, queue_limit, queue_byte_limit, memory, false)
    }

    /// Construct the same pool with optional bounded condition-variable timing.
    /// Observation storage is charged to the supplied pool before workers start.
    /// Ordinary construction leaves these additional clocks and counters disabled.
    ///
    /// # Errors
    /// Rejects invalid limits, observation-credit refusal, or worker startup failure.
    pub fn with_wait_observation(
        workers: usize,
        queue_limit: usize,
        queue_byte_limit: u64,
        memory: LiveMemoryPool,
        observe_waits: bool,
    ) -> Result<Self> {
        if workers == 0 || queue_limit == 0 || queue_byte_limit == 0 {
            return Err(pool_error("compute pool limits must be greater than zero"));
        }
        if queue_byte_limit > memory.snapshot().limit_bytes {
            return Err(pool_error("queue byte limit exceeds shared memory budget"));
        }
        let waits = observe_waits.then(|| WaitStats::new(&memory)).transpose()?;
        let mut pool = Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue {
                    active_worker_limit: workers,
                    ..Queue::default()
                }),
                worker_ready: Condvar::new(),
                producer_ready: Condvar::new(),
                memory,
                worker_count: workers,
                queue_limit,
                queue_byte_limit,
                shutdown: CancellationToken::default(),
                completed: AtomicU64::new(0),
                worker_busy_nanos: AtomicU64::new(0),
                enqueue_wait_nanos: AtomicU64::new(0),
                waits,
            }),
            workers: Vec::with_capacity(workers),
        };
        for index in 0..workers {
            let shared = Arc::clone(&pool.shared);
            let worker = thread::Builder::new()
                .name(format!("shardloom-compute-{index}"))
                .spawn(move || worker_loop(index, &shared))
                .map_err(|error| pool_error(&format!("compute worker startup failed: {error}")))?;
            pool.workers.push(worker);
        }
        Ok(pool)
    }

    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.shared.worker_count
    }

    #[must_use]
    pub fn queue_byte_limit(&self) -> u64 {
        self.shared.queue_byte_limit
    }

    #[must_use]
    pub fn active_worker_limit(&self) -> usize {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_worker_limit
    }

    /// Adjust the active concurrency ceiling (`1..=worker_count`) without
    /// spawning or destroying worker threads. Returns the previous ceiling.
    pub fn set_active_worker_limit(&self, limit: usize) -> usize {
        let clamped = limit.clamp(1, self.shared.worker_count);
        let (previous, should_wake) = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let previous = queue.active_worker_limit;
            queue.active_worker_limit = clamped;
            let should_wake =
                clamped > previous && !queue.jobs.is_empty() && queue.idle_workers > 0;
            (previous, should_wake)
        };
        if should_wake {
            self.shared.worker_ready.notify_all();
        }
        previous
    }

    /// Apply a `PulseWeave` report's admitted WIP ceiling to this pool and
    /// return the previous ceiling.
    #[allow(clippy::must_use_candidate)]
    pub fn apply_pulseweave_report(&self, report: &crate::pulseweave::PulseWeaveReport) -> usize {
        self.set_active_worker_limit(report.batch_window_size(self.shared.worker_count))
    }

    /// Temporarily scope the active worker ceiling and restore the prior limit on drop.
    #[must_use]
    pub fn scope_active_worker_limit(&self, limit: usize) -> ComputePoolWorkerLimitGuard<'_> {
        let previous = self.set_active_worker_limit(limit);
        ComputePoolWorkerLimitGuard {
            pool: self,
            previous,
        }
    }

    #[must_use]
    pub fn memory(&self) -> &LiveMemoryPool {
        &self.shared.memory
    }

    #[must_use]
    pub fn snapshot(&self) -> ComputePoolSnapshot {
        let queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ComputePoolSnapshot {
            workers_created: self.shared.worker_count,
            active_workers: queue.active,
            peak_active_workers: queue.peak_active,
            queued_jobs: queue.jobs.len(),
            peak_queued_jobs: queue.peak_jobs,
            queued_bytes: queue.bytes,
            peak_queued_bytes: queue.peak_bytes,
            completed_jobs: self.shared.completed.load(Ordering::Relaxed),
            worker_busy_nanos: self.shared.worker_busy_nanos.load(Ordering::Relaxed),
            enqueue_wait_nanos: self.shared.enqueue_wait_nanos.load(Ordering::Relaxed),
            waiting_producers: queue.waiting_producers,
            queue_closed: queue.closed,
            waits: self.shared.waits.as_ref().map(|waits| waits.snapshot()),
        }
    }

    /// Perform the same cancellation and joins as Drop, then capture the closed
    /// owner's counters. Returned task results keep their independent credits.
    /// If invoked by one of this pool's workers, that current thread is not
    /// joined; the snapshot then honestly retains its active-worker count.
    #[must_use]
    pub fn close_and_snapshot(mut self) -> ComputePoolSnapshot {
        self.close();
        self.snapshot()
    }

    fn close(&mut self) {
        self.shared.shutdown.cancel();
        {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.closed = true;
        }
        self.shared.worker_ready.notify_all();
        self.shared.producer_ready.notify_all();
        for worker in self.workers.drain(..) {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }

    /// Submit an already-reserved input/closure. Its credits travel with the result.
    /// The task must grow its lease before retaining additional output or state.
    ///
    /// # Errors
    /// Rejects cancellation, foreign reservations, excessive jobs, or a closed pool.
    pub fn submit<T, F>(
        &self,
        work: Budgeted<F>,
        cancellation: CancellationToken,
    ) -> Result<ComputeTask<T>>
    where
        T: Send + 'static,
        F: FnOnce(&WorkerContext, &mut MemoryLease) -> Result<T> + Send + 'static,
    {
        cancellation.check()?;
        let (work, mut lease) = work.into_parts();
        if !self.shared.memory.owns(&lease) {
            return Err(pool_error(
                "compute input belongs to a different memory pool",
            ));
        }
        let bytes = lease.bytes();
        if bytes > self.shared.queue_byte_limit {
            return Err(pool_error("compute input exceeds queue byte limit"));
        }
        let slot = Arc::new(OneshotSlot {
            state: Mutex::new(OneshotInner {
                result: None,
                disconnected: false,
            }),
            ready: Condvar::new(),
        });
        let sender = OneshotSender {
            slot: Arc::clone(&slot),
            sent: false,
        };
        let enqueue_cancellation = cancellation.clone();
        let token = cancellation;
        let run = Box::new(move |worker_index, shared: &Shared| {
            let context = WorkerContext {
                worker_index,
                memory: shared.memory.clone(),
                cancellation: token.clone(),
                shutdown: shared.shutdown.clone(),
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                context.check_cancelled()?;
                let value = work(&context, &mut lease)?;
                context.check_cancelled()?;
                Ok(Budgeted::new(value, lease))
            }))
            .unwrap_or_else(|_| Err(pool_error("compute worker task panicked")));
            if result.is_err() {
                token.cancel();
            }
            sender.send(result);
        });
        let queued = QueuedJob { bytes, run };
        let started = Instant::now();
        let wake_worker = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                enqueue_cancellation.check()?;
                if queue.closed {
                    return Err(pool_error("compute pool is closed"));
                }
                if queue.jobs.len() < self.shared.queue_limit
                    && bytes <= self.shared.queue_byte_limit - queue.bytes
                {
                    break;
                }
                // Timed waiting makes cancellation observable even when no worker frees a slot.
                queue.waiting_producers += 1;
                let wait_started = self.shared.waits.as_ref().map(|_| Instant::now());
                queue = self
                    .shared
                    .producer_ready
                    .wait_timeout(queue, Duration::from_millis(10))
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0;
                queue.waiting_producers -= 1;
                if let (Some(waits), Some(started)) = (&self.shared.waits, wait_started) {
                    waits.capacity_waits.fetch_add(1, Ordering::Relaxed);
                    waits
                        .capacity_wait_nanos
                        .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
                }
            }
            queue.bytes += bytes;
            queue.jobs.push_back(queued);
            queue.peak_bytes = queue.peak_bytes.max(queue.bytes);
            queue.peak_jobs = queue.peak_jobs.max(queue.jobs.len());
            queue.idle_workers > 0 && (queue.closed || queue.active < queue.active_worker_limit)
        };
        self.shared
            .enqueue_wait_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        if wake_worker {
            self.shared.worker_ready.notify_one();
        }
        Ok(ComputeTask { slot })
    }
}

/// Restores the previous [`ComputePool`] active worker limit when dropped.
pub struct ComputePoolWorkerLimitGuard<'a> {
    pool: &'a ComputePool,
    previous: usize,
}

impl Drop for ComputePoolWorkerLimitGuard<'_> {
    fn drop(&mut self) {
        let _ = self.pool.set_active_worker_limit(self.previous);
    }
}

impl Drop for ComputePool {
    fn drop(&mut self) {
        self.close();
    }
}

pub struct ComputeTask<T> {
    slot: Arc<OneshotSlot<T>>,
}

impl<T> ComputeTask<T> {
    /// # Errors
    /// Returns the task's native error or a disconnected-worker error.
    pub fn join(self) -> Result<Budgeted<T>> {
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(result) = state.result.take() {
                return result;
            }
            if state.disconnected {
                return Err(pool_error("compute worker disconnected"));
            }
            state = self
                .slot
                .ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

fn worker_loop(index: usize, shared: &Shared) {
    loop {
        let job = {
            let mut queue = shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                if queue.closed || queue.active < queue.active_worker_limit {
                    if let Some(job) = queue.jobs.pop_front() {
                        queue.bytes -= job.bytes;
                        queue.active += 1;
                        queue.peak_active = queue.peak_active.max(queue.active);
                        debug_assert!(queue.active <= shared.worker_count);
                        let wake_producers = queue.waiting_producers > 0;
                        drop(queue);
                        if wake_producers {
                            shared.producer_ready.notify_all();
                        }
                        break job;
                    }
                    if queue.closed {
                        return;
                    }
                }
                queue.idle_workers += 1;
                let wait_started = shared.waits.as_ref().map(|waits| {
                    waits.waiting_workers.fetch_add(1, Ordering::Relaxed);
                    Instant::now()
                });
                queue = shared
                    .worker_ready
                    .wait(queue)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                queue.idle_workers -= 1;
                if let (Some(waits), Some(started)) = (&shared.waits, wait_started) {
                    waits.waiting_workers.fetch_sub(1, Ordering::Relaxed);
                    waits.empty_queue_waits.fetch_add(1, Ordering::Relaxed);
                    waits
                        .empty_queue_wait_nanos
                        .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
                }
            }
        };
        let started = Instant::now();
        (job.run)(index, shared);
        shared
            .worker_busy_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        shared.completed.fetch_add(1, Ordering::Relaxed);
        let mut queue = shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        queue.active -= 1;
        let wake_worker = !queue.jobs.is_empty()
            && (queue.closed || queue.active < queue.active_worker_limit)
            && queue.idle_workers > 0;
        drop(queue);
        if wake_worker {
            shared.worker_ready.notify_one();
        }
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn pool_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; fallback execution was not attempted"))
}

#[cfg(test)]
#[path = "compute_pool_wait_tests.rs"]
mod wait_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadlines_reach_descendants_without_revoking_other_operations() {
        let session = CancellationToken::default();
        let expired = session.with_deadline(Instant::now());
        let descendant = CancellationToken::from_shared_flag_with_parent(
            Arc::new(AtomicBool::new(false)),
            &expired,
        );
        for token in [expired.clone(), descendant] {
            assert!(token.is_cancelled());
            assert!(
                token
                    .check()
                    .unwrap_err()
                    .to_string()
                    .contains("deadline exceeded")
            );
        }
        assert!(!session.is_cancelled());
        let other = session.with_deadline(Instant::now() + Duration::from_secs(60));
        assert!(other.check().is_ok());
        other.cancel();
        assert!(
            other
                .check()
                .unwrap_err()
                .to_string()
                .contains("execution cancelled")
        );
        assert!(session.check().is_ok());
        let fresh = session.with_deadline(Instant::now() + Duration::from_secs(60));
        session.cancel();
        assert!(fresh.check().is_err());
    }

    #[test]
    fn linked_cancellation_observes_both_owners_without_cancelling_ancestors() {
        let parent = CancellationToken::default();
        let owner = Arc::new(AtomicBool::new(false));
        let child = CancellationToken::from_shared_flag_with_parent(owner.clone(), &parent);
        let grandchild = CancellationToken::from_shared_flag_with_parent(
            Arc::new(AtomicBool::new(false)),
            &child,
        );
        assert!(grandchild.check().is_ok());
        child.cancel();
        assert!(owner.load(Ordering::Acquire));
        assert!(grandchild.check().is_err());
        assert!(parent.check().is_ok());

        let fresh_owner = Arc::new(AtomicBool::new(false));
        let renewed = CancellationToken::from_shared_flag_with_parent(fresh_owner.clone(), &parent);
        assert!(renewed.check().is_ok());
        parent.cancel();
        assert!(renewed.check().is_err());
        assert!(!fresh_owner.load(Ordering::Acquire));
        let pre_cancelled = CancellationToken::from_shared_flag_with_parent(fresh_owner, &parent);
        assert!(pre_cancelled.check().is_err());
    }

    #[test]
    fn cancellation_token_shares_existing_flag_in_both_directions() {
        let flag = Arc::new(AtomicBool::new(false));
        let token = CancellationToken::from_shared_flag(Arc::clone(&flag));
        let clone = token.clone();
        drop(token);
        assert!(!flag.load(Ordering::Acquire));
        flag.store(true, Ordering::Release);
        assert!(clone.check().is_err());
        let other = Arc::new(AtomicBool::new(false));
        let owner = CancellationToken::from_shared_flag(Arc::clone(&other));
        owner.clone().cancel();
        assert!(other.load(Ordering::Acquire));
        drop(other);
        assert!(owner.is_cancelled());
    }

    #[test]
    fn persistent_workers_return_owned_results_and_release_credits() {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let pool = ComputePool::new(2, 2, 512, memory.clone()).unwrap();
        let mut thread_ids = std::collections::HashSet::new();
        for expected in 0..64 {
            let task = pool
                .submit(
                    Budgeted::new(
                        move |_: &WorkerContext, _: &mut MemoryLease| {
                            Ok((expected * 2, thread::current().id()))
                        },
                        memory.reserve(16).unwrap(),
                    ),
                    CancellationToken::default(),
                )
                .unwrap();
            let result = task.join().unwrap();
            assert_eq!(result.value().0, expected * 2);
            thread_ids.insert(result.value().1);
            assert_eq!(memory.snapshot().reserved_bytes, 16);
        }
        assert!(thread_ids.len() <= 2);
        assert_eq!(pool.snapshot().workers_created, 2);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn queue_slot_and_byte_limits_are_enforced_under_contention() {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let pool = ComputePool::new(1, 2, 32, memory.clone()).unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first = pool
            .submit(
                Budgeted::new(
                    move |_: &WorkerContext, _: &mut MemoryLease| {
                        entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(())
                    },
                    memory.reserve(16).unwrap(),
                ),
                CancellationToken::default(),
            )
            .unwrap();
        entered_rx.recv().unwrap();
        let queued = pool
            .submit(
                Budgeted::new(
                    |_: &WorkerContext, _: &mut MemoryLease| Ok(()),
                    memory.reserve(32).unwrap(),
                ),
                CancellationToken::default(),
            )
            .unwrap();
        let token = CancellationToken::default();
        thread::scope(|scope| {
            let token_ref = &token;
            let pool_ref = &pool;
            let memory_ref = &memory;
            let producer = scope.spawn(move || {
                pool_ref.submit(
                    Budgeted::new(
                        |_: &WorkerContext, _: &mut MemoryLease| Ok(()),
                        memory_ref.reserve(1).unwrap(),
                    ),
                    token_ref.clone(),
                )
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            while pool.snapshot().waiting_producers == 0 {
                assert!(
                    Instant::now() < deadline,
                    "producer never reached backpressure"
                );
                thread::yield_now();
            }
            token.cancel();
            assert!(producer.join().unwrap().is_err());
        });
        assert_eq!(pool.snapshot().queued_bytes, 32);
        release_tx.send(()).unwrap();
        drop(first.join().unwrap());
        drop(queued.join().unwrap());
        let snapshot = pool.snapshot();
        assert!(snapshot.peak_queued_jobs <= 2);
        assert!(snapshot.peak_queued_bytes <= 32);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn panics_cancel_work_without_killing_the_pool() {
        let memory = LiveMemoryPool::new(128).unwrap();
        let pool = ComputePool::new(1, 2, 128, memory.clone()).unwrap();
        let token = CancellationToken::default();
        let task = pool
            .submit(
                Budgeted::new(
                    |_: &WorkerContext, _: &mut MemoryLease| -> Result<()> {
                        panic!("injected kernel panic");
                    },
                    memory.reserve(64).unwrap(),
                ),
                token.clone(),
            )
            .unwrap();
        assert!(task.join().is_err());
        assert!(token.is_cancelled());
        let task = pool
            .submit(
                Budgeted::new(
                    |_: &WorkerContext, _: &mut MemoryLease| Ok(7),
                    memory.reserve(64).unwrap(),
                ),
                CancellationToken::default(),
            )
            .unwrap();
        assert_eq!(*task.join().unwrap().value(), 7);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn runtime_drop_drains_cancelled_work_and_joins_threads() {
        let memory = LiveMemoryPool::new(128).unwrap();
        let pool = ComputePool::new(1, 2, 128, memory.clone()).unwrap();
        let (entered, ready) = mpsc::channel();
        let running = pool
            .submit(
                Budgeted::new(
                    move |context: &WorkerContext, _: &mut MemoryLease| -> Result<()> {
                        entered.send(()).unwrap();
                        loop {
                            context.check_cancelled()?;
                            thread::yield_now();
                        }
                    },
                    memory.reserve(32).unwrap(),
                ),
                CancellationToken::default(),
            )
            .unwrap();
        ready.recv().unwrap();
        let queued_ran = Arc::new(AtomicBool::new(false));
        let flag = queued_ran.clone();
        let queued = pool
            .submit(
                Budgeted::new(
                    move |_: &WorkerContext, _: &mut MemoryLease| {
                        flag.store(true, Ordering::Relaxed);
                        Ok(())
                    },
                    memory.reserve(32).unwrap(),
                ),
                CancellationToken::default(),
            )
            .unwrap();
        drop(pool);
        assert!(running.join().is_err());
        assert!(queued.join().is_err());
        assert!(!queued_ran.load(Ordering::Relaxed));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn active_worker_limit_throttles_and_wakes_on_pulseweave_increase() {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let pool = ComputePool::new(4, 8, 512, memory.clone()).unwrap();
        assert_eq!(pool.worker_count(), 4);
        assert_eq!(pool.active_worker_limit(), 4);
        let prior = pool.set_active_worker_limit(2);
        assert_eq!(prior, 4);
        assert_eq!(pool.active_worker_limit(), 2);

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let tx = entered_tx.clone();
            let rx = Arc::clone(&release_rx);
            tasks.push(
                pool.submit(
                    Budgeted::new(
                        move |_: &WorkerContext, _: &mut MemoryLease| {
                            tx.send(()).unwrap();
                            rx.lock().unwrap().recv().unwrap();
                            Ok(())
                        },
                        memory.reserve(16).unwrap(),
                    ),
                    CancellationToken::default(),
                )
                .unwrap(),
            );
        }
        entered_rx.recv().unwrap();
        entered_rx.recv().unwrap();
        assert!(entered_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert_eq!(pool.snapshot().peak_active_workers, 2);

        let previous = pool.set_active_worker_limit(4);
        assert_eq!(previous, 2);
        entered_rx.recv().unwrap();
        entered_rx.recv().unwrap();
        for _ in 0..4 {
            release_tx.send(()).unwrap();
        }
        for task in tasks {
            drop(task.join().unwrap());
        }
        assert_eq!(pool.snapshot().peak_active_workers, 4);

        let input = crate::pulseweave::PulseWeaveInput::new(
            "prepared_local_compute",
            "compute_pool_test",
            "throttle_test",
            vec![
                crate::pulseweave::PulseWeaveTaskShape::new(
                    "t0",
                    "scan",
                    crate::OperatorMemoryClass::Scan,
                    64,
                )
                .unwrap(),
            ],
            1 << 20,
            4,
            256,
        )
        .unwrap();
        let report = crate::pulseweave::plan_pulseweave(input).unwrap();
        let expected_limit = report.batch_window_size(pool.worker_count());
        {
            let _guard = pool.scope_active_worker_limit(1);
            assert_eq!(pool.active_worker_limit(), 1);
            let prev = pool.apply_pulseweave_report(&report);
            assert_eq!(prev, 1);
            assert_eq!(pool.active_worker_limit(), expected_limit.clamp(1, 4));
        }
        assert_eq!(pool.active_worker_limit(), 4);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
