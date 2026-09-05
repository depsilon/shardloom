//! Caller-owned persistent workers with bounded input queues and retained-result credits.

use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use shardloom_core::{Result, ShardLoomError};

use crate::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};

#[derive(Debug, Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// # Errors
    /// Returns a deterministic cancellation error.
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(pool_error("execution cancelled"))
        } else {
            Ok(())
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
    closed: bool,
}

struct Shared {
    queue: Mutex<Queue>,
    changed: Condvar,
    memory: LiveMemoryPool,
    worker_count: usize,
    queue_limit: usize,
    queue_byte_limit: u64,
    shutdown: CancellationToken,
    completed: AtomicU64,
    worker_busy_nanos: AtomicU64,
    enqueue_wait_nanos: AtomicU64,
    waiting_producers: AtomicUsize,
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
        if workers == 0 || queue_limit == 0 || queue_byte_limit == 0 {
            return Err(pool_error("compute pool limits must be greater than zero"));
        }
        if queue_byte_limit > memory.snapshot().limit_bytes {
            return Err(pool_error("queue byte limit exceeds shared memory budget"));
        }
        let mut pool = Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(Queue::default()),
                changed: Condvar::new(),
                memory,
                worker_count: workers,
                queue_limit,
                queue_byte_limit,
                shutdown: CancellationToken::default(),
                completed: AtomicU64::new(0),
                worker_busy_nanos: AtomicU64::new(0),
                enqueue_wait_nanos: AtomicU64::new(0),
                waiting_producers: AtomicUsize::new(0),
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
            workers_created: self.workers.len(),
            active_workers: queue.active,
            peak_active_workers: queue.peak_active,
            queued_jobs: queue.jobs.len(),
            peak_queued_jobs: queue.peak_jobs,
            queued_bytes: queue.bytes,
            peak_queued_bytes: queue.peak_bytes,
            completed_jobs: self.shared.completed.load(Ordering::Relaxed),
            worker_busy_nanos: self.shared.worker_busy_nanos.load(Ordering::Relaxed),
            enqueue_wait_nanos: self.shared.enqueue_wait_nanos.load(Ordering::Relaxed),
            waiting_producers: self.shared.waiting_producers.load(Ordering::Relaxed),
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
        let (sender, receiver) = mpsc::sync_channel(1);
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
            let _ = sender.send(result);
        });
        let queued = QueuedJob { bytes, run };
        let started = Instant::now();
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
            self.shared
                .waiting_producers
                .fetch_add(1, Ordering::Relaxed);
            queue = self
                .shared
                .changed
                .wait_timeout(queue, Duration::from_millis(10))
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
            self.shared
                .waiting_producers
                .fetch_sub(1, Ordering::Relaxed);
        }
        queue.bytes += bytes;
        queue.jobs.push_back(queued);
        queue.peak_bytes = queue.peak_bytes.max(queue.bytes);
        queue.peak_jobs = queue.peak_jobs.max(queue.jobs.len());
        self.shared
            .enqueue_wait_nanos
            .fetch_add(elapsed_nanos(started), Ordering::Relaxed);
        self.shared.changed.notify_all();
        Ok(ComputeTask { receiver })
    }
}

impl Drop for ComputePool {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
        {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            queue.closed = true;
            self.shared.changed.notify_all();
        }
        for worker in self.workers.drain(..) {
            if worker.thread().id() != thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}

pub struct ComputeTask<T> {
    receiver: mpsc::Receiver<Result<Budgeted<T>>>,
}

impl<T> ComputeTask<T> {
    /// # Errors
    /// Returns the task's native error or a disconnected-worker error.
    pub fn join(self) -> Result<Budgeted<T>> {
        self.receiver
            .recv()
            .map_err(|_| pool_error("compute worker disconnected"))?
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
                if let Some(job) = queue.jobs.pop_front() {
                    queue.bytes -= job.bytes;
                    queue.active += 1;
                    queue.peak_active = queue.peak_active.max(queue.active);
                    debug_assert!(queue.active <= shared.worker_count);
                    shared.changed.notify_all();
                    break job;
                }
                if queue.closed {
                    return;
                }
                queue = shared
                    .changed
                    .wait(queue)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        shared.changed.notify_all();
    }
}

fn elapsed_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn pool_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; fallback execution was not attempted"))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
