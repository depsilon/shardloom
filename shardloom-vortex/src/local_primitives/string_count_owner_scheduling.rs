//! Unregistered test-only scheduling over the actual retained partition reducer.
//! Source preparation and complete output export are outside execution clocks.

#![cfg(test)]

use super::super::aggregate_chunk_jobs::ChunkWorkerContext;
use super::{PARTITIONS, ReconcileProgress, StringCountPartial, StringCountPartitions, failed};
use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::{CancellationToken, ComputePool, ComputeTask},
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

const QUEUE_PER_WORKER: usize = 2;
const MAX_ROWS: usize = 131_072;
const MAX_TEXT_BYTES: usize = 256;

#[derive(Clone, Copy, Debug)]
enum Scheduling {
    Dynamic,
    Owner,
}

struct InputChunk {
    partial: StringCountPartial,
    ends: [usize; PARTITIONS],
    _metadata: MemoryLease,
}

struct Input {
    chunks: Vec<Arc<InputChunk>>,
    rows: u64,
    entries: usize,
    ranges: usize,
    _metadata: MemoryLease,
}

impl Input {
    fn prepare(
        input: &[(&str, u64, u64)],
        chunk_rows: usize,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        if input.len() > MAX_ROWS || chunk_rows == 0 || chunk_rows > MAX_ROWS {
            return Err(failed(
                "owner experiment input row geometry exceeds its cap",
            ));
        }
        let mut payload = 0_usize;
        let mut rows = 0_u64;
        for (value, _, count) in input {
            if value.len() > MAX_TEXT_BYTES || *count == 0 {
                return Err(failed("owner experiment text/weight is not admitted"));
            }
            payload = payload
                .checked_add(value.len())
                .ok_or_else(|| failed("input byte overflow"))?;
            rows = rows
                .checked_add(*count)
                .ok_or_else(|| failed("input weight overflow"))?;
        }
        if payload > 16 << 20 {
            return Err(failed("owner experiment input exceeds 16 MiB"));
        }
        let capacity = input.len().div_ceil(chunk_rows);
        let metadata = memory.reserve(bytes::<Arc<InputChunk>>(capacity)?)?;
        let mut chunks = exact_vec(capacity)?;
        let worker = ChunkWorkerContext::Inline(CancellationToken::default());
        let mut ranges = 0;
        for rows in input.chunks(chunk_rows) {
            let owner = memory.reserve(bytes::<InputChunk>(1)? + 2 * size_of::<usize>() as u64)?;
            let mut partial = StringCountPartial::benchmark_weighted(rows, memory)?;
            let ends = partial.arrange_partitions::<PARTITIONS>(&worker)?;
            let mut start = 0;
            for end in ends {
                ranges += usize::from(end > start);
                start = end;
            }
            chunks.push(Arc::new(InputChunk {
                partial,
                ends,
                _metadata: owner,
            }));
        }
        Ok(Self {
            chunks,
            rows,
            entries: input.len(),
            ranges,
            _metadata: metadata,
        })
    }
}

#[derive(Clone)]
struct Range {
    chunk: Arc<InputChunk>,
    partition: usize,
    start: usize,
    end: usize,
}

#[derive(Default)]
struct Counters {
    queued: AtomicUsize,
    peak_queued: AtomicUsize,
    active: AtomicUsize,
    peak_active: AtomicUsize,
    completed: AtomicUsize,
    enqueue_wait: AtomicU64,
    dequeue_wait: AtomicU64,
}

struct QueueState {
    ranges: VecDeque<Range>,
    closed: bool,
}
struct RangeQueue {
    state: Mutex<QueueState>,
    changed: Condvar,
    capacity: usize,
    counters: Arc<Counters>,
    _lease: MemoryLease,
}

impl RangeQueue {
    fn new(
        capacity: usize,
        memory: &LiveMemoryPool,
        counters: &Arc<Counters>,
    ) -> Result<Arc<Self>> {
        let reservation = bytes::<Range>(capacity)?
            .checked_add(bytes::<Self>(1)? + 2 * size_of::<usize>() as u64)
            .ok_or_else(|| failed("owner queue reservation overflow"))?;
        let lease = memory.reserve(reservation)?;
        let mut ranges = VecDeque::new();
        ranges
            .try_reserve_exact(capacity)
            .map_err(|_| failed("owner queue allocation failed"))?;
        if ranges.capacity() != capacity {
            return Err(failed("owner queue capacity exceeds reservation"));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(QueueState {
                ranges,
                closed: false,
            }),
            changed: Condvar::new(),
            capacity,
            counters: Arc::clone(counters),
            _lease: lease,
        }))
    }
    fn push(&self, range: Range, token: &CancellationToken) -> Result<()> {
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| failed("owner queue poisoned"))?;
        loop {
            token.check()?;
            if state.closed {
                return Err(failed("owner queue admission closed"));
            }
            if state.ranges.len() < self.capacity {
                break;
            }
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| failed("owner queue wait poisoned"))?
                .0;
        }
        let queued = self.counters.queued.fetch_add(1, Ordering::AcqRel) + 1;
        self.counters
            .peak_queued
            .fetch_max(queued, Ordering::AcqRel);
        state.ranges.push_back(range);
        add_nanos(&self.counters.enqueue_wait, started)?;
        self.changed.notify_all();
        Ok(())
    }
    fn pop(&self, worker: &ChunkWorkerContext) -> Result<Option<Range>> {
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| failed("owner queue poisoned"))?;
        loop {
            worker.check_cancelled()?;
            if let Some(range) = state.ranges.pop_front() {
                self.counters.queued.fetch_sub(1, Ordering::AcqRel);
                add_nanos(&self.counters.dequeue_wait, started)?;
                self.changed.notify_all();
                return Ok(Some(range));
            }
            if state.closed {
                add_nanos(&self.counters.dequeue_wait, started)?;
                return Ok(None);
            }
            state = self
                .changed
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| failed("owner queue wait poisoned"))?
                .0;
        }
    }
    fn close(&self, discard: bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        if discard {
            self.counters
                .queued
                .fetch_sub(state.ranges.len(), Ordering::AcqRel);
            state.ranges.clear();
        }
        self.changed.notify_all();
    }
}

struct ActiveRange(Arc<Counters>);
impl Drop for ActiveRange {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

fn reduce_range(
    partitions: &StringCountPartitions,
    range: &Range,
    worker: &ChunkWorkerContext,
    counters: &Arc<Counters>,
    token: &CancellationToken,
    cancel_after: Option<usize>,
) -> Result<()> {
    worker.check_cancelled()?;
    let active = counters.active.fetch_add(1, Ordering::AcqRel) + 1;
    counters.peak_active.fetch_max(active, Ordering::AcqRel);
    let _active = ActiveRange(Arc::clone(counters));
    let mut progress = ReconcileProgress {
        cursor: range.start,
        ..ReconcileProgress::default()
    };
    if !partitions.reduce_partition(
        range.partition,
        range.end,
        &range.chunk.partial,
        worker,
        &mut progress,
    )? || progress.cursor != range.end
    {
        return Err(failed("owner experiment reached exact entry/byte pressure"));
    }
    super::add(
        &partitions.committed_rows,
        progress.consumed,
        "experiment weight overflow",
    )?;
    let completed = counters.completed.fetch_add(1, Ordering::AcqRel) + 1;
    if cancel_after.is_some_and(|limit| completed >= limit) {
        token.cancel();
    }
    Ok(())
}

struct Scheduler {
    pool: Option<ComputePool>,
    tasks: Vec<ComputeTask<()>>,
    queues: Vec<Arc<RangeQueue>>,
    token: CancellationToken,
    first_error: Arc<Mutex<Option<shardloom_core::ShardLoomError>>>,
    counters: Arc<Counters>,
    _metadata: MemoryLease,
}

impl Scheduler {
    fn failure_or(&self, error: shardloom_core::ShardLoomError) -> shardloom_core::ShardLoomError {
        self.first_error
            .lock()
            .ok()
            .and_then(|first| first.clone())
            .unwrap_or(error)
    }
    fn new(
        lanes: usize,
        mode: Scheduling,
        memory: &LiveMemoryPool,
        token: CancellationToken,
    ) -> Result<Self> {
        token.check()?;
        if !matches!(lanes, 1 | 2 | 4 | 8) {
            return Err(failed("experiment admits 1/2/4/8 lanes"));
        }
        let workers = lanes - 1;
        let queues = match mode {
            Scheduling::Dynamic => usize::from(workers > 0),
            Scheduling::Owner => workers,
        };
        let metadata_bytes = bytes::<Arc<RangeQueue>>(queues)?
            + bytes::<ComputeTask<()>>(workers)?
            + bytes::<Range>(lanes)?
            + bytes::<Self>(1)?
            + bytes::<Counters>(1)?
            + 1024;
        let metadata = memory.reserve(metadata_bytes)?;
        let counters = Arc::new(Counters::default());
        let mut queue_owners = exact_vec(queues)?;
        for _ in 0..queues {
            let capacity = match mode {
                Scheduling::Dynamic => workers * QUEUE_PER_WORKER,
                Scheduling::Owner => QUEUE_PER_WORKER,
            };
            queue_owners.push(RangeQueue::new(capacity, memory, &counters)?);
        }
        let tasks = exact_vec(workers)?;
        let pool = if workers == 0 {
            None
        } else {
            Some(ComputePool::new(
                workers,
                workers,
                memory.snapshot().limit_bytes,
                memory.clone(),
            )?)
        };
        Ok(Self {
            pool,
            tasks,
            queues: queue_owners,
            token,
            first_error: Arc::new(Mutex::new(None)),
            counters,
            _metadata: metadata,
        })
    }
    fn start(
        &mut self,
        partitions: &Arc<StringCountPartitions>,
        mode: Scheduling,
        cancel_after: Option<usize>,
    ) -> Result<()> {
        let Some(pool) = &self.pool else {
            return Ok(());
        };
        for lane in 0..pool.snapshot().workers_created {
            let queue = Arc::clone(
                &self.queues[match mode {
                    Scheduling::Dynamic => 0,
                    Scheduling::Owner => lane,
                }],
            );
            let partitions = Arc::clone(partitions);
            let counters = Arc::clone(&self.counters);
            let first_error = Arc::clone(&self.first_error);
            let token = self.token.clone();
            let lease = pool.memory().reserve(1024)?;
            let task = pool.submit(
                Budgeted::new(
                    move |context: &shardloom_exec::compute_pool::WorkerContext,
                          _: &mut MemoryLease| {
                        let worker = ChunkWorkerContext::Pool(context.clone());
                        let outcome: Result<()> = (|| {
                            while let Some(range) = queue.pop(&worker)? {
                                reduce_range(
                                    &partitions,
                                    &range,
                                    &worker,
                                    &counters,
                                    &token,
                                    cancel_after,
                                )?;
                            }
                            Ok(())
                        })();
                        if let Err(error) = &outcome {
                            let mut first = first_error
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            first.get_or_insert_with(|| error.clone());
                            token.cancel();
                        }
                        outcome
                    },
                    lease,
                ),
                self.token.clone(),
            )?;
            self.tasks.push(task);
        }
        Ok(())
    }
    fn drain(&mut self) -> Result<()> {
        for queue in &self.queues {
            queue.close(false);
        }
        let mut error = None;
        for task in self.tasks.drain(..) {
            if let Err(failed) = task.join() {
                error.get_or_insert(failed);
            }
        }
        if let Some(primary) = self
            .first_error
            .lock()
            .map_err(|_| failed("owner error state poisoned"))?
            .take()
        {
            return Err(primary);
        }
        if let Some(error) = error {
            return Err(error);
        }
        self.token.check()
    }
}
impl Drop for Scheduler {
    fn drop(&mut self) {
        self.token.cancel();
        for queue in &self.queues {
            queue.close(true);
        }
        for task in self.tasks.drain(..) {
            let _ = task.join();
        }
        // Join the actual persistent workers before queue/control reservations drop.
        drop(self.pool.take());
    }
}

struct Report {
    values: BTreeMap<String, u64>,
    construction_nanos: u64,
    execution_nanos: u64,
    drop_nanos: u64,
    lock_wait_nanos: u64,
    reconcile_nanos: u64,
    enqueue_wait_nanos: u64,
    dequeue_wait_nanos: u64,
    peak_queued_ranges: usize,
    peak_active_ranges: usize,
    completed_ranges: usize,
    peak_reserved_bytes: u64,
    table_reserved_bytes: u64,
    copied_bytes: u64,
    equality_comparisons: u64,
    credit_claims: u64,
    credit_returns: u64,
    rows: u64,
    background_workers: usize,
}

fn run(
    input: &Input,
    mode: Scheduling,
    lanes: usize,
    memory: &LiveMemoryPool,
    entry_limit: usize,
    token: CancellationToken,
    cancel_after: Option<usize>,
) -> Result<Report> {
    let started = Instant::now();
    let mut scheduler = Scheduler::new(lanes, mode, memory, token)?;
    let partitions = StringCountPartitions::try_new(memory, entry_limit, 0)?
        .ok_or_else(|| failed("owner experiment state admission failed"))?;
    scheduler.start(&partitions, mode, cancel_after)?;
    let construction_nanos = nanos(started)?;
    let started = Instant::now();
    let inline = ChunkWorkerContext::Inline(scheduler.token.clone());
    let mut ordinal = 0;
    for chunk in &input.chunks {
        let mut start = 0;
        for (partition, end) in chunk.ends.iter().copied().enumerate() {
            if start != end {
                let range = Range {
                    chunk: Arc::clone(chunk),
                    partition,
                    start,
                    end,
                };
                let lane = match mode {
                    Scheduling::Dynamic => ordinal % lanes,
                    Scheduling::Owner => partition % lanes,
                };
                if lane == lanes - 1 {
                    reduce_range(
                        &partitions,
                        &range,
                        &inline,
                        &scheduler.counters,
                        &scheduler.token,
                        cancel_after,
                    )
                    .map_err(|error| scheduler.failure_or(error))?;
                } else {
                    let queue = match mode {
                        Scheduling::Dynamic => 0,
                        Scheduling::Owner => lane,
                    };
                    scheduler.queues[queue]
                        .push(range, &scheduler.token)
                        .map_err(|error| scheduler.failure_or(error))?;
                }
                ordinal += 1;
            }
            start = end;
        }
    }
    scheduler.drain()?;
    let execution_nanos = nanos(started)?;
    if ordinal != input.ranges {
        return Err(failed("owner experiment did not submit each native range"));
    }
    finish_report(
        input,
        scheduler,
        partitions,
        construction_nanos,
        execution_nanos,
    )
}

struct Export {
    values: BTreeMap<String, u64>,
    copied_bytes: u64,
    table_reserved_bytes: u64,
}

fn export(partitions: &StringCountPartitions) -> Result<Export> {
    let mut result = Export {
        values: BTreeMap::new(),
        copied_bytes: 0,
        table_reserved_bytes: 0,
    };
    for partition in &partitions.partitions {
        let partition = partition
            .lock()
            .map_err(|_| failed("owner output lock poisoned"))?;
        result.copied_bytes = result
            .copied_bytes
            .checked_add(partition.benchmark_payload_bytes_copied)
            .ok_or_else(|| failed("owner copy counter overflow"))?;
        result.table_reserved_bytes = result
            .table_reserved_bytes
            .checked_add(partition.slots_lease.bytes())
            .and_then(|bytes| bytes.checked_add(partition.bytes_lease.bytes()))
            .ok_or_else(|| failed("owner state counter overflow"))?;
        for slot in &partition.slots {
            if slot.count == 0 {
                continue;
            }
            let key = std::str::from_utf8(&partition.bytes[slot.offset..slot.offset + slot.len])
                .map_err(|_| failed("owner output UTF8 invalid"))?;
            if result.values.insert(key.to_owned(), slot.count).is_some() {
                return Err(failed("duplicate output group"));
            }
        }
    }
    Ok(result)
}

fn finish_report(
    input: &Input,
    scheduler: Scheduler,
    partitions: Arc<StringCountPartitions>,
    construction_nanos: u64,
    execution_nanos: u64,
) -> Result<Report> {
    let evidence = partitions.evidence()?;
    if scheduler.counters.active.load(Ordering::Acquire) != 0
        || scheduler.counters.queued.load(Ordering::Acquire) != 0
    {
        return Err(failed("owner experiment ranges remain live after drain"));
    }
    if evidence.rows != input.rows
        || scheduler.counters.completed.load(Ordering::Acquire) != input.ranges
    {
        return Err(failed(
            "owner experiment lost or duplicated complete range contributions",
        ));
    }
    let Export {
        values,
        copied_bytes,
        table_reserved_bytes,
    } = export(&partitions)?;
    if values.len() != evidence.groups {
        return Err(failed(
            "owner experiment group credits differ from complete output",
        ));
    }
    let memory = partitions.memory.clone();
    let background_workers = scheduler
        .pool
        .as_ref()
        .map_or(0, |pool| pool.snapshot().workers_created);
    let mut report = Report {
        values,
        construction_nanos,
        execution_nanos,
        drop_nanos: 0,
        lock_wait_nanos: evidence.lock_wait_nanos,
        reconcile_nanos: evidence.reconcile_nanos,
        enqueue_wait_nanos: scheduler.counters.enqueue_wait.load(Ordering::Acquire),
        dequeue_wait_nanos: scheduler.counters.dequeue_wait.load(Ordering::Acquire),
        peak_queued_ranges: scheduler.counters.peak_queued.load(Ordering::Acquire),
        peak_active_ranges: scheduler.counters.peak_active.load(Ordering::Acquire),
        completed_ranges: input.ranges,
        peak_reserved_bytes: memory.snapshot().peak_reserved_bytes,
        table_reserved_bytes,
        copied_bytes,
        equality_comparisons: evidence.equality_comparisons,
        credit_claims: evidence.entry_credit_claim_calls,
        credit_returns: evidence.entry_credit_return_calls,
        rows: evidence.rows,
        background_workers,
    };
    let started = Instant::now();
    drop(scheduler);
    drop(partitions);
    report.drop_nanos = nanos(started)?;
    if memory.snapshot().reserved_bytes != 0 {
        return Err(failed("owner experiment did not refund all state credits"));
    }
    Ok(report)
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("owner experiment reservation overflow"))
}
fn exact_vec<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| failed("owner experiment allocation failed"))?;
    if values.capacity() != capacity {
        return Err(failed("owner experiment capacity exceeded admission"));
    }
    Ok(values)
}
fn nanos(started: Instant) -> Result<u64> {
    u64::try_from(started.elapsed().as_nanos())
        .map_err(|_| failed("owner experiment clock overflow"))
}
fn add_nanos(counter: &AtomicU64, started: Instant) -> Result<()> {
    let elapsed = nanos(started)?;
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prior| {
            prior.checked_add(elapsed)
        })
        .map(|_| ())
        .map_err(|_| failed("owner experiment duration overflow"))
}

#[path = "string_count_owner_scheduling_benchmark.rs"]
mod benchmark;
#[path = "string_count_owner_scheduling_tests.rs"]
mod tests;
