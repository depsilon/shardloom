//! Complete-key reconciliation on the existing count workers.
//! Keys occur in one partition across the entire query. Chunk-local counts never
//! prune candidates; final partition top-K runs only after the source is drained.

use super::{
    aggregate_chunk_jobs::ChunkWorkerContext,
    string_count_partial::{StringCountPartial, StringCountPartialWork},
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use super::string_count_entry_credits as entry_credits;
use entry_credits::{Claim, EntryBlock, EntryCredits};

pub(super) const PARTITIONS: usize = 64;

#[derive(Clone, Copy, Default)]
struct Slot {
    hash: u64,
    offset: usize,
    len: usize,
    count: u64,
}

struct Partition {
    slots: Vec<Slot>,
    bytes: Vec<u8>,
    groups: usize,
    slots_lease: MemoryLease,
    bytes_lease: MemoryLease,
    selection_lease: Option<MemoryLease>,
}

pub(super) struct PartitionReceipt {
    pub work: StringCountPartialWork,
    pub deferred: Option<Arc<StringCountPartial>>,
}

pub(super) struct PartitionSelection {
    partition: usize,
    indices: Vec<usize>,
    _lease: MemoryLease,
}

pub(super) struct PartitionEvidence {
    pub groups: usize,
    pub rows: u64,
    pub lock_wait_nanos: u64,
    pub reconcile_nanos: u64,
    pub arrange_nanos: u64,
    pub selection_nanos: u64,
    pub equality_comparisons: u64,
    pub comparison_publish_calls: u64,
    pub entry_credit_claim_calls: u64,
    pub entry_credit_granted_entries: u64,
    pub entry_credit_return_calls: u64,
    pub entry_credit_refunded_entries: u64,
    pub entry_credit_wait_calls: u64,
    pub entry_credit_reserved_entries: usize,
    pub entry_credit_block_entries: usize,
}

pub(super) struct StringCountPartitions {
    partitions: Vec<Mutex<Partition>>,
    memory: LiveMemoryPool,
    retained_cap: usize,
    entry_credits: EntryCredits,
    pressure: AtomicBool,
    pub committed_rows: AtomicU64,
    pub lock_wait_nanos: AtomicU64,
    pub reconcile_nanos: AtomicU64,
    pub arrange_nanos: AtomicU64,
    pub selection_nanos: AtomicU64,
    pub equality_comparisons: AtomicU64,
    comparison_publish_calls: AtomicU64,
    _metadata: MemoryLease,
}

#[derive(Default)]
struct ReconcileProgress {
    cursor: usize,
    consumed: u64,
    comparisons: u64,
}

#[derive(Default)]
struct EntryAdmission<'a> {
    block: Option<EntryBlock<'a>>,
    exhausted: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum Update {
    Applied,
    NeedCredits,
    Pressure,
}

impl StringCountPartitions {
    /// Failure to admit this representation leaves the existing C4 route intact.
    pub(super) fn try_new(
        memory: &LiveMemoryPool,
        entry_limit: usize,
        retained_cap: usize,
    ) -> Result<Option<Arc<Self>>> {
        let selection_bytes = retained_cap
            .checked_mul(size_of::<usize>())
            .and_then(|bytes| bytes.checked_mul(PARTITIONS))
            .and_then(|bytes| u64::try_from(bytes).ok());
        let Some(selection_bytes) = selection_bytes else {
            return Ok(None);
        };
        let metadata_bytes = size_of::<Self>()
            .checked_add(PARTITIONS * size_of::<Mutex<Partition>>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("partition metadata size overflowed"))?;
        let Some(total) = metadata_bytes.checked_add(selection_bytes) else {
            return Ok(None);
        };
        let Ok(mut lease) = memory.reserve(total) else {
            return Ok(None);
        };
        let mut partitions = Vec::new();
        if partitions.try_reserve_exact(PARTITIONS).is_err() || partitions.capacity() > PARTITIONS {
            return Ok(None);
        }
        for _ in 0..PARTITIONS {
            partitions.push(Mutex::new(Partition {
                slots: Vec::new(),
                bytes: Vec::new(),
                groups: 0,
                slots_lease: memory.reserve(0)?,
                bytes_lease: memory.reserve(0)?,
                selection_lease: Some(lease.split(selection_bytes / PARTITIONS as u64)?),
            }));
        }
        Ok(Some(Arc::new(Self {
            partitions,
            memory: memory.clone(),
            retained_cap,
            entry_credits: EntryCredits::new(entry_limit),
            pressure: AtomicBool::new(false),
            committed_rows: AtomicU64::new(0),
            lock_wait_nanos: AtomicU64::new(0),
            reconcile_nanos: AtomicU64::new(0),
            arrange_nanos: AtomicU64::new(0),
            selection_nanos: AtomicU64::new(0),
            equality_comparisons: AtomicU64::new(0),
            comparison_publish_calls: AtomicU64::new(0),
            _metadata: lease,
        })))
    }

    pub(super) fn pressure_requested(&self) -> bool {
        self.pressure.load(Ordering::Acquire)
    }
    pub(super) fn request_pressure(&self) {
        self.pressure.store(true, Ordering::Release);
        self.entry_credits.wake();
    }
    /// Exact after reducers drain; a published lower bound while a block is active.
    /// This cumulative count survives replay and storage release.
    pub(super) fn group_count(&self) -> usize {
        self.entry_credits.committed()
    }

    pub(super) fn denied_reservations(&self) -> u64 {
        self.memory.snapshot().denied_reservations
    }

    pub(super) fn evidence(&self) -> Result<PartitionEvidence> {
        let credits = self.entry_credits.evidence()?;
        if credits.reserved != 0 {
            return Err(failed("entry credits remain outstanding at final evidence"));
        }
        Ok(PartitionEvidence {
            groups: credits.committed,
            rows: self.committed_rows.load(Ordering::Acquire),
            lock_wait_nanos: self.lock_wait_nanos.load(Ordering::Acquire),
            reconcile_nanos: self.reconcile_nanos.load(Ordering::Acquire),
            arrange_nanos: self.arrange_nanos.load(Ordering::Acquire),
            selection_nanos: self.selection_nanos.load(Ordering::Acquire),
            equality_comparisons: self.equality_comparisons.load(Ordering::Acquire),
            comparison_publish_calls: self.comparison_publish_calls.load(Ordering::Acquire),
            entry_credit_claim_calls: credits.claim_calls,
            entry_credit_granted_entries: credits.granted_entries,
            entry_credit_return_calls: credits.return_calls,
            entry_credit_refunded_entries: credits.refunded_entries,
            entry_credit_wait_calls: credits.wait_calls,
            entry_credit_reserved_entries: credits.reserved,
            entry_credit_block_entries: entry_credits::BLOCK_ENTRIES,
        })
    }

    pub(super) fn reduce(
        &self,
        mut partial: StringCountPartial,
        worker: &ChunkWorkerContext,
        task_lease: &mut MemoryLease,
    ) -> Result<PartitionReceipt> {
        let started = Instant::now();
        let ends = partial.arrange_partitions::<PARTITIONS>(worker)?;
        elapsed(&self.arrange_nanos, started)?;
        let work = partial.work.clone();
        let mut progress = ReconcileProgress::default();
        for (index, end) in ends.into_iter().enumerate() {
            if progress.cursor == end {
                continue;
            }
            if !self.reduce_partition(index, end, &partial, worker, &mut progress)? {
                break;
            }
        }
        add(
            &self.committed_rows,
            progress.consumed,
            "committed weight overflowed",
        )?;
        let remaining = work
            .rows
            .checked_sub(progress.consumed)
            .ok_or_else(|| failed("partial consumed excess weight"))?;
        let deferred = if remaining == 0 {
            if u64::try_from(progress.cursor).ok() != Some(work.partial_entries) {
                return Err(failed("partial cursor differs from exact weight"));
            }
            None
        } else {
            partial.retain_unconsumed(progress.cursor, remaining);
            partial.retain_deferred_metadata(task_lease)?;
            Some(Arc::new(partial))
        };
        Ok(PartitionReceipt { work, deferred })
    }

    fn reduce_partition(
        &self,
        index: usize,
        end: usize,
        partial: &StringCountPartial,
        worker: &ChunkWorkerContext,
        progress: &mut ReconcileProgress,
    ) -> Result<bool> {
        let mut admission = EntryAdmission::default();
        loop {
            worker.check_cancelled()?;
            if self.pressure_requested() {
                return Ok(false);
            }
            let started = Instant::now();
            let mut partition = self.partitions[index]
                .lock()
                .map_err(|_| failed("partition lock poisoned"))?;
            elapsed(&self.lock_wait_nanos, started)?;
            let started = Instant::now();
            let outcome = partition.reconcile(partial, end, progress, self, worker, &mut admission);
            drop(partition);
            // Publish even on a failed count/insertion. Preserve the primary
            // operation error if evidence accounting also fails.
            let accounting = self
                .publish_comparisons(progress)
                .and_then(|()| elapsed(&self.reconcile_nanos, started));
            let outcome = outcome?;
            accounting?;
            match outcome {
                Update::Applied => return Ok(true),
                Update::Pressure => {
                    self.request_pressure();
                    return Ok(false);
                }
                Update::NeedCredits => {
                    // Return an exhausted block before waiting, and never hold
                    // the partition mutex while another worker owns credits.
                    drop(admission.block.take());
                    match self.entry_credits.claim(end - progress.cursor, || {
                        worker.check_cancelled()?;
                        Ok(!self.pressure_requested())
                    })? {
                        Claim::Block(block) => admission.block = Some(block),
                        Claim::Stopped => return Ok(false),
                        // Relock and recheck the key: another worker may have
                        // inserted it while this worker waited for admission.
                        Claim::Exhausted => admission.exhausted = true,
                    }
                }
            }
        }
    }

    fn publish_comparisons(&self, progress: &mut ReconcileProgress) -> Result<()> {
        let count = std::mem::take(&mut progress.comparisons);
        if count != 0 {
            add(
                &self.equality_comparisons,
                count,
                "comparison count overflowed",
            )?;
            add(
                &self.comparison_publish_calls,
                1,
                "comparison publication count overflowed",
            )?;
        }
        Ok(())
    }

    /// Caller invokes only after every count job joined. Consume owned storage
    /// one partition at a time, releasing its leases after replay finishes.
    pub(super) fn replay_and_release(
        &self,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        for partition in &self.partitions {
            let mut partition = partition
                .lock()
                .map_err(|_| failed("partition lock poisoned"))?;
            for slot in &partition.slots {
                if slot.count != 0 {
                    let value =
                        std::str::from_utf8(&partition.bytes[slot.offset..slot.offset + slot.len])
                            .map_err(|error| failed(&format!("stored UTF-8 invalid: {error}")))?;
                    visit(value, slot.count)?;
                }
            }
            partition.slots = Vec::new();
            partition.bytes = Vec::new();
            partition.groups = 0;
            partition.slots_lease.resize(0)?;
            partition.bytes_lease.resize(0)?;
            partition.selection_lease = None;
        }
        Ok(())
    }

    pub(super) fn release_storage(&self) -> Result<()> {
        for partition in &self.partitions {
            let mut partition = partition
                .lock()
                .map_err(|_| failed("partition lock poisoned"))?;
            partition.slots = Vec::new();
            partition.bytes = Vec::new();
            partition.groups = 0;
            partition.slots_lease.resize(0)?;
            partition.bytes_lease.resize(0)?;
            partition.selection_lease = None;
        }
        Ok(())
    }

    /// A bounded heap of FINAL complete keys. No candidate is discarded during
    /// counting, and each key belongs to precisely one such final selection.
    pub(super) fn select(
        &self,
        index: usize,
        worker: &ChunkWorkerContext,
    ) -> Result<PartitionSelection> {
        worker.check_cancelled()?;
        let started = Instant::now();
        let mut partition = self.partitions[index]
            .lock()
            .map_err(|_| failed("partition lock poisoned"))?;
        let lease = partition
            .selection_lease
            .take()
            .ok_or_else(|| failed("partition selected twice"))?;
        let cap = self.retained_cap.min(partition.groups);
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(cap)
            .map_err(|error| failed(&format!("selection allocation failed: {error}")))?;
        if indices
            .capacity()
            .checked_mul(size_of::<usize>())
            .is_none_or(|bytes| bytes as u64 > lease.bytes())
        {
            return Err(failed("selection capacity exceeded its reservation"));
        }
        for (index, slot) in partition.slots.iter().enumerate() {
            if index % 4096 == 0 {
                worker.check_cancelled()?;
            }
            if slot.count == 0 || cap == 0 {
                continue;
            }
            if indices.len() < cap {
                indices.push(index);
                let mut child = indices.len() - 1;
                while child > 0 {
                    let parent = (child - 1) / 2;
                    if !partition.worse(indices[child], indices[parent]) {
                        break;
                    }
                    indices.swap(child, parent);
                    child = parent;
                }
            } else if partition.worse(indices[0], index) {
                indices[0] = index;
                let mut parent = 0;
                loop {
                    let left = parent * 2 + 1;
                    if left >= indices.len() {
                        break;
                    }
                    let right = left + 1;
                    let child = if right < indices.len()
                        && partition.worse(indices[right], indices[left])
                    {
                        right
                    } else {
                        left
                    };
                    if !partition.worse(indices[child], indices[parent]) {
                        break;
                    }
                    indices.swap(parent, child);
                    parent = child;
                }
            }
        }
        elapsed(&self.selection_nanos, started)?;
        Ok(PartitionSelection {
            partition: index,
            indices,
            _lease: lease,
        })
    }

    pub(super) fn visit_selection(
        &self,
        selection: &PartitionSelection,
        mut visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        let partition = self.partitions[selection.partition]
            .lock()
            .map_err(|_| failed("partition lock poisoned"))?;
        for &index in &selection.indices {
            let slot = partition.slots[index];
            let value = std::str::from_utf8(&partition.bytes[slot.offset..slot.offset + slot.len])
                .map_err(|error| failed(&format!("selected UTF-8 invalid: {error}")))?;
            visit(value, slot.count)?;
        }
        Ok(())
    }
}

impl Partition {
    fn reconcile(
        &mut self,
        partial: &StringCountPartial,
        end: usize,
        progress: &mut ReconcileProgress,
        shared: &StringCountPartitions,
        worker: &ChunkWorkerContext,
        admission: &mut EntryAdmission<'_>,
    ) -> Result<Update> {
        while progress.cursor < end {
            if progress.cursor.is_multiple_of(4096) {
                worker.check_cancelled()?;
                if shared.pressure_requested() {
                    return Ok(Update::Pressure);
                }
            }
            let (bytes, hash, count) = partial.entry(progress.cursor)?;
            if count == 0 {
                return Err(failed("zero-weight partial entry"));
            }
            let next = progress
                .consumed
                .checked_add(count)
                .ok_or_else(|| failed("chunk weight overflowed"))?;
            let outcome = self.update(
                (bytes.as_slice(), hash, count),
                &shared.memory,
                worker,
                admission,
                &mut progress.comparisons,
            )?;
            if outcome != Update::Applied {
                return Ok(outcome);
            }
            progress.consumed = next;
            progress.cursor += 1;
        }
        Ok(Update::Applied)
    }

    fn worse(&self, left: usize, right: usize) -> bool {
        let left = self.slots[left];
        let right = self.slots[right];
        left.count < right.count
            || (left.count == right.count
                && self.bytes[left.offset..left.offset + left.len]
                    > self.bytes[right.offset..right.offset + right.len])
    }

    fn update(
        &mut self,
        (value, hash, count): (&[u8], u64, u64),
        memory: &LiveMemoryPool,
        worker: &ChunkWorkerContext,
        admission: &mut EntryAdmission<'_>,
        comparisons: &mut u64,
    ) -> Result<Update> {
        if !self.slots.is_empty() {
            let index = self.find(value, hash, comparisons)?;
            if self.slots[index].count != 0 {
                self.slots[index].count = self.slots[index]
                    .count
                    .checked_add(count)
                    .ok_or_else(|| failed("complete-key count overflowed u64"))?;
                return Ok(Update::Applied);
            }
        }
        let Some(block) = admission
            .block
            .as_mut()
            .filter(|block| block.remaining() != 0)
        else {
            return Ok(if admission.exhausted {
                Update::Pressure
            } else {
                Update::NeedCredits
            });
        };
        if self.insert(value, hash, count, memory, worker)? {
            block.consume_one()?;
            Ok(Update::Applied)
        } else {
            Ok(Update::Pressure)
        }
    }

    fn insert(
        &mut self,
        value: &[u8],
        hash: u64,
        count: u64,
        memory: &LiveMemoryPool,
        worker: &ChunkWorkerContext,
    ) -> Result<bool> {
        let next_groups = self
            .groups
            .checked_add(1)
            .ok_or_else(|| failed("group count overflowed"))?;
        if self.slots.is_empty() || next_groups > self.slots.len() / 2 {
            let capacity = self
                .slots
                .len()
                .max(8)
                .checked_mul(2)
                .ok_or_else(|| failed("partition table size overflowed"))?;
            let Some((mut slots, lease)) = allocate::<Slot>(capacity, memory)? else {
                return Ok(false);
            };
            slots.resize(capacity, Slot::default());
            for (index, slot) in self.slots.iter().copied().enumerate() {
                if index % 4096 == 0 {
                    worker.check_cancelled()?;
                }
                if slot.count == 0 {
                    continue;
                }
                let mut bucket = hash_bucket(slot.hash, capacity)?;
                while slots[bucket].count != 0 {
                    bucket = (bucket + 1) & (capacity - 1);
                }
                slots[bucket] = slot;
            }
            self.slots = slots;
            self.slots_lease = lease;
        }
        let needed = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or_else(|| failed("partition UTF-8 size overflowed"))?;
        if needed > self.bytes.capacity() {
            let capacity = needed.max(
                self.bytes
                    .capacity()
                    .max(64)
                    .checked_mul(2)
                    .ok_or_else(|| failed("partition byte capacity overflowed"))?,
            );
            let Some((mut bytes, lease)) = allocate::<u8>(capacity, memory)? else {
                return Ok(false);
            };
            bytes.extend_from_slice(&self.bytes);
            self.bytes = bytes;
            self.bytes_lease = lease;
        }
        let mut bucket = hash_bucket(hash, self.slots.len())?;
        while self.slots[bucket].count != 0 {
            bucket = (bucket + 1) & (self.slots.len() - 1);
        }
        self.slots[bucket] = Slot {
            hash,
            offset: self.bytes.len(),
            len: value.len(),
            count,
        };
        self.bytes.extend_from_slice(value);
        self.groups = next_groups;
        Ok(true)
    }

    fn find(&self, value: &[u8], hash: u64, comparisons: &mut u64) -> Result<usize> {
        let mut bucket = hash_bucket(hash, self.slots.len())?;
        loop {
            let slot = self.slots[bucket];
            if slot.count == 0 {
                return Ok(bucket);
            }
            if slot.hash == hash {
                *comparisons = comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("comparison count overflowed"))?;
                if self.bytes[slot.offset..slot.offset + slot.len] == *value {
                    return Ok(bucket);
                }
            }
            bucket = (bucket + 1) & (self.slots.len() - 1);
        }
    }
}

fn hash_bucket(hash: u64, capacity: usize) -> Result<usize> {
    let mask = u64::try_from(capacity - 1).map_err(|_| failed("table mask exceeds u64"))?;
    usize::try_from(hash & mask).map_err(|_| failed("masked table hash exceeds usize"))
}

fn allocate<T>(capacity: usize, memory: &LiveMemoryPool) -> Result<Option<(Vec<T>, MemoryLease)>> {
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("owned partition capacity overflowed"))?;
    let Ok(lease) = memory.reserve(bytes) else {
        return Ok(None);
    };
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() || values.capacity() > capacity {
        return Ok(None);
    }
    Ok(Some((values, lease)))
}

fn elapsed(counter: &AtomicU64, started: Instant) -> Result<()> {
    let nanos =
        u64::try_from(started.elapsed().as_nanos()).map_err(|_| failed("work time overflowed"))?;
    add(counter, nanos, "work time overflowed")
}

fn add(counter: &AtomicU64, value: u64, message: &str) -> Result<()> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(value)
        })
        .map(|_| ())
        .map_err(|_| failed(message))
}

pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex complete-key string count {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "string_count_partitions_tests.rs"]
mod tests;
