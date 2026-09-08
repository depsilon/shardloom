//! All contributions to a complete integer/UTF8 key share one partition.
//! String bytes are interned once per partition, independently of numeric keys.
//! Entry credits and retained-byte leases are separate hard admission limits.

use super::{
    aggregate_chunk_jobs::ChunkWorkerContext,
    compound_count_partial::{self, CompoundPartial, Key, Work, failed},
    string_count_entry_credits::{Claim, EntryBlock, EntryCredits},
};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

pub(super) const PARTITIONS: usize = 64;
#[derive(Clone, Copy, Default)]
struct TextSlot {
    hash: u64,
    offset: usize,
    len: usize,
    occupied: bool,
}
#[derive(Clone, Copy, Default)]
struct Group {
    hash: u64,
    key: Key,
    offset: usize,
    len: usize,
    count: u64,
}
struct Partition {
    groups: Vec<Group>,
    text: Vec<TextSlot>,
    bytes: Vec<u8>,
    group_len: usize,
    text_len: usize,
    groups_lease: MemoryLease,
    text_lease: MemoryLease,
    bytes_lease: MemoryLease,
    selection_lease: Option<MemoryLease>,
}
pub(super) struct Receipt {
    pub work: Work,
    pub deferred: Option<Arc<CompoundPartial>>,
}
pub(super) struct Selection {
    index: usize,
    groups: Vec<usize>,
    _lease: MemoryLease,
}
#[derive(Default)]
pub(super) struct Evidence {
    pub groups: usize,
    pub rows: u64,
    pub strings: u64,
    pub string_bytes_copied: u64,
    pub lock_wait_nanos: u64,
    pub reconcile_nanos: u64,
    pub selection_nanos: u64,
    pub comparisons: u64,
    pub credit_claims: u64,
    pub credit_returns: u64,
}
pub(super) struct CompoundPartitions {
    partitions: Vec<Mutex<Partition>>,
    memory: LiveMemoryPool,
    credits: EntryCredits,
    pressure: AtomicBool,
    numeric_first: bool,
    retained: usize,
    pub rows: AtomicU64,
    strings: AtomicU64,
    string_bytes_copied: AtomicU64,
    lock_wait: AtomicU64,
    reconcile: AtomicU64,
    selection: AtomicU64,
    comparisons: AtomicU64,
    _metadata: MemoryLease,
}

impl CompoundPartitions {
    pub(super) fn try_new(
        memory: &LiveMemoryPool,
        entries: usize,
        retained: usize,
        numeric_first: bool,
    ) -> Result<Option<Arc<Self>>> {
        let Some(selection) = retained
            .checked_mul(size_of::<usize>())
            .and_then(|v| v.checked_mul(PARTITIONS))
            .and_then(|v| u64::try_from(v).ok())
        else {
            return Ok(None);
        };
        let metadata = (size_of::<Self>()
            + 2 * size_of::<usize>()
            + PARTITIONS * size_of::<Mutex<Partition>>()) as u64;
        let Some(total) = metadata.checked_add(selection) else {
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
                groups: Vec::new(),
                text: Vec::new(),
                bytes: Vec::new(),
                group_len: 0,
                text_len: 0,
                groups_lease: memory.reserve(0)?,
                text_lease: memory.reserve(0)?,
                bytes_lease: memory.reserve(0)?,
                selection_lease: Some(lease.split(selection / PARTITIONS as u64)?),
            }));
        }
        Ok(Some(Arc::new(Self {
            partitions,
            memory: memory.clone(),
            credits: EntryCredits::new(entries),
            pressure: AtomicBool::new(false),
            numeric_first,
            retained,
            rows: AtomicU64::new(0),
            strings: AtomicU64::new(0),
            string_bytes_copied: AtomicU64::new(0),
            lock_wait: AtomicU64::new(0),
            reconcile: AtomicU64::new(0),
            selection: AtomicU64::new(0),
            comparisons: AtomicU64::new(0),
            _metadata: lease,
        })))
    }
    pub(super) fn pressured(&self) -> bool {
        self.pressure.load(Ordering::Acquire)
    }
    pub(super) fn request_pressure(&self) {
        self.pressure.store(true, Ordering::Release);
        self.credits.wake();
    }
    pub(super) fn groups(&self) -> usize {
        self.credits.committed()
    }
    pub(super) fn evidence(&self) -> Result<Evidence> {
        let credits = self.credits.evidence()?;
        if credits.reserved != 0 {
            return Err(failed("compound entry credits remain after drain"));
        }
        Ok(Evidence {
            groups: credits.committed,
            rows: self.rows.load(Ordering::Acquire),
            strings: self.strings.load(Ordering::Acquire),
            string_bytes_copied: self.string_bytes_copied.load(Ordering::Acquire),
            lock_wait_nanos: self.lock_wait.load(Ordering::Acquire),
            reconcile_nanos: self.reconcile.load(Ordering::Acquire),
            selection_nanos: self.selection.load(Ordering::Acquire),
            comparisons: self.comparisons.load(Ordering::Acquire),
            credit_claims: credits.claim_calls,
            credit_returns: credits.return_calls,
        })
    }
    pub(super) fn reduce(
        &self,
        mut partial: CompoundPartial,
        worker: &ChunkWorkerContext,
    ) -> Result<Receipt> {
        let ends = partial.arrange::<PARTITIONS>(worker)?;
        let work = partial.work.clone();
        let mut cursor = 0;
        let mut consumed = 0_u64;
        for (index, end) in ends.into_iter().enumerate() {
            if cursor == end {
                continue;
            }
            self.reduce_partition(index, end, &partial, worker, &mut cursor, &mut consumed)?;
            if cursor != end {
                break;
            }
        }
        add(&self.rows, consumed)?;
        let remaining = work
            .rows
            .checked_sub(consumed)
            .ok_or_else(|| failed("partition consumed excess weight"))?;
        let deferred = if remaining == 0 {
            None
        } else {
            partial.retain_suffix(cursor, remaining);
            Some(Arc::new(partial))
        };
        Ok(Receipt { work, deferred })
    }
    fn reduce_partition(
        &self,
        index: usize,
        end: usize,
        partial: &CompoundPartial,
        worker: &ChunkWorkerContext,
        cursor: &mut usize,
        consumed: &mut u64,
    ) -> Result<()> {
        let mut credit: Option<EntryBlock<'_>> = None;
        let mut exhausted = false;
        loop {
            worker.check_cancelled()?;
            if self.pressured() {
                return Ok(());
            }
            let started = Instant::now();
            let mut partition = self.partitions[index]
                .lock()
                .map_err(|_| failed("partition mutex poisoned"))?;
            elapsed(&self.lock_wait, started)?;
            let started = Instant::now();
            let mut comparisons = 0_u64;
            let outcome = (|| -> Result<bool> {
                while *cursor < end {
                    if cursor.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                        if self.pressured() {
                            return Ok(false);
                        }
                    }
                    let (key, bytes, hash, count) = partial.entry(*cursor);
                    let existing = partition.find(key, bytes.as_slice(), hash, &mut comparisons)?;
                    if let Some(bucket) = existing {
                        partition.groups[bucket].count = partition.groups[bucket]
                            .count
                            .checked_add(count)
                            .ok_or_else(|| failed("complete-key weight overflowed"))?;
                    } else {
                        let Some(block) = credit.as_mut().filter(|block| block.remaining() > 0)
                        else {
                            return Ok(true);
                        };
                        if !partition.insert(key, bytes.as_slice(), hash, count, self, worker)? {
                            self.request_pressure();
                            return Ok(false);
                        }
                        block.consume_one()?;
                    }
                    *consumed = consumed
                        .checked_add(count)
                        .ok_or_else(|| failed("consumed weight overflowed"))?;
                    *cursor += 1;
                }
                Ok(false)
            })();
            drop(partition);
            let accounting = add(&self.comparisons, comparisons)
                .and_then(|()| elapsed(&self.reconcile, started));
            let needs_credit = outcome?;
            accounting?;
            if !needs_credit {
                return Ok(());
            }
            drop(credit.take());
            if exhausted {
                self.request_pressure();
                return Ok(());
            }
            match self.credits.claim(end - *cursor, || {
                worker.check_cancelled()?;
                Ok(!self.pressured())
            })? {
                Claim::Block(block) => credit = Some(block),
                Claim::Exhausted => exhausted = true,
                Claim::Stopped => return Ok(()),
            }
            // Recheck complete key after dropping the mutex to claim credits.
        }
    }
    /// Caller drains every job before using either replay or final selection.
    pub(super) fn replay_and_release(
        &self,
        mut visit: impl FnMut(Key, &str, u64) -> Result<()>,
    ) -> Result<()> {
        for partition in &self.partitions {
            let mut partition = partition
                .lock()
                .map_err(|_| failed("partition mutex poisoned"))?;
            for group in &partition.groups {
                if group.count != 0 {
                    visit(group.key, partition.value(*group)?, group.count)?;
                }
            }
            partition.release()?;
        }
        Ok(())
    }
    pub(super) fn release(&self) -> Result<()> {
        for partition in &self.partitions {
            partition
                .lock()
                .map_err(|_| failed("partition mutex poisoned"))?
                .release()?;
        }
        Ok(())
    }
    pub(super) fn select(&self, index: usize, worker: &ChunkWorkerContext) -> Result<Selection> {
        let started = Instant::now();
        let mut partition = self.partitions[index]
            .lock()
            .map_err(|_| failed("partition mutex poisoned"))?;
        let lease = partition
            .selection_lease
            .take()
            .ok_or_else(|| failed("partition selected twice"))?;
        let cap = self.retained.min(partition.group_len);
        let mut groups = Vec::new();
        groups
            .try_reserve_exact(cap)
            .map_err(|error| failed(&error.to_string()))?;
        if groups.capacity() > cap {
            return Err(failed("selection allocation exceeded reservation"));
        }
        for (index, group) in partition.groups.iter().enumerate() {
            if index % 4096 == 0 {
                worker.check_cancelled()?;
            }
            if group.count == 0 || cap == 0 {
                continue;
            }
            if groups.len() < cap {
                groups.push(index);
                let mut child = groups.len() - 1;
                while child > 0 {
                    let parent = (child - 1) / 2;
                    if !partition.worse(groups[child], groups[parent], self.numeric_first) {
                        break;
                    }
                    groups.swap(child, parent);
                    child = parent;
                }
            } else if partition.worse(groups[0], index, self.numeric_first) {
                groups[0] = index;
                let mut parent = 0;
                loop {
                    let left = parent * 2 + 1;
                    if left >= groups.len() {
                        break;
                    }
                    let right = left + 1;
                    let child = if right < groups.len()
                        && partition.worse(groups[right], groups[left], self.numeric_first)
                    {
                        right
                    } else {
                        left
                    };
                    if !partition.worse(groups[child], groups[parent], self.numeric_first) {
                        break;
                    }
                    groups.swap(child, parent);
                    parent = child;
                }
            }
        }
        elapsed(&self.selection, started)?;
        Ok(Selection {
            index,
            groups,
            _lease: lease,
        })
    }
    pub(super) fn visit_selected(
        &self,
        selected: &Selection,
        mut visit: impl FnMut(Key, &str, u64) -> Result<()>,
    ) -> Result<()> {
        let partition = self.partitions[selected.index]
            .lock()
            .map_err(|_| failed("partition mutex poisoned"))?;
        for &index in &selected.groups {
            let group = partition.groups[index];
            visit(group.key, partition.value(group)?, group.count)?;
        }
        Ok(())
    }
}

impl Partition {
    fn value(&self, group: Group) -> Result<&str> {
        std::str::from_utf8(&self.bytes[group.offset..group.offset + group.len])
            .map_err(|error| failed(&error.to_string()))
    }
    fn worse(&self, left: usize, right: usize, numeric_first: bool) -> bool {
        let left = self.groups[left];
        let right = self.groups[right];
        let numeric = left.key.cmp(right.key);
        let text = self.bytes[left.offset..left.offset + left.len]
            .cmp(&self.bytes[right.offset..right.offset + right.len]);
        left.count < right.count
            || left.count == right.count
                && (if numeric_first {
                    numeric.then(text)
                } else {
                    text.then(numeric)
                })
                .is_gt()
    }
    fn find(
        &self,
        key: Key,
        bytes: &[u8],
        hash: u64,
        comparisons: &mut u64,
    ) -> Result<Option<usize>> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        let mut bucket = compound_count_partial::bucket(hash, self.groups.len());
        loop {
            let group = self.groups[bucket];
            if group.count == 0 {
                return Ok(None);
            }
            if group.hash == hash && group.key.bits == key.bits && group.key.signed == key.signed {
                *comparisons = comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("comparison count overflowed"))?;
                if self.bytes[group.offset..group.offset + group.len] == *bytes {
                    return Ok(Some(bucket));
                }
            }
            bucket = (bucket + 1) & (self.groups.len() - 1);
        }
    }
    fn insert(
        &mut self,
        key: Key,
        bytes: &[u8],
        hash: u64,
        count: u64,
        shared: &CompoundPartitions,
        worker: &ChunkWorkerContext,
    ) -> Result<bool> {
        if self.groups.is_empty() || self.group_len + 1 > self.groups.len() / 2 {
            let cap = self
                .groups
                .len()
                .max(8)
                .checked_mul(2)
                .ok_or_else(|| failed("group capacity overflowed"))?;
            let Some((mut groups, lease)) = allocate::<Group>(cap, &shared.memory)? else {
                return Ok(false);
            };
            groups.resize(cap, Group::default());
            for (index, group) in self.groups.iter().copied().enumerate() {
                if index % 4096 == 0 {
                    worker.check_cancelled()?;
                }
                if group.count == 0 {
                    continue;
                }
                let mut bucket = compound_count_partial::bucket(group.hash, cap);
                while groups[bucket].count != 0 {
                    bucket = (bucket + 1) & (cap - 1);
                }
                groups[bucket] = group;
            }
            self.groups = groups;
            self.groups_lease = lease;
        }
        let Some((offset, len)) = self.intern(bytes, shared, worker)? else {
            return Ok(false);
        };
        let mut bucket = compound_count_partial::bucket(hash, self.groups.len());
        while self.groups[bucket].count != 0 {
            bucket = (bucket + 1) & (self.groups.len() - 1);
        }
        self.groups[bucket] = Group {
            hash,
            key,
            offset,
            len,
            count,
        };
        self.group_len += 1;
        Ok(true)
    }
    fn intern(
        &mut self,
        bytes: &[u8],
        shared: &CompoundPartitions,
        worker: &ChunkWorkerContext,
    ) -> Result<Option<(usize, usize)>> {
        let hash = compound_count_partial::string_hash(bytes);
        if !self.text.is_empty() {
            let mut bucket = compound_count_partial::bucket(hash, self.text.len());
            while self.text[bucket].occupied {
                let value = self.text[bucket];
                if value.hash == hash
                    && self.bytes[value.offset..value.offset + value.len] == *bytes
                {
                    return Ok(Some((value.offset, value.len)));
                }
                bucket = (bucket + 1) & (self.text.len() - 1);
            }
        }
        if self.text.is_empty() || self.text_len + 1 > self.text.len() / 2 {
            let cap = self
                .text
                .len()
                .max(8)
                .checked_mul(2)
                .ok_or_else(|| failed("domain capacity overflowed"))?;
            let Some((mut text, lease)) = allocate::<TextSlot>(cap, &shared.memory)? else {
                return Ok(None);
            };
            text.resize(cap, TextSlot::default());
            for (index, value) in self.text.iter().copied().enumerate() {
                if index % 4096 == 0 {
                    worker.check_cancelled()?;
                }
                if !value.occupied {
                    continue;
                }
                let mut bucket = compound_count_partial::bucket(value.hash, cap);
                while text[bucket].occupied {
                    bucket = (bucket + 1) & (cap - 1);
                }
                text[bucket] = value;
            }
            self.text = text;
            self.text_lease = lease;
        }
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| failed("domain bytes overflowed"))?;
        if needed > self.bytes.capacity() {
            let capacity = needed.max(
                self.bytes
                    .capacity()
                    .max(64)
                    .checked_mul(2)
                    .ok_or_else(|| failed("domain growth overflowed"))?,
            );
            let Some((mut values, lease)) = allocate::<u8>(capacity, &shared.memory)? else {
                return Ok(None);
            };
            values.extend_from_slice(&self.bytes);
            add(&shared.string_bytes_copied, self.bytes.len() as u64)?;
            self.bytes = values;
            self.bytes_lease = lease;
        }
        let offset = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        add(&shared.string_bytes_copied, bytes.len() as u64)?;
        add(&shared.strings, 1)?;
        let mut bucket = compound_count_partial::bucket(hash, self.text.len());
        while self.text[bucket].occupied {
            bucket = (bucket + 1) & (self.text.len() - 1);
        }
        self.text[bucket] = TextSlot {
            hash,
            offset,
            len: bytes.len(),
            occupied: true,
        };
        self.text_len += 1;
        Ok(Some((offset, bytes.len())))
    }
    fn release(&mut self) -> Result<()> {
        self.groups = Vec::new();
        self.text = Vec::new();
        self.bytes = Vec::new();
        self.group_len = 0;
        self.text_len = 0;
        self.groups_lease.resize(0)?;
        self.text_lease.resize(0)?;
        self.bytes_lease.resize(0)?;
        self.selection_lease = None;
        Ok(())
    }
}
fn allocate<T>(capacity: usize, memory: &LiveMemoryPool) -> Result<Option<(Vec<T>, MemoryLease)>> {
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("partition capacity overflowed"))?;
    let Ok(lease) = memory.reserve(bytes) else {
        return Ok(None);
    };
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() || values.capacity() > capacity {
        return Ok(None);
    }
    Ok(Some((values, lease)))
}
fn add(counter: &AtomicU64, value: u64) -> Result<()> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(value)
        })
        .map(|_| ())
        .map_err(|_| failed("partition work counter overflowed"))
}
fn elapsed(counter: &AtomicU64, started: Instant) -> Result<()> {
    add(
        counter,
        u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| failed("partition clock overflowed"))?,
    )
}
