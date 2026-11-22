//! Exact scalar UTF8 union. Each content hash has one partition; cardinality
//! is read only after workers drain. Persistent bytes are copied only on misses.

use super::{aggregate_chunk_jobs::ChunkWorkerContext, aggregate_dense_pages::DensePages};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::sync::Mutex;

pub(super) const PARTITIONS: usize = 64;

#[derive(Clone, Copy)]
struct Record {
    hash: u64,
    offset: usize,
    len: usize,
}

struct Partition {
    directory: Vec<usize>,
    records: DensePages<Record>,
    bytes: Vec<u8>,
    directory_lease: MemoryLease,
    bytes_lease: MemoryLease,
}

pub(super) struct Partitions {
    parts: Vec<Mutex<Partition>>,
    memory: LiveMemoryPool,
    _metadata: MemoryLease,
}

impl Partitions {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        let (mut parts, mut metadata) = allocate(PARTITIONS, memory)?;
        // The production owner is an Arc, in addition to the directory of
        // mutexes. Charge that fixed allocation explicitly as well.
        metadata.resize(
            metadata
                .bytes()
                .checked_add((size_of::<Self>() + 2 * size_of::<usize>()) as u64)
                .ok_or_else(|| failed("owner metadata overflow"))?,
        )?;
        for _ in 0..PARTITIONS {
            parts.push(Mutex::new(Partition {
                directory: Vec::new(),
                records: DensePages::new(memory)?,
                bytes: Vec::new(),
                directory_lease: memory.reserve(0)?,
                bytes_lease: memory.reserve(0)?,
            }));
        }
        Ok(Self {
            parts,
            memory: memory.clone(),
            _metadata: metadata,
        })
    }

    pub(super) fn union<'a>(
        &self,
        entries: &mut [(u64, usize)],
        value: impl Fn(usize) -> &'a [u8],
        worker: &ChunkWorkerContext,
    ) -> Result<u64> {
        // Count/radix placement is allocation-free and preserves every key.
        let mut sizes = [0_usize; PARTITIONS];
        for (hash, _) in entries.iter() {
            sizes[partition(*hash)] += 1;
        }
        let mut starts = [0_usize; PARTITIONS];
        let mut ends = [0_usize; PARTITIONS];
        let mut end = 0;
        for ((start, stop), size) in starts.iter_mut().zip(&mut ends).zip(sizes) {
            *start = end;
            end += size;
            *stop = end;
        }
        let mut next = starts;
        for (part, end) in ends.iter().copied().enumerate() {
            while next[part] < end {
                if next[part].is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                let target = partition(entries[next[part]].0);
                if part == target {
                    next[part] += 1;
                } else {
                    entries.swap(next[part], next[target]);
                    next[target] += 1;
                }
            }
        }
        let mut copied = 0_u64;
        for (part, (&start, &end)) in starts.iter().zip(&ends).enumerate() {
            if start == end {
                continue;
            }
            worker.check_cancelled()?;
            let mut owner = self.parts[part]
                .lock()
                .map_err(|_| failed("partition poisoned"))?;
            for (index, &(hash, row)) in entries[start..end].iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                let bytes = value(row);
                if owner.insert(bytes, hash, &self.memory, worker)? {
                    copied = copied
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| failed("copied byte count overflow"))?;
                }
            }
        }
        Ok(copied)
    }

    pub(super) fn cardinality(&self) -> Result<u64> {
        self.parts.iter().try_fold(0_u64, |total, part| {
            let part = part.lock().map_err(|_| failed("partition poisoned"))?;
            total
                .checked_add(part.records.len() as u64)
                .ok_or_else(|| failed("cardinality overflow"))
        })
    }
}

struct NumericPartition {
    slots: Vec<u64>,
    has_zero: bool,
    non_zero_len: usize,
    lease: MemoryLease,
}

pub(super) struct NumericDistinctPartitions {
    parts: Vec<Mutex<NumericPartition>>,
    memory: LiveMemoryPool,
    _metadata: MemoryLease,
}

impl NumericDistinctPartitions {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        let (mut parts, mut metadata) = allocate(PARTITIONS, memory)?;
        metadata.resize(
            metadata
                .bytes()
                .checked_add((size_of::<Self>() + 2 * size_of::<usize>()) as u64)
                .ok_or_else(|| failed("owner metadata overflow"))?,
        )?;
        for _ in 0..PARTITIONS {
            parts.push(Mutex::new(NumericPartition {
                slots: Vec::new(),
                has_zero: false,
                non_zero_len: 0,
                lease: memory.reserve(0)?,
            }));
        }
        Ok(Self {
            parts,
            memory: memory.clone(),
            _metadata: metadata,
        })
    }

    pub(super) fn insert_in_place(
        &self,
        keys: &mut [u64],
        worker: &ChunkWorkerContext,
        rotate_seed: usize,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut sizes = [0_usize; PARTITIONS];
        for (index, &key) in keys.iter().enumerate() {
            if index.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            sizes[partition(hash_u64(key))] += 1;
        }
        let mut starts = [0_usize; PARTITIONS];
        let mut ends = [0_usize; PARTITIONS];
        let mut end = 0_usize;
        for ((start, stop), size) in starts.iter_mut().zip(&mut ends).zip(sizes) {
            *start = end;
            end += size;
            *stop = end;
        }
        let mut next = starts;
        let mut swaps = 0_usize;
        for (part, &part_end) in ends.iter().enumerate() {
            while next[part] < part_end {
                if swaps.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                let target = partition(hash_u64(keys[next[part]]));
                if target == part {
                    next[part] += 1;
                } else {
                    keys.swap(next[part], next[target]);
                    next[target] += 1;
                    swaps += 1;
                }
            }
        }
        let mut done = [false; PARTITIONS];
        for (part, (&start, &stop)) in starts.iter().zip(&ends).enumerate() {
            done[part] = start == stop;
        }
        let start_part = rotate_seed % PARTITIONS;
        // Pass 1: rotated try_lock avoids worker lockstep convoying.
        for offset in 0..PARTITIONS {
            let part = (start_part + offset) % PARTITIONS;
            if done[part] {
                continue;
            }
            worker.check_cancelled()?;
            match self.parts[part].try_lock() {
                Ok(mut owner) => {
                    owner.insert_all(&keys[starts[part]..ends[part]], &self.memory, worker)?;
                    done[part] = true;
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(failed("partition poisoned"));
                }
            }
        }
        // Pass 2: finish remaining non-empty partitions with blocking locks.
        for offset in 0..PARTITIONS {
            let part = (start_part + offset) % PARTITIONS;
            if done[part] {
                continue;
            }
            worker.check_cancelled()?;
            let mut owner = self.parts[part]
                .lock()
                .map_err(|_| failed("partition poisoned"))?;
            owner.insert_all(&keys[starts[part]..ends[part]], &self.memory, worker)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn insert_slice(
        &self,
        keys: &[u64],
        cancellation: &shardloom_exec::compute_pool::CancellationToken,
    ) -> Result<()> {
        let mut scratch = keys.to_vec();
        let worker = ChunkWorkerContext::Inline(cancellation.clone());
        self.insert_in_place(&mut scratch, &worker, 0)
    }

    pub(super) fn cardinality(&self) -> Result<u64> {
        self.parts.iter().try_fold(0_u64, |total, part| {
            let part = part.lock().map_err(|_| failed("partition poisoned"))?;
            let count = u64::from(part.has_zero)
                .checked_add(part.non_zero_len as u64)
                .ok_or_else(|| failed("cardinality overflow"))?;
            total
                .checked_add(count)
                .ok_or_else(|| failed("cardinality overflow"))
        })
    }
}

impl NumericPartition {
    fn insert_all(
        &mut self,
        keys: &[u64],
        memory: &LiveMemoryPool,
        worker: &ChunkWorkerContext,
    ) -> Result<()> {
        for (index, &key) in keys.iter().enumerate() {
            if index.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            self.insert(key, memory, worker)?;
        }
        Ok(())
    }

    fn insert(
        &mut self,
        key: u64,
        memory: &LiveMemoryPool,
        worker: &ChunkWorkerContext,
    ) -> Result<()> {
        if key == 0 {
            self.has_zero = true;
            return Ok(());
        }
        let hash = hash_u64(key);
        let mut vacant = 0_usize;
        if !self.slots.is_empty() {
            let mask = self.slots.len() - 1;
            let mut slot = bucket(hash, self.slots.len());
            let mut probes = 0_usize;
            loop {
                if probes.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                let existing = self.slots[slot];
                if existing == key {
                    return Ok(());
                }
                if existing == 0 {
                    vacant = slot;
                    break;
                }
                slot = (slot + 1) & mask;
                probes += 1;
            }
        }
        let next = self
            .non_zero_len
            .checked_add(1)
            .ok_or_else(|| failed("ordinal overflow"))?;
        if self.slots.is_empty() || next > self.slots.len() / 2 {
            let capacity = self
                .slots
                .len()
                .max(8)
                .checked_mul(2)
                .ok_or_else(|| failed("directory overflow"))?;
            let mask = capacity - 1;
            let (mut replacement, lease) = allocate::<u64>(capacity, memory)?;
            replacement.resize(capacity, 0);
            let mut probes = 0_usize;
            for (index, &existing) in self.slots.iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if existing == 0 {
                    continue;
                }
                let mut slot = bucket(hash_u64(existing), capacity);
                while replacement[slot] != 0 {
                    if probes.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                    }
                    slot = (slot + 1) & mask;
                    probes += 1;
                }
                replacement[slot] = existing;
            }
            let mut slot = bucket(hash, capacity);
            while replacement[slot] != 0 {
                if probes.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                slot = (slot + 1) & mask;
                probes += 1;
            }
            replacement[slot] = key;
            self.slots = replacement;
            self.lease = lease;
            self.non_zero_len = next;
            return Ok(());
        }
        self.slots[vacant] = key;
        self.non_zero_len = next;
        Ok(())
    }
}

#[inline]
fn hash_u64(key: u64) -> u64 {
    let mut x = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl Partition {
    fn find(&self, bytes: &[u8], hash: u64, worker: &ChunkWorkerContext) -> Result<usize> {
        let mut bucket = bucket(hash, self.directory.len());
        loop {
            // Linear probing visits a multiple of 4096 within at most 4096
            // buckets, even across wraparound. Keep hostile chains cancellable.
            if bucket.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            let ordinal = self.directory[bucket];
            if ordinal == 0 {
                return Ok(bucket);
            }
            let record = self.records[ordinal - 1];
            if record.hash == hash
                && self.bytes[record.offset..record.offset + record.len] == *bytes
            {
                return Ok(bucket);
            }
            bucket = (bucket + 1) & (self.directory.len() - 1);
        }
    }

    fn insert(
        &mut self,
        bytes: &[u8],
        hash: u64,
        memory: &LiveMemoryPool,
        worker: &ChunkWorkerContext,
    ) -> Result<bool> {
        let mut vacant = 0;
        if !self.directory.is_empty() {
            vacant = self.find(bytes, hash, worker)?;
            if self.directory[vacant] != 0 {
                return Ok(false);
            }
        }
        let next = self
            .records
            .len()
            .checked_add(1)
            .ok_or_else(|| failed("ordinal overflow"))?;
        if self.directory.is_empty() || next > self.directory.len() / 2 {
            let capacity = self
                .directory
                .len()
                .max(8)
                .checked_mul(2)
                .ok_or_else(|| failed("directory overflow"))?;
            let (mut directory, lease) = allocate(capacity, memory)?;
            directory.resize(capacity, 0);
            for (index, record) in self.records.iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                let mut slot = bucket(record.hash, capacity);
                while directory[slot] != 0 {
                    if slot.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                    }
                    slot = (slot + 1) & (capacity - 1);
                }
                directory[slot] = index + 1;
            }
            self.directory = directory;
            self.directory_lease = lease;
            vacant = self.find(bytes, hash, worker)?;
        }
        if !self.records.reserve_one(memory, failed)? {
            return Err(failed("dense capacity denied"));
        }
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| failed("byte size overflow"))?;
        if needed > self.bytes.capacity() {
            let capacity = needed.max(
                self.bytes
                    .capacity()
                    .max(64)
                    .checked_mul(2)
                    .ok_or_else(|| failed("byte capacity overflow"))?,
            );
            let (mut replacement, lease) = allocate(capacity, memory)?;
            replacement.extend_from_slice(&self.bytes);
            self.bytes = replacement;
            self.bytes_lease = lease;
        }
        // No key becomes visible before every fallible allocation succeeds.
        self.records.push(Record {
            hash,
            offset: self.bytes.len(),
            len: bytes.len(),
        });
        self.bytes.extend_from_slice(bytes);
        self.directory[vacant] = next;
        Ok(true)
    }
}

pub(super) fn partition(hash: u64) -> usize {
    super::string_count_partial::partition_index::<PARTITIONS>(hash)
}

fn bucket(hash: u64, capacity: usize) -> usize {
    #[allow(clippy::cast_possible_truncation)]
    let bucket = hash as usize & (capacity - 1);
    bucket
}

fn allocate<T>(capacity: usize, memory: &LiveMemoryPool) -> Result<(Vec<T>, MemoryLease)> {
    super::aggregate_dense_pages::allocate(capacity, memory, failed)?
        .ok_or_else(|| failed("owned capacity denied"))
}

pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex scalar distinct {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_exec::compute_pool::CancellationToken;

    #[test]
    fn scalar_distinct_collision_probe_observes_cancellation_before_empty_bucket() {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let parts = Partitions::new(&memory).unwrap();
        let token = CancellationToken::default();
        let worker = ChunkWorkerContext::Inline(token.clone());
        let mut part = parts.parts[0].lock().unwrap();
        for i in 0..4100 {
            assert!(
                part.insert(format!("key-{i}").as_bytes(), 1, &memory, &worker)
                    .unwrap()
            );
        }
        assert_eq!(part.records.len(), 4100);
        token.cancel();
        assert!(
            part.find(b"absent", 1, &worker)
                .unwrap_err()
                .to_string()
                .contains("cancel")
        );
        assert_eq!(part.records.len(), 4100);
        drop(part);
        drop(parts);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn numeric_distinct_partitions_zero_collisions_rehash_and_cancel() {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let parts = NumericDistinctPartitions::new(&memory).unwrap();
        let token = CancellationToken::default();
        let mut keys = Vec::new();
        for v in 0_u64..1500 {
            keys.push(v);
            keys.push(v);
        }
        parts.insert_slice(&keys, &token).unwrap();
        assert_eq!(parts.cardinality().unwrap(), 1500);
        // Zero and duplicates do not inflate cardinality.
        parts.insert_slice(&[0, 0, 1, 1499], &token).unwrap();
        assert_eq!(parts.cardinality().unwrap(), 1500);
        token.cancel();
        assert!(
            parts
                .insert_slice(&[9999], &token)
                .unwrap_err()
                .to_string()
                .contains("cancel")
        );
        drop(parts);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
