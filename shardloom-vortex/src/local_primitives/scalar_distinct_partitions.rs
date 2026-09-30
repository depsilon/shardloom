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
        let (mut parts, metadata) = allocate(PARTITIONS, memory)?;
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

impl Partition {
    fn find(&self, bytes: &[u8], hash: u64) -> usize {
        let mut bucket = bucket(hash, self.directory.len());
        loop {
            let ordinal = self.directory[bucket];
            if ordinal == 0 {
                return bucket;
            }
            let record = self.records[ordinal - 1];
            if record.hash == hash
                && self.bytes[record.offset..record.offset + record.len] == *bytes
            {
                return bucket;
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
            vacant = self.find(bytes, hash);
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
                    slot = (slot + 1) & (capacity - 1);
                }
                directory[slot] = index + 1;
            }
            self.directory = directory;
            self.directory_lease = lease;
            vacant = self.find(bytes, hash);
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
