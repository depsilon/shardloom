//! Persistent, weighted complete integer keys. Existing chunk reduction happens
//! first; only complete partitions may discard non-winning groups.

use super::{
    AggregateSingleNumericKey, SingleNumericAggregateOrderCandidate as Candidate,
    aggregate_chunk_jobs::ChunkWorkerContext, compare_single_numeric_candidates,
    numeric_count_partial::NumericCountKey,
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::time::Instant;

pub(super) const PARTITIONS: usize = 64;

#[cfg(test)]
#[path = "numeric_count_partitions_tests.rs"]
mod tests;

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native weighted integer partitions: {message}; no fallback execution was attempted"
    ))
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("capacity byte overflow"))
}

fn partition<K: NumericCountKey>(key: K) -> usize {
    let mut bits = key.aggregate_key().bits;
    bits ^= bits >> 30;
    bits = bits.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    bits ^= bits >> 27;
    bits = bits.wrapping_mul(0x94d0_49bb_1331_11eb);
    usize::try_from((bits ^ (bits >> 31)) & (PARTITIONS as u64 - 1))
        .expect("six-bit integer partition")
}

pub(super) struct Partition<K: NumericCountKey> {
    pairs: Vec<(K, u64)>,
    // Drop storage before refunding capacity credit, including on cancellation.
    lease: MemoryLease,
}

impl<K: NumericCountKey> Partition<K> {
    fn reserve(&mut self, additional: usize) -> Result<u64> {
        let need = self
            .pairs
            .len()
            .checked_add(additional)
            .ok_or_else(|| failed("entry length overflow"))?;
        if need <= self.pairs.capacity() {
            return Ok(0);
        }
        let capacity = need
            .max(
                self.pairs
                    .capacity()
                    .checked_add(self.pairs.capacity() / 4)
                    .ok_or_else(|| failed("capacity growth overflow"))?,
            )
            .max(64);
        let overlap = self
            .lease
            .bytes()
            .checked_add(bytes::<(K, u64)>(capacity)?)
            .ok_or_else(|| failed("growth overlap overflow"))?;
        self.lease.resize(overlap)?;
        self.pairs
            .try_reserve_exact(capacity - self.pairs.len())
            .map_err(failed)?;
        if self.pairs.capacity() > capacity {
            return Err(failed("vector exceeds admitted capacity"));
        }
        self.lease
            .resize(bytes::<(K, u64)>(self.pairs.capacity())?)?;
        Ok(overlap)
    }
}

pub(super) struct Partitions<K: NumericCountKey> {
    parts: Vec<Partition<K>>,
    _owner: MemoryLease,
    rows: u64,
    entries: u64,
    growth_overlap_peak: u64,
    capacity_peak: u64,
}

impl<K: NumericCountKey> Partitions<K> {
    fn owner_bytes() -> Result<u64> {
        Ok(bytes::<Partition<K>>(PARTITIONS)?
            + size_of::<Self>() as u64
            + (PARTITIONS * size_of::<usize>()) as u64)
    }

    #[cfg(test)]
    fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Self::with_owner(memory, memory.reserve(Self::owner_bytes()?)?)
    }

    fn try_new(memory: &LiveMemoryPool) -> Result<Option<Self>> {
        // An optional strategy may decline before reading input. Once admitted,
        // allocation or append failures remain fatal and never trigger replay.
        let Ok(owner) = memory.reserve(Self::owner_bytes()?) else {
            return Ok(None);
        };
        Self::with_owner(memory, owner).map(Some)
    }

    fn with_owner(memory: &LiveMemoryPool, owner: MemoryLease) -> Result<Self> {
        let mut parts = Vec::new();
        parts.try_reserve_exact(PARTITIONS).map_err(failed)?;
        if parts.capacity() > PARTITIONS {
            return Err(failed("partition owner exceeds admission"));
        }
        for _ in 0..PARTITIONS {
            parts.push(Partition {
                pairs: Vec::new(),
                lease: memory.reserve(0)?,
            });
        }
        Ok(Self {
            parts,
            _owner: owner,
            rows: 0,
            entries: 0,
            growth_overlap_peak: 0,
            capacity_peak: 0,
        })
    }

    pub(super) fn append(
        &mut self,
        pairs: &[(K, u64)],
        check: impl Fn() -> Result<()>,
    ) -> Result<()> {
        let mut lengths = [0usize; PARTITIONS];
        let mut rows = 0_u64;
        for (index, &(key, count)) in pairs.iter().enumerate() {
            if index % 1024 == 0 {
                check()?;
            }
            if count == 0 {
                return Err(failed("zero input weight"));
            }
            rows = rows
                .checked_add(count)
                .ok_or_else(|| failed("partial weight overflow"))?;
            lengths[partition(key)] += 1;
        }
        for (part, count) in self.parts.iter_mut().zip(lengths) {
            self.growth_overlap_peak = self.growth_overlap_peak.max(part.reserve(count)?);
        }
        for (index, &pair) in pairs.iter().enumerate() {
            if index % 1024 == 0 {
                check()?;
            }
            self.parts[partition(pair.0)].pairs.push(pair);
        }
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| failed("complete weight overflow"))?;
        self.entries = self
            .entries
            .checked_add(u64::try_from(pairs.len()).map_err(failed)?)
            .ok_or_else(|| failed("entry count overflow"))?;
        self.capacity_peak = self
            .capacity_peak
            .max(self.parts.iter().map(|p| p.lease.bytes()).sum());
        Ok(())
    }

    fn visit_batches(
        &self,
        visit: &mut impl FnMut(
            &mut dyn ExactSizeIterator<Item = (AggregateSingleNumericKey, u64)>,
        ) -> Result<()>,
    ) -> Result<()> {
        for part in &self.parts {
            for batch in part.pairs.chunks(1024) {
                visit(
                    &mut batch
                        .iter()
                        .map(|&(key, count)| (key.aggregate_key(), count)),
                )?;
            }
        }
        Ok(())
    }

    fn evidence(&self) -> Evidence {
        Evidence {
            rows: self.rows,
            entries: self.entries,
            capacity_peak: self.capacity_peak,
            growth_overlap_peak: self.growth_overlap_peak,
            ..Evidence::default()
        }
    }
}

pub(super) enum NumericPartitions {
    Signed32(Partitions<i32>),
    Signed(Partitions<i64>),
    Unsigned(Partitions<u64>),
}

pub(super) enum NumericPartition {
    Signed32(Partition<i32>),
    Signed(Partition<i64>),
    Unsigned(Partition<u64>),
}

impl NumericPartitions {
    pub(super) fn try_new(
        ptype: vortex::array::dtype::PType,
        memory: &LiveMemoryPool,
    ) -> Result<Option<Self>> {
        use vortex::array::dtype::PType;
        match ptype {
            PType::I32 => Partitions::try_new(memory).map(|parts| parts.map(Self::Signed32)),
            PType::I64 => Partitions::try_new(memory).map(|parts| parts.map(Self::Signed)),
            PType::U64 => Partitions::try_new(memory).map(|parts| parts.map(Self::Unsigned)),
            _ => Err(failed("unadmitted integer dtype")),
        }
    }

    pub(super) fn pop(&mut self) -> Option<NumericPartition> {
        match self {
            Self::Signed32(p) => p.parts.pop().map(NumericPartition::Signed32),
            Self::Signed(p) => p.parts.pop().map(NumericPartition::Signed),
            Self::Unsigned(p) => p.parts.pop().map(NumericPartition::Unsigned),
        }
    }

    pub(super) fn visit_batches(
        &self,
        mut visit: impl FnMut(
            &mut dyn ExactSizeIterator<Item = (AggregateSingleNumericKey, u64)>,
        ) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Signed32(p) => p.visit_batches(&mut visit),
            Self::Signed(p) => p.visit_batches(&mut visit),
            Self::Unsigned(p) => p.visit_batches(&mut visit),
        }
    }

    pub(super) fn evidence(&self) -> Evidence {
        match self {
            Self::Signed32(p) => p.evidence(),
            Self::Signed(p) => p.evidence(),
            Self::Unsigned(p) => p.evidence(),
        }
    }
}

#[derive(Default)]
pub(super) struct Evidence {
    pub(super) rows: u64,
    pub(super) reduced_rows: u64,
    pub(super) entries: u64,
    pub(super) capacity_peak: u64,
    pub(super) growth_overlap_peak: u64,
    pub(super) groups: usize,
    pub(super) sort_nanos: u128,
    pub(super) reduce_nanos: u128,
    pub(super) finish_nanos: u128,
    pub(super) dictionary_handoff: bool,
}

pub(super) struct Selection {
    pub(super) retained: Vec<Candidate>,
    pub(super) rows: u64,
    pub(super) groups: usize,
    pub(super) sort_nanos: u128,
    pub(super) reduce_nanos: u128,
}

impl NumericPartition {
    pub(super) fn output_bytes(cap: usize) -> Result<u64> {
        bytes::<Candidate>(cap)?
            .checked_add(size_of::<Selection>() as u64)
            .ok_or_else(|| failed("selection capacity overflow"))
    }

    pub(super) fn reduce(self, cap: usize, worker: &ChunkWorkerContext) -> Result<Selection> {
        match self {
            Self::Signed32(p) => reduce(p, cap, worker),
            Self::Signed(p) => reduce(p, cap, worker),
            Self::Unsigned(p) => reduce(p, cap, worker),
        }
    }
}

fn reduce<K: NumericCountKey>(
    mut part: Partition<K>,
    cap: usize,
    worker: &ChunkWorkerContext,
) -> Result<Selection> {
    worker.check_cancelled()?;
    let started = Instant::now();
    part.pairs.sort_unstable_by_key(|pair| pair.0);
    let sort_nanos = started.elapsed().as_nanos();
    worker.check_cancelled()?;
    let started = Instant::now();
    let mut retained: Vec<Candidate> = Vec::new();
    retained.try_reserve_exact(cap).map_err(failed)?;
    if retained.capacity() > cap {
        return Err(failed("selection exceeds admitted capacity"));
    }
    let (mut begin, mut groups, mut rows) = (0, 0, 0_u64);
    let mut worst = None;
    while begin < part.pairs.len() {
        if groups % 1024 == 0 {
            worker.check_cancelled()?;
        }
        let (key, mut count) = part.pairs[begin];
        let mut end = begin + 1;
        while end < part.pairs.len() && part.pairs[end].0 == key {
            if end % 1024 == 0 {
                worker.check_cancelled()?;
            }
            count = count
                .checked_add(part.pairs[end].1)
                .ok_or_else(|| failed("COUNT overflow"))?;
            end += 1;
        }
        rows = rows
            .checked_add(count)
            .ok_or_else(|| failed("partition weight overflow"))?;
        let candidate = Candidate {
            key: key.aggregate_key(),
            count,
        };
        if retained.len() < cap {
            retained.push(candidate);
        } else if let Some(index) = worst
            && compare_single_numeric_candidates(&candidate, &retained[index]).is_lt()
        {
            retained[index] = candidate;
            worst = None;
        }
        if retained.len() == cap && worst.is_none() {
            worst = retained
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| compare_single_numeric_candidates(left, right))
                .map(|(index, _)| index);
        }
        groups += 1;
        begin = end;
    }
    Ok(Selection {
        retained,
        rows,
        groups,
        sort_nanos,
        reduce_nanos: started.elapsed().as_nanos(),
    })
}
