//! Complete-pair reconciliation. This never ranks local groups: a group may
//! have distinct values in every partition, so all contributions must meet.

use super::super::{
    AggregateSingleNumericKey, SingleNumericAggregateOrderCandidate,
    compare_single_numeric_candidates,
    string_count_entry_credits::{Claim, EntryBlock, EntryCredits},
};
use super::{ChunkWorkerContext, Insert, Pair, PairPartial, PairSet, failed};
use shardloom_core::Result;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    collections::BinaryHeap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

pub(super) const PARTITIONS: usize = 64;
pub(super) fn partition(hash: u64) -> usize {
    usize::try_from(hash.rotate_right(32) & (PARTITIONS as u64 - 1))
        .expect("masked partition hash fits usize")
}

pub(super) struct Receipt {
    pub source_rows: u64,
    pub partial_pairs: usize,
    pub native_execution_nanos: u128,
    pub count_nanos: u128,
    pub capacity_bytes: u64,
    pub comparisons: u64,
    pub numeric_work: [super::NumericWork; 2],
    pub deferred: Option<Arc<PairPartial>>,
}

#[derive(Clone, Copy)]
pub(super) struct Evidence {
    pub pairs: usize,
    pub committed_rows: u64,
    pub comparisons: u64,
    pub lock_wait_nanos: u64,
    pub reconcile_nanos: u64,
    pub group_reduce_nanos: u64,
    pub entry_claims: u64,
    pub entry_returns: u64,
}

pub(super) struct ExactDistinctPartitions {
    partitions: Vec<Mutex<PairSet>>,
    memory: LiveMemoryPool,
    credits: EntryCredits,
    pressure: AtomicBool,
    rows: AtomicU64,
    comparisons: AtomicU64,
    lock_wait: AtomicU64,
    reconcile: AtomicU64,
    group_reduce: AtomicU64,
    _metadata: MemoryLease,
}

impl ExactDistinctPartitions {
    pub(super) fn try_new(memory: &LiveMemoryPool, pair_limit: usize) -> Result<Option<Arc<Self>>> {
        if pair_limit == 0 {
            return Ok(None);
        }
        let metadata = size_of::<Self>()
            .checked_add(2 * size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(PARTITIONS * size_of::<Mutex<PairSet>>()))
            .ok_or_else(|| failed("partition metadata overflowed"))?;
        let Ok(lease) = memory.reserve(metadata as u64) else {
            return Ok(None);
        };
        let mut partitions = Vec::new();
        if partitions.try_reserve_exact(PARTITIONS).is_err() || partitions.capacity() > PARTITIONS {
            return Ok(None);
        }
        for _ in 0..PARTITIONS {
            let Ok(pairs) = PairSet::new(memory) else {
                return Ok(None);
            };
            partitions.push(Mutex::new(pairs));
        }
        Ok(Some(Arc::new(Self {
            partitions,
            memory: memory.clone(),
            credits: EntryCredits::new(pair_limit),
            pressure: AtomicBool::new(false),
            rows: AtomicU64::new(0),
            comparisons: AtomicU64::new(0),
            lock_wait: AtomicU64::new(0),
            reconcile: AtomicU64::new(0),
            group_reduce: AtomicU64::new(0),
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

    pub(super) fn reduce(
        &self,
        mut partial: PairPartial,
        worker: &ChunkWorkerContext,
    ) -> Result<Receipt> {
        worker.check_cancelled()?;
        let ranges = partial.arrange(worker)?;
        let source_rows = partial.rows;
        let partial_pairs = partial.len();
        let native_execution_nanos = partial.native_execution_nanos;
        let count_nanos = partial.count_nanos;
        let capacity_bytes = partial.capacity_bytes;
        let comparisons = partial.comparisons;
        let numeric_work = partial.numeric_work;
        let mut cursor = 0;
        let mut consumed = 0_u64;
        for (index, (_, end)) in ranges.into_iter().enumerate() {
            if cursor == end {
                continue;
            }
            self.reduce_partition(index, end, &partial, worker, &mut cursor, &mut consumed)?;
            if cursor != end {
                break;
            }
        }
        add(&self.rows, consumed)?;
        let deferred = if cursor == partial_pairs {
            None
        } else {
            partial.retain_suffix(cursor)?;
            if consumed.checked_add(partial.rows) != Some(source_rows) {
                return Err(failed("partial prefix/suffix accounting diverged"));
            }
            Some(Arc::new(partial))
        };
        Ok(Receipt {
            source_rows,
            partial_pairs,
            native_execution_nanos,
            count_nanos,
            capacity_bytes,
            comparisons,
            numeric_work,
            deferred,
        })
    }

    fn reduce_partition(
        &self,
        index: usize,
        end: usize,
        partial: &PairPartial,
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
            let mut pairs = self.partitions[index]
                .lock()
                .map_err(|_| failed("pair partition lock poisoned"))?;
            elapsed(&self.lock_wait, started)?;
            let comparisons_before = pairs.comparisons;
            let started = Instant::now();
            let outcome = (|| -> Result<bool> {
                while *cursor < end {
                    if cursor.is_multiple_of(4096) {
                        worker.check_cancelled()?;
                        if self.pressured() {
                            return Ok(false);
                        }
                    }
                    let (pair, weight, hash) = partial.entry(*cursor);
                    let allowed = credit.as_ref().is_some_and(|block| block.remaining() != 0);
                    match pairs.insert_hashed(pair, hash, weight, allowed, worker)? {
                        Insert::Applied { new_pair } => {
                            if new_pair {
                                credit
                                    .as_mut()
                                    .expect("new pair had entry admission")
                                    .consume_one()?;
                            }
                            *consumed = consumed
                                .checked_add(weight)
                                .ok_or_else(|| failed("consumed pair weight overflowed"))?;
                            *cursor += 1;
                        }
                        Insert::NeedsEntryCredit => return Ok(true),
                        Insert::BytePressure => {
                            self.request_pressure();
                            return Ok(false);
                        }
                    }
                }
                Ok(false)
            })();
            let comparisons = pairs.comparisons - comparisons_before;
            drop(pairs);
            add(&self.comparisons, comparisons)?;
            elapsed(&self.reconcile, started)?;
            if !outcome? {
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
            // Another worker may have inserted the key while the lock was
            // dropped. Recheck exact identity before consuming the new credit.
        }
    }

    /// Only use after every reducer has drained. Counts are stable then and
    /// no outstanding entry block may be hidden behind the reported total.
    pub(super) fn evidence(&self) -> Result<Evidence> {
        let credits = self.credits.evidence()?;
        if credits.reserved != 0 {
            return Err(failed("pair entry blocks remain after drain"));
        }
        Ok(Evidence {
            pairs: credits.committed,
            committed_rows: self.rows.load(Ordering::Acquire),
            comparisons: self.comparisons.load(Ordering::Acquire),
            lock_wait_nanos: self.lock_wait.load(Ordering::Acquire),
            reconcile_nanos: self.reconcile.load(Ordering::Acquire),
            group_reduce_nanos: self.group_reduce.load(Ordering::Acquire),
            entry_claims: credits.claim_calls,
            entry_returns: credits.return_calls,
        })
    }

    /// EOF-only second reduction. Pair owners remain untouched until this
    /// succeeds, so denied group capacity can still replay the exact pairs.
    /// `group_limit` separately bounds final group entries; all bytes share the
    /// same pool, including simultaneous pair and final-group table capacity.
    pub(super) fn finish_groups(
        &self,
        group_limit: usize,
        worker: &ChunkWorkerContext,
    ) -> Result<Option<GroupCounts>> {
        worker.check_cancelled()?;
        self.evidence()?;
        if self.pressured() {
            return Err(failed("cannot finish pressured pair partitions"));
        }
        let Ok(mut groups) = PairSet::new(&self.memory) else {
            return Ok(None);
        };
        let started = Instant::now();
        for partition in &self.partitions {
            let pairs = partition
                .lock()
                .map_err(|_| failed("pair partition lock poisoned"))?;
            let mut admitted = true;
            for (index, slot) in pairs.slots.iter().enumerate() {
                if index.is_multiple_of(4096) {
                    worker.check_cancelled()?;
                }
                if slot.weight == 0 {
                    continue;
                }
                // The pair is globally unique, regardless of its input weight.
                let group = Pair::new(
                    slot.pair.group(),
                    super::AggregateIntegerKeyPart {
                        bits: 0,
                        signed: false,
                    },
                );
                let allow_new = groups.pairs < group_limit;
                if !matches!(
                    groups.insert(group, 1, allow_new, worker)?,
                    Insert::Applied { .. }
                ) {
                    admitted = false;
                    break;
                }
            }
            if !admitted {
                elapsed(&self.group_reduce, started)?;
                return Ok(None);
            }
        }
        elapsed(&self.group_reduce, started)?;
        Ok(Some(GroupCounts { groups }))
    }

    /// After job drain, pressure handoff replays complete identities and row
    /// weights. A distinct-only old reducer must union identities, never sum
    /// these row weights as distinct counts.
    pub(super) fn replay_and_release(
        &self,
        mut visit: impl FnMut(Pair, u64) -> Result<()>,
    ) -> Result<()> {
        self.evidence()?;
        for partition in &self.partitions {
            let mut pairs = partition
                .lock()
                .map_err(|_| failed("pair partition lock poisoned"))?;
            pairs.visit(&mut visit)?;
            pairs.clear()?;
        }
        Ok(())
    }
}

pub(super) struct GroupCounts {
    groups: PairSet,
}
impl GroupCounts {
    #[cfg(test)]
    pub(super) fn visit(
        &self,
        mut visit: impl FnMut(super::AggregateIntegerKeyPart, u64) -> Result<()>,
    ) -> Result<()> {
        self.groups
            .visit(|pair, distinct| visit(pair.group(), distinct))
    }

    /// Rank only complete group counts after EOF. The bounded heap uses the
    /// same count-descending/key-ascending comparator as native integer groups.
    pub(super) fn select(
        &self,
        retained: usize,
        worker: &ChunkWorkerContext,
    ) -> Result<Option<FinalGroupCounts>> {
        let capacity = retained.min(self.groups.pairs);
        let bytes = capacity
            .checked_mul(size_of::<RankedGroup>())
            .and_then(|bytes| {
                bytes.checked_add(size_of::<FinalGroupCounts>() + 2 * size_of::<usize>())
            })
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("final distinct selection capacity overflowed"))?;
        let Ok(lease) = self.groups.memory.reserve(bytes) else {
            return Ok(None);
        };
        let mut heap = BinaryHeap::new();
        if heap.try_reserve_exact(capacity).is_err() || heap.capacity() > capacity {
            return Ok(None);
        }
        for (index, slot) in self.groups.slots.iter().enumerate() {
            if index.is_multiple_of(4096) {
                worker.check_cancelled()?;
            }
            if slot.weight == 0 || capacity == 0 {
                continue;
            }
            let key = slot.pair.group();
            let candidate = RankedGroup(SingleNumericAggregateOrderCandidate {
                key: AggregateSingleNumericKey {
                    bits: key.bits,
                    signed: key.signed,
                },
                count: slot.weight,
            });
            if heap.len() < capacity {
                heap.push(candidate);
            } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                *heap.peek_mut().expect("nonempty bounded heap") = candidate;
            }
        }
        // into_sorted_vec reuses the heap allocation; no uncharged second vector.
        Ok(Some(FinalGroupCounts {
            counts: heap.into_sorted_vec(),
            group_count: self.groups.pairs,
            lease,
        }))
    }
}

#[derive(Clone, Copy)]
struct RankedGroup(SingleNumericAggregateOrderCandidate);
impl PartialEq for RankedGroup {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for RankedGroup {}
impl PartialOrd for RankedGroup {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for RankedGroup {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_single_numeric_candidates(&self.0, &other.0)
    }
}

pub(super) struct FinalGroupCounts {
    counts: Vec<RankedGroup>,
    pub group_count: usize,
    lease: MemoryLease,
}
impl FinalGroupCounts {
    pub(super) fn retained_count(&self) -> usize {
        self.counts.len()
    }
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }
    pub(super) fn visit(
        &self,
        mut visit: impl FnMut(super::AggregateIntegerKeyPart, u64) -> Result<()>,
    ) -> Result<()> {
        for RankedGroup(candidate) in &self.counts {
            visit(
                super::AggregateIntegerKeyPart {
                    bits: candidate.key.bits,
                    signed: candidate.key.signed,
                },
                candidate.count,
            )?;
        }
        Ok(())
    }
}

fn add(counter: &AtomicU64, value: u64) -> Result<()> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
            old.checked_add(value)
        })
        .map(|_| ())
        .map_err(|_| failed("partition evidence counter overflowed"))
}
fn elapsed(counter: &AtomicU64, started: Instant) -> Result<()> {
    add(
        counter,
        u64::try_from(started.elapsed().as_nanos())
            .map_err(|_| failed("partition duration exceeds u64"))?,
    )
}
