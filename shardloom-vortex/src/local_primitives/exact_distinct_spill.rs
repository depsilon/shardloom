//! Exact integer-pair runs behind explicit aggregate workspace admission.
//! Small inputs finish in memory; a required flush creates the owned run store.

use super::super::{
    AggregateIntegerKeyPart, AggregateSingleNumericKey, SingleNumericAggregateOrderCandidate,
    compare_single_numeric_candidates,
    query_run_store::{NativeQueryRun, QueryRunSpec, QueryRunStore, QueryRunStorePolicy},
};
use super::Pair;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    collections::BinaryHeap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use vortex::{io::runtime::BlockingRuntime, session::VortexSession};

#[path = "exact_distinct_spill_runs.rs"]
mod runs;
use runs::{BLOCK_ROWS, FAN_IN, Record, RunMerge, blocks, run_dtype};

const MIN_MEMORY_BYTES: u64 = 2 << 20;
const SCRATCH_BYTES: u64 = 128 << 10;
// Four native input blocks, owned primitive views, one output conversion block,
// writer handoff, file-reader contexts/heads and bounded registry metadata.
const MERGE_BYTES: u64 = 704 << 10;
const MAX_BUFFER_PAIRS: usize = 65_536;
const MAX_RUNS: usize = 64;

#[derive(Clone)]
pub(super) struct Policy {
    pub workspace: PathBuf,
    pub quota_bytes: u64,
    pub memory_bytes: u64,
    pub cancellation: Arc<AtomicBool>,
}
impl Policy {
    fn check(&self) -> Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(failed("execution cancelled"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Evidence {
    pub rows: u64,
    pub complete_pairs: u64,
    pub groups: usize,
    pub runs_written: u64,
    pub runs_validated: u64,
    pub merge_passes: u64,
    pub peak_disk_bytes: u64,
    pub peak_reserved_bytes: u64,
    pub buffer_capacity_pairs: usize,
}

struct Run {
    native: NativeQueryRun,
    level: u8,
}

pub(super) struct ExactDistinctSpill {
    policy: Policy,
    memory: LiveMemoryPool,
    store: Option<QueryRunStore>,
    scratch: Option<MemoryLease>,
    runs: Vec<Run>,
    buffer: Vec<Record>,
    _buffer_lease: MemoryLease,
    _policy_metadata: MemoryLease,
    work: Arc<MemoryLease>,
    selected: BinaryHeap<RankedGroup>,
    selection_lease: MemoryLease,
    retained: usize,
    signature: u8,
    evidence: Evidence,
    failed: bool,
}

impl ExactDistinctSpill {
    /// All emergency work and final selection are admitted before workspace
    /// creation. `memory` is the operator's shared reservation pool.
    pub(super) fn new(
        policy: Policy,
        memory: LiveMemoryPool,
        group_signed: bool,
        value_signed: bool,
        retained: usize,
    ) -> Result<Self> {
        policy.check()?;
        if policy.memory_bytes < MIN_MEMORY_BYTES
            || policy.memory_bytes != memory.snapshot().limit_bytes
        {
            return Err(failed(
                "requires at least 2 MiB and the exact shared operator memory envelope",
            ));
        }
        let metadata_bytes = policy
            .workspace
            .capacity()
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(size_of::<Self>()))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| failed("policy path capacity overflowed"))?;
        let policy_metadata = memory.reserve(metadata_bytes)?;
        let capacity = usize::try_from(policy.memory_bytes / 4 / size_of::<Record>() as u64)
            .unwrap_or(MAX_BUFFER_PAIRS)
            .min(MAX_BUFFER_PAIRS);
        let buffer_lease = memory.reserve(bytes::<Record>(capacity)?)?;
        let work = Arc::new(memory.reserve(MERGE_BYTES)?);
        let scratch = memory.reserve(SCRATCH_BYTES)?;
        let selection_bytes = bytes::<RankedGroup>(retained)?
            .checked_add(size_of::<SpilledDistinctResult>() as u64)
            .ok_or_else(|| failed("final selection capacity overflowed"))?;
        let selection_lease = memory.reserve(selection_bytes)?;
        let mut selected = BinaryHeap::new();
        selected
            .try_reserve_exact(retained)
            .map_err(|_| failed("final selection allocation failed"))?;
        if selected.capacity() != retained {
            return Err(failed("final selection allocation exceeded reservation"));
        }
        let buffer = reserved_vec(capacity)?;
        let runs = reserved_vec(MAX_RUNS)?;
        Ok(Self {
            policy,
            memory,
            store: None,
            scratch: Some(scratch),
            runs,
            buffer,
            _buffer_lease: buffer_lease,
            _policy_metadata: policy_metadata,
            work,
            selected,
            selection_lease,
            retained,
            signature: u8::from(group_signed) | (u8::from(value_signed) << 1),
            evidence: Evidence {
                buffer_capacity_pairs: capacity,
                ..Evidence::default()
            },
            failed: false,
        })
    }

    fn check(&self) -> Result<()> {
        self.policy.check()?;
        if self.failed {
            return Err(failed("attempt failed; owned cleanup is required"));
        }
        Ok(())
    }

    fn ensure_store(&mut self) -> Result<()> {
        if self.store.is_none() {
            let scratch = self
                .scratch
                .take()
                .ok_or_else(|| failed("workspace scratch is absent"))?;
            self.store = Some(QueryRunStore::new(
                QueryRunStorePolicy::exact_integer_distinct(
                    self.policy.workspace.clone(),
                    self.policy.quota_bytes,
                    Arc::clone(&self.policy.cancellation),
                ),
                self.memory.clone(),
                scratch,
            )?);
        }
        Ok(())
    }

    /// Transfer every positive weighted pair once. The caller may release the
    /// old partition epoch only after all its contributions were accepted.
    pub(super) fn push(
        &mut self,
        pair: Pair,
        weight: u64,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        let result = self.push_inner(pair, weight, runtime, session);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn push_inner(
        &mut self,
        pair: Pair,
        weight: u64,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        self.check()?;
        if pair.signedness != self.signature || weight == 0 {
            return Err(failed("pair signature or positive weight changed"));
        }
        let rows = self
            .evidence
            .rows
            .checked_add(weight)
            .ok_or_else(|| failed("source row weight overflowed"))?;
        if self.buffer.len() == self.buffer.capacity() {
            self.flush(runtime, session)?;
        }
        self.buffer.push(Record { pair, weight });
        self.evidence.rows = rows;
        Ok(())
    }

    fn flush(&mut self, runtime: &impl BlockingRuntime, session: &VortexSession) -> Result<()> {
        let result = self.flush_inner(runtime, session);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn flush_inner(
        &mut self,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        self.check()?;
        if self.buffer.is_empty() {
            return Ok(());
        }
        // Bounded in-place sort, at most MAX_BUFFER_PAIRS entries. No second
        // row vector or string/scalar intermediate is allocated.
        self.buffer.sort_unstable_by_key(Record::key);
        self.policy.check()?;
        let mut unique = 0_usize;
        for index in 0..self.buffer.len() {
            let row = self.buffer[index];
            if unique != 0 && self.buffer[unique - 1].pair == row.pair {
                self.buffer[unique - 1].weight = self.buffer[unique - 1]
                    .weight
                    .checked_add(row.weight)
                    .ok_or_else(|| failed("run pair weight overflowed"))?;
            } else {
                self.buffer[unique] = row;
                unique += 1;
            }
        }
        self.buffer.truncate(unique);
        let rows = u64::try_from(unique).map_err(|_| failed("run row count overflowed"))?;
        if self.runs.len() == MAX_RUNS {
            return Err(failed("bounded run registry exhausted"));
        }
        self.ensure_store()?;
        let native = self.store.as_mut().expect("admitted store").write_arrays(
            &spec(rows)?,
            self.buffer
                .chunks(BLOCK_ROWS)
                .map(|rows| Ok(runs::array(rows))),
            runtime,
            session,
            &self.work,
        )?;
        self.buffer.clear();
        self.runs.push(Run { native, level: 0 });
        self.compact_levels(runtime, session)
    }

    fn compact_levels(
        &mut self,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        loop {
            let Some(level) = (0..=u8::MAX)
                .find(|level| self.runs.iter().filter(|run| run.level == *level).count() >= FAN_IN)
            else {
                return Ok(());
            };
            self.compact_level(level, runtime, session)?;
        }
    }

    fn compact_level(
        &mut self,
        level: u8,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        self.check()?;
        let next_level = level
            .checked_add(1)
            .ok_or_else(|| failed("run compaction level exhausted"))?;
        let mut inputs = reserved_vec(FAN_IN)?;
        for _ in 0..FAN_IN {
            let index = self
                .runs
                .iter()
                .position(|run| run.level == level)
                .ok_or_else(|| failed("compaction input absent"))?;
            inputs.push(self.runs.remove(index));
        }
        let rows = inputs
            .iter()
            .try_fold(0_u64, |rows, run| rows.checked_add(run.native.rows))
            .ok_or_else(|| failed("compaction row count overflowed"))?;
        let mut merge = RunMerge::new(
            inputs.iter().map(|run| &run.native),
            self.store.as_ref().expect("runs have a store"),
            Arc::clone(&self.work),
            self.policy.clone(),
            self.signature,
            runtime,
            session,
        )?;
        let native = self
            .store
            .as_mut()
            .expect("runs have a store")
            .write_arrays(
                &spec(rows)?,
                blocks(&mut merge),
                runtime,
                session,
                &self.work,
            )?;
        merge.validate()?;
        drop(merge);
        for run in &inputs {
            self.store
                .as_mut()
                .expect("runs have a store")
                .remove(&run.native)?;
        }
        drop(inputs);
        self.runs.push(Run {
            native,
            level: next_level,
        });
        self.evidence.merge_passes = self
            .evidence
            .merge_passes
            .checked_add(1)
            .ok_or_else(|| failed("merge counter overflowed"))?;
        Ok(())
    }

    fn compact_for_final(
        &mut self,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        while self.runs.len() > FAN_IN {
            // Promote the smallest levels only. Each rewrite increases a run's
            // level; an ever-growing accumulated output is not rewritten per flush.
            self.runs
                .sort_unstable_by_key(|run| (run.level, run.native.rows));
            let level = self.runs[FAN_IN - 1].level;
            for run in self.runs.iter_mut().take(FAN_IN) {
                run.level = level;
            }
            self.compact_level(level, runtime, session)?;
        }
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<SpilledDistinctResult> {
        self.check()?;
        if self.runs.is_empty() {
            self.buffer.sort_unstable_by_key(Record::key);
            select_records(
                self.buffer.iter().copied().map(Ok),
                &self.policy,
                &mut self.selected,
                self.retained,
                &mut self.evidence,
            )?;
            self.evidence.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
            return Ok(SpilledDistinctResult {
                selected: self.selected.into_sorted_vec(),
                evidence: self.evidence,
                lease: self.selection_lease,
            });
        }
        self.flush(runtime, session)?;
        self.compact_for_final(runtime, session)?;
        let mut merge = RunMerge::new(
            self.runs.iter().map(|run| &run.native),
            self.store.as_ref().expect("runs have a store"),
            Arc::clone(&self.work),
            self.policy.clone(),
            self.signature,
            runtime,
            session,
        )?;
        select_records(
            &mut merge,
            &self.policy,
            &mut self.selected,
            self.retained,
            &mut self.evidence,
        )?;
        merge.validate()?;
        drop(merge);
        self.policy.check()?;
        let store = self.store.as_ref().expect("runs have a store").snapshot();
        self.evidence.runs_written = store.runs_written;
        self.evidence.runs_validated = store.runs_validated;
        self.evidence.peak_disk_bytes = store.peak_disk_bytes;
        self.evidence.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
        self.store.as_mut().expect("runs have a store").cleanup()?;
        Ok(SpilledDistinctResult {
            selected: self.selected.into_sorted_vec(),
            evidence: self.evidence,
            lease: self.selection_lease,
        })
    }
}

fn select_records(
    records: impl Iterator<Item = Result<Record>>,
    policy: &Policy,
    selected: &mut BinaryHeap<RankedGroup>,
    retained: usize,
    evidence: &mut Evidence,
) -> Result<()> {
    let mut previous = None;
    let mut group: Option<(AggregateIntegerKeyPart, u64)> = None;
    let mut source_rows = 0_u64;
    for (ordinal, record) in records.enumerate() {
        if ordinal.is_multiple_of(4096) {
            policy.check()?;
        }
        let record = record?;
        source_rows = source_rows
            .checked_add(record.weight)
            .ok_or_else(|| failed("merged source weight overflowed"))?;
        if previous == Some(record.pair) {
            continue;
        }
        evidence.complete_pairs = evidence
            .complete_pairs
            .checked_add(1)
            .ok_or_else(|| failed("complete pair count overflowed"))?;
        previous = Some(record.pair);
        if let Some((key, count)) = group.as_mut() {
            if key.bits == record.pair.group_bits {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| failed("distinct group count overflowed"))?;
                continue;
            }
            select_group(selected, retained, *key, *count);
            evidence.groups = evidence
                .groups
                .checked_add(1)
                .ok_or_else(|| failed("group count overflowed"))?;
        }
        group = Some((record.pair.group(), 1));
    }
    if let Some((key, count)) = group {
        select_group(selected, retained, key, count);
        evidence.groups = evidence
            .groups
            .checked_add(1)
            .ok_or_else(|| failed("group count overflowed"))?;
    }
    if source_rows != evidence.rows {
        return Err(failed("complete merge source weights differ"));
    }
    policy.check()
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

fn select_group(
    heap: &mut BinaryHeap<RankedGroup>,
    retained: usize,
    key: AggregateIntegerKeyPart,
    count: u64,
) {
    if retained == 0 {
        return;
    }
    let candidate = RankedGroup(SingleNumericAggregateOrderCandidate {
        key: AggregateSingleNumericKey {
            bits: key.bits,
            signed: key.signed,
        },
        count,
    });
    if heap.len() < retained {
        heap.push(candidate);
    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
        *heap.peek_mut().expect("nonempty bounded selection") = candidate;
    }
}

pub(super) struct SpilledDistinctResult {
    selected: Vec<RankedGroup>,
    pub evidence: Evidence,
    lease: MemoryLease,
}
impl SpilledDistinctResult {
    pub(super) fn retained_count(&self) -> usize {
        self.selected.len()
    }
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }
    pub(super) fn visit(
        &self,
        offset: usize,
        mut visit: impl FnMut(AggregateIntegerKeyPart, u64) -> Result<()>,
    ) -> Result<()> {
        for RankedGroup(candidate) in self.selected.iter().skip(offset) {
            visit(
                AggregateIntegerKeyPart {
                    bits: candidate.key.bits,
                    signed: candidate.key.signed,
                },
                candidate.count,
            )?;
        }
        Ok(())
    }
}

fn spec(rows: u64) -> Result<QueryRunSpec> {
    let metadata_bytes = rows
        .div_ceil(BLOCK_ROWS as u64)
        .checked_mul(1024)
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| failed("run metadata capacity overflowed"))?;
    Ok(QueryRunSpec {
        dtype: run_dtype(),
        rows,
        block_rows: BLOCK_ROWS,
        metadata_bytes,
    })
}

fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("owned capacity overflowed"))
}
fn reserved_vec<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| failed("owned allocation failed"))?;
    if values.capacity() != capacity {
        return Err(failed("allocation capacity exceeded reservation"));
    }
    Ok(values)
}
fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native exact integer DISTINCT spill {reason}; no fallback execution was attempted"
    ))
}

#[cfg(all(test, feature = "vortex-write", unix))]
#[path = "exact_distinct_spill_tests.rs"]
mod tests;
