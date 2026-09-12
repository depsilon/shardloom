//! Private weighted complete-key COUNT runs. Public admission is deliberately
//! separate. Borrowed UTF8 visits resolve dictionary domains before persistence.

use super::{
    AggregateIntegerKeyPart,
    query_run_store::{NativeQueryRun, QueryRunSpec, QueryRunStore, QueryRunStorePolicy},
};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    cmp::Ordering as Cmp,
    collections::BinaryHeap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use vortex::{io::runtime::BlockingRuntime, session::VortexSession};

#[path = "weighted_count_spill_runs.rs"]
mod runs;
use runs::{Record, Row, RunMerge};

const FAN_IN: usize = 4;
const MAX_RUNS: usize = 64;
const BLOCK_BYTES: usize = 64 << 10;
const MAX_BUFFER_ROWS: usize = 65_536;
const MIN_MEMORY_BYTES: u64 = 4 << 20;
const SCRATCH_BYTES: u64 = 128 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KeyOrder {
    Text,
    IntegerText { signed: bool },
    TextInteger { signed: bool },
}
impl KeyOrder {
    fn signature(self) -> u8 {
        match self {
            Self::Text => 0,
            Self::IntegerText { signed } => 1 + u8::from(signed),
            Self::TextInteger { signed } => 3 + u8::from(signed),
        }
    }
    fn key(self, value: Option<AggregateIntegerKeyPart>) -> Result<u64> {
        match (self, value) {
            (Self::Text, None) => Ok(0),
            (Self::IntegerText { signed } | Self::TextInteger { signed }, Some(value))
                if signed == value.signed =>
            {
                Ok(value.bits)
            }
            _ => Err(failed("complete integer key admission changed")),
        }
    }
    fn integer(self, bits: u64) -> Option<AggregateIntegerKeyPart> {
        match self {
            Self::Text => None,
            Self::IntegerText { signed } | Self::TextInteger { signed } => {
                Some(AggregateIntegerKeyPart { bits, signed })
            }
        }
    }
    fn compare(self, left: (u64, &[u8]), right: (u64, &[u8])) -> Cmp {
        let text = left.1.cmp(right.1);
        let signed = matches!(
            self,
            Self::IntegerText { signed: true } | Self::TextInteger { signed: true }
        );
        let flip = if signed { 1_u64 << 63 } else { 0 };
        let number = (left.0 ^ flip).cmp(&(right.0 ^ flip));
        match self {
            Self::Text => text,
            Self::IntegerText { .. } => number.then(text),
            Self::TextInteger { .. } => text.then(number),
        }
    }
}

#[derive(Clone)]
pub(super) struct Policy {
    pub workspace: PathBuf,
    pub quota_bytes: u64,
    pub memory_bytes: u64,
    pub max_key_bytes: usize,
    pub cancellation: Arc<AtomicBool>,
}
impl Policy {
    fn check(&self) -> Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            Err(failed("execution cancelled"))
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct Copies {
    encoded: AtomicU64,
    heads: AtomicU64,
}
impl Copies {
    fn add(counter: &AtomicU64, bytes: usize) -> Result<()> {
        let bytes = u64::try_from(bytes).map_err(|_| failed("copy byte count overflowed"))?;
        counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(bytes)
            })
            .map_err(|_| failed("copy byte counter overflowed"))?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Evidence {
    pub source_weight: u64,
    pub source_records: u64,
    pub initial_run_records: u64,
    pub native_records_written: u64,
    pub native_bytes_written: u64,
    pub groups: u64,
    pub runs_written: u64,
    pub runs_validated: u64,
    pub merge_passes: u64,
    pub peak_disk_bytes: u64,
    pub peak_reserved_bytes: u64,
    pub source_text_bytes_copied: u64,
    pub encoded_text_bytes_copied: u64,
    pub merge_head_text_bytes_copied: u64,
    pub selection_text_bytes_copied: u64,
    pub buffer_rows: usize,
    pub buffer_bytes: usize,
    // Minimum/maximum block geometry among actual persisted runs, zero if lazy.
    pub block_rows: usize,
    pub max_block_rows: usize,
    pub max_run_key_bytes: usize,
}
struct Run {
    native: NativeQueryRun,
    level: u8,
    max_key_bytes: usize,
}

fn block_rows_for_key(max_key_bytes: usize) -> usize {
    (BLOCK_BYTES / (max_key_bytes + 32)).clamp(1, 1024)
}

pub(super) struct WeightedCountSpill {
    policy: Policy,
    order: KeyOrder,
    memory: LiveMemoryPool,
    store: Option<QueryRunStore>,
    scratch: Option<MemoryLease>,
    runs: Vec<Run>,
    buffer: Vec<Record>,
    arena: Vec<u8>,
    _buffer_lease: MemoryLease,
    _metadata: MemoryLease,
    work: Arc<MemoryLease>,
    copies: Arc<Copies>,
    selected: BinaryHeap<Ranked>,
    selection_lease: MemoryLease,
    retained: usize,
    evidence: Evidence,
    failed: bool,
}

impl WeightedCountSpill {
    pub(super) fn new(
        policy: Policy,
        memory: LiveMemoryPool,
        order: KeyOrder,
        retained: usize,
    ) -> Result<Self> {
        policy.check()?;
        if !policy.workspace.is_absolute()
            || policy.quota_bytes < 32 << 10
            || policy.memory_bytes < MIN_MEMORY_BYTES
            || policy.memory_bytes != memory.snapshot().limit_bytes
            || policy.max_key_bytes == 0
            || policy.max_key_bytes > BLOCK_BYTES
            || retained == 0
        {
            return Err(failed(
                "requires an absolute explicit workspace, at least 32 KiB quota/4 MiB shared memory, a 1..=65536 byte key bound and positive retained output",
            ));
        }
        let metadata_bytes = policy
            .workspace
            .capacity()
            .checked_mul(3)
            .and_then(|bytes| {
                bytes.checked_add(
                    size_of::<Self>() + size_of::<Copies>() + MAX_RUNS * size_of::<Run>(),
                )
            })
            .ok_or_else(|| failed("metadata capacity overflowed"))?;
        let metadata = memory.reserve(as_u64(metadata_bytes)?)?;
        let arena_capacity = usize::try_from(policy.memory_bytes / 8)
            .map_err(|_| failed("arena capacity exceeds address space"))?;
        let record_capacity = (arena_capacity / size_of::<Record>()).min(MAX_BUFFER_ROWS);
        let buffer_lease = memory.reserve(
            bytes::<Record>(record_capacity)?
                .checked_add(as_u64(arena_capacity)?)
                .ok_or_else(|| failed("buffer reservation overflowed"))?,
        )?;
        // Four native input blocks + old/new block overlap, output conversion,
        // typed offsets and heads/previous keys. This is conservative operator
        // work, not a claim that all provider allocations use HostAllocator.
        let work_bytes = (512 << 10) + 12 * BLOCK_BYTES + 16 * policy.max_key_bytes;
        let work = Arc::new(memory.reserve(as_u64(work_bytes)?)?);
        let scratch = memory.reserve(SCRATCH_BYTES)?;
        // One pending complete group may coexist with a full retained heap.
        // The retained+1 reservation covers both until replacement drops the
        // old key or the pending group is discarded.
        let selection_bytes = retained
            .checked_add(1)
            .and_then(|count| count.checked_mul(size_of::<Ranked>() + policy.max_key_bytes))
            .and_then(|bytes| bytes.checked_add(size_of::<SpilledCountResult>()))
            .ok_or_else(|| failed("selection capacity overflowed"))?;
        let selection_lease = memory.reserve(as_u64(selection_bytes)?)?;
        let mut selected = BinaryHeap::new();
        selected
            .try_reserve_exact(retained)
            .map_err(|_| failed("selection allocation failed"))?;
        if selected.capacity() != retained {
            return Err(failed("selection capacity exceeded reservation"));
        }
        Ok(Self {
            policy,
            order,
            memory,
            store: None,
            scratch: Some(scratch),
            runs: reserved_vec(MAX_RUNS)?,
            buffer: reserved_vec(record_capacity)?,
            arena: reserved_vec(arena_capacity)?,
            _buffer_lease: buffer_lease,
            _metadata: metadata,
            work,
            copies: Arc::new(Copies::default()),
            selected,
            selection_lease,
            retained,
            evidence: Evidence {
                buffer_rows: record_capacity,
                buffer_bytes: arena_capacity,
                ..Evidence::default()
            },
            failed: false,
        })
    }

    fn check(&self) -> Result<()> {
        self.policy.check()?;
        if self.failed {
            Err(failed("attempt failed; cleanup is required"))
        } else {
            Ok(())
        }
    }

    pub(super) fn push(
        &mut self,
        integer: Option<AggregateIntegerKeyPart>,
        text: &str,
        weight: u64,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        let result = self.push_inner(integer, text, weight, runtime, session);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn push_inner(
        &mut self,
        integer: Option<AggregateIntegerKeyPart>,
        text: &str,
        weight: u64,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        self.check()?;
        let number = self.order.key(integer)?;
        if weight == 0 || text.len() > self.policy.max_key_bytes {
            return Err(failed(
                "requires positive weights and text within the admitted byte bound",
            ));
        }
        let total = self
            .evidence
            .source_weight
            .checked_add(weight)
            .ok_or_else(|| failed("source weight overflowed"))?;
        let records = self
            .evidence
            .source_records
            .checked_add(1)
            .ok_or_else(|| failed("source record counter overflowed"))?;
        if self.buffer.len() == self.buffer.capacity()
            || self.arena.capacity() - self.arena.len() < text.len()
        {
            self.flush(runtime, session)?;
        }
        let offset = self.arena.len();
        self.arena.extend_from_slice(text.as_bytes());
        self.buffer.push(Record {
            number,
            offset,
            len: text.len(),
            weight,
        });
        self.evidence.source_text_bytes_copied = self
            .evidence
            .source_text_bytes_copied
            .checked_add(as_u64(text.len())?)
            .ok_or_else(|| failed("source copy counter overflowed"))?;
        self.evidence.source_weight = total;
        self.evidence.source_records = records;
        Ok(())
    }

    fn ensure_store(&mut self) -> Result<()> {
        if self.store.is_none() {
            // Never borrow a different operator's recovery marker.
            let policy = QueryRunStorePolicy::weighted_utf8_count(
                self.policy.workspace.clone(),
                self.policy.quota_bytes,
                Arc::clone(&self.policy.cancellation),
            );
            self.store = Some(QueryRunStore::new(
                policy,
                self.memory.clone(),
                self.scratch
                    .take()
                    .ok_or_else(|| failed("store scratch missing"))?,
            )?);
        }
        Ok(())
    }
    fn sort_buffer(&mut self) {
        let order = self.order;
        let arena = &self.arena;
        self.buffer.sort_unstable_by(|left, right| {
            order.compare(
                (left.number, left.text(arena)),
                (right.number, right.text(arena)),
            )
        });
    }
    fn coalesce_buffer(&mut self) -> Result<()> {
        let mut unique = 0_usize;
        for index in 0..self.buffer.len() {
            if index.is_multiple_of(4096) {
                self.policy.check()?;
            }
            let row = self.buffer[index];
            if unique != 0
                && self
                    .order
                    .compare(
                        (
                            self.buffer[unique - 1].number,
                            self.buffer[unique - 1].text(&self.arena),
                        ),
                        (row.number, row.text(&self.arena)),
                    )
                    .is_eq()
            {
                self.buffer[unique - 1].weight = self.buffer[unique - 1]
                    .weight
                    .checked_add(row.weight)
                    .ok_or_else(|| failed("complete buffer group weight overflowed"))?;
            } else {
                self.buffer[unique] = row;
                unique += 1;
            }
        }
        self.buffer.truncate(unique);
        Ok(())
    }
    fn record_write(&mut self, run: &Run, initial: bool) -> Result<()> {
        let rows = run.native.rows;
        let bytes = run.native.bytes;
        self.evidence.native_records_written = self
            .evidence
            .native_records_written
            .checked_add(rows)
            .ok_or_else(|| failed("native record counter overflowed"))?;
        self.evidence.native_bytes_written = self
            .evidence
            .native_bytes_written
            .checked_add(bytes)
            .ok_or_else(|| failed("native byte counter overflowed"))?;
        if initial {
            self.evidence.initial_run_records = self
                .evidence
                .initial_run_records
                .checked_add(rows)
                .ok_or_else(|| failed("initial run record counter overflowed"))?;
        }
        self.evidence.block_rows = if self.evidence.block_rows == 0 {
            run.native.block_rows
        } else {
            self.evidence.block_rows.min(run.native.block_rows)
        };
        self.evidence.max_block_rows = self.evidence.max_block_rows.max(run.native.block_rows);
        self.evidence.max_run_key_bytes = self.evidence.max_run_key_bytes.max(run.max_key_bytes);
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
        self.sort_buffer();
        self.policy.check()?;
        self.coalesce_buffer()?;
        if self.runs.len() == MAX_RUNS {
            return Err(failed("run registry exhausted"));
        }
        self.ensure_store()?;
        // The policy reserves for the largest allowed key before any work.
        // Actual bounded-buffer keys determine this run's persisted geometry.
        let max_key_bytes = self.buffer.iter().map(|row| row.len).max().unwrap_or(0);
        let spec = self.spec(as_u64(self.buffer.len())?, max_key_bytes)?;
        let signature = self.order.signature();
        let native = self.store.as_mut().expect("admitted store").write_arrays(
            &spec,
            self.buffer.chunks(spec.block_rows).map(|rows| {
                runs::array(
                    rows.iter()
                        .map(|row| (row.number, row.text(&self.arena), row.weight)),
                    signature,
                    &self.copies,
                )
            }),
            runtime,
            session,
            &self.work,
        )?;
        let run = Run {
            native,
            level: 0,
            max_key_bytes,
        };
        self.record_write(&run, true)?;
        self.buffer.clear();
        self.arena.clear();
        self.runs.push(run);
        loop {
            let Some(level) = (0..=u8::MAX)
                .find(|level| self.runs.iter().filter(|run| run.level == *level).count() >= FAN_IN)
            else {
                return Ok(());
            };
            self.compact(level, runtime, session)?;
        }
    }
    fn spec(&self, rows: u64, max_key_bytes: usize) -> Result<QueryRunSpec> {
        if max_key_bytes > self.policy.max_key_bytes {
            return Err(failed("run geometry exceeds admitted key bound"));
        }
        let block_rows = block_rows_for_key(max_key_bytes);
        let metadata_bytes = rows
            .div_ceil(as_u64(block_rows)?)
            .checked_mul(1024)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| failed("run metadata overflowed"))?;
        Ok(QueryRunSpec {
            dtype: runs::dtype(),
            rows,
            block_rows,
            metadata_bytes,
        })
    }
    fn compact(
        &mut self,
        level: u8,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        self.check()?;
        let next_level = level
            .checked_add(1)
            .ok_or_else(|| failed("compaction level exhausted"))?;
        let mut inputs = reserved_vec(FAN_IN)?;
        for _ in 0..FAN_IN {
            let index = self
                .runs
                .iter()
                .position(|run| run.level == level)
                .ok_or_else(|| failed("compaction input missing"))?;
            inputs.push(self.runs.remove(index));
        }
        let rows = inputs
            .iter()
            .try_fold(0_u64, |rows, run| rows.checked_add(run.native.rows))
            .ok_or_else(|| failed("compaction rows overflowed"))?;
        let max_key_bytes = inputs
            .iter()
            .map(|run| run.max_key_bytes)
            .max()
            .unwrap_or(0);
        let spec = self.spec(rows, max_key_bytes)?;
        let mut merge = RunMerge::new(
            inputs.iter(),
            self.store.as_ref().expect("runs have store"),
            Arc::clone(&self.work),
            self.policy.clone(),
            self.order,
            Arc::clone(&self.copies),
            runtime,
            session,
        )?;
        let native = self.store.as_mut().expect("runs have store").write_arrays(
            &spec,
            runs::blocks(
                &mut merge,
                spec.block_rows,
                self.order.signature(),
                &self.copies,
            ),
            runtime,
            session,
            &self.work,
        )?;
        merge.validate()?;
        drop(merge);
        let run = Run {
            native,
            level: next_level,
            max_key_bytes,
        };
        self.record_write(&run, false)?;
        for run in &inputs {
            self.store
                .as_mut()
                .expect("runs have store")
                .remove(&run.native)?;
        }
        drop(inputs);
        self.runs.push(run);
        self.evidence.merge_passes = self
            .evidence
            .merge_passes
            .checked_add(1)
            .ok_or_else(|| failed("merge counter overflowed"))?;
        Ok(())
    }

    /// Complete-key partitions have consumed the entire source and proved their
    /// disjoint row/group totals. Each partition now offers its exact final K.
    /// No run buffer is populated and no workspace/store is opened on this path.
    pub(super) fn finish_fitted(
        mut self,
        source_rows: u64,
        groups: u64,
        visit: impl FnOnce(&mut dyn FnMut(&str, u64) -> Result<()>) -> Result<()>,
    ) -> Result<SpilledCountResult> {
        self.check()?;
        if self.order != KeyOrder::Text
            || self.evidence.source_weight != 0
            || !self.buffer.is_empty()
            || !self.runs.is_empty()
            || groups > source_rows
            || (groups == 0) != (source_rows == 0)
        {
            return Err(failed(
                "fitted partition finalization changed its complete-source contract",
            ));
        }
        let mut candidates = 0_u64;
        let mut candidate_weight = 0_u64;
        visit(&mut |text, weight| {
            self.policy.check()?;
            if weight == 0 || text.len() > self.policy.max_key_bytes {
                return Err(failed("fitted partition key/weight exceeded admission"));
            }
            candidates = candidates
                .checked_add(1)
                .ok_or_else(|| failed("fitted candidate count overflowed"))?;
            candidate_weight = candidate_weight
                .checked_add(weight)
                .ok_or_else(|| failed("fitted candidate weight overflowed"))?;
            if candidates > groups || candidate_weight > source_rows {
                return Err(failed("fitted candidates exceed complete partition totals"));
            }
            let candidate = Ranked {
                row: Row {
                    number: 0,
                    text: copy_text(text.as_bytes())?,
                    weight,
                },
                order: self.order,
            };
            self.evidence.selection_text_bytes_copied = self
                .evidence
                .selection_text_bytes_copied
                .checked_add(as_u64(text.len())?)
                .ok_or_else(|| failed("fitted selection copy counter overflowed"))?;
            if self.selected.len() < self.retained {
                self.selected.push(candidate);
            } else if self.selected.peek().is_some_and(|worst| candidate < *worst) {
                *self.selected.peek_mut().expect("positive retained") = candidate;
            }
            Ok(())
        })?;
        if self.selected.len()
            != self.retained.min(
                usize::try_from(groups)
                    .map_err(|_| failed("fitted group count exceeds address space"))?,
            )
        {
            return Err(failed(
                "fitted partition candidates omitted retained output",
            ));
        }
        self.policy.check()?;
        self.evidence.source_weight = source_rows;
        // Every complete group was counted once in its owning partition. Only
        // the final candidate union reaches the bounded heap above.
        self.evidence.source_records = groups;
        self.evidence.groups = groups;
        self.evidence.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
        Ok(SpilledCountResult {
            selected: self.selected.into_sorted_vec(),
            order: self.order,
            evidence: self.evidence,
            lease: self.selection_lease,
        })
    }

    pub(super) fn finish(
        mut self,
        runtime: &impl BlockingRuntime,
        session: &VortexSession,
    ) -> Result<SpilledCountResult> {
        self.check()?;
        if self.runs.is_empty() {
            self.sort_buffer();
            let rows = self
                .buffer
                .iter()
                .map(|row| Ok((row.number, row.text(&self.arena), row.weight)));
            select_borrowed(
                rows,
                &self.policy,
                self.order,
                &mut self.selected,
                self.retained,
                &mut self.evidence,
            )?;
        } else {
            self.flush(runtime, session)?;
            while self.runs.len() > FAN_IN {
                self.runs
                    .sort_unstable_by_key(|run| (run.level, run.native.rows));
                let level = self.runs[FAN_IN - 1].level;
                for run in self.runs.iter_mut().take(FAN_IN) {
                    run.level = level;
                }
                self.compact(level, runtime, session)?;
            }
            let mut merge = RunMerge::new(
                self.runs.iter(),
                self.store.as_ref().expect("runs have store"),
                Arc::clone(&self.work),
                self.policy.clone(),
                self.order,
                Arc::clone(&self.copies),
                runtime,
                session,
            )?;
            let mut selection = Selection::new(
                &self.policy,
                self.order,
                &mut self.selected,
                self.retained,
                &mut self.evidence,
            );
            for row in merge.by_ref() {
                let row = row?;
                selection.push(row.number, &row.text, row.weight)?;
            }
            selection.finish()?;
            merge.validate()?;
            drop(merge);
            let snapshot = self.store.as_ref().expect("runs have store").snapshot();
            self.evidence.runs_written = snapshot.runs_written;
            self.evidence.runs_validated = snapshot.runs_validated;
            self.evidence.peak_disk_bytes = snapshot.peak_disk_bytes;
            self.store.as_mut().expect("runs have store").cleanup()?;
        }
        self.policy.check()?;
        self.evidence.encoded_text_bytes_copied = self.copies.encoded.load(Ordering::Relaxed);
        self.evidence.merge_head_text_bytes_copied = self.copies.heads.load(Ordering::Relaxed);
        self.evidence.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
        Ok(SpilledCountResult {
            selected: self.selected.into_sorted_vec(),
            order: self.order,
            evidence: self.evidence,
            lease: self.selection_lease,
        })
    }
}

struct Ranked {
    row: Row,
    order: KeyOrder,
}
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Cmp> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Cmp {
        other.row.weight.cmp(&self.row.weight).then_with(|| {
            self.order.compare(
                (self.row.number, &self.row.text),
                (other.row.number, &other.row.text),
            )
        })
    }
}
struct Selection<'a> {
    policy: &'a Policy,
    order: KeyOrder,
    heap: &'a mut BinaryHeap<Ranked>,
    retained: usize,
    evidence: &'a mut Evidence,
    pending: Option<Row>,
    total: u64,
    ordinal: usize,
}
impl<'a> Selection<'a> {
    fn new(
        policy: &'a Policy,
        order: KeyOrder,
        heap: &'a mut BinaryHeap<Ranked>,
        retained: usize,
        evidence: &'a mut Evidence,
    ) -> Self {
        Self {
            policy,
            order,
            heap,
            retained,
            evidence,
            pending: None,
            total: 0,
            ordinal: 0,
        }
    }
    fn push(&mut self, number: u64, text: &[u8], weight: u64) -> Result<()> {
        if self.ordinal.is_multiple_of(4096) {
            self.policy.check()?;
        }
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or_else(|| failed("merge record counter overflowed"))?;
        if weight == 0 || text.len() > self.policy.max_key_bytes {
            return Err(failed("merged weight/key bounds changed"));
        }
        std::str::from_utf8(text).map_err(|_| failed("merged UTF8 is invalid"))?;
        self.total = self
            .total
            .checked_add(weight)
            .ok_or_else(|| failed("merged source weight overflowed"))?;
        if let Some(pending) = self.pending.as_mut() {
            match self
                .order
                .compare((pending.number, &pending.text), (number, text))
            {
                Cmp::Greater => return Err(failed("complete key order regressed")),
                Cmp::Equal => {
                    pending.weight = pending
                        .weight
                        .checked_add(weight)
                        .ok_or_else(|| failed("complete group weight overflowed"))?;
                    return Ok(());
                }
                Cmp::Less => self.complete()?,
            }
        }
        self.evidence.selection_text_bytes_copied = self
            .evidence
            .selection_text_bytes_copied
            .checked_add(as_u64(text.len())?)
            .ok_or_else(|| failed("selection copy counter overflowed"))?;
        self.pending = Some(Row {
            number,
            text: copy_text(text)?,
            weight,
        });
        Ok(())
    }
    fn complete(&mut self) -> Result<()> {
        let Some(row) = self.pending.take() else {
            return Ok(());
        };
        self.evidence.groups = self
            .evidence
            .groups
            .checked_add(1)
            .ok_or_else(|| failed("group counter overflowed"))?;
        let candidate = Ranked {
            row,
            order: self.order,
        };
        if self.heap.len() < self.retained {
            self.heap.push(candidate);
        } else if self.heap.peek().is_some_and(|worst| candidate < *worst) {
            *self.heap.peek_mut().expect("positive retained") = candidate;
        }
        Ok(())
    }
    fn finish(mut self) -> Result<()> {
        self.complete()?;
        if self.total != self.evidence.source_weight {
            return Err(failed("complete merge source weights differ"));
        }
        self.policy.check()
    }
}
fn select_borrowed<'a>(
    rows: impl Iterator<Item = Result<(u64, &'a [u8], u64)>>,
    policy: &Policy,
    order: KeyOrder,
    heap: &mut BinaryHeap<Ranked>,
    retained: usize,
    evidence: &mut Evidence,
) -> Result<()> {
    let mut selection = Selection::new(policy, order, heap, retained, evidence);
    for row in rows {
        let (number, text, weight) = row?;
        selection.push(number, text, weight)?;
    }
    selection.finish()
}

pub(super) struct SpilledCountResult {
    selected: Vec<Ranked>,
    order: KeyOrder,
    pub evidence: Evidence,
    lease: MemoryLease,
}
impl SpilledCountResult {
    pub(super) fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }
    pub(super) fn visit(
        &self,
        offset: usize,
        mut visit: impl FnMut(Option<AggregateIntegerKeyPart>, &str, u64) -> Result<()>,
    ) -> Result<()> {
        for ranked in self.selected.iter().skip(offset) {
            visit(
                self.order.integer(ranked.row.number),
                std::str::from_utf8(&ranked.row.text)
                    .map_err(|_| failed("selected UTF8 invalid"))?,
                ranked.row.weight,
            )?;
        }
        Ok(())
    }
}
fn copy_text(text: &[u8]) -> Result<Vec<u8>> {
    let mut copy = reserved_vec(text.len())?;
    copy.extend_from_slice(text);
    Ok(copy)
}
fn bytes<T>(capacity: usize) -> Result<u64> {
    as_u64(
        capacity
            .checked_mul(size_of::<T>())
            .ok_or_else(|| failed("owned capacity overflowed"))?,
    )
}
fn as_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| failed("byte/row count overflowed"))
}
fn reserved_vec<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| failed("owned allocation failed"))?;
    if values.capacity() != capacity {
        return Err(failed("owned allocation exceeded reservation"));
    }
    Ok(values)
}
fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native weighted complete-key COUNT spill {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "weighted_count_spill_tests.rs"]
mod tests;
