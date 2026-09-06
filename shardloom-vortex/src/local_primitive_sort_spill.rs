//! Query-owned exact numeric sort runs. Vortex remains the persistence provider;
//! the operator owns admission, byte quotas, bounded fan-in, and exact cleanup.

#[cfg(feature = "vortex-write")]
use super::query_run_store;
pub(super) use super::query_run_store::RunSourceGeneration as SortSourceGeneration;
use super::query_run_store::{
    MARKER_BYTE_RESERVATION, MAX_LIVE_RUNS, NativeQueryRun, QueryRunReader, QueryRunSpec,
    QueryRunStore, QueryRunStorePolicy,
};
use super::{
    LocalVortexRuntime, Result, ShardLoomError, SortRowCandidate, StatValue,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexSortTiePolicy,
    predicate_field_expr, row_export_columns_from_chunk,
};
use crate::{VortexSortSpillPolicy, VortexSortSpillReport};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
#[cfg(feature = "vortex-write")]
use std::path::Path;
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    sync::{Arc, atomic::Ordering},
};
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{DType, Nullability, PType},
        validity::Validity,
    },
    session::VortexSession,
};

const MERGE_FAN_IN: usize = 8;
const MIN_BLOCK_ROWS: usize = 256;
const MAX_BLOCK_ROWS: usize = 1024;
// Each reader can retain the provider's 64 KiB initial footer read, including
// any covered payload. Run metadata itself remains separately reserved.
const MERGE_READER_BYTES: u64 = 64 * 1024;
const MERGE_FIXED_BYTES: u64 = 16 * 1024;
// Covers eight row queues, one scalar conversion, and overlapping input/output
// arrays at the sequential Flat writer. Geometry tests check the type-size sum.
const MERGE_BYTES_PER_BLOCK_ROW: u64 = 1024;
const RUN_METADATA_BYTES_PER_BLOCK: u64 = 1024;
const RUN_METADATA_BASE_BYTES: u64 = 4096;
#[cfg(test)]
type BeforeMaterializationHook = Box<dyn FnOnce()>;

#[cfg(test)]
thread_local! {
    static BEFORE_MATERIALIZATION: std::cell::RefCell<Option<BeforeMaterializationHook>> = const {
        std::cell::RefCell::new(None)
    };
}

#[cfg(test)]
pub(super) fn before_materialization_test_hook() {
    let hook = BEFORE_MATERIALIZATION.with(|hook| hook.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn admit(
    request: &VortexQueryPrimitiveRequest,
    dtype: &vortex::array::dtype::DType,
    memory_budget_bytes: u64,
    limit: usize,
) -> Result<Option<NumericSortSpill>> {
    let Some(sort) = request.sort_rows.as_ref() else {
        return Ok(None);
    };
    let Some(policy) = sort.spill.as_ref() else {
        return Ok(None);
    };
    if !cfg!(feature = "vortex-write")
        || request.kind != VortexQueryPrimitiveKind::SortRows
        || request.predicate.is_some()
        || sort.order_by.len() != 1
        || policy.memory_bytes > memory_budget_bytes
    {
        return Err(spill_error(
            "native sort spill requires the writer feature, one integer key, no predicate, and operator memory within the query budget",
        ));
    }
    let (_, key_dtype) = predicate_field_expr(dtype, &sort.order_by[0].column, request.kind)?;
    let signed = match key_dtype {
        DType::Primitive(PType::I64, Nullability::NonNullable) => true,
        DType::Primitive(PType::U64, Nullability::NonNullable) => false,
        _ => {
            return Err(spill_error(
                "native sort spill admits only non-null Int64 or UInt64 keys",
            ));
        }
    };
    NumericSortSpill::new(
        policy,
        sort.order_by[0].descending,
        sort.tie_policy,
        signed,
        limit,
    )
    .map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SpillRow {
    key: u64,
    tie: u64,
    source: u64,
}

#[derive(Debug)]
struct Run {
    native: NativeQueryRun,
    level: u32,
}

/// Reservation coverage is deliberately scoped to owned sort candidates,
/// conversion/merge batches, run metadata, and checksum scratch. Source scan,
/// provider allocations bypassing their allocator, and final payload output are
/// separate resource scopes; this counter is not process RSS.
pub(super) struct NumericSortSpill {
    policy: VortexSortSpillPolicy,
    store: QueryRunStore,
    runs: Vec<Run>,
    report: VortexSortSpillReport,
    memory: LiveMemoryPool,
    _state: MemoryLease,
    merge: Arc<MemoryLease>,
    capacity_rows: usize,
    block_rows: usize,
    merge_fan_in: usize,
    descending: bool,
    tie_policy: VortexSortTiePolicy,
    signed: bool,
}

impl NumericSortSpill {
    pub(super) fn new(
        policy: &VortexSortSpillPolicy,
        descending: bool,
        tie_policy: VortexSortTiePolicy,
        signed: bool,
        limit: usize,
    ) -> Result<Self> {
        check_cancelled(policy)?;
        if !cfg!(unix)
            || !policy.workspace.is_absolute()
            || policy.memory_bytes < 1024 * 1024
            || policy.quota_bytes < MARKER_BYTE_RESERVATION
        {
            return Err(spill_error(
                "native sort spill requires Unix file identity, at least 32 KiB disk quota, and at least 1 MiB operator memory",
            ));
        }
        let memory = LiveMemoryPool::new(policy.memory_bytes)?;
        let state = memory.reserve(policy.memory_bytes / 4)?;
        let merge = memory.reserve(policy.memory_bytes / 2)?;
        let scratch = memory.reserve(policy.memory_bytes / 8)?;
        let (block_rows, merge_fan_in) = merge_geometry(merge.bytes())?;
        let capacity_rows = usize::try_from(state.bytes() / 256).unwrap_or(usize::MAX);
        if capacity_rows < block_rows || limit > capacity_rows {
            return Err(spill_error(
                "requested sort output exceeds admitted retained-row capacity",
            ));
        }
        if !matches!(
            tie_policy,
            VortexSortTiePolicy::First | VortexSortTiePolicy::Last
        ) {
            return Err(spill_error(
                "native numeric sort spill does not admit tie expansion",
            ));
        }
        let store = QueryRunStore::new(
            QueryRunStorePolicy::numeric_sort(policy),
            memory.clone(),
            scratch,
        )?;
        let this = Self {
            policy: policy.clone(),
            store,
            runs: Vec::with_capacity(MAX_LIVE_RUNS),
            report: VortexSortSpillReport {
                workspace: policy.workspace.clone(),
                quota_bytes: policy.quota_bytes,
                memory_bytes: policy.memory_bytes,
                peak_reserved_bytes: 0,
                peak_disk_bytes: MARKER_BYTE_RESERVATION,
                runs_written: 0,
                runs_validated: 0,
                merge_passes: 0,
                run_block_rows: block_rows,
                merge_fan_in,
                max_open_runs: 0,
                owned_cleanup_completed: false,
            },
            memory,
            _state: state,
            merge: Arc::new(merge),
            capacity_rows,
            block_rows,
            merge_fan_in,
            descending,
            tie_policy,
            signed,
        };
        Ok(this)
    }

    pub(super) fn capacity_rows(&self) -> usize {
        self.capacity_rows
    }

    pub(super) fn flush_if_full(
        &mut self,
        candidates: &mut Vec<SortRowCandidate>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        check_cancelled(&self.policy)?;
        if candidates.len() >= self.capacity_rows {
            self.flush(candidates, runtime, session)?;
        }
        Ok(())
    }

    fn flush(
        &mut self,
        candidates: &mut Vec<SortRowCandidate>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        if candidates.len() > self.capacity_rows {
            return Err(spill_error(
                "sort candidates exceeded their reserved capacity",
            ));
        }
        let mut rows = candidates
            .iter()
            .map(|candidate| self.encode(candidate))
            .collect::<Result<Vec<_>>>()?;
        rows.sort_unstable();
        let count = u64::try_from(rows.len()).map_err(|_| spill_error("sort run row overflow"))?;
        let run = self.write_run(rows.into_iter().map(Ok), count, runtime, session)?;
        self.runs.push(run);
        candidates.clear();
        while self.runs.len() >= 2
            && self.runs[self.runs.len() - 1].level == self.runs[self.runs.len() - 2].level
        {
            self.compact(2, runtime, session)?;
        }
        Ok(())
    }

    fn encode(&self, candidate: &SortRowCandidate) -> Result<SpillRow> {
        if candidate.values.len() != 1 || candidate.source_partition_index != 0 {
            return Err(spill_error("sort spill requires one key and one source"));
        }
        let key = match (self.signed, &candidate.values[0]) {
            (true, StatValue::Int64(value)) => u64::from_ne_bytes(value.to_ne_bytes()) ^ (1 << 63),
            (false, StatValue::UInt64(value)) => *value,
            _ => {
                return Err(spill_error(
                    "sort spill key must remain a non-null integer of its declared type",
                ));
            }
        };
        let ordinal =
            u64::try_from(candidate.ordinal).map_err(|_| spill_error("sort ordinal overflow"))?;
        Ok(SpillRow {
            key: if self.descending { !key } else { key },
            tie: if self.tie_policy == VortexSortTiePolicy::Last {
                !ordinal
            } else {
                ordinal
            },
            source: u64::try_from(candidate.source_ordinal)
                .map_err(|_| spill_error("source ordinal overflow"))?,
        })
    }

    fn metadata_for_rows(rows: u64, block_rows: usize) -> Result<u64> {
        rows.div_ceil(block_rows as u64)
            .checked_mul(RUN_METADATA_BYTES_PER_BLOCK)
            .and_then(|bytes| bytes.checked_add(RUN_METADATA_BASE_BYTES))
            .ok_or_else(|| spill_error("sort run metadata reservation overflow"))
    }

    fn write_run(
        &mut self,
        rows: impl Iterator<Item = Result<SpillRow>>,
        count: u64,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<Run> {
        check_cancelled(&self.policy)?;
        let mut rows = rows;
        let policy = self.policy.clone();
        let block_rows = self.block_rows;
        let blocks = std::iter::from_fn(move || {
            if let Err(error) = check_cancelled(&policy) {
                return Some(Err(error));
            }
            let mut block = Vec::with_capacity(block_rows);
            while block.len() < block_rows {
                match rows.next() {
                    Some(Ok(row)) => block.push(row),
                    Some(Err(error)) => return Some(Err(error)),
                    None => break,
                }
            }
            (!block.is_empty()).then(|| Ok(rows_array(&block)))
        });
        let native = self.store.write_arrays(
            &QueryRunSpec {
                dtype: run_dtype(),
                rows: count,
                block_rows,
                metadata_bytes: Self::metadata_for_rows(count, block_rows)?,
            },
            blocks,
            runtime,
            session,
            &self.merge,
        )?;
        Ok(Run { native, level: 0 })
    }

    fn open_merge<'runtime>(
        &mut self,
        runtime: &'runtime LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<RunMerge<'runtime>> {
        self.report.max_open_runs = self.report.max_open_runs.max(self.runs.len());
        let readers = self
            .runs
            .iter()
            .map(|run| RunReader::open(run, &self.store, Arc::clone(&self.merge), runtime, session))
            .collect::<Result<Vec<_>>>()?;
        RunMerge::new(readers, self.policy.clone(), runtime, self.merge_fan_in)
    }

    fn compact(
        &mut self,
        fan_in: usize,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        if !(2..=self.merge_fan_in).contains(&fan_in) || fan_in > self.runs.len() {
            return Err(spill_error("invalid native sort merge fan-in"));
        }
        let old = self.runs.split_off(self.runs.len() - fan_in);
        let count = old.iter().try_fold(0_u64, |sum, run| {
            sum.checked_add(run.native.rows)
                .ok_or_else(|| spill_error("merged sort run row overflow"))
        })?;
        let level = old
            .iter()
            .map(|run| run.level)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .filter(|level| *level <= 64)
            .ok_or_else(|| spill_error("native sort merge level overflow"))?;
        let readers = old
            .iter()
            .map(|run| RunReader::open(run, &self.store, Arc::clone(&self.merge), runtime, session))
            .collect::<Result<Vec<_>>>()?;
        let merge = RunMerge::new(readers, self.policy.clone(), runtime, self.merge_fan_in)?;
        self.report.max_open_runs = self.report.max_open_runs.max(fan_in + 1);
        let mut output = self.write_run(merge, count, runtime, session)?;
        output.level = level;
        for run in old {
            self.remove_run(&run)?;
        }
        self.runs.push(output);
        self.report.merge_passes += 1;
        Ok(())
    }

    fn remove_run(&mut self, run: &Run) -> Result<()> {
        self.store.remove(&run.native)
    }

    pub(super) fn finish(
        mut self,
        candidates: &mut Vec<SortRowCandidate>,
        offset: usize,
        limit: usize,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<(Vec<SortRowCandidate>, VortexSortSpillReport)> {
        self.flush(candidates, runtime, session)?;
        while self.runs.len() > self.merge_fan_in {
            self.compact(self.merge_fan_in, runtime, session)?;
        }
        let mut merge = self.open_merge(runtime, session)?;
        let mut selected = Vec::with_capacity(limit);
        let end = offset
            .checked_add(limit)
            .ok_or_else(|| spill_error("sort offset plus limit overflow"))?;
        for index in 0..end {
            let Some(row) = merge.next() else {
                break;
            };
            let row = row?;
            if index >= offset {
                let ordinal = if self.tie_policy == VortexSortTiePolicy::Last {
                    !row.tie
                } else {
                    row.tie
                };
                selected.push(SortRowCandidate {
                    ordinal: usize::try_from(ordinal)
                        .map_err(|_| spill_error("sort ordinal overflow"))?,
                    source_partition_index: 0,
                    source_ordinal: usize::try_from(row.source)
                        .map_err(|_| spill_error("source ordinal overflow"))?,
                    values: Vec::new(),
                });
            }
        }
        merge.validate_sources()?;
        drop(merge);
        self.report.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
        let storage = self.store.snapshot();
        self.report.peak_disk_bytes = storage.peak_disk_bytes;
        self.report.runs_written = storage.runs_written;
        self.report.runs_validated = storage.runs_validated;
        self.store.cleanup()?;
        self.report.owned_cleanup_completed = true;
        Ok((selected, self.report.clone()))
    }
}

fn rows_array(rows: &[SpillRow]) -> ArrayRef {
    StructArray::new(
        ["key", "tie", "source"].into(),
        [
            rows.iter()
                .map(|row| row.key)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.tie)
                .collect::<PrimitiveArray>()
                .into_array(),
            rows.iter()
                .map(|row| row.source)
                .collect::<PrimitiveArray>()
                .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .into_array()
}

fn run_dtype() -> vortex::array::dtype::DType {
    rows_array(&[]).dtype().clone()
}

fn merge_geometry(reserved_bytes: u64) -> Result<(usize, usize)> {
    for fan_in in [MERGE_FAN_IN, 4, 2] {
        let fixed = MERGE_FIXED_BYTES + fan_in as u64 * MERGE_READER_BYTES;
        let available_rows = reserved_bytes.saturating_sub(fixed) / MERGE_BYTES_PER_BLOCK_ROW;
        for block_rows in [MAX_BLOCK_ROWS, 512, MIN_BLOCK_ROWS] {
            if block_rows as u64 <= available_rows {
                return Ok((block_rows, fan_in));
            }
        }
    }
    Err(spill_error(
        "sort merge reservation cannot admit its minimum native block",
    ))
}

struct RunReader {
    reader: QueryRunReader,
    rows: VecDeque<SpillRow>,
    remaining: u64,
    previous: Option<SpillRow>,
}
impl RunReader {
    fn open(
        run: &Run,
        store: &QueryRunStore,
        work: Arc<MemoryLease>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<Self> {
        let reader = store.open(&run.native, &run_dtype(), runtime, session, work)?;
        Ok(Self {
            reader,
            rows: VecDeque::with_capacity(run.native.block_rows),
            remaining: run.native.rows,
            previous: None,
        })
    }

    fn refill(&mut self, runtime: &LocalVortexRuntime) -> Result<()> {
        let block = self
            .reader
            .next_block(runtime)?
            .ok_or_else(|| spill_error("native sort run block returned no rows"))?;
        let array = block.array();
        let columns =
            row_export_columns_from_chunk(array, &["key".into(), "tie".into(), "source".into()])?;
        for index in 0..array.len() {
            let mut values = [0_u64; 3];
            for (column, value) in columns.iter().zip(&mut values) {
                let Some(StatValue::UInt64(number)) = column.get(index) else {
                    return Err(spill_error("native sort run contains an invalid primitive"));
                };
                *value = *number;
            }
            self.rows.push_back(SpillRow {
                key: values[0],
                tie: values[1],
                source: values[2],
            });
        }
        self.reader.validate()?;
        Ok(())
    }

    fn next_row(&mut self, runtime: &LocalVortexRuntime) -> Result<Option<SpillRow>> {
        if self.rows.is_empty() && self.remaining != 0 {
            self.refill(runtime)?;
        }
        let Some(row) = self.rows.pop_front() else {
            self.reader.validate()?;
            if self.remaining != 0 {
                return Err(spill_error(
                    "native sort run ended before its declared row count",
                ));
            }
            return Ok(None);
        };
        if self.remaining == 0 || self.previous.is_some_and(|previous| previous > row) {
            return Err(spill_error("native sort run length or order is invalid"));
        }
        self.remaining -= 1;
        self.previous = Some(row);
        Ok(Some(row))
    }
}

struct RunMerge<'runtime> {
    readers: Vec<RunReader>,
    heads: BinaryHeap<Reverse<(SpillRow, usize)>>,
    policy: VortexSortSpillPolicy,
    failed: bool,
    runtime: &'runtime LocalVortexRuntime,
}
impl<'runtime> RunMerge<'runtime> {
    fn validate_sources(&self) -> Result<()> {
        for reader in &self.readers {
            reader.reader.validate()?;
        }
        Ok(())
    }

    fn new(
        mut readers: Vec<RunReader>,
        policy: VortexSortSpillPolicy,
        runtime: &'runtime LocalVortexRuntime,
        fan_in: usize,
    ) -> Result<Self> {
        if readers.len() > fan_in || fan_in > MERGE_FAN_IN {
            return Err(spill_error(
                "native sort merge exceeded its file-handle bound",
            ));
        }
        let mut heads = BinaryHeap::with_capacity(MERGE_FAN_IN);
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = reader.next_row(runtime)? {
                heads.push(Reverse((row, index)));
            }
        }
        Ok(Self {
            readers,
            heads,
            policy,
            failed: false,
            runtime,
        })
    }
}
impl Iterator for RunMerge<'_> {
    type Item = Result<SpillRow>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Err(error) = check_cancelled(&self.policy) {
            self.failed = true;
            return Some(Err(error));
        }
        let Reverse((row, index)) = self.heads.pop()?;
        match self.readers[index].next_row(self.runtime) {
            Ok(Some(next)) => self.heads.push(Reverse((next, index))),
            Ok(None) => {}
            Err(error) => {
                self.failed = true;
                return Some(Err(error));
            }
        }
        Some(Ok(row))
    }
}

fn check_cancelled(policy: &VortexSortSpillPolicy) -> Result<()> {
    if policy.cancellation.load(Ordering::Acquire) {
        Err(spill_error("native sort execution cancelled"))
    } else {
        Ok(())
    }
}
fn spill_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}
/// The public numeric-sort recovery policy keeps its existing namespace and
/// marker schema while delegating storage validation and cleanup.
#[cfg(feature = "vortex-write")]
pub(crate) fn recover(policy: &VortexSortSpillPolicy, directory: &Path) -> Result<()> {
    query_run_store::recover(&QueryRunStorePolicy::numeric_sort(policy), directory)
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_sort_spill_tests.rs"]
mod tests;
