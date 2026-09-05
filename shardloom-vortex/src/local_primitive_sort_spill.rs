//! Query-owned exact numeric sort runs. Vortex remains the persistence provider;
//! the operator owns admission, byte quotas, bounded fan-in, and exact cleanup.

use super::{
    LocalVortexRuntime, Result, ShardLoomError, SortRowCandidate, StatValue,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexSortTiePolicy, native_flat_layout,
    predicate_field_expr, row_export_columns_from_chunk, vortex_error,
};
use crate::{VortexSortSpillPolicy, VortexSortSpillReport};
use sha2::{Digest, Sha256};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{DType, Nullability, PType},
        iter::ArrayIterator,
        validity::Validity,
    },
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::scan::split_by::SplitBy,
    session::VortexSession,
};

const MERGE_FAN_IN: usize = 8;
// A binary carry per level keeps each input row in at most 64 merge generations.
// The u64 row-count contract cannot produce a 65th occupied level.
const MAX_LIVE_RUNS: usize = 64;
const BLOCK_ROWS: usize = 256;
const RUN_METADATA_BYTES_PER_BLOCK: u64 = 1024;
const RUN_METADATA_BASE_BYTES: u64 = 4096;
const OWNERSHIP_MARKER: &str = "owner.json";
const MARKER_BYTE_RESERVATION: u64 = 32 * 1024;
static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
pub(super) type SortSourceGeneration = crate::resident_session::SourceIdentity;

#[cfg(not(unix))]
pub(super) struct SortSourceGeneration;

#[cfg(not(unix))]
impl SortSourceGeneration {
    pub(super) fn reader(
        self: &std::sync::Arc<Self>,
        _: vortex::array::memory::HostAllocatorRef,
        _: vortex::io::runtime::Handle,
        _: usize,
    ) -> std::sync::Arc<dyn vortex::io::VortexReadAt> {
        unreachable!("non-Unix source capture always rejects native sort spill")
    }

    pub(super) fn capture(_: &Path) -> Result<Self> {
        Err(spill_error(
            "native sort source generation requires Unix file identity",
        ))
    }

    pub(super) fn validate(&self) -> Result<()> {
        Err(spill_error(
            "native sort source generation requires Unix file identity",
        ))
    }
}

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
    path: PathBuf,
    identity: (u64, u64),
    rows: u64,
    bytes: u64,
    digest: [u8; 32],
    metadata_bytes: u64,
    level: u32,
}

/// Reservation coverage is deliberately scoped to owned sort candidates,
/// conversion/merge batches, run metadata, and checksum scratch. Source scan,
/// provider allocations bypassing their allocator, and final payload output are
/// separate resource scopes; this counter is not process RSS.
pub(super) struct NumericSortSpill {
    policy: VortexSortSpillPolicy,
    directory: PathBuf,
    owned: Vec<PathBuf>,
    identities: std::collections::BTreeMap<PathBuf, (u64, u64)>,
    marker_identity: Option<(u64, u64)>,
    runs: Vec<Run>,
    next_run: u64,
    live_disk_bytes: u64,
    report: VortexSortSpillReport,
    memory: LiveMemoryPool,
    _state: MemoryLease,
    _merge: MemoryLease,
    _scratch: MemoryLease,
    metadata: MemoryLease,
    capacity_rows: usize,
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
        let metadata = memory.reserve(0)?;
        let capacity_rows = usize::try_from(state.bytes() / 256).unwrap_or(usize::MAX);
        if capacity_rows < BLOCK_ROWS || limit > capacity_rows {
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
        let root_metadata = fs::symlink_metadata(&policy.workspace).map_err(io_error)?;
        if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
            return Err(spill_error(
                "sort spill workspace must be an existing real directory",
            ));
        }
        let root = fs::canonicalize(&policy.workspace).map_err(io_error)?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| spill_error("system clock precedes the Unix epoch"))?
            .as_nanos();
        let directory = root.join(format!(
            "shardloom-query-sort-{}-{nonce}-{}",
            std::process::id(),
            NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed)
        ));
        shardloom_core::plan_workspace_safe_local_output(&root, &directory, false)?;
        let mut directory_builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            directory_builder.mode(0o700);
        }
        directory_builder.create(&directory).map_err(io_error)?;
        let mut this = Self {
            policy: policy.clone(),
            directory,
            owned: Vec::with_capacity(MAX_LIVE_RUNS + 1),
            identities: std::collections::BTreeMap::new(),
            marker_identity: None,
            runs: Vec::with_capacity(MAX_LIVE_RUNS),
            next_run: 0,
            live_disk_bytes: MARKER_BYTE_RESERVATION,
            report: VortexSortSpillReport {
                workspace: policy.workspace.clone(),
                quota_bytes: policy.quota_bytes,
                memory_bytes: policy.memory_bytes,
                peak_reserved_bytes: 0,
                peak_disk_bytes: MARKER_BYTE_RESERVATION,
                runs_written: 0,
                runs_validated: 0,
                merge_passes: 0,
                max_open_runs: 0,
                owned_cleanup_completed: false,
            },
            memory,
            _state: state,
            _merge: merge,
            _scratch: scratch,
            metadata,
            capacity_rows,
            descending,
            tie_policy,
            signed,
        };
        this.write_marker()?;
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

    fn metadata_for_rows(rows: u64) -> Result<u64> {
        rows.div_ceil(BLOCK_ROWS as u64)
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
        if self.owned.len() > MAX_LIVE_RUNS {
            return Err(spill_error("native sort run metadata file bound exceeded"));
        }
        let metadata_bytes = Self::metadata_for_rows(count)?;
        self.metadata.resize(
            self.metadata
                .bytes()
                .checked_add(metadata_bytes)
                .ok_or_else(|| spill_error("sort metadata reservation overflow"))?,
        )?;
        let path = self.directory.join(format!("run-{}.vortex", self.next_run));
        self.next_run = self
            .next_run
            .checked_add(1)
            .ok_or_else(|| spill_error("sort run ID overflow"))?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(io_error)?;
        self.owned.push(path.clone());
        self.identities.insert(path.clone(), file_identity(&path)?);
        self.write_marker()?;
        let available = self
            .policy
            .quota_bytes
            .checked_sub(self.live_disk_bytes)
            .ok_or_else(|| spill_error("sort spill quota exhausted"))?;
        let mut writer = QuotaWriter {
            file,
            remaining: available,
            written: 0,
            digest: Sha256::new(),
        };
        let mut rows = rows;
        let policy = self.policy.clone();
        let blocks = std::iter::from_fn(move || {
            if let Err(error) = check_cancelled(&policy) {
                return Some(Err(vortex::error::vortex_err!("{error}")));
            }
            let mut block = Vec::with_capacity(BLOCK_ROWS);
            while block.len() < BLOCK_ROWS {
                match rows.next() {
                    Some(Ok(row)) => block.push(row),
                    Some(Err(error)) => return Some(Err(vortex::error::vortex_err!("{error}"))),
                    None => break,
                }
            }
            (!block.is_empty()).then(|| Ok(rows_array(&block)))
        });
        let max_chunks = usize::try_from(count.div_ceil(BLOCK_ROWS as u64))
            .map_err(|_| spill_error("native run chunk count overflow"))?;
        let strategy = native_flat_layout::SequentialNativeFlatLayout::strategy(max_chunks);
        let mut native_writer = session
            .write_options()
            .with_strategy(strategy)
            .with_file_statistics(Vec::new())
            .blocking(runtime)
            .writer(&mut writer, run_dtype());
        for block in blocks {
            native_writer
                .push(block.map_err(vortex_error)?)
                .map_err(vortex_error)?;
        }
        let summary = native_writer.finish().map_err(vortex_error)?;
        writer.flush().map_err(io_error)?;
        if summary.row_count() != count
            || summary
                .footer()
                .approx_byte_size()
                .is_none_or(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX) > metadata_bytes)
        {
            return Err(spill_error(
                "native run row count or footer exceeded its declared bound",
            ));
        }
        self.live_disk_bytes = self
            .live_disk_bytes
            .checked_add(writer.written)
            .ok_or_else(|| spill_error("sort disk accounting overflow"))?;
        self.report.peak_disk_bytes = self.report.peak_disk_bytes.max(self.live_disk_bytes);
        self.report.runs_written += 1;
        let run = Run {
            identity: file_identity(&path)?,
            path,
            rows: count,
            bytes: writer.written,
            digest: writer.digest.finalize().into(),
            metadata_bytes,
            level: 0,
        };
        validate_run_bytes(&run)?;
        self.report.runs_validated += 1;
        Ok(run)
    }

    fn open_merge(
        &mut self,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<RunMerge> {
        self.report.max_open_runs = self.report.max_open_runs.max(self.runs.len());
        let readers = self
            .runs
            .iter()
            .map(|run| RunReader::open(run, runtime, session))
            .collect::<Result<Vec<_>>>()?;
        RunMerge::new(readers, self.policy.clone())
    }

    fn compact(
        &mut self,
        fan_in: usize,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
    ) -> Result<()> {
        if !(2..=MERGE_FAN_IN).contains(&fan_in) || fan_in > self.runs.len() {
            return Err(spill_error("invalid native sort merge fan-in"));
        }
        let old = self.runs.split_off(self.runs.len() - fan_in);
        let count = old.iter().try_fold(0_u64, |sum, run| {
            sum.checked_add(run.rows)
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
            .map(|run| RunReader::open(run, runtime, session))
            .collect::<Result<Vec<_>>>()?;
        let merge = RunMerge::new(readers, self.policy.clone())?;
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
        validate_run_bytes(run)?;
        fs::remove_file(&run.path).map_err(io_error)?;
        self.owned.retain(|path| path != &run.path);
        self.identities.remove(&run.path);
        self.live_disk_bytes -= run.bytes;
        self.metadata
            .resize(self.metadata.bytes() - run.metadata_bytes)?;
        self.write_marker()
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
        while self.runs.len() > MERGE_FAN_IN {
            self.compact(MERGE_FAN_IN, runtime, session)?;
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
        drop(merge);
        self.report.peak_reserved_bytes = self.memory.snapshot().peak_reserved_bytes;
        self.cleanup()?;
        self.report.owned_cleanup_completed = true;
        Ok((selected, self.report.clone()))
    }

    fn write_marker(&mut self) -> Result<()> {
        let payload = serde_json::json!({
            "schema": "shardloom.native_numeric_sort_workspace.v1",
            "files": self.owned.iter().map(|path| {
                let identity = self.identities.get(path).copied().unwrap_or((0, 0));
                serde_json::json!({"name": path.file_name().and_then(|name| name.to_str()).unwrap_or(""), "device": identity.0, "inode": identity.1})
            }).collect::<Vec<_>>(),
        });
        let marker = self.directory.join(OWNERSHIP_MARKER);
        if let Some(identity) = self.marker_identity {
            if file_identity(&marker)? != identity {
                return Err(spill_error("sort ownership marker identity changed"));
            }
        } else if fs::symlink_metadata(&marker).is_ok() {
            return Err(spill_error("sort ownership marker already exists"));
        }
        let payload = payload.to_string();
        if payload.len() > (MARKER_BYTE_RESERVATION / 2) as usize {
            return Err(spill_error(
                "sort ownership marker exceeded its byte reservation",
            ));
        }
        shardloom_core::write_workspace_safe_bytes(
            &self.directory,
            &marker,
            true,
            "native sort ownership",
            payload.as_bytes(),
        )?;
        self.marker_identity = Some(file_identity(&marker)?);
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        let marker = self.directory.join(OWNERSHIP_MARKER);
        if let Some(identity) = self.marker_identity
            && file_identity(&marker)? != identity
        {
            return Err(spill_error(
                "sort ownership marker identity changed; workspace preserved",
            ));
        }
        while let Some(path) = self.owned.last() {
            if self.identities.get(path).copied() != Some(file_identity(path)?) {
                return Err(spill_error("owned sort run file identity changed"));
            }
            fs::remove_file(path).map_err(io_error)?;
            self.identities.remove(path);
            self.owned.pop();
        }
        self.identities.clear();
        if self.marker_identity.is_some() {
            fs::remove_file(marker).map_err(io_error)?;
            self.marker_identity = None;
        }
        fs::remove_dir(&self.directory).map_err(io_error)
    }
}

impl Drop for NumericSortSpill {
    fn drop(&mut self) {
        if self.directory.exists() {
            let _ = self.cleanup();
        }
    }
}

struct QuotaWriter {
    file: File,
    remaining: u64,
    written: u64,
    digest: Sha256,
}
impl Write for QuotaWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > self.remaining {
            return Err(std::io::Error::other(
                "native sort spill byte quota exhausted",
            ));
        }
        let count = self.file.write(bytes)?;
        self.digest.update(&bytes[..count]);
        self.remaining -= count as u64;
        self.written += count as u64;
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
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

fn validate_run_bytes(run: &Run) -> Result<()> {
    if file_identity(&run.path)? != run.identity {
        return Err(spill_error("native sort run file identity changed"));
    }
    let metadata = fs::symlink_metadata(&run.path).map_err(io_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != run.bytes {
        return Err(spill_error("native sort run type or byte length changed"));
    }
    let mut file = File::open(&run.path).map_err(io_error)?;
    let mut digest = Sha256::new();
    // The operator reserves at least 128 KiB of checksum/conversion scratch.
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let count = file.read(&mut buffer).map_err(io_error)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    let actual: [u8; 32] = digest.finalize().into();
    if actual != run.digest {
        return Err(spill_error("native sort run checksum changed"));
    }
    Ok(())
}

struct RunReader {
    arrays: Box<dyn ArrayIterator>,
    rows: VecDeque<SpillRow>,
    remaining: u64,
    previous: Option<SpillRow>,
}
impl RunReader {
    fn open(run: &Run, runtime: &LocalVortexRuntime, session: &VortexSession) -> Result<Self> {
        validate_run_bytes(run)?;
        let file = runtime
            .block_on(session.open_options().open_path(&run.path))
            .map_err(vortex_error)?;
        if file.dtype() != &run_dtype() || file.row_count() != run.rows {
            return Err(spill_error("native sort run schema or row count changed"));
        }
        let arrays = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_split_by(SplitBy::RowCount(BLOCK_ROWS))
            .with_concurrency(1)
            .into_array_iter(runtime)
            .map_err(vortex_error)?;
        Ok(Self {
            arrays: Box::new(arrays),
            rows: VecDeque::with_capacity(BLOCK_ROWS),
            remaining: run.rows,
            previous: None,
        })
    }

    fn next_row(&mut self) -> Result<Option<SpillRow>> {
        if self.rows.is_empty()
            && let Some(array) = self.arrays.next()
        {
            let array = array.map_err(vortex_error)?;
            if array.len() > BLOCK_ROWS {
                return Err(spill_error("native sort run read exceeded its block bound"));
            }
            let columns = row_export_columns_from_chunk(
                &array,
                &["key".into(), "tie".into(), "source".into()],
            )?;
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
        }
        let Some(row) = self.rows.pop_front() else {
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

struct RunMerge {
    readers: Vec<RunReader>,
    heads: BinaryHeap<Reverse<(SpillRow, usize)>>,
    policy: VortexSortSpillPolicy,
    failed: bool,
}
impl RunMerge {
    fn new(mut readers: Vec<RunReader>, policy: VortexSortSpillPolicy) -> Result<Self> {
        if readers.len() > MERGE_FAN_IN {
            return Err(spill_error(
                "native sort merge exceeded its file-handle bound",
            ));
        }
        let mut heads = BinaryHeap::with_capacity(MERGE_FAN_IN);
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(row) = reader.next_row()? {
                heads.push(Reverse((row, index)));
            }
        }
        Ok(Self {
            readers,
            heads,
            policy,
            failed: false,
        })
    }
}
impl Iterator for RunMerge {
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
        match self.readers[index].next_row() {
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
fn io_error(error: impl std::fmt::Display) -> ShardLoomError {
    spill_error(&format!("native sort spill I/O: {error}"))
}

fn file_identity(path: &Path) -> Result<(u64, u64)> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(spill_error("owned sort run must remain a regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        Err(spill_error("native sort spill requires Unix file identity"))
    }
}

/// Explicit crash cleanup accepts only an owned immediate child of the admitted
/// workspace. A malformed marker, replaced inode, symlink, or unknown entry
/// leaves the workspace untouched instead of deleting unowned data.
#[cfg(feature = "vortex-write")]
pub(crate) fn recover(policy: &VortexSortSpillPolicy, directory: &Path) -> Result<()> {
    let root = fs::canonicalize(&policy.workspace).map_err(io_error)?;
    let metadata = fs::symlink_metadata(directory).map_err(io_error)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(spill_error("sort recovery requires a real owned directory"));
    }
    let directory = fs::canonicalize(directory).map_err(io_error)?;
    if directory.parent() != Some(root.as_path())
        || !directory
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("shardloom-query-sort-"))
    {
        return Err(spill_error(
            "sort recovery directory is outside its admitted workspace",
        ));
    }
    let marker = directory.join(OWNERSHIP_MARKER);
    let marker_identity = file_identity(&marker)?;
    let marker_metadata = fs::symlink_metadata(&marker).map_err(io_error)?;
    if !marker_metadata.is_file()
        || marker_metadata.file_type().is_symlink()
        || marker_metadata.len() > MARKER_BYTE_RESERVATION / 2
    {
        return Err(spill_error("invalid sort recovery ownership marker"));
    }
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&marker).map_err(io_error)?)
        .map_err(|_| spill_error("invalid sort recovery marker JSON"))?;
    if value.get("schema").and_then(serde_json::Value::as_str)
        != Some("shardloom.native_numeric_sort_workspace.v1")
    {
        return Err(spill_error("unsupported sort recovery marker schema"));
    }
    let files = value
        .get("files")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| spill_error("sort recovery marker requires owned files"))?;
    if files.len() > MAX_LIVE_RUNS + 1 {
        return Err(spill_error("sort recovery marker exceeds file bound"));
    }
    let mut owned = std::collections::BTreeSet::new();
    for file in files {
        let name = file
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| spill_error("sort recovery run name is missing"))?;
        let id = name
            .strip_prefix("run-")
            .and_then(|name| name.strip_suffix(".vortex"));
        if id.is_none_or(|id| id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit())) {
            return Err(spill_error("sort recovery run name is invalid"));
        }
        let path = directory.join(name);
        let device = file
            .get("device")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| spill_error("sort recovery device is missing"))?;
        let inode = file
            .get("inode")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| spill_error("sort recovery inode is missing"))?;
        if !owned.insert(path.clone()) {
            return Err(spill_error("sort recovery run is duplicated"));
        }
        let current_identity = match fs::symlink_metadata(&path) {
            Ok(_) => Some(file_identity(&path)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(io_error(error)),
        };
        if current_identity.is_some_and(|identity| identity != (device, inode)) {
            return Err(spill_error("sort recovery run ownership changed"));
        }
    }
    for entry in fs::read_dir(&directory).map_err(io_error)? {
        let entry = entry.map_err(io_error)?.path();
        if entry != marker && !owned.contains(&entry) {
            return Err(spill_error(
                "sort recovery found an unknown file; owned workspace left intact",
            ));
        }
    }
    if file_identity(&marker)? != marker_identity {
        return Err(spill_error("sort recovery ownership marker changed"));
    }
    for path in owned {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
    }
    fs::remove_file(marker).map_err(io_error)?;
    fs::remove_dir(directory).map_err(io_error)
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_sort_spill_tests.rs"]
mod tests;
