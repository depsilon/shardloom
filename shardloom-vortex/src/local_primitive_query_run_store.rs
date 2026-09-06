//! Private native query-run storage, extracted from numeric sort (RFC 0044).
//!
//! Pinned Vortex 0.85 supplies the file writer, native arrays/Flat layout and
//! exact row-range scan task. This module owns only temporary file admission,
//! overlapping disk quota, checksums, held-generation reads and owned cleanup.
//! Ordering, equality, row conversion, merge geometry and public spill admission
//! remain with the operator. Closed workspace namespaces prevent one family
//! from recovering another family's runs. Numeric sort keeps its marker format.
//!
//! Callers admit native array blocks and keep their conversion/work scope alive.
//! Descriptors and readers retain metadata credits; returned blocks retain both
//! metadata and work credits. Source/provider allocations outside those scopes
//! are not newly claimed as covered, and these reservations are not process RSS.
//! Private-directory ownership is cooperative cleanup, not a filesystem CAS
//! against hostile same-user replacement between an identity check and unlink.

use super::{LocalVortexRuntime, Result, ShardLoomError, native_flat_layout, vortex_error};
use sha2::{Digest, Sha256};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
#[cfg(feature = "vortex-write")]
use std::io::Read;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use vortex::{
    array::{ArrayRef, dtype::DType},
    file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
    io::runtime::BlockingRuntime as _,
    layout::scan::split_by::SplitBy,
    session::VortexSession,
};

pub(super) const MAX_LIVE_RUNS: usize = 64;
pub(super) const OWNERSHIP_MARKER: &str = "owner.json";
pub(super) const MARKER_BYTE_RESERVATION: u64 = 32 * 1024;
const CHECKSUM_SCRATCH_BYTES: usize = 64 * 1024;
static NEXT_WORKSPACE: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
pub(super) type RunSourceGeneration = crate::resident_session::SourceIdentity;

#[cfg(not(unix))]
pub(super) struct RunSourceGeneration;

#[cfg(not(unix))]
impl RunSourceGeneration {
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

    pub(super) fn verify_contents(
        &self,
        _: (u64, u64),
        _: u64,
        _: &[u8; 32],
        _: &mut [u8],
    ) -> Result<()> {
        Err(spill_error("native sort spill requires Unix file identity"))
    }
}

#[derive(Clone, Copy)]
enum RunNamespace {
    NumericSort,
    ExactIntegerDistinct,
}

impl RunNamespace {
    const fn prefix(self) -> &'static str {
        match self {
            Self::NumericSort => "shardloom-query-sort-",
            Self::ExactIntegerDistinct => "shardloom-query-integer-distinct-",
        }
    }

    const fn schema(self) -> &'static str {
        match self {
            Self::NumericSort => "shardloom.native_numeric_sort_workspace.v1",
            Self::ExactIntegerDistinct => "shardloom.native_integer_distinct_workspace.v1",
        }
    }
}

/// A namespace does not authorize public operator admission. Callers cannot
/// supply arbitrary prefixes or recovery schemas.
#[derive(Clone)]
pub(super) struct QueryRunStorePolicy {
    workspace: PathBuf,
    quota_bytes: u64,
    cancellation: Arc<AtomicBool>,
    namespace: RunNamespace,
}

impl QueryRunStorePolicy {
    pub(super) fn numeric_sort(policy: &crate::VortexSortSpillPolicy) -> Self {
        Self {
            workspace: policy.workspace.clone(),
            quota_bytes: policy.quota_bytes,
            cancellation: Arc::clone(&policy.cancellation),
            namespace: RunNamespace::NumericSort,
        }
    }

    #[allow(dead_code)] // Private adapter is authored separately; extraction tests exercise namespace isolation first.
    pub(super) fn exact_integer_distinct(
        workspace: PathBuf,
        quota_bytes: u64,
        cancellation: Arc<AtomicBool>,
    ) -> Self {
        Self {
            workspace,
            quota_bytes,
            cancellation,
            namespace: RunNamespace::ExactIntegerDistinct,
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.cancellation.load(Ordering::Acquire) {
            Err(spill_error("native sort execution cancelled"))
        } else {
            Ok(())
        }
    }
}

/// The operator chooses and pre-admits exact schema, row/block geometry and
/// metadata allowance. This is not permission for arbitrary public spill types.
pub(super) struct QueryRunSpec {
    pub(super) dtype: DType,
    pub(super) rows: u64,
    pub(super) block_rows: usize,
    pub(super) metadata_bytes: u64,
}

#[derive(Debug)]
pub(super) struct NativeQueryRun {
    pub(super) path: PathBuf,
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) block_rows: usize,
    identity: (u64, u64),
    digest: [u8; 32],
    metadata: Arc<MemoryLease>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct QueryRunStoreSnapshot {
    pub(super) live_disk_bytes: u64,
    pub(super) peak_disk_bytes: u64,
    pub(super) runs_written: u64,
    pub(super) runs_validated: u64,
}

pub(super) struct QueryRunStore {
    policy: QueryRunStorePolicy,
    directory: PathBuf,
    owned: Vec<PathBuf>,
    identities: std::collections::BTreeMap<PathBuf, OwnedRunIdentity>,
    marker_identity: Option<(u64, u64)>,
    next_run: u64,
    snapshot: QueryRunStoreSnapshot,
    memory: LiveMemoryPool,
    // Bounded marker serialization and the 64 KiB checksum buffer. Retained
    // path copies have separate checked reservations, including long paths.
    _scratch: MemoryLease,
    _workspace_metadata: MemoryLease,
    failed: bool,
}

struct OwnedRunIdentity {
    identity: (u64, u64),
    _metadata: Arc<MemoryLease>,
}

#[allow(clippy::ptr_arg)] // Retained PathBuf capacity, not just its borrowed path length, is charged.
fn path_reservation(path: &PathBuf, copies: u64) -> Result<u64> {
    u64::try_from(path.capacity().max(path.as_os_str().len()))
        .ok()
        .and_then(|bytes| bytes.checked_mul(copies))
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(|| spill_error("native query run path reservation overflow"))
}

impl QueryRunStore {
    pub(super) fn new(
        policy: QueryRunStorePolicy,
        memory: LiveMemoryPool,
        scratch: MemoryLease,
    ) -> Result<Self> {
        policy.check_cancelled()?;
        if !memory.owns(&scratch) {
            return Err(spill_error(
                "native query run scratch belongs to another memory pool",
            ));
        }
        if !cfg!(unix)
            || !policy.workspace.is_absolute()
            || policy.quota_bytes < MARKER_BYTE_RESERVATION
            || scratch.bytes() < 2 * CHECKSUM_SCRATCH_BYTES as u64
        {
            return Err(spill_error(
                "native query runs require Unix file identity, 32 KiB disk quota and 128 KiB reserved scratch",
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
            "{}{}-{nonce}-{}",
            policy.namespace.prefix(),
            std::process::id(),
            NEXT_WORKSPACE.fetch_add(1, Ordering::Relaxed)
        ));
        let workspace_metadata = memory.reserve(
            path_reservation(&directory, 2)?
                .checked_add(path_reservation(&policy.workspace, 2)?)
                .ok_or_else(|| spill_error("native query workspace reservation overflow"))?,
        )?;
        shardloom_core::plan_workspace_safe_local_output(&root, &directory, false)?;
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt as _;
            builder.mode(0o700);
        }
        builder.create(&directory).map_err(io_error)?;
        let mut store = Self {
            policy,
            directory,
            owned: Vec::with_capacity(MAX_LIVE_RUNS + 1),
            identities: std::collections::BTreeMap::new(),
            marker_identity: None,
            next_run: 0,
            snapshot: QueryRunStoreSnapshot {
                live_disk_bytes: MARKER_BYTE_RESERVATION,
                peak_disk_bytes: MARKER_BYTE_RESERVATION,
                ..QueryRunStoreSnapshot::default()
            },
            memory,
            _scratch: scratch,
            _workspace_metadata: workspace_metadata,
            failed: false,
        };
        store.write_marker()?;
        Ok(store)
    }

    pub(super) const fn snapshot(&self) -> QueryRunStoreSnapshot {
        self.snapshot
    }

    fn check_active(&self) -> Result<()> {
        self.policy.check_cancelled()?;
        if self.failed {
            return Err(spill_error(
                "native query run store failed; owned cleanup is required",
            ));
        }
        Ok(())
    }

    /// Native blocks remain under the caller's existing conversion/work lease.
    /// No `StatValue` or Arrow intermediary is introduced. A failed write makes
    /// this store terminal, so a partial run can never escape quota accounting.
    pub(super) fn write_arrays(
        &mut self,
        spec: &QueryRunSpec,
        blocks: impl Iterator<Item = Result<ArrayRef>>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
        work: &Arc<MemoryLease>,
    ) -> Result<NativeQueryRun> {
        self.check_active()?;
        if !self.memory.owns(work) {
            return Err(spill_error(
                "native query run work belongs to another memory pool",
            ));
        }
        let result = self.write_arrays_inner(spec, blocks, runtime, session, work);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    #[allow(clippy::too_many_lines)] // Keep file creation, accounting and publication in one error scope.
    fn write_arrays_inner(
        &mut self,
        spec: &QueryRunSpec,
        blocks: impl Iterator<Item = Result<ArrayRef>>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
        work: &Arc<MemoryLease>,
    ) -> Result<NativeQueryRun> {
        if self.owned.len() > MAX_LIVE_RUNS {
            return Err(spill_error("native sort run metadata file bound exceeded"));
        }
        if spec.block_rows == 0 || spec.metadata_bytes == 0 || work.bytes() == 0 {
            return Err(spill_error(
                "native query run block and reservation bounds must be positive",
            ));
        }
        let max_chunks = usize::try_from(spec.rows.div_ceil(spec.block_rows as u64))
            .map_err(|_| spill_error("native run chunk count overflow"))?;
        let path = self.directory.join(format!("run-{}.vortex", self.next_run));
        // Vec/map keys, descriptor and transient held-checksum source path.
        // Each open reader receives its own additional source-path reservation.
        let metadata = Arc::new(
            self.memory.reserve(
                spec.metadata_bytes
                    .checked_add(path_reservation(&path, 4)?)
                    .ok_or_else(|| spill_error("native query run metadata reservation overflow"))?,
            )?,
        );
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
        self.identities.insert(
            path.clone(),
            OwnedRunIdentity {
                identity: file_identity(&path)?,
                _metadata: Arc::clone(&metadata),
            },
        );
        self.write_marker()?;
        let available = self
            .policy
            .quota_bytes
            .checked_sub(self.snapshot.live_disk_bytes)
            .ok_or_else(|| spill_error("sort spill quota exhausted"))?;
        let mut writer = QuotaWriter {
            file,
            remaining: available,
            written: 0,
            digest: Sha256::new(),
        };
        let result = self.write_blocks(spec, max_chunks, blocks, runtime, session, &mut writer);
        // Include a partially written failed output. Inputs stay charged until
        // explicit removal, so compaction always admits input/output overlap.
        self.snapshot.live_disk_bytes =
            self.snapshot
                .live_disk_bytes
                .checked_add(writer.written)
                .ok_or_else(|| spill_error("sort disk accounting overflow"))?;
        self.snapshot.peak_disk_bytes = self
            .snapshot
            .peak_disk_bytes
            .max(self.snapshot.live_disk_bytes);
        result?;
        writer.flush().map_err(io_error)?;
        let run = NativeQueryRun {
            identity: file_identity(&path)?,
            path,
            rows: spec.rows,
            bytes: writer.written,
            digest: writer.digest.finalize().into(),
            metadata,
            block_rows: spec.block_rows,
        };
        validate_run_bytes(&run)?;
        self.snapshot.runs_written += 1;
        self.snapshot.runs_validated += 1;
        Ok(run)
    }

    fn write_blocks(
        &self,
        spec: &QueryRunSpec,
        max_chunks: usize,
        mut blocks: impl Iterator<Item = Result<ArrayRef>>,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
        writer: &mut QuotaWriter,
    ) -> Result<()> {
        let strategy = native_flat_layout::SequentialNativeFlatLayout::strategy(max_chunks);
        let mut native_writer = session
            .write_options()
            .with_strategy(strategy)
            .with_file_statistics(Vec::new())
            .blocking(runtime)
            .writer(writer, spec.dtype.clone());
        let mut written_rows = 0_u64;
        loop {
            self.policy.check_cancelled()?;
            let Some(block) = blocks.next() else {
                break;
            };
            let block = block?;
            self.policy.check_cancelled()?;
            let remaining = spec
                .rows
                .checked_sub(written_rows)
                .ok_or_else(|| spill_error("native query run input exceeded its declared rows"))?;
            let expected = remaining.min(spec.block_rows as u64);
            if block.dtype() != &spec.dtype || block.len() as u64 != expected || expected == 0 {
                return Err(spill_error(
                    "native query run input schema or block geometry changed",
                ));
            }
            written_rows = written_rows
                .checked_add(block.len() as u64)
                .ok_or_else(|| spill_error("native query run row overflow"))?;
            native_writer.push(block).map_err(vortex_error)?;
        }
        self.policy.check_cancelled()?;
        let summary = native_writer.finish().map_err(vortex_error)?;
        if written_rows != spec.rows
            || summary.row_count() != spec.rows
            || summary
                .footer()
                .approx_byte_size()
                .is_none_or(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX) > spec.metadata_bytes)
        {
            return Err(spill_error(
                "native run row count or footer exceeded its declared bound",
            ));
        }
        Ok(())
    }

    pub(super) fn open(
        &self,
        run: &NativeQueryRun,
        dtype: &DType,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
        work: Arc<MemoryLease>,
    ) -> Result<QueryRunReader> {
        self.check_active()?;
        if !self.memory.owns(&work) || work.bytes() < 64 * 1024 {
            return Err(spill_error(
                "native query run reader requires shared-pool footer/work credit",
            ));
        }
        self.check_owned(run)?;
        let path_credit = Arc::new(self.memory.reserve(path_reservation(&run.path, 2)?)?);
        QueryRunReader::open(
            run,
            dtype,
            runtime,
            session,
            work,
            Arc::clone(&self.policy.cancellation),
            path_credit,
        )
    }

    fn check_owned(&self, run: &NativeQueryRun) -> Result<()> {
        if self.identities.get(&run.path).map(|entry| entry.identity) != Some(run.identity) {
            return Err(spill_error(
                "native query run does not belong to this store",
            ));
        }
        Ok(())
    }

    pub(super) fn remove(&mut self, run: &NativeQueryRun) -> Result<()> {
        self.check_active()?;
        self.check_owned(run)?;
        validate_run_bytes(run)?;
        fs::remove_file(&run.path).map_err(io_error)?;
        self.owned.retain(|path| path != &run.path);
        self.identities.remove(&run.path);
        self.snapshot.live_disk_bytes -= run.bytes;
        // The caller's descriptor/readers keep metadata charged until they drop.
        self.write_marker()
    }

    pub(super) fn write_marker(&mut self) -> Result<()> {
        let payload = serde_json::json!({
            "schema": self.policy.namespace.schema(),
            "files": self.owned.iter().map(|path| {
                let identity = self.identities.get(path).map_or((0, 0), |entry| entry.identity);
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

    #[cfg(all(test, feature = "vortex-write"))]
    pub(super) fn directory(&self) -> &Path {
        &self.directory
    }

    pub(super) fn cleanup(&mut self) -> Result<()> {
        let marker = self.directory.join(OWNERSHIP_MARKER);
        if let Some(identity) = self.marker_identity
            && file_identity(&marker)? != identity
        {
            return Err(spill_error(
                "sort ownership marker identity changed; workspace preserved",
            ));
        }
        while let Some(path) = self.owned.last() {
            if self.identities.get(path).map(|entry| entry.identity) != Some(file_identity(path)?) {
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
        fs::remove_dir(&self.directory).map_err(io_error)?;
        self.snapshot.live_disk_bytes = 0;
        self.failed = true;
        Ok(())
    }
}

impl Drop for QueryRunStore {
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

/// The 64 KiB checksum buffer is covered by the store's transferred scratch
/// reservation. The same captured descriptor is used by every native read.
fn validate_run_bytes(run: &NativeQueryRun) -> Result<Arc<RunSourceGeneration>> {
    if file_identity(&run.path)? != run.identity {
        return Err(spill_error("native sort run file identity changed"));
    }
    let metadata = fs::symlink_metadata(&run.path).map_err(io_error)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != run.bytes {
        return Err(spill_error("native sort run type or byte length changed"));
    }
    let source = Arc::new(RunSourceGeneration::capture(&run.path)?);
    let mut buffer = vec![0_u8; CHECKSUM_SCRATCH_BYTES].into_boxed_slice();
    source.verify_contents(run.identity, run.bytes, &run.digest, &mut buffer)?;
    if file_identity(&run.path)? != run.identity {
        return Err(spill_error(
            "native sort run file identity changed during verification",
        ));
    }
    Ok(source)
}

/// Exact native block plus ownership of the caller's admitted working scope.
/// Consumers borrow the array while converting/reconciling it; no detached
/// uncharged `ArrayRef` is returned by the store.
pub(super) struct QueryRunBlock {
    array: ArrayRef,
    _metadata: Arc<MemoryLease>,
    _work: Arc<MemoryLease>,
    _path_credit: Arc<MemoryLease>,
}
impl QueryRunBlock {
    pub(super) const fn array(&self) -> &ArrayRef {
        &self.array
    }
}

pub(super) struct QueryRunReader {
    file: vortex::file::VortexFile,
    source: Arc<RunSourceGeneration>,
    next_block_offset: u64,
    block_rows: usize,
    metadata: Arc<MemoryLease>,
    work: Arc<MemoryLease>,
    cancellation: Arc<AtomicBool>,
    path_credit: Arc<MemoryLease>,
}
impl QueryRunReader {
    fn open(
        run: &NativeQueryRun,
        dtype: &DType,
        runtime: &LocalVortexRuntime,
        session: &VortexSession,
        work: Arc<MemoryLease>,
        cancellation: Arc<AtomicBool>,
        path_credit: Arc<MemoryLease>,
    ) -> Result<Self> {
        use vortex::array::memory::MemorySessionExt as _;
        let source = validate_run_bytes(run)?;
        let file = runtime
            .block_on(
                session
                    .open_options()
                    .with_layout_reader_cache()
                    .open_read(source.reader(session.allocator(), runtime.handle(), 1)),
            )
            .map_err(vortex_error)?;
        if file.dtype() != dtype || file.row_count() != run.rows {
            return Err(spill_error("native sort run schema or row count changed"));
        }
        source.validate()?;
        Ok(Self {
            file,
            source,
            next_block_offset: 0,
            block_rows: run.block_rows,
            metadata: Arc::clone(&run.metadata),
            work,
            cancellation,
            path_credit,
        })
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.source.validate()?;
        if self.cancellation.load(Ordering::Acquire) {
            return Err(spill_error("native sort execution cancelled"));
        }
        Ok(())
    }

    #[cfg(all(test, feature = "vortex-write"))]
    pub(super) const fn next_block_offset(&self) -> u64 {
        self.next_block_offset
    }

    pub(super) fn next_block(
        &mut self,
        runtime: &LocalVortexRuntime,
    ) -> Result<Option<QueryRunBlock>> {
        self.validate()?;
        let start = self.next_block_offset;
        if start == self.file.row_count() {
            return Ok(None);
        }
        let end = start
            .saturating_add(self.block_rows as u64)
            .min(self.file.row_count());
        // Pinned scan streams may prefetch by host cores. Drive one exact
        // Flat-leaf task so other run payloads are not prefetched here.
        let mut tasks = self
            .file
            .scan()
            .map_err(vortex_error)?
            .with_row_range(start..end)
            .with_split_by(SplitBy::RowCount(self.block_rows))
            .build()
            .map_err(vortex_error)?;
        if tasks.len() != 1 {
            return Err(spill_error(
                "native sort run block must produce exactly one scan task",
            ));
        }
        let task = tasks
            .pop()
            .ok_or_else(|| spill_error("native sort run block task is absent"))?;
        let array = runtime
            .block_on(task)
            .map_err(vortex_error)?
            .ok_or_else(|| spill_error("native sort run block returned no rows"))?;
        if array.len() as u64 != end - start || array.len() > self.block_rows {
            return Err(spill_error(
                "native sort run read exceeded or truncated its block bound",
            ));
        }
        self.validate()?;
        self.next_block_offset = end;
        Ok(Some(QueryRunBlock {
            array,
            _metadata: Arc::clone(&self.metadata),
            _work: Arc::clone(&self.work),
            _path_credit: Arc::clone(&self.path_credit),
        }))
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
    metadata_identity(&metadata)
}

#[allow(clippy::unnecessary_wraps)] // Non-Unix builds retain the explicit unsupported-identity error.
fn metadata_identity(metadata: &fs::Metadata) -> Result<(u64, u64)> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok((metadata.dev(), metadata.ino()))
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        Err(spill_error("native sort spill requires Unix file identity"))
    }
}

#[cfg(feature = "vortex-write")]
pub(super) fn bounded_marker_bytes(reader: impl Read) -> Result<Vec<u8>> {
    let limit = MARKER_BYTE_RESERVATION / 2;
    let mut bytes = Vec::with_capacity(usize::try_from(limit + 1).map_err(io_error)?);
    reader
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() as u64 > limit {
        return Err(spill_error(
            "sort recovery ownership marker exceeds its byte bound",
        ));
    }
    Ok(bytes)
}

/// Explicit crash cleanup accepts only an owned immediate child of the admitted
/// workspace. A malformed marker, replaced inode, symlink, or unknown entry
/// leaves the workspace untouched instead of deleting unowned data.
#[cfg(feature = "vortex-write")]
#[allow(clippy::too_many_lines)] // Validate all owned entries before starting destructive recovery.
pub(super) fn recover(policy: &QueryRunStorePolicy, directory: &Path) -> Result<()> {
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
            .is_some_and(|name| name.starts_with(policy.namespace.prefix()))
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
    let marker_file = File::open(&marker).map_err(io_error)?;
    if metadata_identity(&marker_file.metadata().map_err(io_error)?)? != marker_identity {
        return Err(spill_error(
            "sort recovery ownership marker changed before read",
        ));
    }
    let marker_bytes = bounded_marker_bytes(&marker_file)?;
    if file_identity(&marker)? != marker_identity {
        return Err(spill_error(
            "sort recovery ownership marker changed during read",
        ));
    }
    let value: serde_json::Value = serde_json::from_slice(&marker_bytes)
        .map_err(|_| spill_error("invalid sort recovery marker JSON"))?;
    if value.get("schema").and_then(serde_json::Value::as_str) != Some(policy.namespace.schema()) {
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

#[cfg(all(test, feature = "vortex-write", unix))]
#[path = "local_primitive_query_run_store_tests.rs"]
mod tests;
