//! Caller-owned native readers, prepared operations, and actual array results.
//!
//! This initial resident surface supports metadata count and bounded projection.
//! It does not cache query answers or imply resident support for other operators.

use std::{
    fs::{File, Metadata},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::SystemTime,
};

use futures::{FutureExt as _, future::BoxFuture};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, LiveMemorySnapshot};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, VortexSessionExecute as _,
        dtype::DType,
        memory::{HostAllocatorRef, MemorySessionExt as _},
    },
    buffer::Alignment,
    error::{VortexResult, vortex_err},
    expr::{BoundExpression, root, select},
    file::{OpenOptionsSessionExt as _, VortexFile},
    io::{
        CoalesceConfig, VortexReadAt,
        runtime::{
            BlockingRuntime as _, Handle,
            current::{CurrentThreadRuntime, CurrentThreadWorkerPool},
        },
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

use crate::owned_buffers::ReservedHostAllocator;

struct RuntimeOwner {
    session: VortexSession,
    runtime: CurrentThreadRuntime,
    _workers: CurrentThreadWorkerPool,
    admission: Mutex<()>,
    memory: LiveMemoryPool,
    parallelism: usize,
    opens: AtomicU64,
    executions: AtomicU64,
}

/// A session shares provider registries and runtime workers across prepared calls.
/// Concurrent callers queue at the session boundary; there is no hidden global.
#[derive(Clone)]
pub struct ResidentVortexSession(Arc<RuntimeOwner>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResidentSessionSnapshot {
    pub prepared_source_opens: u64,
    pub completed_executions: u64,
    pub provider_background_workers: usize,
    pub memory: LiveMemorySnapshot,
}

impl ResidentVortexSession {
    pub(crate) fn memory(&self) -> &LiveMemoryPool {
        &self.0.memory
    }
    /// # Errors
    /// Rejects empty memory or CPU budgets. File generation checks currently
    /// require Unix device/inode/change-time identity; other hosts fail explicitly.
    pub fn new(memory_bytes: u64, max_parallelism: usize) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(resident_error("parallelism must be greater than zero"));
        }
        let parallelism = max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let memory = LiveMemoryPool::new(memory_bytes)?;
        let runtime = CurrentThreadRuntime::new();
        let workers = runtime.new_pool();
        workers.set_workers(parallelism - 1);
        let session = VortexSession::default()
            .with_handle(runtime.handle())
            .with_allocator(Arc::new(ReservedHostAllocator::new(memory.clone())));
        Ok(Self(Arc::new(RuntimeOwner {
            session,
            runtime,
            _workers: workers,
            admission: Mutex::new(()),
            memory,
            parallelism,
            opens: AtomicU64::new(0),
            executions: AtomicU64::new(0),
        })))
    }

    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        ResidentSessionSnapshot {
            prepared_source_opens: self.0.opens.load(Ordering::Relaxed),
            completed_executions: self.0.executions.load(Ordering::Relaxed),
            provider_background_workers: self.0.parallelism - 1,
            memory: self.0.memory.snapshot(),
        }
    }

    /// Open and validate an immutable file generation, retaining the same OS
    /// handle used by subsequent Vortex positional reads.
    ///
    /// # Errors
    /// Rejects inaccessible/nonregular files, unsupported generation identity,
    /// concurrent mutation, invalid Vortex files, and memory admission failures.
    pub fn prepare_file(&self, path: impl AsRef<Path>) -> Result<PreparedVortexSource> {
        let _gate = self
            .0
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        let path = std::path::absolute(path.as_ref()).map_err(native_error)?;
        let file = File::open(&path).map_err(native_error)?;
        let metadata = file.metadata().map_err(native_error)?;
        if !metadata.is_file() {
            return Err(resident_error("source must be a regular file"));
        }
        let generation = FileGeneration::read(&metadata)?;
        let identity = Arc::new(SourceIdentity {
            path,
            file,
            generation,
            invalidated: AtomicBool::new(false),
        });
        identity.validate()?;
        let input = Arc::new(ResidentFileReadAt {
            identity: Arc::clone(&identity),
            allocator: self.0.session.allocator(),
            handle: self.0.runtime.handle(),
            concurrency: self.0.parallelism,
        });
        let file = self
            .0
            .runtime
            .block_on(
                self.0
                    .session
                    .open_options()
                    .with_layout_reader_cache()
                    .open(input),
            )
            .map_err(native_error)?;
        identity.validate()?;
        self.0.opens.fetch_add(1, Ordering::Relaxed);
        Ok(PreparedVortexSource(Arc::new(PreparedSourceOwner {
            file,
            identity,
            runtime: Arc::clone(&self.0),
        })))
    }
}

struct PreparedSourceOwner {
    file: VortexFile,
    identity: Arc<SourceIdentity>,
    runtime: Arc<RuntimeOwner>,
}

#[derive(Clone)]
pub struct PreparedVortexSource(Arc<PreparedSourceOwner>);

impl PreparedVortexSource {
    pub(crate) fn validate_generation(&self) -> Result<()> {
        self.0.identity.validate()
    }
    pub(crate) fn file(&self) -> &VortexFile {
        &self.0.file
    }
    #[must_use]
    pub fn dtype(&self) -> &DType {
        self.0.file.dtype()
    }

    #[must_use]
    pub fn prepare_count(&self) -> PreparedVortexCount {
        PreparedVortexCount(self.clone())
    }

    /// Bind a source-order projection once. Calls still read and execute; no
    /// result cache is populated. Limits apply to the completed result.
    ///
    /// # Errors
    /// Rejects empty/duplicate/unknown fields, zero bounds, and invalid generations.
    pub fn prepare_projection(
        &self,
        columns: &[&str],
        max_rows: u64,
        max_output_bytes: u64,
    ) -> Result<PreparedVortexProjection> {
        self.0.identity.validate()?;
        if columns.is_empty() || max_rows == 0 || max_output_bytes == 0 {
            return Err(resident_error(
                "projection requires fields and positive row/byte bounds",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        if columns.iter().any(|column| !seen.insert(*column)) {
            return Err(resident_error("duplicate projection fields"));
        }
        let projection = select(columns.to_vec(), root())
            .bind(self.dtype())
            .map_err(native_error)?;
        Ok(PreparedVortexProjection {
            source: self.clone(),
            projection,
            filter: None,
            max_rows,
            max_output_bytes,
        })
    }
}

pub struct PreparedVortexCount(PreparedVortexSource);

impl PreparedVortexCount {
    /// Execute a native footer count with generation checks, without parsing SQL,
    /// opening another reader, creating workers, or formatting evidence strings.
    ///
    /// # Errors
    /// Rejects changed source generations and poisoned session admission.
    pub fn execute(&self) -> Result<u64> {
        let source = &self.0.0;
        let _gate = source
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.identity.validate()?;
        let rows = source.file.row_count();
        source.identity.validate()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(rows)
    }
}

pub struct PreparedVortexProjection {
    source: PreparedVortexSource,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    max_rows: u64,
    max_output_bytes: u64,
}

/// Executable native payload, separate from the report-only opaque descriptors.
/// Buffer credits remain attached even when arrays/slices outlive the session.
pub struct OwnedVortexResultBatch {
    arrays: Budgeted<Vec<ArrayRef>>,
    runtime: Arc<RuntimeOwner>,
    rows: u64,
    logical_buffer_bytes: u64,
}

impl OwnedVortexResultBatch {
    pub(crate) fn create_execution_ctx(&self) -> vortex::array::ExecutionCtx {
        self.runtime.session.create_execution_ctx()
    }
    #[must_use]
    pub fn arrays(&self) -> &[ArrayRef] {
        self.arrays.value()
    }

    #[must_use]
    pub const fn row_count(&self) -> u64 {
        self.rows
    }

    #[must_use]
    pub const fn logical_buffer_bytes(&self) -> u64 {
        self.logical_buffer_bytes
    }
}

impl PreparedVortexProjection {
    pub(crate) fn with_filter(mut self, filter: Option<BoundExpression>) -> Self {
        self.filter = filter;
        self
    }
    /// Execute the native scan and return actual arrays, without rendering rows.
    ///
    /// # Errors
    /// Rejects source mutation, scan errors, or row/byte/memory bound violations.
    pub fn execute(&self) -> Result<OwnedVortexResultBatch> {
        let source = &self.source.0;
        let runtime = &source.runtime;
        let _gate = runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.identity.validate()?;
        let scan = source
            .file
            .scan()
            .map_err(native_error)?
            .with_projection(self.projection.clone())
            .with_some_filter(self.filter.clone())
            .with_ordered(true)
            .with_concurrency(runtime.parallelism);
        // Vortex 0.85 rejects filter+limit. Keep the exact filter in the
        // provider and apply the source-order limit to returned array slices.
        let scan = if self.filter.is_none() {
            scan.with_limit(self.max_rows)
        } else {
            scan
        };
        let mut scan = scan
            .into_array_iter(&runtime.runtime)
            .map_err(native_error)?;
        let mut arrays = Vec::new();
        let mut lease = runtime.memory.reserve(0)?;
        let mut rows = 0_u64;
        let mut logical_bytes = 0_u64;
        for array in &mut scan {
            let array = array.map_err(native_error)?;
            let remaining = usize::try_from(self.max_rows - rows).unwrap_or(usize::MAX);
            let array = if array.len() > remaining {
                array.slice(0..remaining).map_err(native_error)?
            } else {
                array
            };
            rows = rows
                .checked_add(u64::try_from(array.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| resident_error("result row count overflow"))?;
            logical_bytes = logical_bytes
                .checked_add(array.nbytes())
                .ok_or_else(|| resident_error("result byte count overflow"))?;
            if rows > self.max_rows || logical_bytes > self.max_output_bytes {
                return Err(resident_error("projection exceeds completed output bounds"));
            }
            if arrays.len() == arrays.capacity() {
                let capacity = arrays.capacity().saturating_mul(2).max(4);
                let bytes = capacity
                    .checked_mul(std::mem::size_of::<ArrayRef>())
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .ok_or_else(|| resident_error("result ownership capacity overflow"))?;
                lease.resize(bytes)?;
                arrays
                    .try_reserve_exact(capacity - arrays.len())
                    .map_err(native_error)?;
            }
            arrays.push(array);
            if rows == self.max_rows {
                break;
            }
        }
        source.identity.validate()?;
        runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(OwnedVortexResultBatch {
            arrays: Budgeted::new(arrays, lease),
            runtime: Arc::clone(runtime),
            rows,
            logical_buffer_bytes: logical_bytes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileGeneration {
    len: u64,
    modified: SystemTime,
    device: u64,
    inode: u64,
    changed: (i64, i64),
}

impl FileGeneration {
    #[cfg(unix)]
    fn read(metadata: &Metadata) -> Result<Self> {
        use std::os::unix::fs::MetadataExt as _;
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().map_err(native_error)?,
            device: metadata.dev(),
            inode: metadata.ino(),
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }

    #[cfg(not(unix))]
    fn read(_: &Metadata) -> Result<Self> {
        Err(resident_error(
            "resident file generation identity is not supported on this platform",
        ))
    }
}

struct SourceIdentity {
    path: PathBuf,
    file: File,
    generation: FileGeneration,
    invalidated: AtomicBool,
}

impl SourceIdentity {
    fn validate(&self) -> Result<()> {
        if self.invalidated.load(Ordering::Acquire) {
            return Err(resident_error(
                "prepared source generation invalidated; prepare the source again",
            ));
        }
        let result = (|| {
            let path = FileGeneration::read(&std::fs::metadata(&self.path).map_err(native_error)?)?;
            let handle = FileGeneration::read(&self.file.metadata().map_err(native_error)?)?;
            if path != self.generation || handle != self.generation {
                return Err(resident_error(
                    "prepared source changed; prepare the source again",
                ));
            }
            Ok(())
        })();
        if result.is_err() {
            self.invalidated.store(true, Ordering::Release);
        }
        result
    }
}

struct ResidentFileReadAt {
    identity: Arc<SourceIdentity>,
    allocator: HostAllocatorRef,
    handle: Handle,
    concurrency: usize,
}

impl VortexReadAt for ResidentFileReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(CoalesceConfig::file())
    }
    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let identity = Arc::clone(&self.identity);
        async move {
            identity
                .validate()
                .map_err(|error| vortex_err!("{error}"))?;
            Ok(identity.generation.len)
        }
        .boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<vortex::array::buffer::BufferHandle>> {
        let identity = Arc::clone(&self.identity);
        let allocator = Arc::clone(&self.allocator);
        let handle = self.handle.clone();
        async move {
            handle
                .spawn_blocking(move || {
                    identity
                        .validate()
                        .map_err(|error| vortex_err!("{error}"))?;
                    if offset
                        .checked_add(u64::try_from(length).unwrap_or(u64::MAX))
                        .is_none_or(|end| end > identity.generation.len)
                    {
                        return Err(vortex_err!(
                            "resident source read exceeds generation length"
                        ));
                    }
                    let mut buffer = allocator.allocate(length, alignment)?;
                    vortex::io::std_file::read_exact_at(
                        &identity.file,
                        buffer.as_mut_slice(),
                        offset,
                    )?;
                    identity
                        .validate()
                        .map_err(|error| vortex_err!("{error}"))?;
                    Ok(vortex::array::buffer::BufferHandle::new_host(
                        buffer.freeze(),
                    ))
                })
                .await
        }
        .boxed()
    }
}

fn resident_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; no fallback execution was attempted"))
}

fn native_error(error: impl std::fmt::Display) -> ShardLoomError {
    resident_error(&error.to_string())
}

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "resident_session_tests.rs"]
mod tests;
