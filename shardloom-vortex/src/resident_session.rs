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
        runtime::{BlockingRuntime as _, Handle, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

use crate::owned_buffers::ReservedHostAllocator;

use crate::resident_worker_group::ResidentWorkerGroup;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "resident_result_json.rs"]
mod result_json;

#[cfg(all(feature = "vortex-local-primitives", unix))]
#[path = "resident_segment_reuse.rs"]
pub(crate) mod segment_reuse;

/// Only the terminal native error boundary may request an uncached replay.
/// Counter deltas or error-message contents are never used to classify it.
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) struct SegmentReuseAttempt {
    enabled: bool,
    retry_requested: bool,
}

#[cfg(all(feature = "vortex-local-primitives", unix))]
impl SegmentReuseAttempt {
    pub(crate) fn request_uncached_retry(&mut self, error: &vortex::error::VortexError) -> bool {
        if self.enabled && crate::owned_buffers::is_owned_reservation_denial(error) {
            self.retry_requested = true;
            true
        } else {
            false
        }
    }
}

struct RuntimeOwner {
    session: VortexSession,
    runtime: CurrentThreadRuntime,
    _workers: ResidentWorkerGroup,
    admission: Mutex<()>,
    memory: LiveMemoryPool,
    parallelism: usize,
    provider_background_workers: usize,
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

    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn with_native_session<T>(
        &self,
        execute: impl FnOnce(&VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<T> {
        let _gate = self
            .0
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        execute(&self.0.session, &self.0.runtime)
    }

    /// Only engine-constructed immutable segment sources may enter here.
    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn prepare_immutable_file(&self, file: VortexFile) -> PreparedVortexSource {
        PreparedVortexSource(Arc::new(PreparedSourceOwner {
            file,
            identity: None,
            runtime: Arc::clone(&self.0),
        }))
    }

    #[cfg(unix)]
    pub(crate) fn native_allocator(&self) -> HostAllocatorRef {
        self.0.session.allocator()
    }

    /// Complete an admitted native array operation using this session's execution
    /// context. The producer must own its buffers through this session allocator;
    /// this is not an admission path for arbitrary externally allocated arrays.
    #[cfg(unix)]
    pub(crate) fn execute_owned_array(
        &self,
        max_rows: u64,
        max_output_bytes: u64,
        execute: impl FnOnce(&VortexSession) -> Result<ArrayRef>,
    ) -> Result<OwnedVortexResultBatch> {
        let _gate = self
            .0
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        let ownership = self
            .0
            .memory
            .reserve(std::mem::size_of::<ArrayRef>() as u64)?;
        let array = execute(&self.0.session)?;
        let rows = u64::try_from(array.len()).map_err(native_error)?;
        let logical_buffer_bytes = array.nbytes();
        if rows > max_rows || logical_buffer_bytes > max_output_bytes {
            return Err(resident_error("projection exceeds completed output bounds"));
        }
        let result = OwnedVortexResultBatch {
            arrays: Budgeted::new(vec![array], ownership),
            runtime: Arc::clone(&self.0),
            rows,
            logical_buffer_bytes,
        };
        self.0.executions.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// # Errors
    /// Rejects empty memory or CPU budgets. File generation checks currently
    /// require Unix device/inode/change-time identity; other hosts fail explicitly.
    pub fn new(memory_bytes: u64, max_parallelism: usize) -> Result<Self> {
        Self::with_cpu_driver_policy(memory_bytes, max_parallelism, false)
    }

    /// The caller and its dedicated compute pool own the CPU budget. Provider
    /// progress runs only while the caller drives the runtime; positional I/O
    /// concurrency remains separately bounded by `max_io_parallelism`.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn for_external_cpu_pool(
        memory_bytes: u64,
        max_io_parallelism: usize,
    ) -> Result<Self> {
        Self::with_cpu_driver_policy(memory_bytes, max_io_parallelism, true)
    }

    fn with_cpu_driver_policy(
        memory_bytes: u64,
        max_parallelism: usize,
        external_cpu_pool: bool,
    ) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(resident_error("parallelism must be greater than zero"));
        }
        let parallelism = max_parallelism
            .min(std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get));
        let memory = LiveMemoryPool::new(memory_bytes)?;
        let runtime = CurrentThreadRuntime::new();
        let provider_background_workers = if external_cpu_pool {
            0
        } else {
            parallelism - 1
        };
        let workers = ResidentWorkerGroup::new(&runtime, provider_background_workers)
            .map_err(native_error)?;
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
            provider_background_workers,
            opens: AtomicU64::new(0),
            executions: AtomicU64::new(0),
        })))
    }

    #[must_use]
    pub fn snapshot(&self) -> ResidentSessionSnapshot {
        ResidentSessionSnapshot {
            prepared_source_opens: self.0.opens.load(Ordering::Relaxed),
            completed_executions: self.0.executions.load(Ordering::Relaxed),
            provider_background_workers: self.0.provider_background_workers,
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
        let identity = Arc::new(SourceIdentity::capture(path.as_ref())?);
        let input = identity.reader(
            self.0.session.allocator(),
            self.0.runtime.handle(),
            self.0.parallelism,
        );
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
            identity: Some(identity),
            runtime: Arc::clone(&self.0),
        })))
    }
}

struct PreparedSourceOwner {
    file: VortexFile,
    // Engine-owned immutable memory needs no external pathname validation.
    identity: Option<Arc<SourceIdentity>>,
    runtime: Arc<RuntimeOwner>,
}

impl PreparedSourceOwner {
    fn validate(&self) -> Result<()> {
        self.identity
            .as_ref()
            .map_or(Ok(()), |identity| identity.validate())
    }
}

#[derive(Clone)]
pub struct PreparedVortexSource(Arc<PreparedSourceOwner>);

impl PreparedVortexSource {
    /// Match prior file admission to the generation held by this native reader,
    /// then validate both its descriptor and current path. This performs no
    /// payload read, provider open, or query execution.
    ///
    /// # Errors
    /// Rejects nonregular or mismatched metadata, changed source generations,
    /// in-memory sources, and poisoned session admission.
    pub fn validate_file_metadata(&self, expected: &Metadata) -> Result<()> {
        let _gate = self
            .0
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        let identity = self.0.identity.as_ref().ok_or_else(|| {
            resident_error("file metadata admission is not available for an in-memory source")
        })?;
        if !expected.is_file() || FileGeneration::read(expected)? != identity.generation {
            return Err(resident_error(
                "prepared source does not match the admitted file generation",
            ));
        }
        identity.validate()
    }

    #[cfg(unix)]
    pub(crate) fn resource_limits(&self) -> (u64, usize) {
        (
            self.0.runtime.memory.snapshot().limit_bytes,
            self.0.runtime.parallelism,
        )
    }

    pub(crate) fn validate_generation(&self) -> Result<()> {
        self.0.validate()
    }

    /// Cheap immutable-metadata preflight before optional duplicate planning.
    /// This neither visits children nor replaces execution generation validation.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn has_segment_reuse_field_root(&self) -> bool {
        self.0
            .file
            .footer()
            .layout()
            .is::<vortex::layout::layouts::struct_::Struct>()
    }

    /// Query-local reuse admission uses the exact lowered filter/projection and
    /// only this retained file's root/schema. It does not open or execute a scan.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn segment_reuse_policy(
        &self,
        predicate: &shardloom_core::PredicateExpr,
        projected_columns: &[shardloom_core::ColumnRef],
    ) -> Result<Option<segment_reuse::SegmentReusePolicy>> {
        let source = &self.0;
        let _gate = source
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.validate()?;
        let policy = segment_reuse::SegmentReusePolicy::for_scan(
            predicate,
            projected_columns,
            source.file.footer().layout().as_ref(),
            source.runtime.memory.snapshot().limit_bytes,
        );
        source.validate()?;
        Ok(policy)
    }

    /// Drive native work under one source generation, allocator, and admission
    /// gate. Callers drain borrowed work inside the closure and expose output
    /// only after this method's final generation validation succeeds.
    #[cfg(all(
        any(feature = "vortex-write", feature = "vortex-local-primitives"),
        unix
    ))]
    pub(crate) fn with_native_execution<T>(
        &self,
        execute: impl FnOnce(&VortexFile, &VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<T> {
        let source = &self.0;
        let _gate = source
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.validate()?;
        let result = execute(
            &source.file,
            &source.runtime.session,
            &source.runtime.runtime,
        )?;
        source.validate()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Use when schema admission rejected the dedicated aggregate CPU pool.
    /// The caller must not create a second compute pool inside this callback.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn with_native_execution_temporary_drivers<T>(
        &self,
        execute: impl FnOnce(&VortexFile, &VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<(T, usize)> {
        let source = &self.0;
        let _gate = source
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.validate()?;
        let additional = source
            .runtime
            .parallelism
            .saturating_sub(1 + source.runtime.provider_background_workers);
        let _workers =
            ResidentWorkerGroup::new(&source.runtime.runtime, additional).map_err(native_error)?;
        let result = execute(
            &source.file,
            &source.runtime.session,
            &source.runtime.runtime,
        )?;
        source.validate()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok((
            result,
            additional + source.runtime.provider_background_workers,
        ))
    }

    /// Reuse compressed segments only within this admitted native operation.
    /// The returned snapshot follows cache close; result-owned slices may still
    /// retain payload credit until their last owner drops. No query answers or
    /// layout readers survive in the cache for a subsequent prepared execution.
    #[cfg(all(
        test,
        feature = "vortex-local-primitives",
        feature = "vortex-write",
        unix
    ))]
    pub(crate) fn with_native_execution_cached<T>(
        &self,
        policy: segment_reuse::SegmentReusePolicy,
        mut execute: impl FnMut(&VortexFile, &VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<(T, segment_reuse::SegmentReuseSnapshot)> {
        self.with_native_execution_cached_retry(policy, |file, session, runtime, _| {
            execute(file, session, runtime)
        })
    }

    #[cfg(all(
        test,
        feature = "vortex-local-primitives",
        feature = "vortex-write",
        unix
    ))]
    pub(crate) fn with_native_execution_cached_retry<T>(
        &self,
        policy: segment_reuse::SegmentReusePolicy,
        execute: impl FnMut(
            &VortexFile,
            &VortexSession,
            &CurrentThreadRuntime,
            &mut SegmentReuseAttempt,
        ) -> Result<T>,
    ) -> Result<(T, segment_reuse::SegmentReuseSnapshot)> {
        self.with_native_execution_cached_retry_with_drivers(policy, false, execute)
    }

    /// Temporary provider drivers are permitted only when the caller has
    /// rejected its dedicated compute pool. All driver creation and teardown
    /// occurs inside the existing source/session execution gate.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn with_native_execution_cached_retry_with_drivers<T>(
        &self,
        policy: segment_reuse::SegmentReusePolicy,
        restore_provider_drivers: bool,
        mut execute: impl FnMut(
            &VortexFile,
            &VortexSession,
            &CurrentThreadRuntime,
            &mut SegmentReuseAttempt,
        ) -> Result<T>,
    ) -> Result<(T, segment_reuse::SegmentReuseSnapshot)> {
        let source = &self.0;
        let _gate = source
            .runtime
            .admission
            .lock()
            .map_err(|_| resident_error("session admission poisoned"))?;
        source.validate()?;
        let additional = if restore_provider_drivers {
            source
                .runtime
                .parallelism
                .saturating_sub(1 + source.runtime.provider_background_workers)
        } else {
            0
        };
        let _workers =
            ResidentWorkerGroup::new(&source.runtime.runtime, additional).map_err(native_error)?;
        let provider_workers = additional + source.runtime.provider_background_workers;
        let started = std::time::Instant::now();
        let generation = Arc::clone(source);
        let Some(cache) = segment_reuse::ScanSegmentReuse::try_new(
            source.file.segment_source(),
            source.runtime.memory.clone(),
            policy,
            move || {
                generation
                    .validate()
                    .map_err(|error| vortex_err!("{error}"))
            },
        )?
        else {
            let mut attempt = SegmentReuseAttempt {
                enabled: false,
                retry_requested: false,
            };
            let result = execute(
                &source.file,
                &source.runtime.session,
                &source.runtime.runtime,
                &mut attempt,
            )?;
            source.validate()?;
            source.runtime.executions.fetch_add(1, Ordering::Relaxed);
            let mut snapshot =
                segment_reuse::SegmentReuseSnapshot::skipped(policy, &source.runtime.memory);
            snapshot.provider_background_workers = provider_workers;
            return Ok((result, snapshot));
        };
        let file = source
            .file
            .clone()
            .with_segment_source(Arc::new(cache.clone()));
        let mut attempt = SegmentReuseAttempt {
            enabled: true,
            retry_requested: false,
        };
        let result = execute(
            &file,
            &source.runtime.session,
            &source.runtime.runtime,
            &mut attempt,
        );
        // Close on both success and failure; a leaked file clone cannot retain
        // cache entries or register new work after this execution boundary.
        cache.close().map_err(native_error)?;
        drop(file);
        let mut snapshot = cache.snapshot().map_err(native_error)?;
        snapshot.provider_background_workers = provider_workers;
        drop(cache);
        source.validate()?;
        let result = if result.is_err() && attempt.retry_requested {
            drop(result);
            snapshot.uncached_replays = 1;
            snapshot.discarded_attempt_nanos =
                u64::try_from(started.elapsed().as_nanos()).map_err(native_error)?;
            let replay_started = std::time::Instant::now();
            let mut attempt = SegmentReuseAttempt {
                enabled: false,
                retry_requested: false,
            };
            let result = execute(
                &source.file,
                &source.runtime.session,
                &source.runtime.runtime,
                &mut attempt,
            );
            source.validate()?;
            snapshot.uncached_replay_nanos =
                u64::try_from(replay_started.elapsed().as_nanos()).map_err(native_error)?;
            snapshot.session = source.runtime.memory.snapshot();
            result?
        } else {
            result?
        };
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok((result, snapshot))
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
        self.0.validate()?;
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
            row_range: None,
            max_rows,
            max_output_bytes,
        })
    }
}

pub struct PreparedVortexCount(PreparedVortexSource);

impl PreparedVortexCount {
    /// Validate file admission against this count's actual retained source.
    /// No count is executed and no provider is opened.
    ///
    /// # Errors
    /// Returns the source's metadata mismatch or generation validation error.
    pub fn validate_file_metadata(&self, expected: &Metadata) -> Result<()> {
        self.0.validate_file_metadata(expected)
    }

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
        source.validate()?;
        let rows = source.file.row_count();
        source.validate()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(rows)
    }
}

pub struct PreparedVortexProjection {
    source: PreparedVortexSource,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    row_range: Option<std::ops::Range<u64>>,
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
    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn with_row_range(mut self, range: std::ops::Range<u64>) -> Result<Self> {
        if range.start > range.end || range.end > self.source.0.file.row_count() {
            return Err(resident_error(
                "projection row range exceeds source generation",
            ));
        }
        self.row_range = Some(range);
        Ok(self)
    }
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
        source.validate()?;
        let scan = source
            .file
            .scan()
            .map_err(native_error)?
            .with_projection(self.projection.clone())
            .with_some_filter(self.filter.clone())
            .with_ordered(true)
            .with_concurrency(runtime.parallelism);
        let scan = if let Some(range) = &self.row_range {
            scan.with_row_range(range.clone())
        } else {
            scan
        };
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
        source.validate()?;
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

pub(crate) struct SourceIdentity {
    path: PathBuf,
    file: File,
    generation: FileGeneration,
    invalidated: AtomicBool,
}

impl SourceIdentity {
    /// Verify bytes from the same held descriptor used by native positional
    /// reads. Generation checks surround hashing; no pathname is reopened.
    #[cfg(unix)]
    pub(crate) fn verify_contents(
        &self,
        expected_identity: (u64, u64),
        expected_bytes: u64,
        expected_digest: &[u8; 32],
        scratch: &mut [u8],
    ) -> Result<()> {
        use sha2::{Digest as _, Sha256};
        use std::os::unix::fs::FileExt as _;
        self.validate()?;
        if (self.generation.device, self.generation.inode) != expected_identity
            || self.generation.len != expected_bytes
            || scratch.is_empty()
        {
            return Err(resident_error(
                "owned native file identity or byte length changed",
            ));
        }
        let mut digest = Sha256::new();
        let mut offset = 0_u64;
        while offset < expected_bytes {
            let capacity = usize::try_from((expected_bytes - offset).min(scratch.len() as u64))
                .map_err(native_error)?;
            let count = self
                .file
                .read_at(&mut scratch[..capacity], offset)
                .map_err(native_error)?;
            if count == 0 {
                return Err(resident_error(
                    "owned native file ended before its declared byte length",
                ));
            }
            digest.update(&scratch[..count]);
            offset = offset.checked_add(count as u64).ok_or_else(|| {
                resident_error("owned native file verification offset overflowed")
            })?;
        }
        self.validate()?;
        let actual: [u8; 32] = digest.finalize().into();
        if &actual != expected_digest {
            return Err(resident_error("owned native file checksum changed"));
        }
        Ok(())
    }

    pub(crate) fn reader(
        self: &Arc<Self>,
        allocator: HostAllocatorRef,
        handle: Handle,
        concurrency: usize,
    ) -> Arc<dyn VortexReadAt> {
        Arc::new(ResidentFileReadAt {
            identity: Arc::clone(self),
            allocator,
            handle,
            concurrency,
        })
    }

    /// Capture a generation for another native path that must reopen the source.
    /// Validation checks both the retained handle and the current source path.
    pub(crate) fn capture(path: &Path) -> Result<Self> {
        let path = std::path::absolute(path).map_err(native_error)?;
        let file = File::open(&path).map_err(native_error)?;
        let metadata = file.metadata().map_err(native_error)?;
        if !metadata.is_file() {
            return Err(resident_error("source must be a regular file"));
        }
        let identity = Self {
            path,
            file,
            generation: FileGeneration::read(&metadata)?,
            invalidated: AtomicBool::new(false),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub(crate) fn validate(&self) -> Result<()> {
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
#[path = "resident_session_read_observer.rs"]
pub(crate) mod read_observer;

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "resident_session_tests.rs"]
mod tests;
