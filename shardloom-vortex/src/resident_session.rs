//! Caller-owned native readers, prepared operations, and actual array results.
//!
//! This initial resident surface supports metadata count and bounded projection.
//! It does not cache query answers or imply resident support for other operators.

use std::{
    fs::Metadata,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use futures::{FutureExt as _, future::BoxFuture};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;
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
use crate::source_identity::FileGeneration;
pub(crate) use crate::source_identity::SourceIdentity;

#[path = "resident_execution_context.rs"]
mod execution_context;
#[path = "resident_io_ownership.rs"]
mod io_ownership;
#[path = "resident_admission.rs"]
mod serving_admission;
pub use execution_context::{NativeExecutionContext, ResidentCallTiming};
pub use io_ownership::ResidentIoSnapshot;
use serving_admission::CallClass;
pub use serving_admission::{ResidentAdmissionSnapshot, ResidentServingPolicy};

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
    serving: Option<Arc<serving_admission::Admission>>,
    io_budget: Option<Arc<io_ownership::IoBudget>>,
    memory: LiveMemoryPool,
    parallelism: usize,
    provider_background_workers: usize,
    opens: AtomicU64,
    executions: AtomicU64,
}

/// A session shares provider registries and runtime workers across prepared calls.
/// Ordinary sessions serialize calls. Explicit serving sessions admit concurrent
/// work within shared CPU, queue and I/O limits; there is no hidden global.
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

    /// Package an array already completed inside this session's admitted source
    /// execution. Its producer reserved metadata before allocation and attached
    /// payload credits through our allocator. This does not execute or lock.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn own_completed_array(
        &self,
        array: ArrayRef,
        ownership: shardloom_exec::live_memory::MemoryLease,
    ) -> Result<OwnedVortexResultBatch> {
        let rows = u64::try_from(array.len()).map_err(native_error)?;
        let logical_buffer_bytes = array.nbytes();
        if rows > 65_536
            || logical_buffer_bytes > 8 * 1024 * 1024
            || ownership.bytes() < std::mem::size_of::<ArrayRef>() as u64
        {
            return Err(resident_error(
                "completed aggregate exceeds output ownership admission",
            ));
        }
        Ok(OwnedVortexResultBatch {
            dtype: array.dtype().clone(),
            arrays: Budgeted::new(vec![array], ownership),
            runtime: Arc::clone(&self.0),
            rows,
            logical_buffer_bytes,
        })
    }

    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn with_native_session<T>(
        &self,
        execute: impl FnOnce(&VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<T> {
        let _context = self
            .0
            .enter(CallClass::General, CancellationToken::default())?;
        execute(&self.0.session, &self.0.runtime)
    }

    /// Admit construction work without recording a completed query. Nested
    /// operators borrow the supplied context instead of reacquiring admission.
    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn with_native_execution_context<T>(
        &self,
        cancellation: &CancellationToken,
        execute: impl FnOnce(&NativeExecutionContext<'_>) -> Result<T>,
    ) -> Result<T> {
        let context = self.0.enter(CallClass::General, cancellation.clone())?;
        let result = execute(&context);
        context.drain_io();
        let completion = context.check_cancelled();
        let result = result?;
        completion?;
        Ok(result)
    }

    /// Reject a borrowed grant from another session before construction or I/O.
    #[cfg(unix)]
    pub(crate) fn validate_execution_context(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if !context.belongs_to(&self.0) {
            return Err(resident_error(
                "native execution context belongs to a different session",
            ));
        }
        context.check_cancelled()
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
        let _context = self
            .0
            .enter(CallClass::General, CancellationToken::default())?;
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
            dtype: array.dtype().clone(),
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

    /// Create a concurrent session with bounded per-class FIFO admission.
    /// General calls own `general_cpu_lanes` including the caller. When P >= 2,
    /// one CPU lane can remain available to metadata calls during general work.
    /// Drivers used by general operations are per-operation and join before the
    /// CPU grant is returned; this constructor creates no persistent CPU drivers.
    ///
    /// # Errors
    /// Rejects invalid CPU, memory, queue and I/O envelopes.
    pub fn with_serving_policy(
        memory_bytes: u64,
        max_parallelism: usize,
        policy: ResidentServingPolicy,
    ) -> Result<Self> {
        let mut session = Self::with_cpu_driver_policy(memory_bytes, max_parallelism, true)?;
        let owner = Arc::get_mut(&mut session.0)
            .ok_or_else(|| resident_error("new serving session is not uniquely owned"))?;
        owner.serving = Some(serving_admission::Admission::new(
            policy,
            owner.parallelism,
            &owner.memory,
        )?);
        owner.io_budget = Some(io_ownership::IoBudget::new(
            policy.max_io_requests,
            policy.max_io_bytes,
        ));
        owner.parallelism = policy.general_cpu_lanes;
        Ok(session)
    }

    #[must_use]
    pub fn admission_snapshot(&self) -> Option<ResidentAdmissionSnapshot> {
        self.0
            .serving
            .as_ref()
            .map(|admission| admission.snapshot())
    }

    #[must_use]
    pub fn io_snapshot(&self) -> Option<ResidentIoSnapshot> {
        self.0.io_budget.as_ref().map(|io| io.snapshot())
    }

    /// Stop accepting serving calls and release queued callers with cancellation
    /// errors. Already active calls retain their owners until they finish/drain.
    /// Ordinary exclusive sessions are unaffected.
    pub fn close_admission(&self) {
        if let Some(admission) = &self.0.serving {
            admission.close();
        }
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
            serving: None,
            io_budget: None,
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
        let context = self
            .0
            .enter(CallClass::General, CancellationToken::default())?;
        let identity = Arc::new(SourceIdentity::capture(path.as_ref())?);
        let input = identity.reader_scoped(
            self.0.session.allocator(),
            self.0.runtime.handle(),
            self.0.parallelism,
            context.io_scope(),
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
        context.drain_io();
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
    /// Retain the source's existing owner without creating a second runtime.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn retained_session(&self) -> ResidentVortexSession {
        ResidentVortexSession(Arc::clone(&self.0.runtime))
    }

    /// Match prior file admission to the generation held by this native reader,
    /// then validate both its descriptor and current path. This performs no
    /// payload read, provider open, or query execution.
    ///
    /// # Errors
    /// Rejects nonregular or mismatched metadata, changed source generations,
    /// in-memory sources, and poisoned session admission.
    pub fn validate_file_metadata(&self, expected: &Metadata) -> Result<()> {
        let _context = self
            .0
            .runtime
            .enter(CallClass::Metadata, CancellationToken::default())?;
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
        let context = source
            .runtime
            .enter(CallClass::Metadata, CancellationToken::default())?;
        self.segment_reuse_policy_in_context(&context, predicate, projected_columns)
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn segment_reuse_policy_in_context(
        &self,
        context: &NativeExecutionContext<'_>,
        predicate: &shardloom_core::PredicateExpr,
        projected_columns: &[shardloom_core::ColumnRef],
    ) -> Result<Option<segment_reuse::SegmentReusePolicy>> {
        self.retained_session()
            .validate_execution_context(context)?;
        let source = &self.0;
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
        self.with_native_execution_controlled(&CancellationToken::default(), |file, context| {
            execute(file, context.native_session(), context.runtime())
        })
    }

    /// Admit once; composed operators borrow this context instead of reacquiring
    /// the same session. CPU jobs must be joined inside the callback. The context
    /// closes and drains its native I/O before returning its CPU grant.
    #[cfg(all(
        any(feature = "vortex-write", feature = "vortex-local-primitives"),
        unix
    ))]
    pub(crate) fn with_native_execution_controlled<T>(
        &self,
        cancellation: &CancellationToken,
        execute: impl FnOnce(&VortexFile, &NativeExecutionContext<'_>) -> Result<T>,
    ) -> Result<T> {
        let source = &self.0;
        let context = source
            .runtime
            .enter(CallClass::General, cancellation.clone())?;
        let result = self.with_admitted_native_execution(&context, execute)?;
        context.drain_io();
        context.check_cancelled()?;
        if source.runtime.serving.is_some() {
            source.validate()?;
        }
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Compose another source/operator under the current operation grant.
    /// This neither reacquires admission nor reports a second completed call.
    #[cfg(all(
        any(feature = "vortex-write", feature = "vortex-local-primitives"),
        unix
    ))]
    pub(crate) fn with_admitted_native_execution<T>(
        &self,
        context: &NativeExecutionContext<'_>,
        execute: impl FnOnce(&VortexFile, &NativeExecutionContext<'_>) -> Result<T>,
    ) -> Result<T> {
        context.check_general_execution()?;
        if !context.belongs_to(&self.0.runtime) {
            return Err(resident_error(
                "native execution context belongs to a different session",
            ));
        }
        context.check_cancelled()?;
        self.0.validate()?;
        let file = context.file_view(&self.0.file, self.0.identity.as_ref());
        let result = execute(&file, context)?;
        context.check_cancelled()?;
        self.0.validate()?;
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
        let context = source
            .runtime
            .enter(CallClass::General, CancellationToken::default())?;
        let result = self.with_admitted_native_execution_temporary_drivers(&context, execute)?;
        context.drain_io();
        context.check_cancelled()?;
        source.validate()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn with_admitted_native_execution_temporary_drivers<T>(
        &self,
        context: &NativeExecutionContext<'_>,
        execute: impl FnOnce(&VortexFile, &VortexSession, &CurrentThreadRuntime) -> Result<T>,
    ) -> Result<(T, usize)> {
        context.check_general_execution()?;
        self.retained_session()
            .validate_execution_context(context)?;
        let source = &self.0;
        source.validate()?;
        let additional = context
            .cpu_lanes()
            .saturating_sub(1 + source.runtime.provider_background_workers);
        let workers =
            ResidentWorkerGroup::new(&source.runtime.runtime, additional).map_err(native_error)?;
        let file = context.file_view(&source.file, source.identity.as_ref());
        let result = execute(&file, &source.runtime.session, &source.runtime.runtime)?;
        drop(file);
        drop(workers);
        context.check_cancelled()?;
        source.validate()?;
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
        execute: impl FnMut(
            &VortexFile,
            &VortexSession,
            &CurrentThreadRuntime,
            &mut SegmentReuseAttempt,
        ) -> Result<T>,
    ) -> Result<(T, segment_reuse::SegmentReuseSnapshot)> {
        let source = &self.0;
        let context = source
            .runtime
            .enter(CallClass::General, CancellationToken::default())?;
        let result = self.with_admitted_native_execution_cached_retry_with_drivers(
            &context,
            policy,
            restore_provider_drivers,
            execute,
        )?;
        context.drain_io();
        context.check_cancelled()?;
        if source.runtime.serving.is_some() {
            source.validate()?;
        }
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok(result)
    }

    /// Borrow the outer operation while owning only this stage's cache and
    /// provider drivers. Closing a stage never closes the outer I/O scope.
    #[allow(clippy::too_many_lines)]
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn with_admitted_native_execution_cached_retry_with_drivers<T>(
        &self,
        context: &NativeExecutionContext<'_>,
        policy: segment_reuse::SegmentReusePolicy,
        restore_provider_drivers: bool,
        mut execute: impl FnMut(
            &VortexFile,
            &VortexSession,
            &CurrentThreadRuntime,
            &mut SegmentReuseAttempt,
        ) -> Result<T>,
    ) -> Result<(T, segment_reuse::SegmentReuseSnapshot)> {
        context.check_general_execution()?;
        self.retained_session()
            .validate_execution_context(context)?;
        let source = &self.0;
        source.validate()?;
        let operation_file = context.file_view(&source.file, source.identity.as_ref());
        let additional = if restore_provider_drivers {
            context
                .cpu_lanes()
                .saturating_sub(1 + source.runtime.provider_background_workers)
        } else {
            0
        };
        let workers =
            ResidentWorkerGroup::new(&source.runtime.runtime, additional).map_err(native_error)?;
        let provider_workers = additional + source.runtime.provider_background_workers;
        let started = std::time::Instant::now();
        let generation = Arc::clone(source);
        let Some(cache) = segment_reuse::ScanSegmentReuse::try_new(
            operation_file.segment_source(),
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
                &operation_file,
                &source.runtime.session,
                &source.runtime.runtime,
                &mut attempt,
            )?;
            drop(operation_file);
            drop(workers);
            context.check_cancelled()?;
            source.validate()?;
            let mut snapshot =
                segment_reuse::SegmentReuseSnapshot::skipped(policy, &source.runtime.memory);
            snapshot.provider_background_workers = provider_workers;
            return Ok((result, snapshot));
        };
        let file = operation_file
            .as_ref()
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
            context.check_cancelled()?;
            snapshot.uncached_replays = 1;
            snapshot.discarded_attempt_nanos =
                u64::try_from(started.elapsed().as_nanos()).map_err(native_error)?;
            let replay_started = std::time::Instant::now();
            let mut attempt = SegmentReuseAttempt {
                enabled: false,
                retry_requested: false,
            };
            let result = execute(
                &operation_file,
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
        drop(operation_file);
        drop(workers);
        context.check_cancelled()?;
        if source.runtime.serving.is_some() {
            source.validate()?;
        }
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
        self.execute_with_cancellation(&CancellationToken::default())
    }

    /// Execute a generation-validated footer count. In a serving session,
    /// cancellation while queued is observed without waiting for bulk work.
    /// # Errors
    /// Rejects cancellation, source changes, or closed/full serving admission.
    pub fn execute_with_cancellation(&self, cancellation: &CancellationToken) -> Result<u64> {
        self.execute_timed(cancellation).map(|(result, _)| result)
    }

    /// Return the count and this call's admission/service timings.
    /// # Errors
    /// Returns the same cancellation, generation and admission errors as execute.
    pub fn execute_timed(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(u64, ResidentCallTiming)> {
        let source = &self.0.0;
        let context = source
            .runtime
            .enter(CallClass::Metadata, cancellation.clone())?;
        source.validate()?;
        context.check_cancelled()?;
        let rows = source.file.row_count();
        source.validate()?;
        context.check_cancelled()?;
        source.runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok((rows, context.timing()))
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
    dtype: DType,
    arrays: Budgeted<Vec<ArrayRef>>,
    runtime: Arc<RuntimeOwner>,
    rows: u64,
    logical_buffer_bytes: u64,
}

impl OwnedVortexResultBatch {
    /// Authoritative result schema, including results with no data arrays.
    #[must_use]
    pub const fn dtype(&self) -> &DType {
        &self.dtype
    }

    /// Validate the complete batch sequence before native composition or export.
    /// Empty results keep their schema and must still declare zero rows.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(crate) fn validate_schema_and_rows(&self) -> Result<()> {
        let rows = self.arrays().iter().try_fold(0_u64, |rows, array| {
            if array.dtype() != &self.dtype {
                return Err(resident_error("completed result arrays disagree on dtype"));
            }
            rows.checked_add(u64::try_from(array.len()).map_err(native_error)?)
                .ok_or_else(|| resident_error("completed result row count overflow"))
        })?;
        if rows != self.rows {
            return Err(resident_error(
                "completed result arrays disagree on row count",
            ));
        }
        Ok(())
    }

    #[cfg(all(feature = "vortex-write", unix))]
    pub(crate) fn retained_session(&self) -> ResidentVortexSession {
        ResidentVortexSession(Arc::clone(&self.runtime))
    }
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
        self.execute_with_cancellation(&CancellationToken::default())
    }

    /// Complete this projection with the supplied operation cancellation flag.
    /// Queued serving calls cancel before provider work. Active calls check at
    /// native array boundaries, then drain admitted I/O before releasing grants.
    /// # Errors
    /// Rejects cancellation, source changes, provider failures and resource bounds.
    pub fn execute_with_cancellation(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<OwnedVortexResultBatch> {
        self.execute_timed(cancellation).map(|(result, _)| result)
    }

    /// Return complete native arrays and this call's admission/service timings.
    /// Rendering, caller-owned transport and returned-result drop are separate.
    /// # Errors
    /// Returns the same generation, cancellation, scan and ownership errors as execute.
    pub fn execute_timed(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(OwnedVortexResultBatch, ResidentCallTiming)> {
        let source = &self.source.0;
        let runtime = &source.runtime;
        let context = runtime.enter(CallClass::General, cancellation.clone())?;
        let additional = if runtime.serving.is_some() {
            context.cpu_lanes().saturating_sub(1)
        } else {
            0
        };
        let workers =
            ResidentWorkerGroup::new(&runtime.runtime, additional).map_err(native_error)?;
        source.validate()?;
        let file = context.file_view(&source.file, source.identity.as_ref());
        let scan = file
            .scan()
            .map_err(native_error)?
            .with_projection(self.projection.clone())
            .with_some_filter(self.filter.clone())
            .with_ordered(true)
            .with_concurrency(context.cpu_lanes());
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
            context.check_cancelled()?;
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
        drop(scan);
        drop(file);
        drop(workers);
        context.drain_io();
        context.check_cancelled()?;
        source.validate()?;
        runtime.executions.fetch_add(1, Ordering::Relaxed);
        Ok((
            OwnedVortexResultBatch {
                dtype: self.projection.dtype().clone(),
                arrays: Budgeted::new(arrays, lease),
                runtime: Arc::clone(runtime),
                rows,
                logical_buffer_bytes: logical_bytes,
            },
            context.timing(),
        ))
    }
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
        self.reader_scoped(allocator, handle, concurrency, None)
    }

    fn reader_scoped(
        self: &Arc<Self>,
        allocator: HostAllocatorRef,
        handle: Handle,
        concurrency: usize,
        scope: Option<Arc<io_ownership::IoScope>>,
    ) -> Arc<dyn VortexReadAt> {
        Arc::new(ResidentFileReadAt {
            identity: Arc::clone(self),
            allocator,
            handle,
            concurrency,
            scope,
        })
    }
}

#[derive(Clone)]
struct ResidentFileReadAt {
    identity: Arc<SourceIdentity>,
    allocator: HostAllocatorRef,
    handle: Handle,
    concurrency: usize,
    scope: Option<Arc<io_ownership::IoScope>>,
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
        let scope = self.scope.clone();
        async move {
            let job = scope
                .as_ref()
                .map(|scope| scope.admit(length))
                .transpose()
                .map_err(|error| vortex_err!("{error}"))?;
            let completion = handle
                .spawn_blocking(move || {
                    let result = (|| {
                        if let Some(job) = &job {
                            job.check_cancelled()
                                .map_err(|error| vortex_err!("{error}"))?;
                        }
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
                    })();
                    io_ownership::ReadCompletion { result, _job: job }
                })
                .await;
            completion.result
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

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "resident_file_serving_tests.rs"]
mod file_serving_tests;

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "resident_concurrent_serving_tests.rs"]
mod concurrent_serving_tests;

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "resident_file_pruning_tests.rs"]
mod file_pruning_tests;

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "memory_file_composition_tests.rs"]
mod composition_tests;

#[cfg(all(test, unix, feature = "vortex-write"))]
#[path = "memory_file_composition_bench.rs"]
mod composition_bench;
