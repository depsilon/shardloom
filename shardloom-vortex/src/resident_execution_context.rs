//! A borrowed operation boundary; nested operators use this grant directly.

use super::{
    CallClass, RuntimeOwner, SourceIdentity,
    io_ownership::{IoBudget, IoScope},
    resident_error,
    serving_admission::Permit,
};
use futures::FutureExt as _;
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use std::{
    borrow::Cow,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::{Duration, Instant},
};
use vortex::{
    error::{SharedVortexResult, vortex_err},
    file::{
        OpenOptionsSessionExt as _, SegmentMap, VortexFile,
        segments::{FileSegmentSource, RequestMetrics},
    },
    io::runtime::BlockingRuntime as _,
    layout::segments::{SegmentFuture, SegmentId, SegmentSource, SharedSegmentSource},
    session::VortexSession,
};

enum Gate<'a> {
    Exclusive(MutexGuard<'a, ()>, Duration),
    Serving(Permit),
}

/// Per-call native timings. Service includes generation checks and scoped I/O
/// drain, but excludes caller-side result conversion and admission-guard drop.
#[derive(Debug, Clone, Copy)]
pub struct ResidentCallTiming {
    pub queue: Duration,
    pub service: Duration,
}

/// Borrowed CPU, cancellation and I/O ownership for one admitted native operation.
/// It cannot be constructed or cloned by an operator. Nested operators borrow it
/// rather than reacquiring request admission. A callback must join its own compute
/// jobs before returning. Running OS reads drain before this grant is released.
pub struct NativeExecutionContext<'a> {
    owner: &'a RuntimeOwner,
    class: CallClass,
    cancellation: CancellationToken,
    io: OnceLock<Arc<IoScope>>,
    admitted: Instant,
    gate: Gate<'a>,
    opened_sources: Mutex<OpenedSources>,
}

#[derive(Default)]
struct OpenedSources {
    // Held descriptors and their control credits survive all native reads.
    values: Vec<(Arc<SourceIdentity>, MemoryLease)>,
    capacity: Option<MemoryLease>,
}

impl RuntimeOwner {
    pub(super) fn enter(
        &self,
        class: CallClass,
        cancellation: CancellationToken,
    ) -> Result<NativeExecutionContext<'_>> {
        cancellation.check()?;
        let queued = Instant::now();
        let gate = if let Some(admission) = &self.serving {
            Gate::Serving(admission.admit(class, 0, &cancellation)?)
        } else {
            let guard = self
                .admission
                .lock()
                .map_err(|_| resident_error("session admission poisoned"))?;
            Gate::Exclusive(guard, queued.elapsed())
        };
        let admitted = Instant::now();
        cancellation.check()?;
        let io = if class == CallClass::General {
            self.io_budget
                .as_ref()
                .map(|budget| IoScope::new(Arc::clone(budget), &self.memory, cancellation.clone()))
                .transpose()?
        } else {
            None
        };
        Ok(NativeExecutionContext {
            owner: self,
            class,
            cancellation,
            io: io.map_or_else(OnceLock::new, OnceLock::from),
            admitted,
            gate,
            opened_sources: Mutex::new(OpenedSources::default()),
        })
    }
}

impl NativeExecutionContext<'_> {
    /// Metadata-only admission cannot be expanded into scanning or construction.
    pub(crate) fn check_general_execution(&self) -> Result<()> {
        self.check_cancelled()?;
        if self.class != CallClass::General {
            return Err(resident_error(
                "native execution requires a general operation grant",
            ));
        }
        Ok(())
    }

    /// Effective CPU grant includes the calling thread.
    #[must_use]
    pub fn cpu_lanes(&self) -> usize {
        match &self.gate {
            Gate::Exclusive(guard, _) => {
                let () = &**guard;
                self.owner.cpu_limit()
            }
            Gate::Serving(permit) => permit.lanes,
        }
    }

    #[must_use]
    pub fn queue_time(&self) -> Duration {
        match &self.gate {
            Gate::Exclusive(_, queue) => *queue,
            Gate::Serving(permit) => permit.queue_time,
        }
    }

    #[must_use]
    pub fn timing(&self) -> ResidentCallTiming {
        ResidentCallTiming {
            queue: self.queue_time(),
            service: self.admitted.elapsed(),
        }
    }

    /// # Errors
    /// Returns the existing deterministic cooperative cancellation error.
    pub fn check_cancelled(&self) -> Result<()> {
        self.cancellation.check()
    }

    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    #[must_use]
    pub fn native_session(&self) -> &VortexSession {
        &self.owner.session
    }

    #[must_use]
    pub fn runtime(&self) -> &vortex::io::runtime::current::CurrentThreadRuntime {
        &self.owner.runtime
    }

    #[must_use]
    pub fn memory(&self) -> &shardloom_exec::live_memory::LiveMemoryPool {
        &self.owner.memory
    }

    pub(super) fn belongs_to(&self, owner: &RuntimeOwner) -> bool {
        std::ptr::eq(self.owner, owner)
    }

    pub(super) fn io_scope(&self) -> Option<Arc<IoScope>> {
        self.io.get().cloned()
    }

    pub(super) fn drain_io(&self) {
        if let Some(scope) = self.io.get() {
            scope.close_and_drain(&self.owner.runtime);
        }
    }

    /// Synchronous primitive adapters can open another file under this same
    /// operation. Its generation stays held until the final I/O drain, without
    /// retaining every file's decoded footer or creating a second runtime.
    pub(crate) async fn open_local_file(
        &self,
        path: &std::path::Path,
    ) -> vortex::error::VortexResult<VortexFile> {
        let core = || -> Result<Arc<SourceIdentity>> {
            self.check_general_execution()?;
            // A conservative control estimate, distinct from provider buffer
            // accounting. Reserve before opening or copying the local pathname.
            let bytes = u64::try_from(path.as_os_str().len())
                .ok()
                .and_then(|bytes| bytes.checked_mul(2))
                .and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(|| resident_error("source identity capacity overflow"))?;
            let ownership = self.owner.memory.reserve(bytes)?;
            let mut sources = self
                .opened_sources
                .lock()
                .map_err(|_| resident_error("source generation ownership poisoned"))?;
            let OpenedSources { values, capacity } = &mut *sources;
            if values.len() == values.capacity() {
                let target = values
                    .capacity()
                    .max(4)
                    .checked_mul(2)
                    .ok_or_else(|| resident_error("source generation capacity overflow"))?;
                let bytes = target
                    .checked_mul(std::mem::size_of::<(Arc<SourceIdentity>, MemoryLease)>())
                    .and_then(|bytes| u64::try_from(bytes).ok())
                    .ok_or_else(|| resident_error("source generation capacity overflow"))?;
                let credit = capacity.get_or_insert(self.owner.memory.reserve(0)?);
                credit.resize(
                    credit
                        .bytes()
                        .checked_add(bytes)
                        .ok_or_else(|| resident_error("source generation capacity overflow"))?,
                )?;
                values
                    .try_reserve_exact(target - values.len())
                    .map_err(|error| resident_error(&error.to_string()))?;
                if values.capacity() > target {
                    return Err(resident_error(
                        "source generation allocation exceeded admission",
                    ));
                }
                credit.resize(bytes)?;
            }
            self.check_cancelled()?;
            let identity = Arc::new(SourceIdentity::capture(path)?);
            values.push((Arc::clone(&identity), ownership));
            Ok(identity)
        };
        let identity = core().map_err(|error| vortex_err!("{error}"))?;
        let scope = self
            .file_io_scope()
            .map_err(|error| vortex_err!("{error}"))?;
        // Opening creates a provider read driver even for a footer-only count.
        // Track that driver's destruction before replacing its segment source;
        // the operation cannot return while it still owns the shared grant.
        let input = Arc::new(super::ResidentFileReadAt {
            identity: Arc::clone(&identity),
            allocator: self.owner.session.allocator(),
            handle: self.owner.runtime.handle(),
            concurrency: self.cpu_lanes(),
            scope: Some(Arc::clone(scope)),
            reader_owner: Some(
                scope
                    .retain_reader(&self.owner.memory)
                    .map_err(|error| vortex_err!("{error}"))?,
            ),
        });
        let file = self
            .owner
            .session
            .open_options()
            .with_layout_reader_cache()
            .open(input)
            .await?;
        self.check_cancelled()
            .map_err(|error| vortex_err!("{error}"))?;
        identity
            .validate()
            .map_err(|error| vortex_err!("{error}"))?;
        self.file_view(&file, Some(&identity))
            .map(Cow::into_owned)
            .map_err(|error| vortex_err!("{error}"))
    }

    pub(crate) fn finish_opened_sources(&self) -> Result<()> {
        self.drain_io();
        self.check_cancelled()?;
        let sources = self
            .opened_sources
            .lock()
            .map_err(|_| resident_error("source generation ownership poisoned"))?;
        for (identity, _) in &sources.values {
            self.check_cancelled()?;
            identity.validate()?;
        }
        Ok(())
    }

    fn file_io_scope(&self) -> Result<&Arc<IoScope>> {
        if self.io.get().is_none() {
            // Ordinary calls need the same cancellation/drain boundary as
            // serving calls. Allocate it only for file execution: metadata and
            // retained in-memory results do not acquire an unused I/O owner.
            // Their reader concurrency and live allocator remain the limits;
            // this private budget adds lifetime tracking, not a serving policy.
            let budget = self
                .owner
                .io_budget
                .clone()
                .unwrap_or_else(|| IoBudget::new(usize::MAX, u64::MAX));
            let scope = IoScope::deferred(budget, &self.owner.memory, self.cancellation.clone())?;
            let _ = self.io.set(scope);
        }
        self.io
            .get()
            .ok_or_else(|| resident_error("native file execution has no I/O owner"))
    }

    pub(super) fn file_view<'a>(
        &self,
        file: &'a VortexFile,
        identity: Option<&Arc<SourceIdentity>>,
    ) -> Result<Cow<'a, VortexFile>> {
        let Some(identity) = identity else {
            return Ok(Cow::Borrowed(file));
        };
        self.check_general_execution()?;
        let scope = self.file_io_scope()?;
        // Keep the held descriptor, parsed footer and user metadata. A fresh
        // provider reader tree gives this operation an exact I/O lifetime; it
        // does not reopen the file or cache an answer. Batch readers are unchanged.
        let reader = super::ResidentFileReadAt {
            identity: Arc::clone(identity),
            allocator: self.owner.session.allocator(),
            handle: self.owner.runtime.handle(),
            concurrency: self.cpu_lanes(),
            scope: Some(Arc::clone(scope)),
            reader_owner: None,
        };
        let source = DeferredFileSegments {
            segments: file.footer().segment_map().clone(),
            reader,
            scope: Arc::clone(scope),
            memory: self.owner.memory.clone(),
            source: OnceLock::new(),
            #[cfg(all(test, unix, feature = "vortex-write"))]
            observation: super::read_observer::operation_observation(),
        };
        let source: Arc<dyn SegmentSource> = Arc::new(source);
        #[cfg(all(test, unix, feature = "vortex-write"))]
        let source = super::file_pruning_tests::observe_operation_segments(source);
        Ok(Cow::Owned(file.clone().with_segment_source(source)))
    }
}

/// Keep footer inspection and pruning ahead of reader admission. The first
/// actual segment request admits the existing native provider exactly once;
/// failures remain attached to this operation and never reopen/replay a source.
struct DeferredFileSegments {
    segments: SegmentMap,
    reader: super::ResidentFileReadAt,
    scope: Arc<IoScope>,
    memory: shardloom_exec::live_memory::LiveMemoryPool,
    source: OnceLock<SharedVortexResult<SharedSegmentSource<FileSegmentSource>>>,
    #[cfg(all(test, unix, feature = "vortex-write"))]
    observation: Option<super::read_observer::OperationObservation>,
}

impl SegmentSource for DeferredFileSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let source = self.source.get_or_init(|| {
            let reader = super::ResidentFileReadAt {
                reader_owner: Some(
                    self.scope
                        .retain_reader(&self.memory)
                        .map_err(|error| Arc::new(vortex_err!("{error}")))?,
                ),
                ..self.reader.clone()
            };
            #[cfg(all(test, unix, feature = "vortex-write"))]
            let reader =
                super::read_observer::observe_operation_reader(reader, self.observation.as_ref());
            // Preserve the provider's normal live-request sharing after the
            // source handoff. Multiple readers of one segment share its read
            // and owned buffer; weak request entries do not cache answers.
            Ok(SharedSegmentSource::new(
                FileSegmentSource::open_with_segments(
                    self.segments.clone(),
                    reader,
                    self.reader.handle.clone(),
                    RequestMetrics::new(
                        &vortex::metrics::DefaultMetricsRegistry::default(),
                        Vec::new(),
                    ),
                ),
            ))
        });
        match source {
            Ok(source) => source.request(id),
            Err(error) => futures::future::ready(Err(error.into())).boxed(),
        }
    }
}

impl Drop for NativeExecutionContext<'_> {
    fn drop(&mut self) {
        self.drain_io();
    }
}

use vortex::array::memory::MemorySessionExt as _;
