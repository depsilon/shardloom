//! A borrowed operation boundary; nested operators use this grant directly.

use super::{
    CallClass, RuntimeOwner, SourceIdentity,
    io_ownership::{IoBudget, IoScope},
    resident_error,
    serving_admission::Permit,
};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    borrow::Cow,
    sync::{Arc, MutexGuard, OnceLock},
    time::{Duration, Instant},
};
use vortex::{
    file::{
        VortexFile,
        segments::{FileSegmentSource, RequestMetrics},
    },
    io::runtime::BlockingRuntime as _,
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
                self.owner.parallelism
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

    pub(super) fn file_view<'a>(
        &self,
        file: &'a VortexFile,
        identity: Option<&Arc<SourceIdentity>>,
    ) -> Result<Cow<'a, VortexFile>> {
        let Some(identity) = identity else {
            return Ok(Cow::Borrowed(file));
        };
        self.check_general_execution()?;
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
            let scope = IoScope::new(budget, &self.owner.memory, self.cancellation.clone())?;
            let _ = self.io.set(scope);
        }
        let scope = self
            .io
            .get()
            .ok_or_else(|| resident_error("native file execution has no I/O owner"))?;
        // Keep the held descriptor, parsed footer and user metadata. A fresh
        // provider reader tree gives this operation an exact I/O lifetime; it
        // does not reopen the file or cache an answer. Batch readers are unchanged.
        let reader = super::ResidentFileReadAt {
            identity: Arc::clone(identity),
            allocator: self.owner.session.allocator(),
            handle: self.owner.runtime.handle(),
            concurrency: self.cpu_lanes(),
            scope: Some(Arc::clone(scope)),
            _reader_owner: Some(scope.retain_reader(&self.owner.memory)?),
        };
        #[cfg(all(test, unix, feature = "vortex-write"))]
        let reader = super::read_observer::observe_operation_reader(reader);
        let metrics = RequestMetrics::new(
            &vortex::metrics::DefaultMetricsRegistry::default(),
            Vec::new(),
        );
        let source = FileSegmentSource::open(
            Arc::clone(file.footer().segment_map()),
            reader,
            self.owner.runtime.handle(),
            metrics,
        );
        let source: Arc<dyn vortex::layout::segments::SegmentSource> = Arc::new(source);
        #[cfg(all(test, unix, feature = "vortex-write"))]
        let source = super::file_pruning_tests::observe_operation_segments(source);
        Ok(Cow::Owned(file.clone().with_segment_source(source)))
    }
}

impl Drop for NativeExecutionContext<'_> {
    fn drop(&mut self) {
        self.drain_io();
    }
}

use vortex::array::memory::MemorySessionExt as _;
