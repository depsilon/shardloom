//! A borrowed operation boundary; nested operators use this grant directly.

use super::{
    CallClass, RuntimeOwner, SourceIdentity, io_ownership::IoScope, resident_error,
    serving_admission::Permit,
};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    borrow::Cow,
    sync::{Arc, MutexGuard},
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
    io: Option<Arc<IoScope>>,
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
            io,
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
        self.io.clone()
    }

    pub(super) fn drain_io(&self) {
        if let Some(scope) = &self.io {
            scope.close_and_drain(&self.owner.runtime);
        }
    }

    pub(super) fn file_view<'a>(
        &self,
        file: &'a VortexFile,
        identity: Option<&Arc<SourceIdentity>>,
    ) -> Cow<'a, VortexFile> {
        let (Some(scope), Some(identity)) = (&self.io, identity) else {
            return Cow::Borrowed(file);
        };
        // Keep the held descriptor, parsed footer and user metadata. A fresh
        // provider reader tree gives this operation an exact I/O lifetime; it
        // does not reopen the file or cache an answer. Batch readers are unchanged.
        let reader = super::ResidentFileReadAt {
            identity: Arc::clone(identity),
            allocator: self.owner.session.allocator(),
            handle: self.owner.runtime.handle(),
            concurrency: self.cpu_lanes(),
            scope: Some(Arc::clone(scope)),
        };
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
        Cow::Owned(file.clone().with_segment_source(Arc::new(source)))
    }
}

impl Drop for NativeExecutionContext<'_> {
    fn drop(&mut self) {
        self.drain_io();
    }
}

use vortex::array::memory::MemorySessionExt as _;
