//! Test-only observations of completed native positional filesystem reads.
//!
//! Successful exact-read bytes include coalescing gaps and repeated reads, even
//! when their consumer has been cancelled. They are not physical device bytes:
//! the OS page cache is outside this observer. Failed exact reads may have read
//! an unknown partial prefix, reported separately and never counted as zero I/O.
//! Native payload allocations use the supplied allocator. Bounded observation
//! metadata is separate; neither boundary claims to account for process RSS.

use std::{
    path::Path,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use futures::{FutureExt as _, future::BoxFuture};
use shardloom_core::Result;
use vortex::{
    array::{buffer::BufferHandle, memory::HostAllocatorRef},
    buffer::Alignment,
    error::{VortexResult, vortex_err},
    io::{CoalesceConfig, VortexReadAt, runtime::Handle},
};

use super::{SourceIdentity, resident_error};

/// Logical requested-byte bounds; the native allocator separately owns any
/// alignment padding. Observation records have an independent fixed limit.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_field_names)] // Distinguish admission caps from observed counters.
pub(crate) struct ReadObservationLimits {
    pub(crate) max_read_bytes: usize,
    pub(crate) max_attempted_bytes: u64,
    pub(crate) max_requests: usize,
    pub(crate) max_in_flight: usize,
}

impl Default for ReadObservationLimits {
    fn default() -> Self {
        Self {
            max_read_bytes: 8 * 1024 * 1024,
            max_attempted_bytes: 128 * 1024 * 1024,
            max_requests: 512,
            max_in_flight: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompletedReadRange {
    pub(crate) offset: u64,
    pub(crate) length: usize,
}

/// `closed` distinguishes an idle checkpoint from the final drained snapshot.
/// Failed-read partial bytes are unknown; completed bytes are a lower bound on
/// filesystem bytes in any observation with `failed_read_calls != 0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadObservation {
    pub(crate) closed: bool,
    pub(crate) admitted_requests: usize,
    pub(crate) attempted_bytes: u64,
    pub(crate) completed_read_calls: usize,
    pub(crate) completed_read_bytes: u64,
    pub(crate) failed_read_calls: usize,
    pub(crate) failed_before_read: usize,
    pub(crate) cancelled_before_read: usize,
    pub(crate) rejected_requests: u64,
    pub(crate) pending_jobs: usize,
    pub(crate) peak_in_flight: usize,
    pub(crate) completed_ranges: Vec<CompletedReadRange>,
}

#[derive(Clone, Copy)]
enum ReadOutcome {
    Pending,
    Reading,
    Completed,
    FailedRead,
    FailedBeforeRead,
    CancelledBeforeRead,
}

struct ReadEvent {
    range: CompletedReadRange,
    outcome: ReadOutcome,
}

struct State {
    closed: bool,
    attempted_bytes: u64,
    rejected_requests: u64,
    pending: usize,
    peak_pending: usize,
    events: Vec<ReadEvent>,
}

struct SharedObservation {
    limits: ReadObservationLimits,
    state: Mutex<State>,
    idle: Condvar,
}

impl SharedObservation {
    fn lock(&self) -> VortexResult<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| vortex_err!("filesystem read observation lock poisoned"))
    }

    fn admit(
        self: &Arc<Self>,
        offset: u64,
        length: usize,
        file_size: u64,
    ) -> VortexResult<ReadJob> {
        let mut state = self.lock()?;
        let length_u64 = u64::try_from(length).ok();
        let end = length_u64.and_then(|length| offset.checked_add(length));
        let total = length_u64.and_then(|length| state.attempted_bytes.checked_add(length));
        let diagnostic = if state.closed {
            Some("filesystem read observer is closed")
        } else if length == 0 || end.is_none_or(|end| end > file_size) {
            Some("filesystem read request is empty or exceeds retained source length")
        } else if length > self.limits.max_read_bytes {
            Some("filesystem read request exceeds per-read byte limit")
        } else if total.is_none_or(|total| total > self.limits.max_attempted_bytes) {
            Some("filesystem read request exceeds total attempted-byte limit")
        } else if state.events.len() >= self.limits.max_requests {
            Some("filesystem read observation event limit exceeded")
        } else if state.pending >= self.limits.max_in_flight {
            Some("filesystem read observation pending-job limit exceeded")
        } else {
            None
        };
        if let Some(diagnostic) = diagnostic {
            // Closing freezes all counters, including rejection accounting.
            if !state.closed {
                state.rejected_requests = state
                    .rejected_requests
                    .checked_add(1)
                    .ok_or_else(|| vortex_err!("filesystem read rejection counter overflow"))?;
            }
            return Err(vortex_err!("{diagnostic}"));
        }
        let index = state.events.len();
        state.events.push(ReadEvent {
            range: CompletedReadRange { offset, length },
            outcome: ReadOutcome::Pending,
        });
        state.attempted_bytes = total.expect("admitted total was checked");
        state.pending += 1;
        state.peak_pending = state.peak_pending.max(state.pending);
        Ok(ReadJob {
            shared: Arc::clone(self),
            index,
        })
    }
}

/// The guard moves into the actual blocking closure, then its completion
/// envelope. A cancelled queued closure drops it without reading. A cancelled
/// running closure retains it until its result is discarded, releasing native
/// payload buffers before the pending count reaches zero.
struct ReadJob {
    shared: Arc<SharedObservation>,
    index: usize,
}

impl ReadJob {
    fn set_outcome(&self, outcome: ReadOutcome) -> VortexResult<()> {
        self.shared.lock()?.events[self.index].outcome = outcome;
        Ok(())
    }
}

impl Drop for ReadJob {
    fn drop(&mut self) {
        // Release the job even during panic unwinding. Ordinary observation
        // methods still reject the poisoned state instead of certifying it.
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let event = &mut state.events[self.index];
        event.outcome = match event.outcome {
            ReadOutcome::Pending => ReadOutcome::CancelledBeforeRead,
            ReadOutcome::Reading => ReadOutcome::FailedRead,
            terminal => terminal,
        };
        state.pending -= 1;
        self.shared.idle.notify_all();
    }
}

struct ReadCompletion {
    // Field order releases a cancelled result's payload before its job guard.
    result: VortexResult<BufferHandle>,
    _job: ReadJob,
}

#[derive(Default)]
struct ReadHooks {
    before_read: Option<Arc<dyn Fn() + Send + Sync>>,
    after_read: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// A native file reader using exactly one retained source identity and handle.
/// `read_at` only admits work when polled; merely constructing a future performs
/// no observation allocation or filesystem I/O.
#[derive(Clone)]
pub(crate) struct ObservedFileReadAt {
    identity: Arc<SourceIdentity>,
    allocator: HostAllocatorRef,
    handle: Handle,
    shared: Arc<SharedObservation>,
    hooks: Arc<ReadHooks>,
}

impl ObservedFileReadAt {
    pub(crate) fn new(
        path: &Path,
        allocator: HostAllocatorRef,
        handle: Handle,
        limits: ReadObservationLimits,
    ) -> Result<Self> {
        // This is a bounded test harness, not a general-purpose telemetry store.
        if limits.max_read_bytes == 0
            || limits.max_attempted_bytes == 0
            || limits.max_requests == 0
            || limits.max_requests > 4096
            || limits.max_in_flight == 0
            || limits.max_in_flight > limits.max_requests
        {
            return Err(resident_error(
                "invalid bounded filesystem observation limits",
            ));
        }
        let mut events = Vec::new();
        events.try_reserve_exact(limits.max_requests).map_err(|_| {
            resident_error("could not reserve bounded filesystem observation records")
        })?;
        Ok(Self {
            identity: Arc::new(SourceIdentity::capture(path)?),
            allocator,
            handle,
            shared: Arc::new(SharedObservation {
                limits,
                state: Mutex::new(State {
                    closed: false,
                    attempted_bytes: 0,
                    rejected_requests: 0,
                    pending: 0,
                    peak_pending: 0,
                    events,
                }),
                idle: Condvar::new(),
            }),
            hooks: Arc::new(ReadHooks::default()),
        })
    }

    pub(crate) fn validate_generation(&self) -> VortexResult<()> {
        self.identity
            .validate()
            .map_err(|error| vortex_err!("{error}"))
    }

    /// An idle checkpoint is not final while admission remains open. No waiting,
    /// runtime driving, generation probing, or filesystem reads occur here.
    pub(crate) fn snapshot(&self) -> VortexResult<ReadObservation> {
        let state = self.shared.lock()?;
        Self::snapshot_state(&state)
    }

    /// Drop scan/file owners and unused read futures before draining, keeping the
    /// `CurrentThreadRuntime` alive. The bound covers this observer's admitted
    /// closures and completion envelopes, not every upstream runtime task. A
    /// timeout is an explicit failure and never claims that blocking I/O stopped.
    /// Validate the source separately before accepting query correctness.
    pub(crate) fn close_and_drain(&self, timeout: Duration) -> VortexResult<ReadObservation> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| vortex_err!("filesystem observation drain timeout overflow"))?;
        let mut state = self.shared.lock()?;
        state.closed = true;
        while state.pending != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(vortex_err!(
                    "filesystem observation drain timed out with {} pending jobs",
                    state.pending
                ));
            }
            state = self
                .shared
                .idle
                .wait_timeout(state, remaining)
                .map_err(|_| vortex_err!("filesystem observation drain lock poisoned"))?
                .0;
        }
        Self::snapshot_state(&state)
    }

    fn snapshot_state(state: &State) -> VortexResult<ReadObservation> {
        if state.pending != 0 {
            return Err(vortex_err!(
                "filesystem read observation is not quiescent: {} pending jobs",
                state.pending
            ));
        }
        let mut result = ReadObservation {
            closed: state.closed,
            admitted_requests: state.events.len(),
            attempted_bytes: state.attempted_bytes,
            completed_read_calls: 0,
            completed_read_bytes: 0,
            failed_read_calls: 0,
            failed_before_read: 0,
            cancelled_before_read: 0,
            rejected_requests: state.rejected_requests,
            pending_jobs: 0,
            peak_in_flight: state.peak_pending,
            completed_ranges: Vec::with_capacity(state.events.len()),
        };
        for event in &state.events {
            match event.outcome {
                ReadOutcome::Completed => {
                    result.completed_read_calls += 1;
                    result.completed_read_bytes +=
                        u64::try_from(event.range.length).expect("admitted range length fits u64");
                    result.completed_ranges.push(event.range);
                }
                ReadOutcome::FailedRead => result.failed_read_calls += 1,
                ReadOutcome::FailedBeforeRead => result.failed_before_read += 1,
                ReadOutcome::CancelledBeforeRead => result.cancelled_before_read += 1,
                ReadOutcome::Pending | ReadOutcome::Reading => {
                    return Err(vortex_err!(
                        "filesystem observation has unfinished event without a pending job"
                    ));
                }
            }
        }
        Ok(result)
    }

    fn read_blocking(
        &self,
        job: &ReadJob,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> VortexResult<BufferHandle> {
        let prepared = self
            .validate_generation()
            .and_then(|()| self.allocator.allocate(length, alignment));
        let mut buffer = match prepared {
            Ok(buffer) => buffer,
            Err(error) => {
                job.set_outcome(ReadOutcome::FailedBeforeRead)?;
                return Err(error);
            }
        };
        if let Some(hook) = &self.hooks.before_read {
            hook();
        }
        job.set_outcome(ReadOutcome::Reading)?;
        if let Err(error) =
            vortex::io::std_file::read_exact_at(&self.identity.file, buffer.as_mut_slice(), offset)
        {
            job.set_outcome(ReadOutcome::FailedRead)?;
            return Err(error.into());
        }
        // This is the evidence boundary: the positional read really completed.
        // Record it even when a later generation check or result delivery fails.
        job.set_outcome(ReadOutcome::Completed)?;
        if let Some(hook) = &self.hooks.after_read {
            hook();
        }
        self.validate_generation()?;
        Ok(BufferHandle::new_host(buffer.freeze()))
    }
}

impl VortexReadAt for ObservedFileReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(CoalesceConfig::file())
    }

    fn concurrency(&self) -> usize {
        self.shared.limits.max_in_flight
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let reader = self.clone();
        async move {
            if reader.shared.lock()?.closed {
                return Err(vortex_err!("filesystem read observer is closed"));
            }
            reader.validate_generation()?;
            Ok(reader.identity.generation.len)
        }
        .boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let reader = self.clone();
        async move {
            let job = reader
                .shared
                .admit(offset, length, reader.identity.generation.len)?;
            let handle = reader.handle.clone();
            let completion = handle
                .spawn_blocking(move || {
                    let result = reader.read_blocking(&job, offset, length, alignment);
                    ReadCompletion { result, _job: job }
                })
                .await;
            completion.result
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::{ObservedFileReadAt, ReadHooks, ReadObservationLimits};
    use crate::owned_buffers::ReservedHostAllocator;
    use shardloom_exec::live_memory::LiveMemoryPool;
    use std::{
        fs::{self, OpenOptions},
        path::PathBuf,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use vortex::{
        buffer::Alignment,
        io::{
            VortexReadAt,
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        },
    };

    static NEXT: AtomicUsize = AtomicUsize::new(0);
    const DRAIN: Duration = Duration::from_secs(10);

    struct Fixture {
        directory: PathBuf,
        path: PathBuf,
        runtime: CurrentThreadRuntime,
        memory: LiveMemoryPool,
        reader: ObservedFileReadAt,
    }

    impl Fixture {
        fn new(limits: ReadObservationLimits) -> Self {
            let directory = std::env::temp_dir().join(format!(
                "shardloom-read-observer-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            let path = directory.join("bytes.bin");
            fs::write(&path, (0..=255_u8).collect::<Vec<_>>()).unwrap();
            let runtime = CurrentThreadRuntime::new();
            let memory = LiveMemoryPool::new(1024 * 1024).unwrap();
            let reader = ObservedFileReadAt::new(
                &path,
                Arc::new(ReservedHostAllocator::new(memory.clone())),
                runtime.handle(),
                limits,
            )
            .unwrap();
            Self {
                directory,
                path,
                runtime,
                memory,
                reader,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.reader.close_and_drain(DRAIN).unwrap();
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }

    #[derive(Default)]
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl Gate {
        fn block(&self) {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self.changed.wait(state).unwrap();
            }
        }
        fn wait_started(&self) {
            let state = self.state.lock().unwrap();
            let (state, timeout) = self
                .changed
                .wait_timeout_while(state, DRAIN, |state| !state.0)
                .unwrap();
            assert!(
                state.0 && !timeout.timed_out(),
                "read closure did not start"
            );
        }
        fn release(&self) {
            self.state.lock().unwrap().1 = true;
            self.changed.notify_all();
        }
    }

    struct ReleaseGate(Arc<Gate>);

    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[test]
    fn unpolled_future_does_no_work_and_closed_snapshot_cannot_change() {
        let fixture = Fixture::new(ReadObservationLimits::default());
        drop(fixture.reader.read_at(0, 128, Alignment::none()));
        let checkpoint = fixture.reader.snapshot().unwrap();
        assert!(!checkpoint.closed);
        assert_eq!(checkpoint.admitted_requests, 0);
        assert_eq!(fixture.memory.snapshot().peak_reserved_bytes, 0);
        let final_snapshot = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert!(final_snapshot.closed);
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 1, Alignment::none()))
                .is_err()
        );
        assert!(fixture.runtime.block_on(fixture.reader.size()).is_err());
        assert_eq!(fixture.reader.snapshot().unwrap(), final_snapshot);
    }

    #[test]
    fn dropping_an_admitted_unstarted_closure_releases_its_job_without_io() {
        let fixture = Fixture::new(ReadObservationLimits::default());
        let job = fixture.reader.shared.admit(0, 16, 256).unwrap();
        // The provider owns this same guard through its queued FnOnce. Dropping
        // an unstarted closure must release it even if its body never executes.
        let queued: Box<dyn FnOnce() + Send> = Box::new(move || drop(job));
        assert!(fixture.reader.snapshot().is_err());
        drop(queued);
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.admitted_requests, 1);
        assert_eq!(observed.cancelled_before_read, 1);
        assert_eq!(observed.completed_read_calls, 0);
        assert_eq!(observed.pending_jobs, 0);
        assert_eq!(fixture.memory.snapshot().peak_reserved_bytes, 0);
    }

    #[test]
    fn cancelled_started_read_is_counted_inside_closure_and_drained_with_its_buffer() {
        let mut fixture = Fixture::new(ReadObservationLimits {
            max_in_flight: 1,
            ..ReadObservationLimits::default()
        });
        let gate = Arc::new(Gate::default());
        let _release_on_unwind = ReleaseGate(Arc::clone(&gate));
        let read_gate = Arc::clone(&gate);
        fixture.reader.hooks = Arc::new(ReadHooks {
            before_read: Some(Arc::new(move || read_gate.block())),
            after_read: None,
        });
        let mut read = fixture.reader.read_at(16, 64, Alignment::none());
        assert!(
            fixture
                .runtime
                .block_on(async { futures::poll!(&mut read) })
                .is_pending()
        );
        gate.wait_started();
        assert!(fixture.reader.snapshot().is_err());
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 1, Alignment::none()))
                .is_err()
        );
        drop(read);
        assert!(fixture.reader.close_and_drain(Duration::ZERO).is_err());
        gate.release();
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.completed_read_calls, 1);
        assert_eq!(observed.completed_read_bytes, 64);
        assert_eq!(observed.completed_ranges[0].offset, 16);
        assert_eq!(observed.completed_ranges[0].length, 64);
        assert_eq!(observed.rejected_requests, 1);
        assert_eq!(observed.pending_jobs, 0);
        assert_eq!(observed.peak_in_flight, 1);
        assert_eq!(fixture.memory.snapshot().reserved_bytes, 0);
        fixture.reader.validate_generation().unwrap();
    }

    #[test]
    fn range_byte_and_event_bounds_reject_before_native_allocation() {
        for (limits, ranges, accepted, rejected) in [
            (
                ReadObservationLimits {
                    max_read_bytes: 8,
                    ..ReadObservationLimits::default()
                },
                vec![(0, 9), (255, 2), (u64::MAX, 1), (0, 0)],
                0,
                4,
            ),
            (
                ReadObservationLimits {
                    max_attempted_bytes: 8,
                    ..ReadObservationLimits::default()
                },
                vec![(0, 8), (8, 1)],
                1,
                1,
            ),
            (
                ReadObservationLimits {
                    max_requests: 1,
                    max_in_flight: 1,
                    ..ReadObservationLimits::default()
                },
                vec![(0, 8), (8, 1)],
                1,
                1,
            ),
        ] {
            let fixture = Fixture::new(limits);
            for (offset, length) in ranges {
                drop(fixture.runtime.block_on(fixture.reader.read_at(
                    offset,
                    length,
                    Alignment::none(),
                )));
            }
            let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
            assert_eq!(observed.admitted_requests, accepted);
            assert_eq!(observed.completed_read_calls, accepted);
            assert_eq!(observed.rejected_requests, rejected);
            assert_eq!(fixture.memory.snapshot().reserved_bytes, 0);
            if accepted == 0 {
                assert_eq!(fixture.memory.snapshot().peak_reserved_bytes, 0);
            }
        }
    }

    #[test]
    fn retained_buffer_credits_follow_returned_owner_after_read_jobs_drain() {
        let fixture = Fixture::new(ReadObservationLimits::default());
        let result = fixture
            .runtime
            .block_on(fixture.reader.read_at(7, 32, Alignment::new(64)))
            .unwrap();
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.completed_read_bytes, 32);
        assert!(fixture.memory.snapshot().reserved_bytes >= 32);
        let buffer = fixture
            .runtime
            .block_on(result.try_into_host().unwrap())
            .unwrap();
        assert_eq!(buffer.as_slice(), &(7..39_u8).collect::<Vec<_>>());
        drop(buffer);
        assert_eq!(fixture.memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn replacement_fails_before_read_and_keeps_foreign_file() {
        let fixture = Fixture::new(ReadObservationLimits::default());
        let foreign = fixture.directory.join("foreign");
        fs::write(&foreign, [55_u8; 256]).unwrap();
        fs::rename(&foreign, &fixture.path).unwrap();
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 16, Alignment::none()))
                .is_err()
        );
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.failed_before_read, 1);
        assert_eq!(observed.completed_read_bytes, 0);
        assert_eq!(fixture.memory.snapshot().peak_reserved_bytes, 0);
        assert!(fixture.reader.validate_generation().is_err());
        assert_eq!(fs::read(&fixture.path).unwrap(), [55_u8; 256]);
    }

    #[test]
    fn native_allocator_denial_is_not_reported_as_a_filesystem_read() {
        let mut fixture = Fixture::new(ReadObservationLimits::default());
        let memory = LiveMemoryPool::new(16).unwrap();
        fixture.reader.allocator = Arc::new(ReservedHostAllocator::new(memory.clone()));
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 128, Alignment::none()))
                .is_err()
        );
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.failed_before_read, 1);
        assert_eq!(observed.failed_read_calls, 0);
        assert_eq!(observed.completed_read_calls, 0);
        assert_eq!(observed.pending_jobs, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        fixture.reader.validate_generation().unwrap();
    }

    #[test]
    fn exact_read_failure_has_unknown_partial_bytes_and_releases_allocation() {
        let mut fixture = Fixture::new(ReadObservationLimits::default());
        let path = fixture.path.clone();
        fixture.reader.hooks = Arc::new(ReadHooks {
            before_read: Some(Arc::new(move || {
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(8)
                    .unwrap();
            })),
            after_read: None,
        });
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 128, Alignment::none()))
                .is_err()
        );
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.failed_read_calls, 1);
        assert_eq!(observed.completed_read_calls, 0);
        assert_eq!(observed.completed_read_bytes, 0); // Not a claim of zero physical work.
        assert!(observed.completed_ranges.is_empty());
        assert_eq!(fixture.memory.snapshot().reserved_bytes, 0);
        assert!(fixture.reader.validate_generation().is_err());
    }

    #[test]
    fn mutation_after_successful_read_rejects_values_but_preserves_completed_bytes() {
        let mut fixture = Fixture::new(ReadObservationLimits::default());
        let path = fixture.path.clone();
        fixture.reader.hooks = Arc::new(ReadHooks {
            before_read: None,
            after_read: Some(Arc::new(move || {
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(8)
                    .unwrap();
            })),
        });
        assert!(
            fixture
                .runtime
                .block_on(fixture.reader.read_at(0, 128, Alignment::none()))
                .is_err()
        );
        let observed = fixture.reader.close_and_drain(DRAIN).unwrap();
        assert_eq!(observed.completed_read_calls, 1);
        assert_eq!(observed.completed_read_bytes, 128);
        assert_eq!(observed.failed_read_calls, 0);
        assert_eq!(fixture.memory.snapshot().reserved_bytes, 0);
        assert!(fixture.reader.validate_generation().is_err());
    }
}
