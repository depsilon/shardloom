//! Bounded request admission. CPU credits include each executing caller.

use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use shardloom_core::Result;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};

use super::resident_error;

/// Explicit concurrent-serving envelope. Ordinary sessions keep their existing
/// exclusive batch policy. General calls receive a fixed CPU grant, including
/// their caller; policy can reserve a separate one-CPU metadata lane at P >= 2.
#[derive(Debug, Clone, Copy)]
pub struct ResidentServingPolicy {
    pub general_cpu_lanes: usize,
    pub reserve_metadata_lane: bool,
    pub max_queued_calls: usize,
    /// Bound for admission-ticket metadata, not caller-retained request payloads.
    /// Public calls currently enqueue only their fixed-size ticket.
    pub max_queued_call_bytes: u64,
    /// Global bounds across all prepared sources in this session.
    pub max_io_requests: usize,
    pub max_io_bytes: u64,
}

impl Default for ResidentServingPolicy {
    fn default() -> Self {
        Self {
            general_cpu_lanes: 1,
            reserve_metadata_lane: true,
            max_queued_calls: 64,
            max_queued_call_bytes: 1 << 20,
            max_io_requests: 32,
            max_io_bytes: 128 << 20,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResidentAdmissionSnapshot {
    pub closed: bool,
    pub active_calls: usize,
    pub active_cpu_lanes: usize,
    pub peak_active_calls: usize,
    pub peak_active_cpu_lanes: usize,
    pub queued_calls: usize,
    /// Queued admission metadata only; excludes caller-retained payloads.
    pub queued_call_bytes: u64,
    pub peak_queued_calls: usize,
    pub peak_queued_call_bytes: u64,
    pub admitted_calls: u64,
    pub rejected_calls: u64,
    pub cancelled_queued_calls: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CallClass {
    General,
    Metadata,
}

#[derive(Clone, Copy)]
struct Ticket {
    id: u64,
    class: CallClass,
    bytes: u64,
}

struct State {
    queue: Vec<Ticket>,
    active_threads: Vec<std::thread::ThreadId>,
    next_id: u64,
    general_lanes: usize,
    metadata_active: bool,
    snapshot: ResidentAdmissionSnapshot,
}

pub(super) struct Admission {
    state: Mutex<State>,
    changed: Condvar,
    policy: ResidentServingPolicy,
    general_capacity: usize,
    separate_metadata: bool,
    _metadata: MemoryLease,
}

impl Admission {
    pub(super) fn new(
        policy: ResidentServingPolicy,
        total_cpu: usize,
        memory: &LiveMemoryPool,
    ) -> Result<Arc<Self>> {
        let separate_metadata = policy.reserve_metadata_lane && total_cpu > 1;
        let general_capacity = total_cpu.saturating_sub(usize::from(separate_metadata));
        if policy.general_cpu_lanes == 0
            || policy.general_cpu_lanes > general_capacity
            || policy.max_queued_calls == 0
            || policy.max_queued_calls > 65_536
            || policy.max_queued_call_bytes < size_of::<Ticket>() as u64
            || policy.max_io_requests == 0
            || policy.max_io_bytes == 0
        {
            return Err(resident_error(
                "invalid concurrent serving admission envelope",
            ));
        }
        let bytes = policy
            .max_queued_calls
            .checked_mul(size_of::<Ticket>())
            .and_then(|bytes| {
                total_cpu
                    .checked_mul(size_of::<std::thread::ThreadId>())
                    .and_then(|active| bytes.checked_add(active))
            })
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| resident_error("serving queue capacity overflow"))?;
        let metadata = memory.reserve(bytes)?;
        let mut queue = Vec::new();
        queue
            .try_reserve_exact(policy.max_queued_calls)
            .map_err(super::native_error)?;
        let mut active_threads = Vec::new();
        active_threads
            .try_reserve_exact(total_cpu)
            .map_err(super::native_error)?;
        if queue.capacity() > policy.max_queued_calls || active_threads.capacity() > total_cpu {
            return Err(resident_error(
                "serving admission allocation exceeds its reserved capacity",
            ));
        }
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                queue,
                active_threads,
                next_id: 0,
                general_lanes: 0,
                metadata_active: false,
                snapshot: ResidentAdmissionSnapshot::default(),
            }),
            changed: Condvar::new(),
            policy,
            general_capacity,
            separate_metadata,
            _metadata: metadata,
        }))
    }

    #[allow(clippy::too_many_lines)] // One lock-protected queue admission state machine.
    pub(super) fn admit(
        self: &Arc<Self>,
        class: CallClass,
        bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Permit> {
        cancellation.check()?;
        let started = Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| resident_error("serving admission poisoned"))?;
        let thread = std::thread::current().id();
        if state.active_threads.contains(&thread) {
            return Err(resident_error(
                "nested serving admission is prohibited; borrow the existing native execution context",
            ));
        }
        let bytes = bytes
            .checked_add(size_of::<Ticket>() as u64)
            .ok_or_else(|| resident_error("serving request byte count overflow"))?;
        if state.snapshot.closed
            || state.queue.len() == self.policy.max_queued_calls
            || bytes
                > self
                    .policy
                    .max_queued_call_bytes
                    .saturating_sub(state.snapshot.queued_call_bytes)
        {
            state.snapshot.rejected_calls += 1;
            return Err(resident_error(
                "serving admission is closed or its bounded queue is full",
            ));
        }
        let class = if self.separate_metadata {
            class
        } else {
            CallClass::General
        };
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| resident_error("serving request identity overflow"))?;
        state.queue.push(Ticket { id, class, bytes });
        state.snapshot.queued_call_bytes += bytes;
        state.snapshot.queued_calls = state.queue.len();
        state.snapshot.peak_queued_calls = state.snapshot.peak_queued_calls.max(state.queue.len());
        state.snapshot.peak_queued_call_bytes = state
            .snapshot
            .peak_queued_call_bytes
            .max(state.snapshot.queued_call_bytes);
        loop {
            let position = state
                .queue
                .iter()
                .position(|entry| entry.id == id)
                .expect("admitted waiter owns its queue entry");
            if state.snapshot.closed || cancellation.is_cancelled() {
                Self::remove_waiter(&mut state, position);
                state.snapshot.cancelled_queued_calls += 1;
                self.changed.notify_all();
                return Err(resident_error("serving request cancelled before execution"));
            }
            let first = state.queue.iter().position(|entry| entry.class == class) == Some(position);
            let lanes = match class {
                CallClass::General => self.policy.general_cpu_lanes,
                CallClass::Metadata => 1,
            };
            let available = match class {
                CallClass::General => lanes <= self.general_capacity - state.general_lanes,
                CallClass::Metadata => !state.metadata_active,
            };
            if first && available {
                Self::remove_waiter(&mut state, position);
                match class {
                    CallClass::General => state.general_lanes += lanes,
                    CallClass::Metadata => state.metadata_active = true,
                }
                state.snapshot.active_calls += 1;
                state.snapshot.active_cpu_lanes += lanes;
                state.snapshot.peak_active_calls = state
                    .snapshot
                    .peak_active_calls
                    .max(state.snapshot.active_calls);
                state.snapshot.peak_active_cpu_lanes = state
                    .snapshot
                    .peak_active_cpu_lanes
                    .max(state.snapshot.active_cpu_lanes);
                state.snapshot.admitted_calls += 1;
                state.active_threads.push(thread);
                self.changed.notify_all();
                return Ok(Permit {
                    admission: Arc::clone(self),
                    class,
                    lanes,
                    thread,
                    queue_time: started.elapsed(),
                });
            }
            // The existing cancellation flag has no notifier. This matches the
            // compute pool's bounded cancellation observation while all slots are held.
            state = match self.changed.wait_timeout(state, Duration::from_millis(10)) {
                Ok((state, _)) => state,
                Err(poisoned) => {
                    let (mut state, _) = poisoned.into_inner();
                    if let Some(position) = state.queue.iter().position(|entry| entry.id == id) {
                        Self::remove_waiter(&mut state, position);
                    }
                    self.changed.notify_all();
                    return Err(resident_error("serving admission poisoned"));
                }
            };
        }
    }

    fn remove_waiter(state: &mut State, position: usize) {
        let entry = state.queue.remove(position);
        state.snapshot.queued_call_bytes -= entry.bytes;
        state.snapshot.queued_calls = state.queue.len();
    }

    pub(super) fn snapshot(&self) -> ResidentAdmissionSnapshot {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
    }

    pub(super) fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .closed = true;
        self.changed.notify_all();
    }
}

pub(super) struct Permit {
    admission: Arc<Admission>,
    class: CallClass,
    thread: std::thread::ThreadId,
    pub(super) lanes: usize,
    pub(super) queue_time: Duration,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self
            .admission
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.class {
            CallClass::General => state.general_lanes -= self.lanes,
            CallClass::Metadata => state.metadata_active = false,
        }
        state.snapshot.active_calls -= 1;
        state.snapshot.active_cpu_lanes -= self.lanes;
        let position = state
            .active_threads
            .iter()
            .position(|thread| *thread == self.thread)
            .expect("active call owns its thread entry");
        state.active_threads.swap_remove(position);
        self.admission.changed.notify_all();
    }
}
