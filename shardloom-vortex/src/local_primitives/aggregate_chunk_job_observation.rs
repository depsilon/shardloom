//! Test-only bounded end-of-owner capture; no per-row trace or scheduling choice.

use serde_json::{Value, json};
use shardloom_exec::compute_pool::ComputePoolSnapshot;
use std::cell::RefCell;

thread_local! {
    static CURRENT: RefCell<Option<State>> = const { RefCell::new(None) };
}

struct State {
    observe_waits: bool,
    records: Vec<Record>,
}

#[derive(Debug)]
pub(in super::super) struct Record {
    pub payload_type: &'static str,
    pub submitted: u64,
    pub joined: u64,
    pub window_limit: usize,
    pub peak_outstanding: usize,
    pub pending_before_drop: usize,
    pub outstanding_after_drain: usize,
    pub cancelled_before_drop: bool,
    pub stage_retired_before_drop: bool,
    pub worker_busy_nanos: u64,
    pub inline_busy_nanos: u128,
    pub join_wait_nanos: u128,
    pub pool: Option<ComputePoolSnapshot>,
}

impl Record {
    pub(in super::super) fn completed_json(
        &self,
        parallelism: usize,
        observe_waits: bool,
    ) -> Value {
        assert_eq!(self.submitted, self.joined);
        assert_eq!(self.pending_before_drop, 0);
        assert_eq!(self.outstanding_after_drain, 0);
        assert!(!self.cancelled_before_drop);
        assert!(self.peak_outstanding <= self.window_limit);
        if parallelism == 1 {
            assert!(self.pool.is_none());
        }
        if let Some(pool) = self.pool {
            assert!(pool.queue_closed);
            assert_eq!(pool.active_workers, 0);
            assert_eq!(pool.queued_jobs, 0);
            assert_eq!(pool.queued_bytes, 0);
            assert_eq!(pool.waiting_producers, 0);
            assert_eq!(pool.completed_jobs, self.submitted);
            assert!(pool.workers_created < parallelism);
            assert_eq!(pool.waits.is_some(), observe_waits);
            if let Some(waits) = pool.waits {
                assert_eq!(waits.waiting_workers, 0);
            }
        }
        json!({
            "payload_type": self.payload_type,
            "submitted": self.submitted,
            "joined": self.joined,
            "window_limit": self.window_limit,
            "peak_outstanding": self.peak_outstanding,
            "pending_before_drop": self.pending_before_drop,
            "outstanding_after_drain": self.outstanding_after_drain,
            "cancelled_before_drop": self.cancelled_before_drop,
            "stage_retired_before_drop": self.stage_retired_before_drop,
            "worker_busy_nanos": self.worker_busy_nanos,
            "inline_busy_nanos": u64::try_from(self.inline_busy_nanos).unwrap(),
            "join_wait_nanos": u64::try_from(self.join_wait_nanos).unwrap(),
            "closed_pool": self.pool.map(pool_json),
        })
    }
}

fn pool_json(pool: ComputePoolSnapshot) -> Value {
    json!({
        "workers_created": pool.workers_created,
        "active_workers": pool.active_workers,
        "peak_active_workers": pool.peak_active_workers,
        "queued_jobs": pool.queued_jobs,
        "peak_queued_jobs": pool.peak_queued_jobs,
        "queued_bytes": pool.queued_bytes,
        "peak_queued_bytes": pool.peak_queued_bytes,
        "completed_jobs": pool.completed_jobs,
        "worker_busy_nanos": pool.worker_busy_nanos,
        "enqueue_wait_nanos": pool.enqueue_wait_nanos,
        "waiting_producers": pool.waiting_producers,
        "queue_closed": pool.queue_closed,
        "waits": pool.waits.map(|waits| json!({
            "completed_empty_queue_waits": waits.completed_empty_queue_waits,
            "empty_queue_wait_nanos": waits.empty_queue_wait_nanos,
            "completed_capacity_waits": waits.completed_capacity_waits,
            "capacity_wait_nanos": waits.capacity_wait_nanos,
            "waiting_workers": waits.waiting_workers,
        })),
    })
}

pub(in super::super) struct Session {
    active: bool,
    // The capture belongs to the creating thread, including cleanup on unwind.
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Session {
    pub(in super::super) fn start(observe_waits: bool) -> Self {
        CURRENT.with(|current| {
            assert!(current.borrow().is_none(), "nested worker observation");
            *current.borrow_mut() = Some(State {
                observe_waits,
                records: Vec::with_capacity(16),
            });
        });
        Self {
            active: true,
            _same_thread: std::marker::PhantomData,
        }
    }

    pub(in super::super) fn finish(mut self) -> Vec<Record> {
        let records = CURRENT.with(|current| current.borrow_mut().take().unwrap().records);
        self.active = false;
        records
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.active {
            CURRENT.with(|current| drop(current.borrow_mut().take()));
        }
    }
}

pub(super) fn active() -> bool {
    CURRENT.with(|current| current.borrow().is_some())
}

pub(super) fn observe_waits() -> bool {
    CURRENT.with(|current| current.borrow().as_ref().is_some_and(|s| s.observe_waits))
}

pub(super) fn record(record: Record) {
    CURRENT.with(|current| {
        if let Some(state) = current.borrow_mut().as_mut() {
            assert!(state.records.len() < 16, "worker observation owner cap");
            state.records.push(record);
        }
    });
}
