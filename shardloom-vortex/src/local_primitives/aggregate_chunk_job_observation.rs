//! Test-only bounded end-of-owner capture; no per-row trace or scheduling choice.

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
