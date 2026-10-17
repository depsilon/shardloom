use super::*;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::time::Duration;

#[derive(Default)]
struct Gate {
    opened: Mutex<bool>,
    changed: Condvar,
}

impl Gate {
    fn open(&self) {
        *self.opened.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn wait(&self) {
        let (opened, _) = self
            .changed
            .wait_timeout_while(
                self.opened.lock().unwrap(),
                Duration::from_secs(5),
                |opened| !*opened,
            )
            .unwrap();
        assert!(*opened, "test controller did not release the worker");
    }
}

struct ReleaseOnDrop(Arc<Gate>, Arc<Gate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.open();
        self.1.open();
    }
}

struct OwnedState {
    worker_index: usize,
    lease: Option<MemoryLease>,
    merges: Option<Arc<AtomicUsize>>,
    dropped: Option<mpsc::Sender<usize>>,
}

impl Drop for OwnedState {
    fn drop(&mut self) {
        drop(self.lease.take());
        if let Some(dropped) = &self.dropped {
            let _ = dropped.send(self.worker_index);
        }
    }
}

impl VortexMorselThreadLocalState for OwnedState {
    fn new(worker_index: usize) -> Self {
        Self {
            worker_index,
            lease: None,
            merges: None,
            dropped: None,
        }
    }

    fn observe_morsel(&mut self, _: &VortexMorselWorkUnit) -> Result<()> {
        Ok(())
    }

    fn merge_from(&mut self, other: Self) -> Result<()> {
        if let Some(merges) = &other.merges {
            merges.fetch_add(1, Ordering::SeqCst);
        }
        Ok(())
    }

    fn evidence_summary(&self) -> String {
        "worker-lifetime-test".into()
    }
}

#[derive(Clone, Copy)]
enum Failure {
    Error,
    Panic,
}

fn failed_schedule_releases_all_owners(failures: Vec<Option<Failure>>, expected_error: &str) {
    let count = failures.len();
    assert!(count >= 2 && failures.last().unwrap().is_none());
    let memory = LiveMemoryPool::new(8 * u64::try_from(count).unwrap()).unwrap();
    let worker_memory = memory.clone();
    let returned_memory = memory.clone();
    let merges = Arc::new(AtomicUsize::new(0));
    let worker_merges = Arc::clone(&merges);
    let start = Arc::new(Gate::default());
    let release = Arc::new(Gate::default());
    let cleanup = ReleaseOnDrop(Arc::clone(&start), Arc::clone(&release));
    let worker_start = Arc::clone(&start);
    let worker_release = Arc::clone(&release);
    let (ready, waiting) = mpsc::channel();
    let (dropped, released) = mpsc::channel();
    let (returned, observed) = mpsc::channel();
    let work = (0..count)
        .map(|index| {
            VortexMorselWorkUnit::new("lifetime", "segment", u64::try_from(index).unwrap(), 1, 8)
                .unwrap()
        })
        .collect();
    let plan = VortexMorselSchedulerPlan::new(
        "lifetime",
        work,
        VortexMorselSchedulerPolicy::new(count).unwrap(),
    )
    .unwrap();
    let caller = std::thread::spawn(move || {
        let result = execute_vortex_morsel_scheduler_with_observer::<OwnedState, _>(
            plan,
            move |state, _| {
                state.lease = Some(worker_memory.reserve(8)?);
                state.merges = Some(Arc::clone(&worker_merges));
                state.dropped = Some(dropped.clone());
                ready.send(state.worker_index).unwrap();
                worker_start.wait();
                match failures[state.worker_index] {
                    Some(Failure::Error) => Err(ShardLoomError::InvalidOperation(format!(
                        "worker {} rejected its morsel; fallback execution was not attempted",
                        state.worker_index
                    ))),
                    Some(Failure::Panic) => panic!("injected worker panic"),
                    None => {
                        worker_release.wait();
                        Ok(())
                    }
                }
            },
        );
        // Sample the actual return boundary before sending anything that could
        // release another worker. A failed call must own no worker reservation.
        let reserved_at_return = returned_memory.snapshot().reserved_bytes;
        let error = result.err().map(|error| error.to_string());
        returned.send((reserved_at_return, error)).unwrap();
    });
    let ready_workers = (0..count)
        .map(|_| waiting.recv_timeout(Duration::from_secs(5)))
        .collect::<std::result::Result<Vec<_>, _>>();
    start.open();
    // This deadline only releases the deliberately blocked callback. Acceptance
    // checks the reservation observed at return, not elapsed time or sleeping.
    let early = observed.recv_timeout(Duration::from_millis(250));
    release.open();
    let outcome = early.or_else(|_| observed.recv_timeout(Duration::from_secs(5)));
    let caller_result = caller.join();
    drop(cleanup);
    let dropped_workers = (0..count)
        .map(|_| released.recv_timeout(Duration::from_secs(5)))
        .collect::<std::result::Result<Vec<_>, _>>();
    assert_eq!(
        memory.snapshot().reserved_bytes,
        0,
        "test cleanup must drain"
    );
    let mut ready_workers = ready_workers.unwrap();
    ready_workers.sort_unstable();
    assert_eq!(ready_workers, (0..count).collect::<Vec<_>>());
    let mut dropped_workers = dropped_workers.unwrap();
    dropped_workers.sort_unstable();
    assert_eq!(dropped_workers, ready_workers);
    caller_result.unwrap();
    let (reserved_at_return, error) = outcome.unwrap();
    assert_eq!(
        reserved_at_return, 0,
        "error escaped with a live worker owner"
    );
    assert_eq!(
        merges.load(Ordering::SeqCst),
        0,
        "failed states must not merge"
    );
    assert!(error.unwrap().contains(expected_error));
}

#[test]
fn observer_error_drains_later_worker_and_releases_its_reservation_before_return() {
    failed_schedule_releases_all_owners(
        vec![Some(Failure::Error), None],
        "worker 0 rejected its morsel",
    );
}

#[test]
fn worker_panic_drains_later_worker_and_releases_its_reservation_before_return() {
    failed_schedule_releases_all_owners(
        vec![Some(Failure::Panic), None],
        "morsel scheduler worker panicked; fallback execution was not attempted",
    );
}

#[test]
fn earlier_error_survives_later_panic_after_all_workers_are_drained() {
    failed_schedule_releases_all_owners(
        vec![Some(Failure::Error), Some(Failure::Panic), None],
        "worker 0 rejected its morsel",
    );
}

#[test]
fn earlier_panic_survives_later_error_after_all_workers_are_drained() {
    failed_schedule_releases_all_owners(
        vec![Some(Failure::Panic), Some(Failure::Error), None],
        "morsel scheduler worker panicked; fallback execution was not attempted",
    );
}
