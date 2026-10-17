use super::*;
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Condvar, atomic::AtomicUsize},
    time::Duration,
};

#[derive(Default)]
struct ExitGate {
    open: Mutex<bool>,
    changed: Condvar,
}

impl ExitGate {
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }

    fn wait(&self) {
        let (open, _) = self
            .changed
            .wait_timeout_while(self.open.lock().unwrap(), Duration::from_secs(5), |open| {
                !*open
            })
            .unwrap();
        assert!(*open, "test controller did not release the worker");
    }
}

struct ReleaseOnDrop(Arc<ExitGate>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct StartedWorker {
    lease: Option<MemoryLease>,
    exited: mpsc::Sender<usize>,
    index: usize,
}

impl Drop for StartedWorker {
    fn drop(&mut self) {
        drop(self.lease.take());
        let _ = self.exited.send(self.index);
    }
}

fn startup_tasks() -> Vec<ParquetRowGroupReadTask> {
    (0..4)
        .map(|task_index| ParquetRowGroupReadTask {
            task_index,
            row_groups: vec![task_index],
        })
        .collect()
}

fn startup_failure_drains_started_workers(fail_after: usize, panic_on_start: bool) {
    let memory = LiveMemoryPool::new(32).unwrap();
    let worker_memory = memory.clone();
    let gate = Arc::new(ExitGate::default());
    let cleanup = ReleaseOnDrop(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let task_calls = Arc::new(AtomicUsize::new(0));
    let calls = Arc::clone(&task_calls);
    let (ready, started) = mpsc::channel();
    let (exited, stopped) = mpsc::channel();
    let (returned, outcome) = mpsc::channel();
    let caller = thread::spawn(move || {
        let result = catch_unwind(AssertUnwindSafe(|| {
            ParquetRowGroupParallelRecordBatchReader::with_task_runner_and_spawner(
                Arc::new(Schema::empty()),
                startup_tasks(),
                4,
                move |_, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                |index, work| {
                    if index == fail_after {
                        assert!(!panic_on_start, "injected Parquet startup panic");
                        return Err(std::io::Error::other("injected Parquet startup failure"));
                    }
                    let owner = StartedWorker {
                        lease: Some(worker_memory.reserve(8).unwrap()),
                        exited: exited.clone(),
                        index,
                    };
                    let ready = ready.clone();
                    let gate = Arc::clone(&worker_gate);
                    thread::Builder::new().spawn(move || {
                        let _owner = owner;
                        ready.send(index).unwrap();
                        work();
                        gate.wait();
                    })
                },
            )
        }));
        // The observation precedes any controller message or cleanup release.
        let reserved_at_return = worker_memory.snapshot().reserved_bytes;
        let error = match result {
            Ok(Err(error)) => error.to_string(),
            Err(payload) if panic_on_start => {
                assert_eq!(
                    payload.downcast_ref::<&str>(),
                    Some(&"injected Parquet startup panic")
                );
                "injected Parquet startup panic".to_string()
            }
            _ => panic!("startup failure was not preserved"),
        };
        returned.send((reserved_at_return, error)).unwrap();
    });
    let started_workers = (0..fail_after)
        .map(|_| started.recv_timeout(Duration::from_secs(5)))
        .collect::<std::result::Result<Vec<_>, _>>();
    // The deadline just releases blocked exit guards. The assertion uses the
    // actual memory reservation captured at constructor return/unwind.
    let early = outcome.recv_timeout(Duration::from_millis(250));
    gate.release();
    let returned = early.or_else(|_| outcome.recv_timeout(Duration::from_secs(5)));
    let caller_result = caller.join();
    drop(cleanup);
    let stopped_workers = (0..fail_after)
        .map(|_| stopped.recv_timeout(Duration::from_secs(5)))
        .collect::<std::result::Result<Vec<_>, _>>();
    assert_eq!(
        memory.snapshot().reserved_bytes,
        0,
        "test cleanup must drain"
    );
    caller_result.unwrap();
    let mut started_workers = started_workers.unwrap();
    started_workers.sort_unstable();
    let mut stopped_workers = stopped_workers.unwrap();
    stopped_workers.sort_unstable();
    assert_eq!(started_workers, (0..fail_after).collect::<Vec<_>>());
    assert_eq!(stopped_workers, started_workers);
    assert_eq!(
        task_calls.load(Ordering::SeqCst),
        0,
        "failed startup must not dispatch tasks"
    );
    let (reserved_at_return, error) = returned.unwrap();
    assert_eq!(
        reserved_at_return, 0,
        "startup escaped with a live worker owner"
    );
    if panic_on_start {
        assert_eq!(error, "injected Parquet startup panic");
    } else {
        assert!(error.contains(&format!(
            "Parquet row-group worker {fail_after} could not start"
        )));
        assert!(error.contains("injected Parquet startup failure"));
    }
}

#[test]
fn partial_worker_startup_error_joins_started_workers_before_return() {
    for fail_after in [1, 3] {
        startup_failure_drains_started_workers(fail_after, false);
    }
}

#[test]
fn partial_worker_startup_unwind_joins_started_workers_before_unwind_escapes() {
    for fail_after in [1, 3] {
        startup_failure_drains_started_workers(fail_after, true);
    }
}

#[test]
fn first_worker_startup_failure_has_no_worker_or_source_owners() {
    startup_failure_drains_started_workers(0, false);
}
