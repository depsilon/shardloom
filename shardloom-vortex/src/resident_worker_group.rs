//! Owned CPU drivers for the pinned provider's public current-thread runtime.
//!
//! Upstream `CurrentThreadWorkerPool` detaches its threads and polls shutdown.
//! These drivers instead wake immediately and join at the resident boundary.
//! Blocking I/O workers belong to a separate upstream pool: an in-progress
//! blocking read is not interrupted or synchronously joined by this group.

use std::{io, thread};

use futures::channel::oneshot;
use vortex::io::runtime::{BlockingRuntime as _, current::CurrentThreadRuntime};

pub(crate) struct ResidentWorkerGroup {
    workers: Vec<ResidentWorker>,
}

struct ResidentWorker {
    shutdown: Option<oneshot::Sender<()>>,
    thread: thread::JoinHandle<()>,
}

impl ResidentWorkerGroup {
    pub(crate) fn new(runtime: &CurrentThreadRuntime, count: usize) -> io::Result<Self> {
        Self::with_spawner(count, |index, shutdown| {
            let runtime = runtime.clone();
            thread::Builder::new()
                .name(format!("shardloom-resident-driver-{index}"))
                .spawn(move || drive_until_shutdown(&runtime, shutdown))
        })
    }

    fn with_spawner(
        count: usize,
        mut spawn: impl FnMut(usize, oneshot::Receiver<()>) -> io::Result<thread::JoinHandle<()>>,
    ) -> io::Result<Self> {
        let mut group = Self {
            workers: Vec::with_capacity(count),
        };
        for index in 0..count {
            let (shutdown, receiver) = oneshot::channel();
            // On partial failure, the already constructed group stops and joins
            // every successfully started driver before propagating the error.
            let thread = spawn(index, receiver)?;
            group.workers.push(ResidentWorker {
                shutdown: Some(shutdown),
                thread,
            });
        }
        Ok(group)
    }
}

fn drive_until_shutdown(runtime: &CurrentThreadRuntime, shutdown: oneshot::Receiver<()>) {
    runtime.block_on(async {
        let _ = shutdown.await;
    });
}

impl Drop for ResidentWorkerGroup {
    fn drop(&mut self) {
        // Wake all drivers before waiting, including those that have not started.
        for worker in &mut self.workers {
            if let Some(shutdown) = worker.shutdown.take() {
                let _ = shutdown.send(());
            }
        }
        let current = thread::current().id();
        for worker in self.workers.drain(..) {
            if worker.thread.thread().id() != current {
                // Teardown must not panic during another error's unwinding.
                let _ = worker.thread.join();
            }
            // A defensive self-drop cannot join its own thread. Its stop signal
            // is already set; that one driver exits when its active task returns.
            // Normal public caller teardown joins every background CPU driver.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    struct Exited(mpsc::Sender<()>);
    impl Drop for Exited {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn owned_drivers_execute_native_provider_tasks() {
        let runtime = CurrentThreadRuntime::new();
        let workers = ResidentWorkerGroup::new(&runtime, 2).unwrap();
        let (sent, received) = mpsc::channel();
        runtime
            .handle()
            .spawn(async move {
                sent.send(thread::current().id()).unwrap();
            })
            .detach();
        let actual = received.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            workers
                .workers
                .iter()
                .any(|worker| worker.thread.thread().id() == actual)
        );
        drop(workers);
    }

    #[test]
    fn repeated_teardown_and_partial_spawn_failure_join_all_started_drivers() {
        let runtime = CurrentThreadRuntime::new();
        for fail_after in [None, Some(2)] {
            for _ in 0..10 {
                let (exited, observed) = mpsc::channel();
                let group = ResidentWorkerGroup::with_spawner(3, |index, shutdown| {
                    if fail_after == Some(index) {
                        return Err(io::Error::other("injected driver spawn failure"));
                    }
                    let runtime = runtime.clone();
                    let exit = Exited(exited.clone());
                    thread::Builder::new().spawn(move || {
                        let _exit = exit;
                        drive_until_shutdown(&runtime, shutdown);
                    })
                });
                assert_eq!(group.is_err(), fail_after.is_some());
                drop(group);
                // No sleep or eventual polling: all exit guards ran before Drop
                // or failed construction returned to this caller.
                assert_eq!(observed.try_iter().count(), fail_after.unwrap_or(3));
            }
        }
    }

    #[test]
    fn last_owner_on_a_driver_avoids_self_join_and_exits_after_its_task() {
        let runtime = CurrentThreadRuntime::new();
        let (exited, observed) = mpsc::channel();
        let workers = ResidentWorkerGroup::with_spawner(1, |_, shutdown| {
            let runtime = runtime.clone();
            let exit = Exited(exited.clone());
            thread::Builder::new().spawn(move || {
                let _exit = exit;
                drive_until_shutdown(&runtime, shutdown);
            })
        })
        .unwrap();
        let (finished, completion) = mpsc::channel();
        runtime
            .handle()
            .spawn(async move {
                drop(workers);
                finished.send(()).unwrap();
            })
            .detach();
        completion.recv_timeout(Duration::from_secs(5)).unwrap();
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
