//! Serialized complete-operation configuration over one retained native owner.

use super::{ResidentVortexSession, resident_error};
use shardloom_core::{ExecutionResourceRequest, ExecutionResources, Result};
use shardloom_exec::{
    compute_pool::{CancellationToken, bounded_cpu_parallelism},
    live_memory::LiveMemoryPool,
};
use std::cell::Cell;
use std::sync::{
    Arc, Mutex, TryLockError,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Owner {
    session: ResidentVortexSession,
    configured: ExecutionResources,
    operations: Mutex<()>,
    operation_thread: Mutex<Option<std::thread::ThreadId>>,
    waiting_for_consumer: AtomicBool,
}

struct OperationThread<'a>(&'a Mutex<Option<std::thread::ThreadId>>);

thread_local! {
    // Lock ordering only: never stores data, resource grants or session owners.
    static OPERATION_DEPTH: Cell<usize> = const { Cell::new(0) };
}

struct ConsumerWait<'a>(&'a AtomicBool);

impl Drop for ConsumerWait<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Drop for OperationThread<'_> {
    fn drop(&mut self) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        OPERATION_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// An opt-in controller for callers that permit resource overrides between
/// complete operations. It keeps the same runtime, arrays and memory leases.
/// Fixed and concurrent-serving sessions retain their existing allocation model.
///
/// Use this controller for every allocating operation on its derived plans,
/// sources, results and columnar streams. The callback may call existing session
/// methods but must not recursively enter this controller. Returned data remains
/// valid after the controller is dropped because it retains its native owners.
#[derive(Clone)]
pub struct ConfigurableResidentSession(Arc<Owner>);

impl ConfigurableResidentSession {
    /// Create one explicit allocation without persistent background CPU drivers.
    /// Admitted native operations create/join their drivers within their grant.
    /// # Errors
    /// Returns native runtime or resource-admission errors.
    pub fn new(configured: ExecutionResources) -> Result<Self> {
        let memory = LiveMemoryPool::configurable(configured.memory_bytes())?;
        let mut session =
            ResidentVortexSession::with_memory_pool(memory, configured.max_parallelism(), true)?;
        let owner = Arc::get_mut(&mut session.0)
            .ok_or_else(|| resident_error("new configurable session is not uniquely owned"))?;
        owner.configured_parallelism = Some(AtomicUsize::new(owner.parallelism));
        Ok(Self(Arc::new(Owner {
            session,
            configured,
            operations: Mutex::new(()),
            operation_thread: Mutex::new(None),
            waiting_for_consumer: AtomicBool::new(false),
        })))
    }

    /// The initial declaration is the inheritance source for each new call.
    /// A previous call's override does not become an implicit session default.
    #[must_use]
    pub fn configured_resources(&self) -> ExecutionResources {
        self.0.configured
    }

    /// Whether two controllers share the same runtime, operation admission and
    /// retained memory owner. This does not compare numeric allocation values.
    #[must_use]
    pub fn has_same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Inspect existing counters without changing allocation or entering work.
    #[must_use]
    pub fn snapshot(&self) -> super::ResidentSessionSnapshot {
        self.0.session.snapshot()
    }

    /// Resolve an already explicit declaration against the controller's
    /// administrator ceilings, then hold it through the complete callback.
    /// Preparation, retained state, execution and export share the same pool.
    /// A new lower allocation is rejected if retained owners cannot fit; a
    /// higher authorized allocation never creates a second memory grant.
    ///
    /// The last admitted limit stays with retained results after return. Each
    /// subsequent operation explicitly admits its own resolved declaration.
    /// # Errors
    /// Rejects authorization violations, cancellation, poisoned admission, and
    /// allocations smaller than retained ownership, before invoking the callback.
    pub fn with_resources<T>(
        &self,
        resources: ExecutionResources,
        cancellation: &CancellationToken,
        operation: impl FnOnce(&ResidentVortexSession, ExecutionResources) -> Result<T>,
    ) -> Result<T> {
        let resources = self.authorized(resources)?;
        let thread = std::thread::current().id();
        if self
            .0
            .operation_thread
            .lock()
            .map_err(|_| resident_error("operation thread ownership poisoned"))?
            .as_ref()
            == Some(&thread)
        {
            return Err(resident_error(
                "recursive calls on the same operation controller are not admitted; retain a native source before entering a producer callback",
            ));
        }
        let _operation = loop {
            cancellation.check()?;
            match self.0.operations.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => {
                    if OPERATION_DEPTH.with(|depth| depth.get() != 0) {
                        return Err(resident_error(
                            "a nested operation cannot wait for another busy session; finish the other operation before transferring its columnar data",
                        ));
                    }
                    if self.0.waiting_for_consumer.load(Ordering::Acquire) {
                        return Err(resident_error(
                            "another operation is waiting for its incremental consumer; continue or close that stream before starting another operation on this session",
                        ));
                    }
                    std::thread::park_timeout(std::time::Duration::from_millis(5));
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(resident_error("operation resource admission poisoned"));
                }
            }
        };
        *self
            .0
            .operation_thread
            .lock()
            .map_err(|_| resident_error("operation thread ownership poisoned"))? = Some(thread);
        OPERATION_DEPTH.with(|depth| depth.set(depth.get() + 1));
        let _thread = OperationThread(&self.0.operation_thread);
        cancellation.check()?;
        {
            // Also exclude any already-entered native operation while changing
            // the pool and CPU limits. Retained array drops need no admission.
            let _native = self.0.session.0.admission.lock().map_err(|_| {
                resident_error("session admission poisoned during resource configuration")
            })?;
            cancellation.check()?;
            self.0
                .session
                .0
                .memory
                .set_limit_bytes(resources.memory_bytes())?;
            self.0
                .session
                .0
                .configured_parallelism
                .as_ref()
                .ok_or_else(|| {
                    resident_error("operation resource controller lost its CPU configuration")
                })?
                .store(
                    bounded_cpu_parallelism(resources.max_parallelism()),
                    Ordering::Release,
                );
        }
        cancellation.check()?;
        operation(&self.0.session, resources)
    }

    /// Mark a held operation as waiting for caller demand. Reentrant work from
    /// the consumer's thread then fails explicitly instead of waiting for the
    /// same caller to advance its stream. This does not release the allocation.
    /// # Errors
    /// Requires this thread to hold the controller's admitted operation.
    pub fn with_consumer_wait<T>(&self, wait: impl FnOnce() -> Result<T>) -> Result<T> {
        if self
            .0
            .operation_thread
            .lock()
            .map_err(|_| resident_error("operation thread ownership poisoned"))?
            .as_ref()
            != Some(&std::thread::current().id())
        {
            return Err(resident_error(
                "consumer waiting requires an admitted operation",
            ));
        }
        self.0.waiting_for_consumer.store(true, Ordering::Release);
        let _waiting = ConsumerWait(&self.0.waiting_for_consumer);
        wait()
    }

    fn authorized(&self, resources: ExecutionResources) -> Result<ExecutionResources> {
        let checked = ExecutionResources::resolve(
            ExecutionResourceRequest {
                memory_gb: None,
                memory_bytes: Some(resources.memory_bytes()),
                max_parallelism: Some(resources.max_parallelism()),
                origin: resources.memory_origin(),
            },
            Some(self.0.configured),
            resources.limits(),
        )?;
        ExecutionResources::from_declaration(
            checked.memory_bytes(),
            checked.max_parallelism(),
            resources.memory_origin(),
            resources.parallelism_origin(),
            checked.limits(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{ExecutionResourceLimits, ExecutionResourceOrigin};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn resources(bytes: u64, lanes: usize) -> ExecutionResources {
        ExecutionResources::from_bytes(bytes, lanes, ExecutionResourceOrigin::ExecutionCall)
            .unwrap()
    }

    #[test]
    fn operation_overrides_keep_one_pool_and_refuse_to_disown_retained_results() {
        let configured = resources(1024 * 1024, 3);
        let controller = ConfigurableResidentSession::new(configured).unwrap();
        let cancellation = CancellationToken::default();
        let (pool, retained) = controller
            .with_resources(configured, &cancellation, |session, grant| {
                assert_eq!(grant, configured);
                assert_eq!(session.snapshot().provider_background_workers, 0);
                let pool = session.shared_memory_pool();
                let retained = pool.reserve(800 * 1024)?;
                Ok((pool, retained))
            })
            .unwrap();
        let invoked = AtomicBool::new(false);
        let rejected =
            controller.with_resources(resources(512 * 1024, 1), &cancellation, |_, _| {
                invoked.store(true, Ordering::Relaxed);
                Ok(())
            });
        assert!(
            rejected
                .unwrap_err()
                .to_string()
                .contains("retained owners")
        );
        assert!(!invoked.load(Ordering::Relaxed));
        assert_eq!(pool.snapshot().limit_bytes, configured.memory_bytes());
        controller
            .with_resources(
                resources(2 * 1024 * 1024, 1),
                &cancellation,
                |session, _| {
                    assert!(session.shared_memory_pool().owns(&retained));
                    assert_eq!(session.snapshot().admitted_max_parallelism, 1);
                    assert_eq!(session.snapshot().memory.reserved_bytes, 800 * 1024);
                    let extra = session.shared_memory_pool().reserve(1024 * 1024)?;
                    assert!(pool.owns(&extra));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(controller.configured_resources(), configured);
        drop(retained);
        controller
            .with_resources(resources(1, 2), &cancellation, |session, _| {
                assert_eq!(session.snapshot().memory.limit_bytes, 1);
                assert_eq!(
                    session.snapshot().admitted_max_parallelism,
                    bounded_cpu_parallelism(2)
                );
                assert!(session.shared_memory_pool().reserve(2).is_err());
                Ok(())
            })
            .unwrap();
        drop(controller);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert_eq!(
            pool.snapshot().peak_reserved_bytes,
            800 * 1024 + 1024 * 1024
        );
    }

    #[test]
    fn operation_resource_ceilings_and_per_field_origins_survive_overrides() {
        let limits = ExecutionResourceLimits::new(Some(4096), Some(4)).unwrap();
        let configured = ExecutionResources::from_declaration(
            2048,
            2,
            ExecutionResourceOrigin::Platform,
            ExecutionResourceOrigin::Platform,
            Some(limits),
        )
        .unwrap();
        let controller = ConfigurableResidentSession::new(configured).unwrap();
        let cancellation = CancellationToken::default();
        for denied in [resources(4097, 1), resources(2048, 5)] {
            let result: Result<()> = controller.with_resources(denied, &cancellation, |_, _| {
                panic!("an unauthorized operation must not inspect input")
            });
            assert_eq!(
                result.unwrap_err().to_diagnostic().code,
                shardloom_core::DiagnosticCode::ConfigurationError
            );
        }
        let override_resources = ExecutionResources::from_declaration(
            3072,
            2,
            ExecutionResourceOrigin::ExecutionCall,
            ExecutionResourceOrigin::Platform,
            None,
        )
        .unwrap();
        controller
            .with_resources(override_resources, &cancellation, |session, admitted| {
                assert_eq!(admitted.limits(), Some(limits));
                assert_eq!(
                    admitted.memory_origin(),
                    ExecutionResourceOrigin::ExecutionCall
                );
                assert_eq!(
                    admitted.parallelism_origin(),
                    ExecutionResourceOrigin::Platform
                );
                assert_eq!(session.snapshot().memory.limit_bytes, 3072);
                Ok(())
            })
            .unwrap();
        assert_eq!(controller.configured_resources(), configured);
    }

    #[test]
    fn queued_operation_cancels_without_waiting_for_the_running_call() {
        let configured = resources(1024, 1);
        let controller = ConfigurableResidentSession::new(configured).unwrap();
        let cancellation = CancellationToken::default();
        let _running = controller.0.operations.lock().unwrap();
        std::thread::scope(|scope| {
            let (started, waiting) = std::sync::mpsc::channel();
            let (done, completed) = std::sync::mpsc::channel();
            let controller = controller.clone();
            let queued_cancellation = cancellation.clone();
            let queued = scope.spawn(move || {
                started.send(()).unwrap();
                let result: Result<()> =
                    controller.with_resources(configured, &queued_cancellation, |_, _| {
                        panic!("a cancelled queued operation must not consume input")
                    });
                done.send(result.is_err()).unwrap();
            });
            waiting.recv().unwrap();
            cancellation.cancel();
            assert!(
                completed
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap()
            );
            queued.join().unwrap();
        });
    }

    #[test]
    fn recursive_operation_refuses_and_releases_thread_ownership() {
        let configured = resources(1024, 1);
        let controller = ConfigurableResidentSession::new(configured).unwrap();
        let cancellation = CancellationToken::default();
        assert!(controller.has_same_owner(&controller.clone()));
        assert!(!controller.has_same_owner(&ConfigurableResidentSession::new(configured).unwrap()));
        controller
            .with_resources(configured, &cancellation, |_, _| {
                let error = controller
                    .with_resources(configured, &cancellation, |_, _| -> Result<()> {
                        panic!("recursive producer callback must never execute");
                    })
                    .unwrap_err();
                assert!(error.to_string().contains("recursive calls"));
                Ok(())
            })
            .unwrap();
        controller
            .with_resources(configured, &cancellation, |_, _| Ok(()))
            .unwrap();
    }

    #[test]
    fn consumer_demand_wait_refuses_interleaved_work_and_releases_its_guard() {
        let configured = resources(1024, 1);
        let controller = ConfigurableResidentSession::new(configured).unwrap();
        assert!(controller.with_consumer_wait(|| Ok(())).is_err());
        std::thread::scope(|scope| {
            let (started, ready) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let held = controller.clone();
            let worker = scope.spawn(move || {
                held.with_resources(configured, &CancellationToken::default(), |_, _| {
                    held.with_consumer_wait(|| {
                        started.send(()).unwrap();
                        released.recv().unwrap();
                        Err::<(), _>(resident_error("test consumer failure"))
                    })
                })
            });
            ready
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            let (reported, result) = std::sync::mpsc::channel();
            let queued = controller.clone();
            let attempted = scope.spawn(move || {
                let outcome =
                    queued.with_resources(configured, &CancellationToken::default(), |_, _| Ok(()));
                reported.send(outcome).unwrap();
            });
            let outcome = result.recv_timeout(std::time::Duration::from_secs(5));
            // Release even on timeout so the test never strands its own worker.
            release.send(()).unwrap();
            attempted.join().unwrap();
            let error = outcome.unwrap().unwrap_err();
            assert!(error.to_string().contains("incremental consumer"));
            assert!(worker.join().unwrap().is_err());
        });
        controller
            .with_resources(configured, &CancellationToken::default(), |_, _| Ok(()))
            .unwrap();
    }

    #[test]
    fn crossed_session_callbacks_never_wait_while_holding_each_others_grants() {
        let configured = resources(1024, 1);
        let left = ConfigurableResidentSession::new(configured).unwrap();
        let right = ConfigurableResidentSession::new(configured).unwrap();
        let cancellation = CancellationToken::default();
        std::thread::scope(|scope| {
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let (reported, results) = std::sync::mpsc::channel();
            let workers = [(left.clone(), right.clone()), (right.clone(), left.clone())]
                .into_iter()
                .map(|(outer, inner)| {
                    let barrier = Arc::clone(&barrier);
                    let reported = reported.clone();
                    let cancellation = cancellation.clone();
                    scope.spawn(move || {
                        outer.with_resources(configured, &cancellation, |_, _| {
                            barrier.wait();
                            let outcome =
                                inner.with_resources(configured, &cancellation, |_, _| Ok(()));
                            reported.send(outcome).unwrap();
                            // Both outer grants remain held until both attempts end.
                            barrier.wait();
                            Ok(())
                        })
                    })
                })
                .collect::<Vec<_>>();
            let first = results.recv_timeout(std::time::Duration::from_secs(5));
            let second = results.recv_timeout(std::time::Duration::from_secs(5));
            if first.is_err() || second.is_err() {
                cancellation.cancel();
            }
            for worker in workers {
                worker.join().unwrap().unwrap();
            }
            for outcome in [first.unwrap(), second.unwrap()] {
                assert!(
                    outcome
                        .unwrap_err()
                        .to_string()
                        .contains("nested operation cannot wait")
                );
            }
        });
        left.with_resources(configured, &CancellationToken::default(), |_, _| {
            right.with_resources(configured, &CancellationToken::default(), |_, _| Ok(()))
        })
        .unwrap();
    }
}
