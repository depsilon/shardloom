use super::*;
use std::sync::{Barrier, mpsc};

struct RetainedPartial {
    memory: LiveMemoryPool,
    credit_at_drop: Arc<AtomicU64>,
}

impl Drop for RetainedPartial {
    fn drop(&mut self) {
        self.credit_at_drop
            .store(self.memory.snapshot().reserved_bytes, Ordering::Release);
    }
}

fn completed_partial(
    jobs: &mut AggregateChunkJobs<RetainedPartial>,
    memory: &LiveMemoryPool,
    credit_at_drop: &Arc<AtomicU64>,
) -> CompletedChunk<RetainedPartial> {
    let partial = RetainedPartial {
        memory: memory.clone(),
        credit_at_drop: Arc::clone(credit_at_drop),
    };
    jobs.submit(40, move |_, _| Ok(partial)).unwrap();
    jobs.join_next().unwrap().unwrap()
}

#[test]
fn cancelled_completed_results_refuse_borrowed_and_owned_merge() {
    for parallelism in [1, 3] {
        for owned in [false, true] {
            let memory = LiveMemoryPool::new(128).unwrap();
            let credit_at_drop = Arc::new(AtomicU64::new(0));
            let mut jobs = AggregateChunkJobs::new(parallelism, 1, 128, memory.clone()).unwrap();
            let completed = completed_partial(&mut jobs, &memory, &credit_at_drop);
            jobs.cancel();
            let mut called = false;
            let result = if owned {
                completed.consume_owned(|_| {
                    called = true;
                    Ok(())
                })
            } else {
                completed.consume(|_| {
                    called = true;
                    Ok(())
                })
            };
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("execution cancelled")
            );
            assert!(!called);
            assert_eq!(credit_at_drop.load(Ordering::Acquire), 40);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(jobs.outstanding(), 0);
        }
    }
}

#[test]
fn cancellation_during_completed_merge_rejects_success_and_preserves_merge_errors() {
    for parallelism in [1, 3] {
        for owned in [false, true] {
            for fail_merge in [false, true] {
                let memory = LiveMemoryPool::new(128).unwrap();
                let credit_at_drop = Arc::new(AtomicU64::new(0));
                let mut jobs =
                    AggregateChunkJobs::new(parallelism, 1, 128, memory.clone()).unwrap();
                let completed = completed_partial(&mut jobs, &memory, &credit_at_drop);
                let merge = || {
                    assert_eq!(memory.snapshot().reserved_bytes, 40);
                    assert!(jobs.is_full());
                    jobs.cancel();
                    if fail_merge {
                        Err(failed("original merge failure"))
                    } else {
                        Ok(())
                    }
                };
                let result = if owned {
                    completed.consume_owned(|_| merge())
                } else {
                    completed.consume(|_| merge())
                };
                let error = result.unwrap_err().to_string();
                assert!(error.contains(if fail_merge {
                    "original merge failure"
                } else {
                    "execution cancelled"
                }));
                assert_eq!(credit_at_drop.load(Ordering::Acquire), 40);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                assert_eq!(jobs.outstanding(), 0);
            }
        }
    }
}

#[test]
fn panicking_completed_merge_cancels_siblings_and_releases_retained_owners() {
    for parallelism in [1, 3] {
        for owned in [false, true] {
            let memory = LiveMemoryPool::new(128).unwrap();
            let credit_at_drop = Arc::new(AtomicU64::new(0));
            let sibling_credit_at_drop = Arc::new(AtomicU64::new(0));
            let mut jobs = AggregateChunkJobs::new(parallelism, 2, 128, memory.clone()).unwrap();
            let completed = completed_partial(&mut jobs, &memory, &credit_at_drop);
            let sibling = RetainedPartial {
                memory: memory.clone(),
                credit_at_drop: sibling_credit_at_drop,
            };
            jobs.submit(16, move |_, _| Ok(sibling)).unwrap();
            let panic = catch_unwind(AssertUnwindSafe(|| {
                if owned {
                    completed.consume_owned::<()>(|_| panic!("injected merge panic"))
                } else {
                    completed.consume::<()>(|_| panic!("injected merge panic"))
                }
            }));
            assert!(panic.is_err());
            assert!(jobs.check_cancelled().is_err());
            assert!(credit_at_drop.load(Ordering::Acquire) >= 40);
            assert_eq!(jobs.outstanding(), 1);
            drop(jobs);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn completed_merge_preserves_an_observed_sibling_failure_before_cancellation() {
    for owned in [false, true] {
        let memory = LiveMemoryPool::new(128).unwrap();
        let credit_at_drop = Arc::new(AtomicU64::new(0));
        let mut jobs = AggregateChunkJobs::new(3, 2, 128, memory.clone()).unwrap();
        let completed = completed_partial(&mut jobs, &memory, &credit_at_drop);
        jobs.submit(16, |_, _| Err(failed("original provider failure")))
            .unwrap();
        let provider_error = jobs.join_next().err().unwrap().to_string();
        assert!(provider_error.contains("original provider failure"));
        let merge_error = if owned {
            completed.consume_owned::<()>(|_| panic!("cancelled merge ran"))
        } else {
            completed.consume::<()>(|_| panic!("cancelled merge ran"))
        }
        .unwrap_err()
        .to_string();
        assert_eq!(merge_error, provider_error);
        assert_eq!(credit_at_drop.load(Ordering::Acquire), 40);
        assert_eq!(jobs.outstanding(), 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn operation_cancellation_reaches_workers_without_stage_drop_cancelling_the_parent() {
    for parallelism in [1, 3] {
        let memory = LiveMemoryPool::new(128).unwrap();
        let parent = CancellationToken::default();
        let sibling = CancellationToken::from_shared_flag_with_parent(Arc::default(), &parent);
        let mut jobs =
            AggregateChunkJobs::for_operation(parallelism, 1, 128, memory.clone(), Some(&parent))
                .unwrap();
        jobs.submit(40, |worker, _| {
            worker.check_cancelled()?;
            Ok(7)
        })
        .unwrap();
        jobs.join_next()
            .unwrap()
            .unwrap()
            .consume(|value| {
                assert_eq!(*value, 7);
                Ok(())
            })
            .unwrap();
        drop(jobs);
        assert!(!parent.is_cancelled());
        assert!(!sibling.is_cancelled());
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        let mut jobs = AggregateChunkJobs::<()>::for_operation(
            parallelism,
            1,
            128,
            memory.clone(),
            Some(&parent),
        )
        .unwrap();
        let operation = parent.clone();
        let submitted = jobs.submit(40, move |worker, _| {
            operation.cancel();
            worker.check_cancelled()?;
            panic!("worker did not observe its enclosing operation");
        });
        let error = match submitted {
            Ok(_) => jobs.join_next().err().unwrap(),
            Err(error) => error,
        };
        assert!(error.to_string().contains("execution cancelled"));
        drop(jobs);
        assert!(sibling.is_cancelled());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn owned_completion_retains_lease_and_window_through_success_and_error_merge() {
    struct OwnedPartial {
        memory: LiveMemoryPool,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for OwnedPartial {
        fn drop(&mut self) {
            assert_eq!(self.memory.snapshot().reserved_bytes, 40);
            self.dropped.store(true, Ordering::Release);
        }
    }
    for parallelism in [1, 3] {
        for fail_merge in [false, true] {
            let memory = LiveMemoryPool::new(128).unwrap();
            let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let owned = OwnedPartial {
                memory: memory.clone(),
                dropped: Arc::clone(&dropped),
            };
            let mut jobs = AggregateChunkJobs::new(parallelism, 1, 128, memory.clone()).unwrap();
            jobs.submit(40, move |_, _| Ok(owned)).unwrap();
            let completed = jobs.join_next().unwrap().unwrap();
            assert_eq!(completed.ordinal(), 0);
            let result = completed.consume_owned(|partial| {
                assert!(jobs.is_full());
                assert_eq!(jobs.outstanding(), 1);
                drop(partial);
                assert!(dropped.load(Ordering::Acquire));
                assert_eq!(memory.snapshot().reserved_bytes, 40);
                assert_eq!(jobs.outstanding(), 1);
                if fail_merge {
                    Err(failed("owned merge fixture"))
                } else {
                    Ok(())
                }
            });
            assert_eq!(result.is_err(), fail_merge);
            assert_eq!(jobs.check_cancelled().is_err(), fail_merge);
            assert_eq!(jobs.outstanding(), 0);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            drop(jobs);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
fn shared_cancellation_interrupts_owned_worker_join_and_retirement_keeps_success_live() {
    use std::sync::atomic::AtomicBool;
    let memory = LiveMemoryPool::new(128).unwrap();
    let flag = Arc::new(AtomicBool::new(false));
    let mut jobs = AggregateChunkJobs::<()>::with_cancellation(
        2,
        2,
        128,
        memory.clone(),
        CancellationToken::from_shared_flag(Arc::clone(&flag)),
    )
    .unwrap();
    let (started, entered) = mpsc::channel();
    jobs.submit(16, move |worker, _| {
        started.send(()).unwrap();
        loop {
            worker.check_cancelled()?;
            std::thread::yield_now();
        }
    })
    .unwrap();
    let cancel = Arc::clone(&flag);
    let canceller = std::thread::spawn(move || {
        let reached = entered.recv_timeout(std::time::Duration::from_secs(5));
        cancel.store(true, Ordering::Release);
        reached.unwrap();
    });
    assert!(jobs.join_next().is_err());
    canceller.join().unwrap();
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let successful = Arc::new(AtomicBool::new(false));
    let mut jobs = AggregateChunkJobs::<u64>::with_cancellation(
        2,
        2,
        128,
        memory.clone(),
        CancellationToken::from_shared_flag(Arc::clone(&successful)),
    )
    .unwrap();
    jobs.submit(16, |_, _| Ok(7)).unwrap();
    assert!(jobs.retire().is_err());
    jobs.join_next()
        .unwrap()
        .unwrap()
        .consume(|value| {
            assert_eq!(*value, 7);
            Ok(())
        })
        .unwrap();
    jobs.retire().unwrap();
    assert!(jobs.pool_snapshot().is_none());
    drop(jobs);
    assert!(!successful.load(Ordering::Acquire));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn initial_reservation_denial_never_runs_or_advances_job_and_remains_retryable() {
    for parallelism in [1, 2] {
        let memory = LiveMemoryPool::new(128).unwrap();
        let mut jobs = AggregateChunkJobs::<u64>::new(parallelism, 2, 128, memory.clone()).unwrap();
        let competing = memory.reserve(128).unwrap();
        let error = match jobs
            .try_submit(32, |_, _| panic!("unadmitted job ran"))
            .unwrap()
        {
            SubmitOutcome::InitialCapacityDenied(error) => error,
            SubmitOutcome::Submitted(_) => panic!("exhausted pool admitted a job"),
        };
        assert!(error.to_string().contains("memory reservation denied"));
        let legacy_error = jobs.submit(32, |_, _| panic!("unadmitted legacy job ran"));
        assert_eq!(legacy_error.err().unwrap().to_string(), error.to_string());
        assert_eq!(jobs.submitted(), 0);
        assert_eq!(jobs.outstanding(), 0);
        assert_eq!(jobs.peak_outstanding(), 0);
        assert_eq!(memory.snapshot().denied_reservations, 2);
        drop(competing);
        assert!(matches!(
            jobs.try_submit(32, |context, _| {
                context.check_cancelled()?;
                Ok(73)
            })
            .unwrap(),
            SubmitOutcome::Submitted(0)
        ));
        let completed = jobs.join_next().unwrap().unwrap();
        assert_eq!(completed.ordinal(), 0);
        assert_eq!(*completed.value(), 73);
        drop(completed);
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn try_submit_preserves_execution_errors_despite_coincident_capacity_denial() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut jobs = AggregateChunkJobs::<u64>::new(1, 2, 128, memory.clone()).unwrap();
    assert!(jobs.try_submit(129, |_, _| Ok(0)).is_err());
    assert_eq!(memory.snapshot().denied_reservations, 0);
    let concurrent = memory.clone();
    let result = jobs.try_submit(32, move |_, _| {
        assert!(concurrent.reserve(128).is_err());
        Err(failed("injected provider corruption"))
    });
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("injected provider corruption")
    );
    assert_eq!(memory.snapshot().denied_reservations, 1);
    assert!(jobs.try_submit(32, |_, _| Ok(0)).is_err());
    assert_eq!(jobs.submitted(), 0);
    assert_eq!(jobs.outstanding(), 0);
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn completed_results_keep_window_and_memory_until_ordered_merge_finishes() {
    for parallelism in [1, 2, 4, 8, 12] {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let mut jobs = AggregateChunkJobs::new(parallelism, 2, 1024, memory.clone()).unwrap();
        jobs.submit(64, |context, lease| {
            context.check_cancelled()?;
            lease.resize(128)?;
            Ok(17_u64)
        })
        .unwrap();
        jobs.submit(64, |_, _| Ok(29)).unwrap();
        let first = jobs.join_next().unwrap().unwrap();
        assert_eq!(first.ordinal(), 0);
        assert_eq!(first.reserved_bytes(), 128);
        assert!(jobs.is_full());
        assert!(jobs.submit(1, |_, _| Ok(0)).is_err());
        assert_eq!(first.consume(|value| Ok(*value)).unwrap(), 17);
        assert_eq!(jobs.outstanding(), 1);
        jobs.submit(64, |_, _| Ok(41)).unwrap();
        let second = jobs.join_next().unwrap().unwrap();
        assert_eq!(second.ordinal(), 1);
        assert_eq!(*second.value(), 29);
        drop(second);
        let third = jobs.join_next().unwrap().unwrap();
        assert_eq!(third.ordinal(), 2);
        assert_eq!(*third.value(), 41);
        drop(third);
        assert_eq!(jobs.peak_outstanding(), 2);
        assert_eq!(jobs.submitted(), 3);
        assert_eq!(jobs.joined(), 3);
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn worker_completion_order_does_not_change_chunk_merge_order() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let mut jobs = AggregateChunkJobs::new(3, 2, 1024, memory.clone()).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let (completed, observed) = mpsc::channel();
    jobs.submit(64, move |_, _| {
        first_barrier.wait();
        observed.recv().unwrap();
        Ok(1)
    })
    .unwrap();
    jobs.submit(64, move |_, _| {
        barrier.wait();
        completed.send(()).unwrap();
        Ok(2)
    })
    .unwrap();
    for expected in [1, 2] {
        jobs.join_next()
            .unwrap()
            .unwrap()
            .consume(|value| {
                assert_eq!(*value, expected);
                Ok(())
            })
            .unwrap();
    }
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn growth_failure_and_early_drop_drain_without_credit_leaks() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut jobs = AggregateChunkJobs::new(2, 2, 128, memory.clone()).unwrap();
    jobs.submit(64, |_, lease| {
        lease.resize(129)?;
        Ok(1)
    })
    .unwrap();
    assert!(jobs.join_next().is_err());
    assert!(jobs.submit(1, |_, _| Ok(0)).is_err());
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut jobs = AggregateChunkJobs::new(2, 2, 128, memory.clone()).unwrap();
    jobs.submit(64, |context, _| {
        context.check_cancelled()?;
        Ok(vec![0_u8; 64])
    })
    .unwrap();
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn inline_and_parallel_jobs_share_input_limits_and_panic_cleanup() {
    for workers in [1, 2] {
        let memory = LiveMemoryPool::new(1024).unwrap();
        let mut jobs = AggregateChunkJobs::<u64>::new(workers, 2, 64, memory.clone()).unwrap();
        assert!(jobs.submit(65, |_, _| Ok(1)).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let submitted = jobs.submit(64, |_, _| panic!("injected worker panic"));
        if workers == 1 {
            assert!(submitted.is_err());
        } else {
            submitted.unwrap();
            assert!(jobs.join_next().is_err());
        }
        assert!(jobs.submit(1, |_, _| Ok(2)).is_err());
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn ordered_join_preserves_real_failure_instead_of_a_cancelled_sibling_error() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut jobs = AggregateChunkJobs::<u64>::new(3, 2, 128, memory.clone()).unwrap();
    jobs.submit(32, |context, _| {
        let started = Instant::now();
        loop {
            context.check_cancelled()?;
            if started.elapsed() > std::time::Duration::from_secs(2) {
                return Err(failed("test cancellation was not observed"));
            }
            std::thread::yield_now();
        }
    })
    .unwrap();
    jobs.submit(32, |_, lease| {
        lease.resize(129)?;
        Ok(1)
    })
    .unwrap();
    let error = jobs.join_next().err().unwrap().to_string();
    assert!(error.contains("memory reservation denied"));
    assert!(!error.contains("execution cancelled"));
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn failed_merge_cancels_pending_work_and_retains_no_window_credits() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut jobs = AggregateChunkJobs::<u64>::new(1, 2, 128, memory.clone()).unwrap();
    jobs.submit(32, |_, _| Ok(1)).unwrap();
    jobs.submit(32, |_, _| Ok(2)).unwrap();
    let error = jobs
        .join_next()
        .unwrap()
        .unwrap()
        .consume(|_| Err::<(), _>(failed("injected ordered merge failure")));
    assert!(error.is_err());
    assert!(jobs.submit(32, |_, _| Ok(3)).is_err());
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
