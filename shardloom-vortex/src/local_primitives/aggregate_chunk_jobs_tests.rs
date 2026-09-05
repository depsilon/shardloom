use super::*;
use std::sync::{Barrier, mpsc};

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
