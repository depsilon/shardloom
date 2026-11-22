use super::*;

fn until(mut observed: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !observed() {
        assert!(Instant::now() < deadline, "worker state was not reached");
        thread::yield_now();
    }
}

#[test]
fn disabled_observation_stays_unavailable_after_worker_closure() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let pool = ComputePool::new(2, 2, 128, memory.clone()).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(pool.snapshot().waits.is_none());
    let snapshot = pool.close_and_snapshot();
    assert!(snapshot.queue_closed);
    assert_eq!(snapshot.workers_created, 2);
    assert_eq!(snapshot.active_workers, 0);
    assert_eq!(snapshot.queued_jobs, 0);
    assert!(snapshot.waits.is_none());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn observed_empty_queue_waits_finish_before_closed_snapshot_and_refund() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let pool = ComputePool::with_wait_observation(2, 2, 128, memory.clone(), true).unwrap();
    assert_eq!(
        memory.snapshot().reserved_bytes,
        size_of::<WaitStats>() as u64
    );
    until(|| pool.snapshot().waits.unwrap().waiting_workers == 2);
    assert!(!pool.snapshot().queue_closed);
    let snapshot = pool.close_and_snapshot();
    let waits = snapshot.waits.unwrap();
    assert!(snapshot.queue_closed);
    assert_eq!(snapshot.workers_created, 2);
    assert_eq!(snapshot.active_workers, 0);
    assert_eq!(snapshot.queued_jobs, 0);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(waits.waiting_workers, 0);
    assert!(waits.completed_empty_queue_waits >= 2);
    assert!(waits.empty_queue_wait_nanos > 0);
    assert_eq!(waits.completed_capacity_waits, 0);
    assert_eq!(waits.capacity_wait_nanos, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn observed_worker_exit_does_not_refund_a_retained_result() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let pool = ComputePool::with_wait_observation(1, 2, 128, memory.clone(), true).unwrap();
    until(|| pool.snapshot().waits.unwrap().waiting_workers == 1);
    let result = pool
        .submit(
            Budgeted::new(
                |_: &WorkerContext, _: &mut MemoryLease| Ok(vec![7_u8; 64]),
                memory.reserve(64).unwrap(),
            ),
            CancellationToken::default(),
        )
        .unwrap()
        .join()
        .unwrap();
    let snapshot = pool.close_and_snapshot();
    assert_eq!(snapshot.completed_jobs, 1);
    assert_eq!(snapshot.active_workers, 0);
    assert_eq!(snapshot.queued_jobs, 0);
    assert_eq!(snapshot.waits.unwrap().completed_capacity_waits, 0);
    assert_eq!(snapshot.waits.unwrap().capacity_wait_nanos, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 64);
    assert_eq!(result.value().as_slice(), &[7_u8; 64]);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn capacity_waits_include_cancelled_admission_but_not_successful_dispatch_overhead() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let pool = ComputePool::with_wait_observation(1, 2, 32, memory.clone(), true).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let active = pool
        .submit(
            Budgeted::new(
                move |_: &WorkerContext, _: &mut MemoryLease| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(())
                },
                memory.reserve(16).unwrap(),
            ),
            CancellationToken::default(),
        )
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let queued = pool
        .submit(
            Budgeted::new(
                |_: &WorkerContext, _: &mut MemoryLease| Ok(()),
                memory.reserve(32).unwrap(),
            ),
            CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(pool.snapshot().waits.unwrap().completed_capacity_waits, 0);
    let cancel = CancellationToken::default();
    thread::scope(|scope| {
        let pool = &pool;
        let memory = &memory;
        let token = cancel.clone();
        let producer = scope.spawn(move || {
            pool.submit(
                Budgeted::new(
                    |_: &WorkerContext, _: &mut MemoryLease| Ok(()),
                    memory.reserve(1).unwrap(),
                ),
                token,
            )
        });
        until(|| pool.snapshot().waiting_producers == 1);
        cancel.cancel();
        assert!(producer.join().unwrap().is_err());
    });
    let blocked = pool.snapshot();
    assert_eq!(blocked.waiting_producers, 0);
    assert_eq!(blocked.queued_bytes, 32);
    assert!(blocked.waits.unwrap().completed_capacity_waits >= 1);
    assert!(blocked.waits.unwrap().capacity_wait_nanos > 0);
    release_tx.send(()).unwrap();
    drop(active.join().unwrap());
    drop(queued.join().unwrap());
    let final_snapshot = pool.close_and_snapshot();
    assert_eq!(final_snapshot.completed_jobs, 2);
    assert_eq!(final_snapshot.active_workers, 0);
    assert_eq!(final_snapshot.queued_jobs, 0);
    assert_eq!(final_snapshot.waiting_producers, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn observation_credit_refusal_precedes_worker_startup() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = ComputePool::with_wait_observation(1, 1, 1, memory.clone(), true)
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    let ordinary = ComputePool::new(1, 1, 1, memory.clone()).unwrap();
    assert!(ordinary.close_and_snapshot().waits.is_none());
}
