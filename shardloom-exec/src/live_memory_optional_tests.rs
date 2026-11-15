use crate::live_memory::LiveMemoryPool;
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn optional_reservation_refusal_and_success_work_for_fixed_and_configurable_pools() {
    for pool in [
        LiveMemoryPool::new(100).unwrap(),
        LiveMemoryPool::configurable(100).unwrap(),
    ] {
        let existing = pool.reserve(80).unwrap();
        let before = pool.snapshot();

        assert!(pool.try_reserve(21).unwrap().is_none());
        let refused = pool.snapshot();
        assert_eq!(refused.reserved_bytes, 80);
        assert_eq!(refused.denied_reservations, before.denied_reservations + 1);

        let optional = pool.try_reserve(20).unwrap().unwrap();
        let admitted = pool.snapshot();
        assert_eq!(admitted.reserved_bytes, 100);
        assert_eq!(admitted.peak_reserved_bytes, 100);
        drop((optional, existing));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn optional_reservation_observes_reconfigured_limit_without_changing_retained_credit() {
    let pool = LiveMemoryPool::configurable(100).unwrap();
    let existing = pool.reserve(40).unwrap();
    pool.set_limit_bytes(60).unwrap();
    let before = pool.snapshot();

    assert!(pool.try_reserve(21).unwrap().is_none());
    let refused = pool.snapshot();
    assert_eq!(refused.reserved_bytes, 40);
    assert_eq!(existing.bytes(), 40);
    assert!(pool.owns(&existing));
    assert_eq!(refused.denied_reservations, before.denied_reservations + 1);

    let optional = pool.try_reserve(20).unwrap().unwrap();
    assert_eq!(pool.snapshot().reserved_bytes, 60);
    assert_eq!(existing.bytes(), 40);
    assert!(pool.owns(&existing));
    drop(optional);
    assert_eq!(pool.snapshot().reserved_bytes, 40);
    assert_eq!(existing.bytes(), 40);
    drop(existing);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn optional_reservation_handles_maximum_budget_overflow_and_zero_bytes() {
    let pool = LiveMemoryPool::new(u64::MAX).unwrap();
    let full = pool.reserve(u64::MAX).unwrap();
    let before = pool.snapshot();

    assert!(pool.try_reserve(1).unwrap().is_none());
    let refused = pool.snapshot();
    assert_eq!(refused.reserved_bytes, u64::MAX);
    assert_eq!(refused.denied_reservations, before.denied_reservations + 1);

    let zero = pool.try_reserve(0).unwrap().unwrap();
    assert_eq!(pool.snapshot().reserved_bytes, u64::MAX);
    drop((zero, full));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn concurrent_optional_reservations_never_overcommit_and_release_all_credits() {
    const WORKERS: usize = 8;
    const RESERVATION_BYTES: u64 = 25;

    let pool = LiveMemoryPool::new(100).unwrap();
    let ready = Barrier::new(WORKERS + 1);
    let release = Barrier::new(WORKERS + 1);
    let admitted = AtomicUsize::new(0);

    std::thread::scope(|scope| {
        let mut workers = Vec::with_capacity(WORKERS);
        for _ in 0..WORKERS {
            let pool = pool.clone();
            let ready = &ready;
            let release = &release;
            let admitted = &admitted;
            workers.push(scope.spawn(move || {
                ready.wait();
                let lease = pool.try_reserve(RESERVATION_BYTES).unwrap();
                if lease.is_some() {
                    admitted.fetch_add(1, Ordering::Relaxed);
                }
                ready.wait();
                release.wait();
                drop(lease);
            }));
        }

        ready.wait();
        ready.wait();
        let admitted_count = admitted.load(Ordering::Relaxed);
        let reserved = pool.snapshot().reserved_bytes;
        release.wait();

        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(admitted_count, 4);
        assert_eq!(reserved, 100);
    });

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.peak_reserved_bytes, 100);
    assert_eq!(snapshot.denied_reservations, 4);
}
