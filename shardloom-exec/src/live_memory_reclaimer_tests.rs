use crate::live_memory::{LiveMemoryPool, MemoryLease, OptionalMemoryReclaimer};
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct FakeOptionalMemoryOwner {
    calls: AtomicUsize,
    lease: Mutex<Option<MemoryLease>>,
}

impl FakeOptionalMemoryOwner {
    fn new(lease: Option<MemoryLease>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            lease: Mutex::new(lease),
        }
    }
}

impl OptionalMemoryReclaimer for FakeOptionalMemoryOwner {
    fn reclaim_optional_memory(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut lease) = self.lease.try_lock() {
            drop(lease.take());
        }
    }
}

fn reclaimer_weak(owner: &Arc<FakeOptionalMemoryOwner>) -> Weak<dyn OptionalMemoryReclaimer> {
    let endpoint: Arc<dyn OptionalMemoryReclaimer> = owner.clone();
    Arc::downgrade(&endpoint)
}

#[test]
fn mandatory_reservation_reclaims_optional_owner_but_optional_reservation_does_not() {
    let pool = LiveMemoryPool::new(100).unwrap();
    let cache = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(60).unwrap(),
    )));
    assert!(
        pool.try_install_optional_reclaimer(reclaimer_weak(&cache))
            .unwrap()
    );

    assert!(pool.try_reserve(50).unwrap().is_none());
    assert_eq!(cache.calls.load(Ordering::Relaxed), 0);
    assert_eq!(pool.snapshot().reserved_bytes, 60);

    let mandatory = pool.reserve(50).unwrap();
    assert_eq!(cache.calls.load(Ordering::Relaxed), 1);
    assert!(cache.lease.try_lock().unwrap().is_none());
    assert_eq!(pool.snapshot().reserved_bytes, 50);
    drop(mandatory);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn mandatory_adaptive_admission_reclaims_once_without_hiding_retained_owners() {
    use crate::adaptive_work::{AdaptiveWorkSizer, WorkCost, WorkLimits};

    let pool = LiveMemoryPool::new(100).unwrap();
    let optional = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(60).unwrap(),
    )));
    assert!(
        pool.try_install_optional_reclaimer(reclaimer_weak(&optional))
            .unwrap()
    );
    let held = pool.reserve(20).unwrap();
    let sizing = AdaptiveWorkSizer::default();
    let limits = WorkLimits {
        min_units: 1,
        max_units: 2,
        max_in_flight: 1,
    };
    let cost = WorkCost {
        fixed_bytes: 0,
        bytes_per_unit: 50,
    };
    assert!(
        sizing
            .select("required", pool.snapshot(), cost, limits)
            .is_err()
    );
    assert_eq!(
        optional.calls.load(Ordering::Relaxed),
        0,
        "advice stays side-effect-free"
    );
    let selected = sizing
        .select_in_pool("required", &pool, cost, limits)
        .unwrap();
    assert_eq!(selected.units(), 1);
    assert_eq!(selected.headroom_bytes(), 80);
    assert_eq!(optional.calls.load(Ordering::Relaxed), 1);
    assert_eq!(
        pool.snapshot().reserved_bytes,
        20,
        "selection grants no memory"
    );
    assert!(pool.owns(&held));
    assert_eq!(held.bytes(), 20);
    assert_eq!(sizing.report().completed_windows, 0);
    let required = pool.reserve(selected.estimated_bytes_per_lane()).unwrap();
    assert!(
        sizing
            .select_in_pool("required", &pool, cost, limits)
            .is_err()
    );
    assert_eq!(optional.calls.load(Ordering::Relaxed), 2);
    assert_eq!(pool.snapshot().reserved_bytes, 70);
    assert!(pool.owns(&required));
    drop((required, held, optional));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn failed_mandatory_reservation_reclaims_once_and_keeps_mandatory_owner() {
    let pool = LiveMemoryPool::new(100).unwrap();
    let optional = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(20).unwrap(),
    )));
    assert!(
        pool.try_install_optional_reclaimer(reclaimer_weak(&optional))
            .unwrap()
    );
    let mandatory = pool.reserve(80).unwrap();

    assert!(pool.reserve(101).is_err());
    assert_eq!(optional.calls.load(Ordering::Relaxed), 1);
    assert!(optional.lease.try_lock().unwrap().is_none());
    assert_eq!(mandatory.bytes(), 80);
    assert!(pool.owns(&mandatory));
    assert_eq!(pool.snapshot().reserved_bytes, 80);

    drop((optional, mandatory));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn configurable_shrink_reclaims_optional_lease_before_checking_new_limit() {
    let pool = LiveMemoryPool::configurable(100).unwrap();
    let optional = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(60).unwrap(),
    )));
    assert!(
        pool.try_install_optional_reclaimer(reclaimer_weak(&optional))
            .unwrap()
    );
    let mandatory = pool.reserve(20).unwrap();

    pool.set_limit_bytes(40).unwrap();
    assert_eq!(optional.calls.load(Ordering::Relaxed), 1);
    assert_eq!(pool.snapshot().limit_bytes, 40);
    assert_eq!(pool.snapshot().reserved_bytes, 20);
    assert_eq!(mandatory.bytes(), 20);

    assert!(pool.set_limit_bytes(10).is_err());
    assert_eq!(optional.calls.load(Ordering::Relaxed), 2);
    assert_eq!(mandatory.bytes(), 20);
    assert!(pool.owns(&mandatory));
    assert_eq!(pool.snapshot().limit_bytes, 40);
    assert_eq!(pool.snapshot().reserved_bytes, 20);

    drop((optional, mandatory));
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn registration_is_single_owner_weak_and_releases_all_lease_credits() {
    let pool = LiveMemoryPool::new(100).unwrap();
    let first = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(10).unwrap(),
    )));
    let first_weak = reclaimer_weak(&first);
    assert!(
        pool.try_install_optional_reclaimer(first_weak.clone())
            .unwrap()
    );

    let second = Arc::new(FakeOptionalMemoryOwner::new(Some(
        pool.reserve(20).unwrap(),
    )));
    let second_weak = reclaimer_weak(&second);
    assert!(
        !pool
            .try_install_optional_reclaimer(second_weak.clone())
            .unwrap()
    );
    assert_eq!(pool.snapshot().reserved_bytes, 30);

    drop(first);
    assert!(first_weak.upgrade().is_none());
    assert_eq!(pool.snapshot().reserved_bytes, 20);
    assert!(
        pool.try_install_optional_reclaimer(second_weak.clone())
            .unwrap()
    );
    drop(second);
    assert!(second_weak.upgrade().is_none());
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}
