//! Lifetime-bound accounting for owned execution buffers and state.
//!
//! This is a reservation pool, not a replacement allocator. Producers must reserve
//! capacity before allocation and keep the lease with every retained payload.

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

use shardloom_core::{Result, ShardLoomError};

#[derive(Debug)]
struct PoolState {
    limit: PoolLimit,
    live: AtomicU64,
    peak: AtomicU64,
    denied: AtomicU64,
}

#[derive(Debug)]
enum PoolLimit {
    Fixed(u64),
    Configurable(RwLock<Allocation>),
}

#[derive(Debug)]
struct Allocation {
    limit_bytes: u64,
    peak_reserved_bytes: AtomicU64,
}

/// One shared budget for source, queue, operator, merge, and result ownership.
#[derive(Debug, Clone)]
pub struct LiveMemoryPool(Arc<PoolState>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveMemorySnapshot {
    pub limit_bytes: u64,
    pub reserved_bytes: u64,
    /// Lifetime high-water mark; it can exceed a later, smaller allocation.
    pub peak_reserved_bytes: u64,
    /// Peak since the last successful allocation update, including retained
    /// owners at admission. For a fixed pool this is the lifetime peak.
    pub allocation_peak_reserved_bytes: u64,
    pub denied_reservations: u64,
}

impl LiveMemoryPool {
    /// # Errors
    /// Rejects an empty budget.
    pub fn new(limit_bytes: u64) -> Result<Self> {
        Self::with_limit(PoolLimit::Fixed(limit_bytes))
    }

    /// Create a pool whose allocation may change between complete operations.
    /// Every clone and retained lease continues to use this same owner. Callers
    /// must serialize whole operations around allocation changes; the pool lock
    /// only makes changing the limit atomic with concurrent reservations.
    /// # Errors
    /// Rejects an empty budget.
    pub fn configurable(limit_bytes: u64) -> Result<Self> {
        Self::with_limit(PoolLimit::Configurable(RwLock::new(Allocation {
            limit_bytes,
            peak_reserved_bytes: AtomicU64::new(0),
        })))
    }

    fn with_limit(limit: PoolLimit) -> Result<Self> {
        let limit_bytes = match &limit {
            PoolLimit::Fixed(limit) => *limit,
            PoolLimit::Configurable(limit) => {
                limit
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .limit_bytes
            }
        };
        if limit_bytes == 0 {
            return Err(memory_error("memory budget must be greater than zero"));
        }
        Ok(Self(Arc::new(PoolState {
            limit,
            live: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            denied: AtomicU64::new(0),
        })))
    }

    /// Change an explicitly configurable pool without replacing any owner or
    /// releasing credits. A smaller allocation must cover every retained lease.
    /// Authorization belongs to the caller's validated resource configuration.
    /// Every successful call starts a new allocation peak at retained usage,
    /// even when the limit is unchanged. The lifetime peak is never reset.
    /// # Errors
    /// Rejects fixed pools, zero, or a limit below current live reservations.
    /// A rejected change leaves the previous allocation intact.
    pub fn set_limit_bytes(&self, limit_bytes: u64) -> Result<()> {
        let PoolLimit::Configurable(limit) = &self.0.limit else {
            return Err(memory_error("this memory pool has a fixed allocation"));
        };
        if limit_bytes == 0 {
            return Err(memory_error("memory budget must be greater than zero"));
        }
        let mut limit = limit
            .write()
            .map_err(|_| memory_error("memory allocation configuration poisoned"))?;
        let live = self.0.live.load(Ordering::Acquire);
        if live > limit_bytes {
            return Err(memory_error(&format!(
                "memory allocation cannot cover retained owners: reserved={live}, requested_limit={limit_bytes}"
            )));
        }
        limit.limit_bytes = limit_bytes;
        *limit.peak_reserved_bytes.get_mut() = live;
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> LiveMemorySnapshot {
        match &self.0.limit {
            PoolLimit::Fixed(limit) => self.snapshot_with_limit(*limit, &self.0.peak),
            PoolLimit::Configurable(limit) => {
                // Keep the allocation and its high-water mark in the same epoch
                // while reading; admission cannot replace either independently.
                let limit = limit
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                self.snapshot_with_limit(limit.limit_bytes, &limit.peak_reserved_bytes)
            }
        }
    }

    fn snapshot_with_limit(
        &self,
        limit_bytes: u64,
        allocation_peak: &AtomicU64,
    ) -> LiveMemorySnapshot {
        let reserved_bytes = self.0.live.load(Ordering::Acquire);
        // A concurrent successful reservation publishes live usage before its
        // peak. That already-admitted usage is also a valid observed peak.
        let allocation_peak_reserved_bytes =
            allocation_peak.load(Ordering::Acquire).max(reserved_bytes);
        LiveMemorySnapshot {
            limit_bytes,
            reserved_bytes,
            peak_reserved_bytes: self
                .0
                .peak
                .load(Ordering::Acquire)
                .max(allocation_peak_reserved_bytes),
            allocation_peak_reserved_bytes,
            denied_reservations: self.0.denied.load(Ordering::Relaxed),
        }
    }

    #[must_use]
    pub fn owns(&self, lease: &MemoryLease) -> bool {
        Arc::ptr_eq(&self.0, &lease.pool.0)
    }

    /// Reserve before allocating. The lease releases its bytes on drop.
    ///
    /// # Errors
    /// Rejects requests exceeding the shared budget, including arithmetic overflow.
    pub fn reserve(&self, bytes: u64) -> Result<MemoryLease> {
        self.acquire(bytes)?;
        Ok(MemoryLease {
            pool: self.clone(),
            bytes,
        })
    }

    fn acquire(&self, bytes: u64) -> Result<()> {
        match &self.0.limit {
            PoolLimit::Fixed(limit) => self.acquire_with_limit(bytes, *limit).map(|_| ()),
            PoolLimit::Configurable(limit) => {
                let limit = limit
                    .read()
                    .map_err(|_| memory_error("memory allocation configuration poisoned"))?;
                let reserved = self.acquire_with_limit(bytes, limit.limit_bytes)?;
                limit
                    .peak_reserved_bytes
                    .fetch_max(reserved, Ordering::AcqRel);
                Ok(())
            }
        }
    }

    fn acquire_with_limit(&self, bytes: u64, limit: u64) -> Result<u64> {
        let mut live = self.0.live.load(Ordering::Acquire);
        loop {
            let Some(next) = live.checked_add(bytes).filter(|next| *next <= limit) else {
                self.0.denied.fetch_add(1, Ordering::Relaxed);
                return Err(memory_error(&format!(
                    "memory reservation denied: requested={bytes}, reserved={live}, limit={limit}"
                )));
            };
            match self
                .0
                .live
                .compare_exchange_weak(live, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.0.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(next);
                }
                Err(observed) => live = observed,
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.0.live.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// Non-cloneable ownership of a reservation. Share it through `Arc` with its payload.
#[derive(Debug)]
pub struct MemoryLease {
    pool: LiveMemoryPool,
    bytes: u64,
}

impl MemoryLease {
    /// Reuse this reservation's owner across subsequent overlapping stages.
    /// Borrowing the pool does not issue another allocation or release credits.
    #[must_use]
    pub const fn pool(&self) -> &LiveMemoryPool {
        &self.pool
    }

    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Grow before allocating, shrink after releasing the corresponding payload.
    /// A failed growth leaves the original reservation intact.
    ///
    /// # Errors
    /// Returns the shared admission error if growth cannot be reserved.
    pub fn resize(&mut self, bytes: u64) -> Result<()> {
        if bytes > self.bytes {
            self.pool.acquire(bytes - self.bytes)?;
        } else {
            self.pool.release(self.bytes - bytes);
        }
        self.bytes = bytes;
        Ok(())
    }

    /// Transfer some reserved capacity without charging it twice.
    ///
    /// # Errors
    /// Rejects a split larger than this lease.
    pub fn split(&mut self, bytes: u64) -> Result<Self> {
        if bytes > self.bytes {
            return Err(memory_error("cannot split more bytes than a lease owns"));
        }
        self.bytes -= bytes;
        Ok(Self {
            pool: self.pool.clone(),
            bytes,
        })
    }

    /// Consolidate ownership from the same pool without a release/reacquire gap.
    ///
    /// # Errors
    /// Rejects unrelated pools. Both leases are preserved on error.
    pub fn absorb(&mut self, other: &mut Self) -> Result<()> {
        if !Arc::ptr_eq(&self.pool.0, &other.pool.0) {
            return Err(memory_error("cannot combine leases from different pools"));
        }
        self.bytes = self
            .bytes
            .checked_add(other.bytes)
            .ok_or_else(|| memory_error("memory lease size overflow"))?;
        other.bytes = 0;
        Ok(())
    }
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.pool.release(self.bytes);
    }
}

/// An owned payload whose reservation lives until the payload is released.
/// Field order deliberately drops the value before returning its memory credits.
#[derive(Debug)]
pub struct Budgeted<T> {
    value: T,
    lease: MemoryLease,
}

impl<T> Budgeted<T> {
    #[must_use]
    pub const fn new(value: T, lease: MemoryLease) -> Self {
        Self { value, lease }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }

    /// Inspect the reservation identity without releasing or transferring credit.
    #[must_use]
    pub const fn lease(&self) -> &MemoryLease {
        &self.lease
    }

    #[must_use]
    pub fn into_parts(self) -> (T, MemoryLease) {
        (self.value, self.lease)
    }
}

fn memory_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{message}; fallback execution was not attempted"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn growth_failure_preserves_capacity_and_drop_releases_it() {
        let pool = LiveMemoryPool::new(100).unwrap();
        let mut lease = pool.reserve(80).unwrap();
        assert!(lease.resize(101).is_err());
        assert_eq!(lease.bytes(), 80);
        lease.resize(20).unwrap();
        let other = pool.reserve(80).unwrap();
        assert_eq!(pool.snapshot().peak_reserved_bytes, 100);
        drop((lease, other));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn overflow_and_cross_pool_transfer_fail_without_losing_ownership() {
        let pool = LiveMemoryPool::new(u64::MAX).unwrap();
        let mut full = pool.reserve(u64::MAX).unwrap();
        assert!(pool.reserve(1).is_err());
        let other_pool = LiveMemoryPool::new(10).unwrap();
        let mut other = other_pool.reserve(10).unwrap();
        assert!(full.absorb(&mut other).is_err());
        assert_eq!(full.bytes(), u64::MAX);
        assert_eq!(other.bytes(), 10);
        drop((full, other));
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert_eq!(other_pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn shared_payload_keeps_credits_until_the_last_owner_drops() {
        let pool = LiveMemoryPool::new(64).unwrap();
        let mut lease = pool.reserve(64).unwrap();
        let mut half = lease.split(32).unwrap();
        lease.absorb(&mut half).unwrap();
        let result = Arc::new(Budgeted::new(vec![0_u8; 64], lease));
        let retained = Arc::clone(&result);
        drop((result, half));
        assert_eq!(pool.snapshot().reserved_bytes, 64);
        assert_eq!(retained.value().len(), 64);
        drop(retained);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn concurrent_reservations_never_overcommit() {
        let pool = LiveMemoryPool::new(100).unwrap();
        let barrier = Arc::new(Barrier::new(12));
        std::thread::scope(|scope| {
            for _ in 0..12 {
                let pool = pool.clone();
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    let lease = pool.reserve(25);
                    barrier.wait();
                    assert!(pool.snapshot().reserved_bytes <= 100);
                    drop(lease);
                });
            }
        });
        assert_eq!(pool.snapshot().peak_reserved_bytes, 100);
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert_eq!(pool.snapshot().denied_reservations, 8);
    }

    #[test]
    fn unwind_releases_reservations() {
        let pool = LiveMemoryPool::new(100).unwrap();
        let _ = std::panic::catch_unwind(|| {
            let _lease = pool.reserve(100).unwrap();
            panic!("injected operator failure");
        });
        assert_eq!(pool.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn configurable_allocation_keeps_retained_owners_and_refuses_too_small_changes() {
        let pool = LiveMemoryPool::configurable(100).unwrap();
        let retained_pool = pool.clone();
        let mut retained = pool.reserve(80).unwrap();
        assert!(pool.set_limit_bytes(79).is_err());
        assert!(pool.set_limit_bytes(0).is_err());
        assert_eq!(pool.snapshot().limit_bytes, 100);
        assert_eq!(retained.bytes(), 80);
        pool.set_limit_bytes(80).unwrap();
        assert!(retained_pool.reserve(1).is_err());
        pool.set_limit_bytes(160).unwrap();
        retained.resize(150).unwrap();
        assert!(retained_pool.owns(&retained));
        assert_eq!(retained_pool.snapshot().reserved_bytes, 150);
        drop(retained);
        retained_pool.set_limit_bytes(1).unwrap();
        assert_eq!(pool.snapshot().reserved_bytes, 0);
        assert_eq!(pool.snapshot().peak_reserved_bytes, 150);
        assert_eq!(pool.snapshot().limit_bytes, 1);
        assert!(
            LiveMemoryPool::new(100)
                .unwrap()
                .set_limit_bytes(200)
                .is_err()
        );
    }

    #[test]
    fn configurable_reservation_and_reconfiguration_are_atomic() {
        for _ in 0..64 {
            let pool = LiveMemoryPool::configurable(100).unwrap();
            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                let reserve = scope.spawn(|| {
                    barrier.wait();
                    pool.reserve(80)
                });
                barrier.wait();
                let changed = pool.set_limit_bytes(50);
                let reserved = reserve.join().unwrap();
                assert_ne!(changed.is_ok(), reserved.is_ok());
                let snapshot = pool.snapshot();
                assert!(snapshot.reserved_bytes <= snapshot.limit_bytes);
                assert!(snapshot.reserved_bytes <= snapshot.allocation_peak_reserved_bytes);
                assert!(snapshot.allocation_peak_reserved_bytes <= snapshot.limit_bytes);
                drop(reserved);
            });
            assert_eq!(pool.snapshot().reserved_bytes, 0);
        }
    }

    #[test]
    fn allocation_peaks_cover_retained_and_transient_owners_without_erasing_history() {
        let pool = LiveMemoryPool::configurable(1000).unwrap();
        let mut retained = pool.reserve(900).unwrap();
        retained.resize(40).unwrap();
        pool.set_limit_bytes(100).unwrap();
        let first = pool.snapshot();
        assert_eq!(first.reserved_bytes, 40);
        assert_eq!(first.peak_reserved_bytes, 900);
        assert_eq!(first.allocation_peak_reserved_bytes, 40);
        let mut transient = pool.reserve(20).unwrap();
        transient.resize(50).unwrap();
        drop(transient);
        let completed = pool.snapshot();
        assert_eq!(completed.reserved_bytes, 40);
        assert_eq!(completed.allocation_peak_reserved_bytes, 90);
        assert_eq!(completed.peak_reserved_bytes, 900);
        for invalid in [0, 39] {
            assert!(pool.set_limit_bytes(invalid).is_err());
            assert_eq!(pool.snapshot(), completed);
        }
        // A same-size complete operation has its own peak and retains old owners.
        pool.set_limit_bytes(100).unwrap();
        assert!(pool.owns(&retained));
        assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 40);
        assert!(pool.reserve(61).is_err());
        assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 40);
        assert_eq!(pool.snapshot().denied_reservations, 1);
        let full = pool.reserve(60).unwrap();
        assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 100);
        drop((full, retained));
        pool.set_limit_bytes(1).unwrap();
        let empty = pool.snapshot();
        assert_eq!(empty.reserved_bytes, 0);
        assert_eq!(empty.allocation_peak_reserved_bytes, 0);
        assert_eq!(empty.peak_reserved_bytes, 900);
        assert_eq!(empty.denied_reservations, 1);
    }

    #[test]
    fn concurrent_reservations_contribute_to_one_allocation_peak() {
        let pool = LiveMemoryPool::configurable(500).unwrap();
        drop(pool.reserve(450).unwrap());
        pool.set_limit_bytes(120).unwrap();
        let barrier = Barrier::new(6);
        std::thread::scope(|scope| {
            for _ in 0..6 {
                let pool = &pool;
                let barrier = &barrier;
                scope.spawn(move || {
                    let retained = pool.reserve(20).unwrap();
                    barrier.wait();
                    assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 120);
                    drop(retained);
                });
            }
        });
        let observed = pool.snapshot();
        assert_eq!(observed.reserved_bytes, 0);
        assert_eq!(observed.allocation_peak_reserved_bytes, 120);
        assert_eq!(observed.peak_reserved_bytes, 450);
        assert_eq!(observed.denied_reservations, 0);
    }

    #[test]
    fn fixed_pool_allocation_peak_covers_its_whole_lifetime() {
        let pool = LiveMemoryPool::new(100).unwrap();
        drop(pool.reserve(80).unwrap());
        assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 80);
        assert_eq!(pool.snapshot().peak_reserved_bytes, 80);
        assert!(pool.set_limit_bytes(100).is_err());
        assert_eq!(pool.snapshot().allocation_peak_reserved_bytes, 80);
    }
}
