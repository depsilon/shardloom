//! Lifetime-bound accounting for owned execution buffers and state.
//!
//! This is a reservation pool, not a replacement allocator. Producers must reserve
//! capacity before allocation and keep the lease with every retained payload.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use shardloom_core::{Result, ShardLoomError};

#[derive(Debug)]
struct PoolState {
    limit: u64,
    live: AtomicU64,
    peak: AtomicU64,
    denied: AtomicU64,
}

/// One shared budget for source, queue, operator, merge, and result ownership.
#[derive(Debug, Clone)]
pub struct LiveMemoryPool(Arc<PoolState>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveMemorySnapshot {
    pub limit_bytes: u64,
    pub reserved_bytes: u64,
    pub peak_reserved_bytes: u64,
    pub denied_reservations: u64,
}

impl LiveMemoryPool {
    /// # Errors
    /// Rejects an empty budget.
    pub fn new(limit_bytes: u64) -> Result<Self> {
        if limit_bytes == 0 {
            return Err(memory_error("memory budget must be greater than zero"));
        }
        Ok(Self(Arc::new(PoolState {
            limit: limit_bytes,
            live: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            denied: AtomicU64::new(0),
        })))
    }

    #[must_use]
    pub fn snapshot(&self) -> LiveMemorySnapshot {
        LiveMemorySnapshot {
            limit_bytes: self.0.limit,
            reserved_bytes: self.0.live.load(Ordering::Acquire),
            peak_reserved_bytes: self.0.peak.load(Ordering::Acquire),
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
        let mut live = self.0.live.load(Ordering::Acquire);
        loop {
            let Some(next) = live.checked_add(bytes).filter(|next| *next <= self.0.limit) else {
                self.0.denied.fetch_add(1, Ordering::Relaxed);
                return Err(memory_error(&format!(
                    "memory reservation denied: requested={bytes}, reserved={live}, limit={}",
                    self.0.limit
                )));
            };
            match self
                .0
                .live
                .compare_exchange_weak(live, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.0.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(());
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
}
