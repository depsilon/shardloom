//! Native operator containers reserve replacement overlap before allocation.

use super::vortex_error;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};

fn failed() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native state capacity overflow; no fallback execution was attempted".into(),
    )
}

pub(super) fn bytes<T>(capacity: usize) -> Result<u64> {
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(failed)
}

/// Values drop before the container credit; contained payloads own their credits.
pub(super) struct ReservedVec<T> {
    pub(super) values: Vec<T>,
    lease: MemoryLease,
}

impl<T> ReservedVec<T> {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            lease: memory.reserve(0)?,
        })
    }

    pub(super) fn reserve_one(&mut self) -> Result<()> {
        self.reserve(1)
    }

    pub(super) fn reserve(&mut self, additional: usize) -> Result<()> {
        let needed = self
            .values
            .len()
            .checked_add(additional)
            .ok_or_else(failed)?;
        if needed <= self.values.capacity() {
            return Ok(());
        }
        let capacity = self
            .values
            .capacity()
            .max(4)
            .checked_mul(2)
            .ok_or_else(failed)?
            .max(needed);
        let new_bytes = bytes::<T>(capacity)?;
        self.lease.resize(
            self.lease
                .bytes()
                .checked_add(new_bytes)
                .ok_or_else(failed)?,
        )?;
        self.values
            .try_reserve_exact(capacity - self.values.len())
            .map_err(vortex_error)?;
        if self.values.capacity() > capacity {
            return Err(failed());
        }
        self.lease.resize(new_bytes)
    }

    pub(super) fn push(&mut self, value: T) -> Result<()> {
        self.reserve_one()?;
        self.values.push(value);
        Ok(())
    }

    pub(super) fn into_parts(self) -> (Vec<T>, MemoryLease) {
        (self.values, self.lease)
    }
}
