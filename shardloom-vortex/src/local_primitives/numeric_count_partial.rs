//! Exact nonnullable integer COUNT(*) work for bounded native source chunks.
//!
//! Each worker owns a sorted vector of all keys and counts. Its output capacity
//! is admitted before allocation and remains leased through ordered merging.
//! Input/provider allocations and allocator overhead are separate scopes. There
//! is no per-chunk top-K pruning, floating-point coercion, or fallback route.

use super::aggregate_chunk_jobs::ChunkWorkerContext;
use shardloom_core::{Result, ShardLoomError};
#[cfg(test)]
use shardloom_exec::live_memory::LiveMemoryPool;
use shardloom_exec::live_memory::MemoryLease;
#[cfg(test)]
use std::cmp::Ordering;
use std::{fmt::Debug, mem::size_of};

mod sealed {
    pub trait Sealed {}
    impl Sealed for i64 {}
    impl Sealed for u64 {}
}

pub(super) trait NumericCountKey:
    sealed::Sealed + Copy + Ord + Debug + Send + 'static
{
}
impl NumericCountKey for i64 {}
impl NumericCountKey for u64 {}

#[derive(Debug)]
pub(super) struct OwnedNumericCounts<K: NumericCountKey> {
    // Payload must be dropped before returning its allocation credits.
    pairs: Vec<(K, u64)>,
    rows: u64,
    lease: MemoryLease,
}

impl<K: NumericCountKey> OwnedNumericCounts<K> {
    #[cfg(test)]
    pub(super) fn empty(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            pairs: Vec::new(),
            rows: 0,
            lease: memory.reserve(0)?,
        })
    }

    pub(super) fn pairs(&self) -> &[(K, u64)] {
        &self.pairs
    }

    pub(super) const fn rows(&self) -> u64 {
        self.rows
    }

    pub(super) const fn reserved_bytes(&self) -> u64 {
        self.lease.bytes()
    }
}

pub(super) fn partial_bytes<K: NumericCountKey>(rows: usize) -> Result<u64> {
    rows.checked_mul(size_of::<(K, u64)>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("partial capacity overflowed"))
}

/// The caller pre-reserves `partial_bytes(values.len())` in its task lease.
/// Splitting transfers those credits into the returned vector, so they cannot
/// disappear when the worker's source or task envelope is dropped.
pub(super) fn count_numeric_values<K: NumericCountKey>(
    values: &[K],
    context: &ChunkWorkerContext,
    task_lease: &mut MemoryLease,
) -> Result<OwnedNumericCounts<K>> {
    context.check_cancelled()?;
    let rows = u64::try_from(values.len()).map_err(|_| failed("row count overflowed"))?;
    let lease = task_lease.split(partial_bytes::<K>(values.len())?)?;
    let mut pairs = reserved_pairs(values.len())?;
    for (index, &key) in values.iter().enumerate() {
        if index % 1024 == 0 {
            context.check_cancelled()?;
        }
        pairs.push((key, 1_u64));
    }
    context.check_cancelled()?;
    // Unstable sorting uses no allocation and the count is insensitive to
    // ordering among identical integer keys. Input chunks bound this work.
    pairs.sort_unstable_by_key(|pair| pair.0);
    context.check_cancelled()?;
    let mut groups = 0_usize;
    for index in 0..pairs.len() {
        if index % 1024 == 0 {
            context.check_cancelled()?;
        }
        let (key, count) = pairs[index];
        if groups > 0 && pairs[groups - 1].0 == key {
            pairs[groups - 1].1 = checked_count(pairs[groups - 1].1, count)?;
        } else {
            pairs[groups] = (key, count);
            groups += 1;
        }
    }
    // Retain the original reservation: truncate changes logical length, not
    // allocation capacity. Do not report released scratch that still exists.
    pairs.truncate(groups);
    Ok(OwnedNumericCounts { pairs, rows, lease })
}

/// Preserve a native constant key as one weighted entry, without row expansion.
/// Native metadata/encoded dispatch remains ahead of generic slice processing.
pub(super) fn count_numeric_constant<K: NumericCountKey>(
    key: K,
    rows: u64,
    context: &ChunkWorkerContext,
    task_lease: &mut MemoryLease,
) -> Result<OwnedNumericCounts<K>> {
    context.check_cancelled()?;
    let capacity = usize::from(rows > 0);
    let lease = task_lease.split(partial_bytes::<K>(capacity)?)?;
    let mut pairs = reserved_pairs(capacity)?;
    if rows > 0 {
        pairs.push((key, rows));
    }
    Ok(OwnedNumericCounts { pairs, rows, lease })
}

/// Complete one balanced pair merge, preserving both inputs on failure.
/// Production can instead consume weighted `pairs()` into admitted global
/// typed state. Repeatedly rebuilding a growing global vector is not an admitted
/// broad integration strategy: its work grows with chunks times global keys.
#[cfg(test)]
pub(super) fn merge_numeric_counts<K: NumericCountKey>(
    left: &OwnedNumericCounts<K>,
    right: &OwnedNumericCounts<K>,
    memory: &LiveMemoryPool,
    check_cancelled: impl Fn() -> Result<()>,
) -> Result<OwnedNumericCounts<K>> {
    check_cancelled()?;
    if !memory.owns(&left.lease) || !memory.owns(&right.lease) {
        return Err(failed("merge inputs must share the output memory pool"));
    }
    let rows = checked_count(left.rows, right.rows)?;
    let capacity = left
        .pairs
        .len()
        .checked_add(right.pairs.len())
        .ok_or_else(|| failed("merge capacity overflowed"))?;
    let lease = memory.reserve(partial_bytes::<K>(capacity)?)?;
    let mut pairs = reserved_pairs(capacity)?;
    let (mut l, mut r) = (0, 0);
    while l < left.pairs.len() || r < right.pairs.len() {
        if pairs.len() % 1024 == 0 {
            check_cancelled()?;
        }
        match (left.pairs.get(l), right.pairs.get(r)) {
            (Some(&(left_key, left_count)), Some(&(right_key, right_count))) => {
                match left_key.cmp(&right_key) {
                    Ordering::Less => {
                        pairs.push((left_key, left_count));
                        l += 1;
                    }
                    Ordering::Greater => {
                        pairs.push((right_key, right_count));
                        r += 1;
                    }
                    Ordering::Equal => {
                        pairs.push((left_key, checked_count(left_count, right_count)?));
                        l += 1;
                        r += 1;
                    }
                }
            }
            (Some(&pair), None) => {
                pairs.push(pair);
                l += 1;
            }
            (None, Some(&pair)) => {
                pairs.push(pair);
                r += 1;
            }
            (None, None) => unreachable!("merge loop has at least one input key"),
        }
    }
    check_cancelled()?;
    Ok(OwnedNumericCounts { pairs, rows, lease })
}

fn reserved_pairs<K: NumericCountKey>(capacity: usize) -> Result<Vec<(K, u64)>> {
    let mut pairs = Vec::new();
    pairs
        .try_reserve_exact(capacity)
        .map_err(|error| failed(&format!("partial allocation failed: {error}")))?;
    if pairs.capacity() != capacity {
        return Err(failed(
            "allocated vector capacity differs from admitted capacity",
        ));
    }
    Ok(pairs)
}

fn checked_count(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| failed("COUNT(*) overflowed u64"))
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex integer count partial {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "numeric_count_partial_tests.rs"]
mod tests;
