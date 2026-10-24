//! Exact native row membership with an insertion-ordered duplicate chain.
//! Hashes select candidates; the caller's native key owners prove equality.

use super::{compound_count_partial, native_capacity::ReservedVec};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

const EMPTY: usize = usize::MAX;

struct Entry {
    hash: u64,
    first: usize,
    last: usize,
}

struct Link {
    row: usize,
    next: usize,
}

pub(super) struct RowIndex {
    buckets: ReservedVec<usize>,
    entries: ReservedVec<Entry>,
    links: ReservedVec<Link>,
    memory: LiveMemoryPool,
}

impl RowIndex {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            buckets: ReservedVec::new(memory)?,
            entries: ReservedVec::new(memory)?,
            links: ReservedVec::new(memory)?,
            memory: memory.clone(),
        })
    }

    pub(super) fn find(
        &self,
        hash: u64,
        cancellation: &CancellationToken,
        mut equal: impl FnMut(usize) -> Result<bool>,
    ) -> Result<Option<usize>> {
        cancellation.check()?;
        if self.buckets.values.is_empty() {
            return Ok(None);
        }
        let mut bucket = compound_count_partial::bucket(hash, self.buckets.values.len());
        let mut visited = 0usize;
        loop {
            if visited.is_multiple_of(1024) {
                cancellation.check()?;
            }
            let position = self.buckets.values[bucket];
            if position == EMPTY {
                cancellation.check()?;
                return Ok(None);
            }
            let entry = &self.entries.values[position];
            if entry.hash == hash && equal(self.links.values[entry.first].row)? {
                cancellation.check()?;
                return Ok(Some(position));
            }
            bucket = (bucket + 1) & (self.buckets.values.len() - 1);
            visited += 1;
        }
    }

    /// Retain this row even when its key already exists. Publication follows all
    /// potentially failing reservations, preserving the prior duplicate chain.
    pub(super) fn insert(
        &mut self,
        hash: u64,
        row: usize,
        cancellation: &CancellationToken,
        equal: impl FnMut(usize) -> Result<bool>,
    ) -> Result<usize> {
        self.insert_with_duplicates(hash, row, true, cancellation, equal)
            .map(|(position, _)| position)
    }

    /// Search once and retain only a new key. The boolean reports publication;
    /// an existing key acquires no new link or reservation.
    pub(super) fn insert_unique(
        &mut self,
        hash: u64,
        row: usize,
        cancellation: &CancellationToken,
        equal: impl FnMut(usize) -> Result<bool>,
    ) -> Result<(usize, bool)> {
        self.insert_with_duplicates(hash, row, false, cancellation, equal)
    }

    fn insert_with_duplicates(
        &mut self,
        hash: u64,
        row: usize,
        retain_duplicate: bool,
        cancellation: &CancellationToken,
        equal: impl FnMut(usize) -> Result<bool>,
    ) -> Result<(usize, bool)> {
        let existing = self.find(hash, cancellation, equal)?;
        if !retain_duplicate && let Some(position) = existing {
            return Ok((position, false));
        }
        self.links.reserve_one()?;
        if existing.is_none() {
            self.entries.reserve_one()?;
            self.reserve_entry(cancellation)?;
        }
        // Resolve every fallible operation before publishing a row or link.
        let vacant = existing
            .is_none()
            .then(|| vacant_bucket(&self.buckets.values, hash, cancellation))
            .transpose()?;
        cancellation.check()?;
        let link = self.links.values.len();
        self.links.values.push(Link { row, next: EMPTY });
        if let Some(position) = existing {
            let entry = &mut self.entries.values[position];
            self.links.values[entry.last].next = link;
            entry.last = link;
            return Ok((position, false));
        }
        let position = self.entries.values.len();
        self.entries.values.push(Entry {
            hash,
            first: link,
            last: link,
        });
        self.buckets.values[vacant.expect("new entry has a vacant bucket")] = position;
        Ok((position, true))
    }

    fn reserve_entry(&mut self, cancellation: &CancellationToken) -> Result<()> {
        cancellation.check()?;
        let needed = self
            .entries
            .values
            .len()
            .checked_add(1)
            .ok_or_else(failed)?;
        if needed <= self.buckets.values.len() / 2 {
            return Ok(());
        }
        let capacity = needed
            .checked_mul(2)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(failed)?
            .max(16);
        // Both bucket arrays retain their own credits until rehash is complete.
        let mut next = ReservedVec::new(&self.memory)?;
        next.reserve(capacity)?;
        next.values.resize(capacity, EMPTY);
        for (position, entry) in self.entries.values.iter().enumerate() {
            let bucket = vacant_bucket(&next.values, entry.hash, cancellation)?;
            next.values[bucket] = position;
        }
        cancellation.check()?;
        self.buckets = next;
        Ok(())
    }

    pub(super) fn rows(&self, entry: usize) -> Result<Rows<'_>> {
        let first = self.entries.values.get(entry).ok_or_else(failed)?.first;
        Ok(Rows {
            index: self,
            next: first,
        })
    }

    pub(super) fn unique_rows(&self) -> usize {
        self.entries.values.len()
    }
}

pub(super) struct Rows<'a> {
    index: &'a RowIndex,
    next: usize,
}

impl Iterator for Rows<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if self.next == EMPTY {
            return None;
        }
        let link = &self.index.links.values[self.next];
        self.next = link.next;
        Some(link.row)
    }
}

fn vacant_bucket(buckets: &[usize], hash: u64, cancellation: &CancellationToken) -> Result<usize> {
    let mut bucket = compound_count_partial::bucket(hash, buckets.len());
    let mut visited = 0usize;
    while buckets[bucket] != EMPTY {
        if visited.is_multiple_of(1024) {
            cancellation.check()?;
        }
        bucket = (bucket + 1) & (buckets.len() - 1);
        visited += 1;
    }
    cancellation.check()?;
    Ok(bucket)
}

fn failed() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "native relational row index exceeds its admitted bounds; no fallback execution was attempted"
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_insert_searches_collision_keys_once_and_keeps_only_first_rows() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..256 {
            let key = row % 64;
            let mut comparisons = 0;
            let (position, inserted) = index
                .insert_unique(7, row, &cancellation, |old| {
                    comparisons += 1;
                    Ok(old % 64 == key)
                })
                .unwrap();
            assert_eq!(position, key);
            assert_eq!(inserted, row < 64);
            assert_eq!(comparisons, if inserted { key } else { key + 1 });
            assert_eq!(index.rows(position).unwrap().collect::<Vec<_>>(), [key]);
        }
        assert_eq!(index.unique_rows(), 64);
        assert_eq!(index.links.values.len(), 64);
        // The ordinary mode still adds duplicates in source order.
        index
            .insert(7, 256, &cancellation, |old| Ok(old == 0))
            .unwrap();
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), [0, 256]);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn unique_insert_preserves_publication_on_denial_error_and_cancellation() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        index
            .insert_unique(0, 0, &cancellation, |_| Ok(false))
            .unwrap();
        let hold = memory
            .reserve((1 << 20) - memory.snapshot().reserved_bytes)
            .unwrap();
        assert_eq!(
            index
                .insert_unique(0, 1, &cancellation, |_| Ok(true))
                .unwrap(),
            (0, false)
        );
        assert!(
            index
                .insert_unique(0, 1, &cancellation, |_| Err(failed()))
                .is_err()
        );
        assert_eq!(index.unique_rows(), 1);
        assert_eq!(index.links.values.len(), 1);
        drop(hold);
        let cancelled = CancellationToken::default();
        assert!(
            index
                .insert_unique(0, 1, &cancelled, |_| {
                    cancelled.cancel();
                    Ok(false)
                })
                .is_err()
        );
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), [0]);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        let memory = LiveMemoryPool::new(64).unwrap();
        let mut denied = RowIndex::new(&memory).unwrap();
        assert!(
            denied
                .insert_unique(0, 0, &cancellation, |_| Ok(false))
                .is_err()
        );
        assert_eq!(denied.unique_rows(), 0);
        assert!(denied.links.values.is_empty());
        drop(denied);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn complete_hash_collisions_keep_distinct_keys_and_every_duplicate_in_source_order() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        {
            let mut index = RowIndex::new(&memory).unwrap();
            let keys = ["a", "b", "a", "c", "a", "b", "", "東京\0"];
            for (row, value) in keys.iter().enumerate() {
                index
                    .insert(7, row, &cancellation, |old| Ok(keys[old] == *value))
                    .unwrap();
            }
            for (key, expected) in [
                ("a", vec![0, 2, 4]),
                ("b", vec![1, 5]),
                ("c", vec![3]),
                ("", vec![6]),
                ("東京\0", vec![7]),
            ] {
                let entry = index
                    .find(7, &cancellation, |row| Ok(keys[row] == key))
                    .unwrap()
                    .unwrap();
                assert_eq!(index.rows(entry).unwrap().collect::<Vec<_>>(), expected);
            }
            assert_eq!(index.unique_rows(), 5);
            assert_eq!(index.find(7, &cancellation, |_| Ok(false)).unwrap(), None);
            assert_eq!(index.find(8, &cancellation, |_| Ok(true)).unwrap(), None);
            assert!(index.rows(5).is_err());
            assert!(memory.snapshot().reserved_bytes > 0);
        }
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn growth_keeps_all_collision_chains_and_denial_does_not_publish_a_partial_row() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..1024 {
            index
                .insert(0, row, &cancellation, |old| Ok(old % 257 == row % 257))
                .unwrap();
        }
        for key in 0..257 {
            let entry = index
                .find(0, &cancellation, |row| Ok(row % 257 == key))
                .unwrap()
                .unwrap();
            assert_eq!(
                index.rows(entry).unwrap().collect::<Vec<_>>(),
                (key..1024).step_by(257).collect::<Vec<_>>()
            );
        }
        assert_eq!(index.unique_rows(), 257);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        let memory = LiveMemoryPool::new(64).unwrap();
        let mut denied = RowIndex::new(&memory).unwrap();
        assert!(denied.insert(0, 0, &cancellation, |_| Ok(false)).is_err());
        assert_eq!(denied.unique_rows(), 0);
        assert_eq!(denied.find(0, &cancellation, |_| Ok(true)).unwrap(), None);
        drop(denied);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn cancellation_interrupts_collision_search_without_publishing_a_row() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let active = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..2048 {
            index.insert(0, row, &active, |_| Ok(false)).unwrap();
        }
        let cancelled = CancellationToken::default();
        let mut compared = 0usize;
        assert!(
            index
                .insert(0, 2048, &cancelled, |_| {
                    compared += 1;
                    cancelled.cancel();
                    Ok(false)
                })
                .is_err()
        );
        assert!(compared <= 1024);
        assert_eq!(index.unique_rows(), 2048);
        assert_eq!(index.links.values.len(), 2048);
        assert!(index.reserve_entry(&cancelled).is_err());
        assert!(index.find(0, &cancelled, |_| Ok(true)).is_err());
        for row in [0, 1024, 2047] {
            let entry = index
                .find(0, &active, |old| Ok(old == row))
                .unwrap()
                .unwrap();
            assert_eq!(index.rows(entry).unwrap().collect::<Vec<_>>(), [row]);
        }
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
