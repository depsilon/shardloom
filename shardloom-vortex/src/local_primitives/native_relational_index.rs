//! Exact native row membership with an inline first row and ordered duplicates.
//! Hashes select candidates; the caller's native key owners prove equality.

use super::{compound_count_partial, native_capacity::ReservedVec};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

const EMPTY: usize = usize::MAX;
const EMPTY_CONTROL: u8 = 0x80;
const GROUP_BYTES: usize = 8;
const HIGH_BITS: u64 = 0x8080_8080_8080_8080;

struct Entry {
    hash: u64,
    first: usize,
    duplicate_tail: usize,
}

struct Link {
    row: usize,
    next: usize,
}

pub(super) struct RowIndex {
    buckets: ReservedVec<usize>,
    controls: Option<ReservedVec<u8>>,
    entries: ReservedVec<Entry>,
    links: ReservedVec<Link>,
    memory: LiveMemoryPool,
}

impl RowIndex {
    pub(super) fn new(memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            buckets: ReservedVec::new(memory)?,
            controls: None,
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
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.row_index_lookups += 1;
            counts.row_index_large_lookups += u64::from(self.buckets.values.len() >= 128);
        });
        if self.buckets.values.is_empty() {
            return Ok(None);
        }
        if let Some(controls) = &self.controls {
            return self.find_with_controls(hash, &controls.values, cancellation, equal);
        }
        let mut bucket = compound_count_partial::bucket(hash, self.buckets.values.len());
        let mut visited = 0usize;
        loop {
            if visited.is_multiple_of(1024) {
                cancellation.check()?;
            }
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.row_index_probe_slots += 1;
                counts.row_index_bucket_reads += 1;
            });
            let position = self.buckets.values[bucket];
            if position == EMPTY {
                cancellation.check()?;
                return Ok(None);
            }
            let entry = &self.entries.values[position];
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.row_index_entry_reads += 1;
                counts.row_index_fingerprint_candidates +=
                    u64::from(entry.hash >> 57 == hash >> 57);
                if self.buckets.values.len() >= 128 {
                    counts.row_index_large_entry_reads += 1;
                    counts.row_index_large_fingerprint_candidates +=
                        u64::from(entry.hash >> 57 == hash >> 57);
                }
                counts.row_index_equality_calls += u64::from(entry.hash == hash);
            });
            if entry.hash == hash && equal(entry.first)? {
                cancellation.check()?;
                return Ok(Some(position));
            }
            bucket = (bucket + 1) & (self.buckets.values.len() - 1);
            visited += 1;
        }
    }

    fn find_with_controls(
        &self,
        hash: u64,
        controls: &[u8],
        cancellation: &CancellationToken,
        mut equal: impl FnMut(usize) -> Result<bool>,
    ) -> Result<Option<usize>> {
        let mask = self.buckets.values.len() - 1;
        let fingerprint = fingerprint(hash);
        let mut bucket = compound_count_partial::bucket(hash, self.buckets.values.len());
        let mut visited = 0usize;
        loop {
            if visited.is_multiple_of(1024) {
                cancellation.check()?;
            }
            let (mut matches, empty) = probe_group(controls, bucket, fingerprint);
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.row_index_metadata_groups += 1;
            });
            #[cfg(all(test, debug_assertions))]
            let mut consumed = 0usize;
            while matches != 0 {
                let lane = matches.trailing_zeros() as usize / 8;
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.row_index_probe_slots += (lane + 1 - consumed) as u64;
                    counts.row_index_bucket_reads += 1;
                    consumed = lane + 1;
                });
                let position = self.buckets.values[(bucket + lane) & mask];
                let entry = &self.entries.values[position];
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.row_index_entry_reads += 1;
                    counts.row_index_large_entry_reads += 1;
                    counts.row_index_fingerprint_candidates += 1;
                    counts.row_index_large_fingerprint_candidates += 1;
                    counts.row_index_equality_calls += u64::from(entry.hash == hash);
                });
                if entry.hash == hash && equal(entry.first)? {
                    cancellation.check()?;
                    return Ok(Some(position));
                }
                matches &= matches - 1;
            }
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                // A word load can inspect beyond the logical stopping point.
                // Count only through the first empty slot, or this full group.
                counts.row_index_probe_slots += ((empty + 1).min(GROUP_BYTES) - consumed) as u64;
            });
            if empty < GROUP_BYTES {
                cancellation.check()?;
                return Ok(None);
            }
            bucket = (bucket + GROUP_BYTES) & mask;
            visited += GROUP_BYTES;
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
        if existing.is_some() {
            self.links.reserve_one()?;
        } else {
            self.entries.reserve_one()?;
            self.reserve_entry(cancellation)?;
        }
        // Resolve every fallible operation before publishing a row or link.
        let vacant = existing
            .is_none()
            .then(|| vacant_bucket(&self.buckets.values, hash, cancellation))
            .transpose()?;
        cancellation.check()?;
        if let Some(position) = existing {
            let entry = &mut self.entries.values[position];
            let link = self.links.values.len();
            let tail = entry.duplicate_tail;
            // A circular duplicate list keeps its head in the tail link. The
            // inline first row therefore needs no link, without growing Entry.
            let next = if tail == EMPTY {
                link
            } else {
                self.links.values[tail].next
            };
            self.links.values.push(Link { row, next });
            if tail != EMPTY {
                self.links.values[tail].next = link;
            }
            entry.duplicate_tail = link;
            return Ok((position, false));
        }
        let position = self.entries.values.len();
        self.entries.values.push(Entry {
            hash,
            first: row,
            duplicate_tail: EMPTY,
        });
        let bucket = vacant.expect("new entry has a vacant bucket");
        self.buckets.values[bucket] = position;
        if let Some(controls) = &mut self.controls {
            publish_control(&mut controls.values, bucket, hash);
        }
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
        // Old buckets and controls remain charged while replacements are built.
        let mut next = ReservedVec::new(&self.memory)?;
        next.reserve(capacity)?;
        next.values.resize(capacity, EMPTY);
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.row_index_bucket_initialization_bytes += (capacity * size_of::<usize>()) as u64;
        });
        let mut next_controls = optional_controls(&self.memory, capacity)?;
        for (position, entry) in self.entries.values.iter().enumerate() {
            let bucket = vacant_bucket(&next.values, entry.hash, cancellation)?;
            next.values[bucket] = position;
            if let Some(controls) = &mut next_controls {
                publish_control(&mut controls.values, bucket, entry.hash);
            }
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.row_index_rehash_entries += 1;
            });
        }
        cancellation.check()?;
        self.buckets = next;
        self.controls = next_controls;
        Ok(())
    }

    pub(super) fn rows(&self, entry: usize) -> Result<Rows<'_>> {
        let entry = self.entries.values.get(entry).ok_or_else(failed)?;
        let tail = entry.duplicate_tail;
        Ok(Rows {
            index: self,
            first: Some(entry.first),
            next: if tail == EMPTY {
                EMPTY
            } else {
                self.links.values[tail].next
            },
            tail,
        })
    }

    pub(super) fn unique_rows(&self) -> usize {
        self.entries.values.len()
    }
}

fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8
}

fn optional_controls(memory: &LiveMemoryPool, capacity: usize) -> Result<Option<ReservedVec<u8>>> {
    if capacity < 128 {
        return Ok(None);
    }
    let bytes = capacity.checked_add(GROUP_BYTES - 1).ok_or_else(failed)?;
    let snapshot = memory.snapshot();
    let available = snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes);
    if u64::try_from(bytes).map_err(|_| failed())? > available / 16 {
        return Ok(None);
    }
    // A fresh container reserves exactly capacity + 7 bytes. Once admitted,
    // allocation errors propagate; they are not optional-layout decisions.
    let mut controls = ReservedVec::new(memory)?;
    controls.reserve(bytes)?;
    controls.values.resize(bytes, EMPTY_CONTROL);
    #[cfg(all(test, debug_assertions))]
    super::native_relational_observation::record(|counts| {
        counts.row_index_metadata_initialization_bytes += bytes as u64;
    });
    Ok(Some(controls))
}

fn publish_control(controls: &mut [u8], bucket: usize, hash: u64) {
    controls[bucket] = fingerprint(hash);
    if bucket < GROUP_BYTES - 1 {
        controls[controls.len() - (GROUP_BYTES - 1) + bucket] = fingerprint(hash);
    }
}

fn matching_controls(word: u64, fingerprint: u8) -> u64 {
    let different = word ^ (u64::from(fingerprint) * 0x0101_0101_0101_0101);
    // Each masked byte plus 127 is at most 254: no inter-byte carry can create
    // a false match. A high bit remains clear exactly for a zero difference.
    !(different | ((different & 0x7f7f_7f7f_7f7f_7f7f) + 0x7f7f_7f7f_7f7f_7f7f)) & HIGH_BITS
}

fn probe_group(controls: &[u8], bucket: usize, fingerprint: u8) -> (u64, usize) {
    // Seven mirrored controls preserve linear visitation for every wraparound
    // start. Little-endian conversion makes lane order independent of the host.
    let word = u64::from_le_bytes(
        controls[bucket..bucket + GROUP_BYTES]
            .try_into()
            .expect("probe group and mirrored tail are present"),
    );
    let empty = (word & HIGH_BITS).trailing_zeros() as usize / 8;
    let mut matches = matching_controls(word, fingerprint);
    if empty < GROUP_BYTES {
        matches &= (1u64 << (8 * empty)) - 1;
    }
    (matches, empty)
}

pub(super) struct Rows<'a> {
    index: &'a RowIndex,
    first: Option<usize>,
    next: usize,
    tail: usize,
}

impl Iterator for Rows<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        if self.next == EMPTY {
            return None;
        }
        let link = &self.index.links.values[self.next];
        self.next = if self.next == self.tail {
            EMPTY
        } else {
            link.next
        };
        Some(link.row)
    }
}

fn vacant_bucket(buckets: &[usize], hash: u64, cancellation: &CancellationToken) -> Result<usize> {
    let mut bucket = compound_count_partial::bucket(hash, buckets.len());
    let mut visited = 0usize;
    while buckets[bucket] != EMPTY {
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| counts.row_index_vacancy_reads += 1);
        if visited.is_multiple_of(1024) {
            cancellation.check()?;
        }
        bucket = (bucket + 1) & (buckets.len() - 1);
        visited += 1;
    }
    #[cfg(all(test, debug_assertions))]
    super::native_relational_observation::record(|counts| counts.row_index_vacancy_reads += 1);
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
#[path = "native_relational_index_metadata_tests.rs"]
mod metadata_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(debug_assertions)]
    fn probe_observations_distinguish_entry_reads_fingerprints_and_exact_equality() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        // Both initial buckets are zero, but their high-bit fingerprints differ.
        index.insert(0, 10, &cancellation, |_| Ok(false)).unwrap();
        index
            .insert((1 << 57) | 16, 20, &cancellation, |_| Ok(false))
            .unwrap();
        let session = super::super::native_relational_observation::Session::start();
        assert_eq!(
            index
                .find(32, &cancellation, |_| panic!("full hash differs"))
                .unwrap(),
            None
        );
        let counts = session.finish();
        assert_eq!(counts.row_index_lookups, 1);
        assert_eq!(counts.row_index_probe_slots, 3);
        assert_eq!(counts.row_index_bucket_reads, 3);
        assert_eq!(counts.row_index_entry_reads, 2);
        assert_eq!(counts.row_index_fingerprint_candidates, 1);
        assert_eq!(counts.row_index_equality_calls, 0);
        let session = super::super::native_relational_observation::Session::start();
        assert_eq!(
            index.find(0, &cancellation, |row| Ok(row == 10)).unwrap(),
            Some(0)
        );
        let counts = session.finish();
        assert_eq!(counts.row_index_lookups, 1);
        assert_eq!(counts.row_index_probe_slots, 1);
        assert_eq!(counts.row_index_bucket_reads, 1);
        assert_eq!(counts.row_index_entry_reads, 1);
        assert_eq!(counts.row_index_fingerprint_candidates, 1);
        assert_eq!(counts.row_index_equality_calls, 1);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn unique_rows_fit_entry_and_bucket_grant_without_duplicate_storage() {
        // Eight entries and sixteen buckets: 320 bytes on a 64-bit target.
        // The old per-row link allocation cannot fit this grant.
        let grant = u64::try_from(8 * size_of::<Entry>() + 16 * size_of::<usize>()).unwrap();
        let memory = LiveMemoryPool::new(grant).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..8 {
            index.insert(0, row, &cancellation, |_| Ok(false)).unwrap();
        }
        for row in 0..8 {
            let entry = index
                .find(0, &cancellation, |old| Ok(old == row))
                .unwrap()
                .unwrap();
            assert_eq!(index.rows(entry).unwrap().collect::<Vec<_>>(), [row]);
        }
        assert!(index.links.values.is_empty());
        assert_eq!(index.links.values.capacity(), 0);
        assert_eq!(memory.snapshot().reserved_bytes, grant);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn first_duplicate_and_duplicate_growth_denials_preserve_rows_and_allow_retry() {
        let memory = LiveMemoryPool::new(4096).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        index
            .insert(7, usize::MAX, &cancellation, |_| Ok(false))
            .unwrap();
        let hold = memory
            .reserve(4096 - memory.snapshot().reserved_bytes)
            .unwrap();
        assert!(
            index
                .insert(7, 0, &cancellation, |old| Ok(old == usize::MAX))
                .is_err()
        );
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), [usize::MAX]);
        assert!(index.links.values.is_empty());
        drop(hold);
        for row in 0..8 {
            index
                .insert(7, row, &cancellation, |old| Ok(old == usize::MAX))
                .unwrap();
        }
        let expected: Vec<_> = std::iter::once(usize::MAX).chain(0..8).collect();
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), expected);
        let hold = memory
            .reserve(4096 - memory.snapshot().reserved_bytes)
            .unwrap();
        assert!(index.insert(7, 8, &cancellation, |_| Ok(true)).is_err());
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), expected);
        assert_eq!(index.links.values.len(), 8);
        drop(hold);
        let cancelled = CancellationToken::default();
        assert!(
            index
                .insert(7, 8, &cancelled, |_| {
                    cancelled.cancel();
                    Ok(true)
                })
                .is_err()
        );
        assert!(
            index
                .insert(7, 8, &cancellation, |_| Err(failed()))
                .is_err()
        );
        assert_eq!(index.rows(0).unwrap().collect::<Vec<_>>(), expected);
        index.insert(7, 8, &cancellation, |_| Ok(true)).unwrap();
        assert_eq!(
            index.rows(0).unwrap().collect::<Vec<_>>(),
            std::iter::once(usize::MAX).chain(0..9).collect::<Vec<_>>()
        );
        assert_eq!(index.unique_rows(), 1);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

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
        assert!(index.links.values.is_empty());
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
        assert!(index.links.values.is_empty());
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
    fn populated_bucket_growth_denial_keeps_entry_ids_and_duplicate_chains() {
        const GRANT: u64 = 1 << 20;
        let memory = LiveMemoryPool::new(GRANT).unwrap();
        let cancellation = CancellationToken::default();
        let mut index = RowIndex::new(&memory).unwrap();
        let positions: Vec<_> = (0..64)
            .map(|row| index.insert(7, row, &cancellation, |_| Ok(false)).unwrap())
            .collect();
        for row in [1003, 2003] {
            assert_eq!(
                index
                    .insert(7, row, &cancellation, |old| Ok(old == 3))
                    .unwrap(),
                positions[3]
            );
        }
        assert_eq!(index.buckets.values.len(), 128);
        // Admit entry capacity first, so the denied operation reaches replacement
        // bucket admission with a populated table and an existing duplicate chain.
        index.entries.reserve_one().unwrap();
        let before = index.buckets.values.clone();
        let reserved = memory.snapshot().reserved_bytes;
        let hold = memory.reserve(GRANT - reserved).unwrap();
        assert!(index.insert(7, 64, &cancellation, |_| Ok(false)).is_err());
        assert_eq!(memory.snapshot().reserved_bytes, GRANT);
        assert_eq!(index.buckets.values, before);
        assert_eq!(index.unique_rows(), 64);
        let check_existing = |index: &RowIndex| {
            for (row, position) in positions.iter().copied().enumerate() {
                assert_eq!(
                    index.find(7, &cancellation, |old| Ok(old == row)).unwrap(),
                    Some(position)
                );
                let expected = if row == 3 {
                    vec![3, 1003, 2003]
                } else {
                    vec![row]
                };
                assert_eq!(index.rows(position).unwrap().collect::<Vec<_>>(), expected);
            }
        };
        check_existing(&index);
        drop(hold);
        assert_eq!(memory.snapshot().reserved_bytes, reserved);
        assert_eq!(
            index.insert(7, 64, &cancellation, |_| Ok(false)).unwrap(),
            64
        );
        assert_eq!(index.buckets.values.len(), 256);
        assert_eq!(index.unique_rows(), 65);
        check_existing(&index);
        assert_eq!(index.rows(64).unwrap().collect::<Vec<_>>(), [64]);
        drop(index);
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
        assert!(index.links.values.is_empty());
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
