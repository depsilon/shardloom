//! Optional adaptation of an immutable, fully validated native join build.

use super::super::native_capacity::{ReservedVec, bytes};
use super::{EMPTY, RowIndex, failed};
use shardloom_core::{Result, ShardLoomError, native_join_work::NativeJoinWork};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::cell::Cell;

const FILTER_MIN_KEYS: usize = 128;
const HOT_MIN_ROWS: usize = 64;

#[derive(Default)]
pub(super) struct State {
    sealed: bool,
    filter: Option<ReservedVec<u64>>,
    hot: Option<Hot>,
    work: Cell<NativeJoinWork>,
    overflow: Cell<bool>,
}

struct Range {
    entry: usize,
    start: usize,
    end: usize,
}

struct Hot {
    ranges: ReservedVec<Range>,
    rows: ReservedVec<usize>,
}

impl RowIndex {
    pub(in super::super) fn ensure_building(&self) -> Result<()> {
        if self.adaptive.sealed {
            return Err(ShardLoomError::InvalidOperation(
                "native join build is sealed; no further rows may be inserted; no fallback execution was attempted".into(),
            ));
        }
        Ok(())
    }

    /// Only completed join owners call this. Mutable grouping/window/set
    /// owners never acquire a filter whose membership they can invalidate.
    pub(in super::super) fn seal_join(
        &mut self,
        duplicates: bool,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        cancellation.check()?;
        if self.adaptive.sealed {
            return Ok(());
        }
        let mut state = State::default();
        let mut work = NativeJoinWork::default();
        work.runtime_filter.considered = 1;
        work.runtime_filter.build_keys = self.unique_rows() as u64;
        if self.unique_rows() < FILTER_MIN_KEYS {
            work.runtime_filter.declined_small = 1;
        } else {
            let words = self
                .unique_rows()
                .checked_mul(16)
                .and_then(|bits| bits.checked_add(63))
                .map(|bits| bits / 64)
                .and_then(usize::checked_next_power_of_two)
                .ok_or_else(failed)?;
            let bytes = bytes::<u64>(words)?;
            if has_headroom(&self.memory, bytes) {
                state.filter = ReservedVec::try_with_capacity(&self.memory, words)?;
            }
            if let Some(filter) = &mut state.filter {
                filter.values.resize(words, 0);
                for (row, entry) in self.entries.values.iter().enumerate() {
                    if row.is_multiple_of(1024) {
                        cancellation.check()?;
                    }
                    for (word, mask) in positions(entry.hash, words) {
                        filter.values[word] |= mask;
                    }
                }
                work.runtime_filter.admitted = 1;
                work.runtime_filter.retained_bytes = bytes;
            } else {
                work.runtime_filter.declined_memory = 1;
            }
        }
        if duplicates {
            work.skew.considered = 1;
            let mut groups = 0usize;
            let mut rows = 0usize;
            for entry in 0..self.entries.values.len() {
                cancellation.check()?;
                let len = self.group_len(entry, cancellation)?;
                if len >= HOT_MIN_ROWS {
                    groups = groups.checked_add(1).ok_or_else(failed)?;
                    rows = rows.checked_add(len).ok_or_else(failed)?;
                }
            }
            work.skew.hot_groups = groups as u64;
            work.skew.hot_rows = rows as u64;
            if groups != 0 {
                let capacity = bytes::<Range>(groups)?
                    .checked_add(bytes::<usize>(rows)?)
                    .ok_or_else(failed)?;
                if has_headroom(&self.memory, capacity) {
                    state.hot = self.prepare_hot(groups, rows, cancellation)?;
                }
                if state.hot.is_some() {
                    work.skew.admitted_groups = groups as u64;
                    work.skew.admitted_rows = rows as u64;
                } else {
                    work.skew.declined_memory = 1;
                }
            }
        }
        cancellation.check()?;
        state.work.set(work);
        state.sealed = true;
        self.adaptive = state;
        Ok(())
    }

    fn group_len(&self, position: usize, cancellation: &CancellationToken) -> Result<usize> {
        let entry = &self.entries.values[position];
        let mut len = 1usize;
        let tail = entry.duplicate_tail;
        if tail != EMPTY {
            let mut link = self.links.values[tail].next;
            loop {
                if len.is_multiple_of(1024) {
                    cancellation.check()?;
                }
                len = len.checked_add(1).ok_or_else(failed)?;
                if link == tail {
                    break;
                }
                link = self.links.values[link].next;
            }
        }
        Ok(len)
    }

    fn prepare_hot(
        &self,
        groups: usize,
        rows: usize,
        cancellation: &CancellationToken,
    ) -> Result<Option<Hot>> {
        let Some(mut ranges) = ReservedVec::try_with_capacity(&self.memory, groups)? else {
            return Ok(None);
        };
        let Some(mut ordinals) = ReservedVec::try_with_capacity(&self.memory, rows)? else {
            return Ok(None);
        };
        for entry in 0..self.entries.values.len() {
            cancellation.check()?;
            if self.group_len(entry, cancellation)? < HOT_MIN_ROWS {
                continue;
            }
            let start = ordinals.values.len();
            // No adaptive ranges are published until the entire build succeeds.
            for (offset, row) in self.rows(entry)?.enumerate() {
                if offset.is_multiple_of(1024) {
                    cancellation.check()?;
                }
                ordinals.values.push(row);
            }
            ranges.values.push(Range {
                entry,
                start,
                end: ordinals.values.len(),
            });
        }
        if ranges.values.len() != groups || ordinals.values.len() != rows {
            return Err(failed());
        }
        Ok(Some(Hot {
            ranges,
            rows: ordinals,
        }))
    }

    pub(in super::super) fn join_work(&self) -> Result<NativeJoinWork> {
        if self.adaptive.overflow.get() {
            return Err(counter_overflow());
        }
        Ok(self.adaptive.work.get())
    }
}

impl State {
    pub(super) fn might_contain(&self, hash: u64) -> Result<bool> {
        let Some(filter) = &self.filter else {
            return Ok(true);
        };
        let possible = positions(hash, filter.values.len())
            .into_iter()
            .all(|(word, mask)| filter.values[word] & mask != 0);
        let mut work = self.work.get();
        increment(&mut work.runtime_filter.probe_rows)?;
        if !possible {
            increment(&mut work.runtime_filter.rejected_rows)?;
            increment(&mut work.runtime_filter.index_lookups_avoided)?;
        }
        self.work.set(work);
        Ok(possible)
    }

    pub(super) fn hot_rows(&self, entry: usize) -> Result<Option<&[usize]>> {
        let Some(hot) = &self.hot else {
            return Ok(None);
        };
        let Ok(position) = hot
            .ranges
            .values
            .binary_search_by_key(&entry, |range| range.entry)
        else {
            return Ok(None);
        };
        let range = &hot.ranges.values[position];
        let mut work = self.work.get();
        increment(&mut work.skew.range_visits)?;
        self.work.set(work);
        Ok(Some(&hot.rows.values[range.start..range.end]))
    }

    pub(super) fn record_hot_row(&self) {
        let mut work = self.work.get();
        if increment(&mut work.skew.rows_visited).is_err() {
            // Iterator item compatibility is preserved. The completed join
            // refuses its certificate if a counter cannot be represented.
            self.overflow.set(true);
        } else {
            self.work.set(work);
        }
    }
}

fn has_headroom(memory: &LiveMemoryPool, bytes: u64) -> bool {
    let snapshot = memory.snapshot();
    bytes <= snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) / 16
}

fn increment(value: &mut u64) -> Result<()> {
    *value = value.checked_add(1).ok_or_else(counter_overflow)?;
    Ok(())
}

fn counter_overflow() -> ShardLoomError {
    ShardLoomError::InvalidOperation("native join work counter overflow".into())
}

fn positions(hash: u64, words: usize) -> [(usize, u64); 3] {
    // Three distinct rotations of the already mixed native equality hash.
    // Every build and probe uses the identical bit mapping; positives never
    // replace exact key equality, including deliberately colliding hashes.
    [hash, hash.rotate_left(21), hash.rotate_left(42)]
        .map(|hash| ((hash >> 6) as usize & (words - 1), 1u64 << (hash & 63)))
}

#[cfg(test)]
#[path = "native_relational_index_adaptive_tests.rs"]
mod tests;
