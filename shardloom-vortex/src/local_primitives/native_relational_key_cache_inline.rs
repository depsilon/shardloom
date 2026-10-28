//! Exact identities for repeated native inline keys, confined to one batch.
//! Sampling admits a cache; only the caller's exact row index admits a group.

use super::{EMPTY, KeyColumn, Probe, ReservedVec, Result, bytes, failed};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::hash::Hasher as _;
use vortex::array::arrays::bool::BoolArrayExt as _;

#[cfg(test)]
#[path = "native_relational_key_cache_inline_tests.rs"]
mod tests;

const SLOTS: usize = 512;
const MIN_ROWS: usize = 512;
const SAMPLE_ROWS: usize = 256;
const MIN_SAMPLE_HITS: usize = 32;

#[derive(Clone, Copy)]
struct Entry {
    exact: [u128; 2],
    group: usize,
}

const VACANT: Entry = Entry {
    exact: [0; 2],
    group: EMPTY,
};

pub(in crate::local_primitives) struct Cache<'a> {
    keys: &'a [KeyColumn],
    entries: ReservedVec<Entry>,
}

impl<'a> Cache<'a> {
    pub(super) fn new(
        keys: &'a [KeyColumn],
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>> {
        cancellation.check()?;
        if !(1..=2).contains(&keys.len()) || keys[0].len() < MIN_ROWS || !keys.iter().all(eligible)
        {
            return Ok(None);
        }
        if keys.iter().any(|key| key.len() != keys[0].len()) {
            return Err(failed("inline key cache domains changed row count"));
        }
        let required = bytes::<Entry>(SLOTS)?;
        let snapshot = memory.snapshot();
        if required > snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) / 16 {
            return Ok(None);
        }
        let mut entries = ReservedVec::new(memory)?;
        entries.reserve(SLOTS)?;
        entries.values.resize(SLOTS, VACANT);
        let mut cache = Self { keys, entries };
        let mut hits = 0;
        for row in 0..SAMPLE_ROWS {
            if row.is_multiple_of(64) {
                cancellation.check()?;
            }
            if let Some(exact) = cache.identity(row)? {
                let slot = bucket(exact);
                let entry = &mut cache.entries.values[slot];
                if entry.group != EMPTY && entry.exact == exact {
                    hits += 1;
                }
                *entry = Entry { exact, group: 0 };
            }
        }
        cancellation.check()?;
        if hits < MIN_SAMPLE_HITS {
            return Ok(None);
        }
        // Sample entries have never reached RowIndex. None may serve a probe.
        cache.entries.values.fill(VACANT);
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.key_cache_batches += 1;
            counts.key_cache_slots += SLOTS as u64;
            counts.key_inline_cache_batches += 1;
        });
        Ok(Some(cache))
    }

    fn identity(&self, row: usize) -> Result<Option<[u128; 2]>> {
        let mut exact = [0; 2];
        for (part, key) in exact.iter_mut().zip(self.keys) {
            let Some(value) = identity(key, row)? else {
                return Ok(None);
            };
            *part = value;
        }
        Ok(Some(exact))
    }

    pub(super) fn probe(&self, row: usize) -> Result<Option<Probe>> {
        let Some(exact) = self.identity(row)? else {
            return Ok(None);
        };
        let slot = bucket(exact);
        let entry = &self.entries.values[slot];
        let group = (entry.group != EMPTY && entry.exact == exact).then_some(entry.group);
        #[cfg(all(test, debug_assertions))]
        if group.is_some() {
            crate::local_primitives::native_relational_observation::record(|counts| {
                counts.key_inline_cache_hits += 1;
            });
        }
        Ok(Some(Probe {
            slot,
            exact: Some(exact),
            group,
        }))
    }

    pub(super) fn insert(&mut self, probe: Probe, group: usize) {
        self.entries.values[probe.slot] = Entry {
            exact: probe
                .exact
                .expect("inline cache probe retains its exact key"),
            group,
        };
    }
}

fn eligible(key: &KeyColumn) -> bool {
    match key {
        KeyColumn::Null(_) | KeyColumn::Boolean { .. } => true,
        KeyColumn::Numeric(values) => values.is_integer(),
        // A dictionary code domain is not a raw value identity. Its existing
        // single-column directory remains authoritative; composite dictionaries
        // retain exact ordinary lookup, including late invalid-code failures.
        KeyColumn::Variable(values) => values.codes.is_none(),
        _ => false,
    }
}

fn identity(key: &KeyColumn, row: usize) -> Result<Option<u128>> {
    if row >= key.len() {
        return Err(failed("inline key cache row exceeds its native owner"));
    }
    Ok(Some(match key {
        KeyColumn::Null(_) => 0,
        KeyColumn::Numeric(values) => values
            .integer_ordinal(row)?
            .map_or(1_u128 << 64, i128::cast_unsigned),
        KeyColumn::Boolean { values, valid } => {
            if valid.value(row) {
                u128::from(values.bit_buffer_view().value(row))
            } else {
                2
            }
        }
        KeyColumn::Variable(values) => {
            if !values.valid.value(row) {
                // The lower 32 bits of an inline view are its length (0..=12).
                // This marker cannot alias empty or embedded-NUL text/binary.
                return Ok(Some(1_u128 << 31));
            }
            let view = &values.values.views()[row];
            if !view.is_inlined() {
                return Ok(None);
            }
            // Unused inline padding is not logical data. Native sources may
            // contain nonzero padding, so mask it before proving identity.
            let bits = 32 + view.len() * 8;
            let mask = if bits == 128 {
                u128::MAX
            } else {
                (1_u128 << bits) - 1
            };
            view.as_u128() & mask
        }
        _ => return Err(failed("inline key cache encountered an unbound domain")),
    }))
}

fn bucket(exact: [u128; 2]) -> usize {
    let mut hash = rustc_hash::FxHasher::default();
    hash.write_u128(exact[0]);
    hash.write_u128(exact[1]);
    usize::try_from(hash.finish() & (SLOTS as u64 - 1)).expect("bounded cache slot")
}
