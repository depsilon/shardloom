//! Bounded per-batch reuse of identities proved by the ordinary exact index.

use super::{KeyColumn, NativeNumericOwner, VariableColumn, failed};
use crate::local_primitives::native_capacity::{ReservedVec, bytes};
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};

const EMPTY: usize = usize::MAX;
const MAX_SLOTS: usize = 16_384;

#[path = "native_relational_key_cache_inline.rs"]
mod inline;

/// Both cache forms publish only identities proved by the ordinary row index.
pub(in crate::local_primitives) enum KeyLookupCache<'a> {
    Directory(Directory<'a>),
    Inline(inline::Cache<'a>),
}

#[derive(Clone, Copy)]
pub(in crate::local_primitives) struct Probe {
    slot: usize,
    exact: Option<[u128; 2]>,
    group: Option<usize>,
}

impl Probe {
    pub(in crate::local_primitives) fn group(self) -> Option<usize> {
        self.group
    }
}

impl<'a> KeyLookupCache<'a> {
    pub(in crate::local_primitives) fn new(
        key: &'a KeyColumn,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>> {
        Ok(Directory::new(key, memory, cancellation)?.map(Self::Directory))
    }

    pub(in crate::local_primitives) fn for_columns(
        keys: &'a [KeyColumn],
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>> {
        if let [key] = keys
            && (matches!(key, KeyColumn::Numeric(_))
                || matches!(key, KeyColumn::Variable(values) if values.codes.is_some()))
        {
            // Preserve the established single numeric/dictionary admission.
            return Self::new(key, memory, cancellation);
        }
        Ok(inline::Cache::new(keys, memory, cancellation)?.map(Self::Inline))
    }

    pub(in crate::local_primitives) fn probe(&self, row: usize) -> Result<Option<Probe>> {
        match self {
            Self::Directory(directory) => {
                let slot = directory.slot(row)?;
                Ok(Some(Probe {
                    slot,
                    exact: None,
                    group: directory.get(slot),
                }))
            }
            Self::Inline(cache) => cache.probe(row),
        }
    }

    pub(in crate::local_primitives) fn insert(&mut self, probe: Probe, group: usize) {
        match self {
            Self::Directory(directory) => directory.insert(probe.slot, group),
            Self::Inline(cache) => cache.insert(probe, group),
        }
    }
}

enum Domain<'a> {
    Integer {
        values: &'a NativeNumericOwner,
        min: i128,
    },
    Dictionary(&'a VariableColumn),
}

/// This directory never supplies an unproved identity. The caller resolves a
/// slot's first visit through exact hashing/equality and only then publishes it.
/// Its borrow confines codes and integer offsets to this batch's value domain.
pub(in crate::local_primitives) struct Directory<'a> {
    domain: Domain<'a>,
    groups: ReservedVec<usize>,
}

impl<'a> Directory<'a> {
    pub(in crate::local_primitives) fn new(
        key: &'a KeyColumn,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>> {
        cancellation.check()?;
        if key.len() < 64 {
            return Ok(None);
        }
        // Slot zero represents NULL, including nullable dictionary values.
        let max_slots = MAX_SLOTS.min(key.len() / 4);
        let (domain, slots) = match key {
            KeyColumn::Numeric(values) if values.is_integer() => {
                let Some((min, span)) =
                    values.bounded_integer_range(max_slots - 1, cancellation)?
                else {
                    return Ok(None);
                };
                (Domain::Integer { values, min }, span + 1)
            }
            KeyColumn::Variable(values) if values.codes.is_some() => {
                let Some(slots) = values.values.len().checked_add(1) else {
                    return Ok(None);
                };
                if slots > max_slots {
                    return Ok(None);
                }
                (Domain::Dictionary(values), slots)
            }
            _ => return Ok(None),
        };
        // ReservedVec's initial allocation has at least eight slots. Keep the
        // optional directory small relative to remaining operator/output room.
        let required = bytes::<usize>(slots.max(8))?;
        let snapshot = memory.snapshot();
        if required > snapshot.limit_bytes.saturating_sub(snapshot.reserved_bytes) / 16 {
            return Ok(None);
        }
        let mut groups = ReservedVec::new(memory)?;
        groups.reserve(slots)?;
        groups.values.resize(slots, EMPTY);
        cancellation.check()?;
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.key_cache_batches += 1;
            counts.key_cache_slots += slots as u64;
        });
        Ok(Some(Self { domain, groups }))
    }

    pub(in crate::local_primitives) fn slot(&self, row: usize) -> Result<usize> {
        let slot = match &self.domain {
            Domain::Integer { values, min } => values
                .integer_ordinal(row)?
                .map(|value| usize::try_from(value - min + 1))
                .transpose()
                .map_err(|_| failed("integer key directory offset exceeds its domain"))?
                .unwrap_or(0),
            Domain::Dictionary(values) => values.index(row)?.map_or(0, |index| index + 1),
        };
        if slot >= self.groups.values.len() {
            return Err(failed("key directory slot exceeds its admitted domain"));
        }
        Ok(slot)
    }

    pub(in crate::local_primitives) fn get(&self, slot: usize) -> Option<usize> {
        let group = self.groups.values[slot];
        (group != EMPTY).then_some(group)
    }

    pub(in crate::local_primitives) fn insert(&mut self, slot: usize, group: usize) {
        self.groups.values[slot] = group;
    }
}
