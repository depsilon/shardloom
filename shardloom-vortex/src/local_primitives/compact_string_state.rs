//! Isolated exact string-count representation; not selected by runtime dispatch.
//! Compact references are local to one arena, never native dictionary IDs.

use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Admission<T> {
    Ready(T),
    Pressure,
    OutsideCompactRange,
}

/// Arena-local address. Slab size is fixed for the lifetime of its owner.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct CompactTextRef {
    offset: u32,
    len: u32,
}

struct Slab {
    bytes: Vec<u8>,
    lease: MemoryLease,
}

/// A retained slice keeps the complete backing slab and its allocation credit.
#[derive(Clone)]
pub(super) struct OwnedSlabText {
    slab: Option<Arc<Slab>>,
    start: usize,
    len: usize,
}

impl OwnedSlabText {
    pub(super) fn as_str(&self) -> &str {
        self.slab.as_ref().map_or("", |slab| {
            std::str::from_utf8(&slab.bytes[self.start..self.start + self.len])
                .expect("only complete UTF-8 strings enter a slab")
        })
    }
}

pub(super) struct SlabStringArena {
    slabs: Vec<Arc<Slab>>,
    directory_lease: MemoryLease,
    memory: LiveMemoryPool,
    slab_bytes: usize,
    address_limit: u32,
    bytes_copied: u64,
    directory_moves: u64,
}

impl SlabStringArena {
    pub(super) fn new(memory: &LiveMemoryPool, slab_bytes: usize) -> Result<Self> {
        Self::with_address_limit(memory, slab_bytes, u32::MAX)
    }

    fn with_address_limit(
        memory: &LiveMemoryPool,
        slab_bytes: usize,
        address_limit: u32,
    ) -> Result<Self> {
        if !slab_bytes.is_power_of_two() || u32::try_from(slab_bytes).is_err() {
            return Err(failed("slab capacity must be a power of two within u32"));
        }
        Ok(Self {
            slabs: Vec::new(),
            directory_lease: memory.reserve(0)?,
            memory: memory.clone(),
            slab_bytes,
            address_limit,
            bytes_copied: 0,
            directory_moves: 0,
        })
    }

    /// Append exact bytes; interning/equality belongs to the calling table.
    /// Old payloads never move. A shared tail starts a fresh slab on next append.
    pub(super) fn append(&mut self, text: &str) -> Result<Admission<CompactTextRef>> {
        if text.is_empty() {
            return Ok(Admission::Ready(CompactTextRef::default()));
        }
        if text.len() > self.slab_bytes {
            return Ok(Admission::OutsideCompactRange);
        }
        let copied = self
            .bytes_copied
            .checked_add(text.len() as u64)
            .ok_or_else(|| failed("copy count overflow"))?;
        let reusable = self.slabs.last().is_some_and(|slab| {
            Arc::strong_count(slab) == 1 && self.slab_bytes - slab.bytes.len() >= text.len()
        });
        let (index, start) = if reusable {
            (
                self.slabs.len() - 1,
                self.slabs.last().expect("nonempty tail").bytes.len(),
            )
        } else {
            (self.slabs.len(), 0)
        };
        let Some(offset) = index
            .checked_mul(self.slab_bytes)
            .and_then(|value| value.checked_add(start))
        else {
            return Ok(Admission::OutsideCompactRange);
        };
        let Some(end) = offset.checked_add(text.len()) else {
            return Ok(Admission::OutsideCompactRange);
        };
        let (Ok(offset), Ok(end), Ok(len)) = (
            u32::try_from(offset),
            u32::try_from(end),
            u32::try_from(text.len()),
        ) else {
            return Ok(Admission::OutsideCompactRange);
        };
        if end > self.address_limit {
            return Ok(Admission::OutsideCompactRange);
        }
        if !reusable {
            if self.slabs.len() == self.slabs.capacity() {
                let Some(capacity) = self.slabs.capacity().max(2).checked_mul(2) else {
                    return Ok(Admission::OutsideCompactRange);
                };
                let Some((mut replacement, lease)) =
                    allocate::<Arc<Slab>>(capacity, &self.memory, 0)?
                else {
                    return Ok(Admission::Pressure);
                };
                let moves = self
                    .directory_moves
                    .checked_add(self.slabs.len() as u64)
                    .ok_or_else(|| failed("directory move count overflow"))?;
                // Move existing owners; do not clone their data or credits.
                replacement.append(&mut self.slabs);
                self.slabs = replacement;
                self.directory_lease = lease;
                self.directory_moves = moves;
            }
            let overhead = size_of::<Slab>() + 2 * size_of::<usize>();
            let Some((bytes, lease)) = allocate::<u8>(self.slab_bytes, &self.memory, overhead)?
            else {
                return Ok(Admission::Pressure);
            };
            self.slabs.push(Arc::new(Slab { bytes, lease }));
        }
        Arc::get_mut(self.slabs.last_mut().expect("admitted tail"))
            .expect("shared tails allocate a new slab")
            .bytes
            .extend_from_slice(text.as_bytes());
        self.bytes_copied = copied;
        Ok(Admission::Ready(CompactTextRef { offset, len }))
    }

    /// References must come from this arena. They are private query-state
    /// offsets, not interchangeable IDs from unrelated dictionary generations.
    pub(super) fn get(&self, text: CompactTextRef) -> &str {
        if text.len == 0 {
            return "";
        }
        let offset = text.offset as usize;
        let slab = &self.slabs[offset / self.slab_bytes];
        let start = offset % self.slab_bytes;
        std::str::from_utf8(&slab.bytes[start..start + text.len as usize])
            .expect("only complete UTF-8 strings enter a slab")
    }

    pub(super) fn retain(&self, text: CompactTextRef) -> OwnedSlabText {
        OwnedSlabText {
            slab: (text.len != 0)
                .then(|| Arc::clone(&self.slabs[text.offset as usize / self.slab_bytes])),
            start: text.offset as usize % self.slab_bytes,
            len: text.len as usize,
        }
    }

    pub(super) fn owned_bytes(&self) -> u64 {
        self.directory_lease.bytes()
            + self
                .slabs
                .iter()
                .map(|slab| slab.lease.bytes())
                .sum::<u64>()
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Slot {
    hash: u64,
    count: u64,
    text: CompactTextRef,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactCountWork {
    pub(crate) rows: u64,
    pub(crate) updates: u64,
    pub(crate) probes: u64,
    pub(crate) full_hash_comparisons: u64,
    pub(crate) rehashed_slots: u64,
    pub(crate) payload_bytes_copied: u64,
    pub(crate) directory_owner_moves: u64,
    pub(crate) slabs: usize,
    pub(crate) groups: usize,
    pub(crate) slot_bytes: usize,
    pub(crate) owned_bytes: u64,
}

/// Exact all-key counts with the same 50% maximum occupancy as the retained
/// control. It does not prune top-K, merge partitions or reinterpret Dict codes.
pub(super) struct CompactStringCounts {
    slots: Vec<Slot>,
    slots_lease: MemoryLease,
    arena: SlabStringArena,
    groups: usize,
    work: CompactCountWork,
}

impl CompactStringCounts {
    pub(super) fn new(memory: &LiveMemoryPool, slab_bytes: usize) -> Result<Self> {
        Ok(Self {
            slots: Vec::new(),
            slots_lease: memory.reserve(0)?,
            arena: SlabStringArena::new(memory, slab_bytes)?,
            groups: 0,
            work: CompactCountWork::default(),
        })
    }

    /// The supplied full hash must be consistent for equal UTF-8 bytes, as in
    /// the retained partials. Colliding hashes still require complete equality.
    pub(super) fn update(
        &mut self,
        value: &str,
        hash: u64,
        count: u64,
        mut check_cancelled: impl FnMut() -> Result<()>,
    ) -> Result<Admission<()>> {
        check_cancelled()?;
        if count == 0 {
            return Err(failed("zero-weight count"));
        }
        if value.len() > self.arena.slab_bytes {
            return Ok(Admission::OutsideCompactRange);
        }
        let rows = self
            .work
            .rows
            .checked_add(count)
            .ok_or_else(|| failed("row count overflow"))?;
        let updates = self
            .work
            .updates
            .checked_add(1)
            .ok_or_else(|| failed("update count overflow"))?;
        if !self.slots.is_empty() {
            let index = self.find(value, hash, &mut check_cancelled)?;
            if self.slots[index].count != 0 {
                let next = self.slots[index]
                    .count
                    .checked_add(count)
                    .ok_or_else(|| failed("exact count overflow"))?;
                self.slots[index].count = next;
                self.work.rows = rows;
                self.work.updates = updates;
                return Ok(Admission::Ready(()));
            }
        }
        let Some(groups) = self.groups.checked_add(1) else {
            return Ok(Admission::OutsideCompactRange);
        };
        if self.slots.is_empty() || groups > self.slots.len() / 2 {
            let Some(capacity) = self.slots.len().max(8).checked_mul(2) else {
                return Ok(Admission::OutsideCompactRange);
            };
            let Some((mut slots, lease)) = allocate::<Slot>(capacity, &self.arena.memory, 0)?
            else {
                return Ok(Admission::Pressure);
            };
            slots.resize(capacity, Slot::default());
            let mut moved = 0_u64;
            for (index, slot) in self.slots.iter().copied().enumerate() {
                if index.is_multiple_of(4096) {
                    check_cancelled()?;
                }
                if slot.count == 0 {
                    continue;
                }
                let mut bucket = bucket(slot.hash, capacity);
                let mut probes = 0_usize;
                while slots[bucket].count != 0 {
                    probes += 1;
                    if probes.is_multiple_of(4096) {
                        check_cancelled()?;
                    }
                    bucket = (bucket + 1) & (capacity - 1);
                }
                slots[bucket] = slot;
                moved += 1;
            }
            let total_moved = self
                .work
                .rehashed_slots
                .checked_add(moved)
                .ok_or_else(|| failed("rehash count overflow"))?;
            self.slots = slots;
            self.slots_lease = lease;
            self.work.rehashed_slots = total_moved;
        }
        let index = self.find(value, hash, &mut check_cancelled)?;
        check_cancelled()?;
        let text = match self.arena.append(value)? {
            Admission::Ready(text) => text,
            Admission::Pressure => return Ok(Admission::Pressure),
            Admission::OutsideCompactRange => return Ok(Admission::OutsideCompactRange),
        };
        self.slots[index] = Slot { hash, count, text };
        self.groups = groups;
        self.work.rows = rows;
        self.work.updates = updates;
        Ok(Admission::Ready(()))
    }

    fn find(
        &mut self,
        value: &str,
        hash: u64,
        check_cancelled: &mut impl FnMut() -> Result<()>,
    ) -> Result<usize> {
        let mut index = bucket(hash, self.slots.len());
        let mut probes = 0_usize;
        loop {
            probes += 1;
            if probes.is_multiple_of(4096) {
                check_cancelled()?;
            }
            self.work.probes = self
                .work
                .probes
                .checked_add(1)
                .ok_or_else(|| failed("probe count overflow"))?;
            let slot = self.slots[index];
            if slot.count == 0 {
                return Ok(index);
            }
            if slot.hash == hash {
                self.work.full_hash_comparisons = self
                    .work
                    .full_hash_comparisons
                    .checked_add(1)
                    .ok_or_else(|| failed("comparison count overflow"))?;
                if self.arena.get(slot.text) == value {
                    return Ok(index);
                }
            }
            index = (index + 1) & (self.slots.len() - 1);
        }
    }

    pub(super) fn entries(&self) -> impl Iterator<Item = (&str, u64)> {
        self.slots
            .iter()
            .filter(|slot| slot.count != 0)
            .map(|slot| (self.arena.get(slot.text), slot.count))
    }

    pub(super) fn retained_value(&self, value: &str) -> Option<OwnedSlabText> {
        self.slots
            .iter()
            .find(|slot| slot.count != 0 && self.arena.get(slot.text) == value)
            .map(|slot| self.arena.retain(slot.text))
    }

    pub(super) fn evidence(&self) -> CompactCountWork {
        CompactCountWork {
            payload_bytes_copied: self.arena.bytes_copied,
            directory_owner_moves: self.arena.directory_moves,
            slabs: self.arena.slabs.len(),
            groups: self.groups,
            slot_bytes: size_of::<Slot>(),
            owned_bytes: self.slots_lease.bytes() + self.arena.owned_bytes(),
            ..self.work
        }
    }
}

#[allow(clippy::cast_possible_truncation)] // Low hash bits alone index a power-of-two table.
fn bucket(hash: u64, capacity: usize) -> usize {
    (hash as usize) & (capacity - 1)
}

fn allocate<T>(
    capacity: usize,
    memory: &LiveMemoryPool,
    extra: usize,
) -> Result<Option<(Vec<T>, MemoryLease)>> {
    let bytes = capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| bytes.checked_add(extra))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| failed("allocation capacity overflow"))?;
    let Ok(lease) = memory.reserve(bytes) else {
        return Ok(None);
    };
    let mut values = Vec::new();
    if values.try_reserve_exact(capacity).is_err() || values.capacity() != capacity {
        return Ok(None);
    }
    Ok(Some((values, lease)))
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "compact native string state: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "compact_string_state_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "compact_string_state_benchmark.rs"]
pub(super) mod benchmark;
