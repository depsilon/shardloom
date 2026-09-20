//! Exact byte lookup into the existing owned UTF8 chunk dictionary.
//!
//! A duplicate's full byte equality to a validated string proves UTF8 validity.
//! Hash equality alone does not. Cached hashes also avoid rereading strings on
//! directory growth. Values retain their first-seen IDs and Arc ownership.

use std::{hash::Hasher, sync::Arc};

use rustc_hash::FxHasher;
use shardloom_core::{Result, ShardLoomError};

#[derive(Clone, Copy, Default)]
struct Slot {
    hash: u64,
    id: u32,
    occupied: bool,
}

#[derive(Default)]
pub(super) struct Utf8ChunkDictionary {
    slots: Vec<Slot>,
    values: Vec<Arc<str>>,
    copied_bytes: u64,
}

impl Utf8ChunkDictionary {
    pub(super) fn intern(&mut self, column: &str, bytes: &[u8]) -> Result<u32> {
        let mut hasher = FxHasher::default();
        hasher.write(bytes);
        self.intern_hashed(column, bytes, hasher.finish())
    }

    fn intern_hashed(&mut self, column: &str, bytes: &[u8], hash: u64) -> Result<u32> {
        let mut bucket = 0;
        if !self.slots.is_empty() {
            bucket = self.find(bytes, hash);
            if self.slots[bucket].occupied {
                return Ok(self.slots[bucket].id);
            }
        }
        let value = std::str::from_utf8(bytes).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "local Vortex aggregate direct UTF-8 column '{column}' had invalid UTF-8: {error}; no fallback execution was attempted"
            ))
        })?;
        let id = u32::try_from(self.values.len()).map_err(|_| failed("exceeded u32 entries"))?;
        // Keep at least a quarter of the buckets empty, and grow only on misses.
        if self.values.len() >= self.slots.len() - self.slots.len() / 4 {
            self.grow()?;
            bucket = self.find(bytes, hash);
        }
        self.values
            .try_reserve(1)
            .map_err(|_| failed("could not allocate owned value directory"))?;
        let owned: Arc<str> = Arc::from(value);
        self.copied_bytes += value.len() as u64;
        self.values.push(owned);
        self.slots[bucket] = Slot {
            hash,
            id,
            occupied: true,
        };
        Ok(id)
    }

    fn find(&self, bytes: &[u8], hash: u64) -> usize {
        let mut bucket = hash_bucket(hash, self.slots.len());
        loop {
            let slot = self.slots[bucket];
            if !slot.occupied
                || (slot.hash == hash && self.values[slot.id as usize].as_bytes() == bytes)
            {
                return bucket;
            }
            bucket = (bucket + 1) & (self.slots.len() - 1);
        }
    }

    fn grow(&mut self) -> Result<()> {
        let capacity = self
            .slots
            .len()
            .max(8)
            .checked_mul(2)
            .ok_or_else(|| failed("directory capacity overflowed"))?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| failed("could not allocate hash directory"))?;
        slots.resize(capacity, Slot::default());
        for slot in self.slots.iter().copied().filter(|slot| slot.occupied) {
            let mut bucket = hash_bucket(slot.hash, capacity);
            while slots[bucket].occupied {
                bucket = (bucket + 1) & (capacity - 1);
            }
            slots[bucket] = slot;
        }
        self.slots = slots;
        Ok(())
    }

    pub(super) fn into_values(self) -> (Vec<Arc<str>>, u64) {
        (self.values, self.copied_bytes)
    }
}

fn hash_bucket(hash: u64, capacity: usize) -> usize {
    // Capacity is a nonzero power of two. Truncation preserves its low bits.
    #[allow(clippy::cast_possible_truncation)]
    let bucket = hash as usize & (capacity - 1);
    bucket
}

fn failed(detail: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex aggregate direct UTF-8 chunk dictionary {detail}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_ids_survive_full_hash_collisions_and_growth() {
        for hash in [0, u64::MAX] {
            let mut dictionary = Utf8ChunkDictionary::default();
            let values: Vec<_> = (0..1000)
                .map(|i| format!("shared-prefix-東京-{i}"))
                .collect();
            for (id, value) in values.iter().enumerate() {
                assert_eq!(
                    dictionary
                        .intern_hashed("text", value.as_bytes(), hash)
                        .unwrap() as usize,
                    id
                );
            }
            for (id, value) in values.iter().enumerate().rev() {
                assert_eq!(
                    dictionary
                        .intern_hashed("text", value.as_bytes(), hash)
                        .unwrap() as usize,
                    id
                );
            }
            let (owned, copied) = dictionary.into_values();
            assert_eq!(
                owned.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
                values
            );
            assert_eq!(copied, values.iter().map(|v| v.len() as u64).sum::<u64>());
        }
    }

    #[test]
    fn invalid_new_bytes_are_rejected_even_on_hash_collision() {
        let mut dictionary = Utf8ChunkDictionary::default();
        assert_eq!(dictionary.intern_hashed("text", b"valid", 0).unwrap(), 0);
        for invalid in [&b"\xff"[..], &b"\xc3"[..], &b"\xc0\x80"[..]] {
            let error = dictionary
                .intern_hashed("text", invalid, 0)
                .unwrap_err()
                .to_string();
            assert!(error.contains("column 'text' had invalid UTF-8"));
            assert!(error.contains("no fallback execution was attempted"));
        }
        assert_eq!(dictionary.intern_hashed("text", b"next", 0).unwrap(), 1);
        assert_eq!(dictionary.intern_hashed("text", b"valid", 0).unwrap(), 0);
    }

    #[test]
    fn repeated_unicode_empty_and_nul_values_keep_owned_first_seen_ids() {
        let values = ["", "東京🙂", "embedded\0nul", "é", "e\u{301}"];
        let mut dictionary = Utf8ChunkDictionary::default();
        for _ in 0..3 {
            for (id, value) in values.iter().enumerate() {
                let temporary = value.to_string();
                assert_eq!(
                    dictionary.intern("text", temporary.as_bytes()).unwrap() as usize,
                    id
                );
            }
        }
        let (owned, copied) = dictionary.into_values();
        assert_eq!(
            owned.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
            values
        );
        assert_eq!(copied, values.iter().map(|v| v.len() as u64).sum::<u64>());
    }
}
