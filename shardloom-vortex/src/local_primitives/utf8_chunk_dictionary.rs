//! Exact byte lookup with native UTF8 buffer ownership.
//!
//! A duplicate's full byte equality to a validated string proves UTF8 validity.
//! Hash equality alone does not. Cached hashes also avoid rereading strings on
//! directory growth. Values retain their first-seen IDs. Chunk consumers borrow
//! native strings; escaping aggregate state explicitly promotes independent keys.

use std::hash::Hasher;

use super::native_utf8::{Utf8DictionaryValue, borrowed_bytes};

use rustc_hash::FxHasher;
use shardloom_core::{Result, ShardLoomError};
use vortex::{array::arrays::VarBinViewArray, buffer::BufferString};

#[derive(Clone, Copy, Default)]
struct Slot {
    hash: u64,
    id: u32,
    occupied: bool,
}

#[derive(Default)]
pub(super) struct Utf8ChunkDictionary {
    slots: Vec<Slot>,
    values: Vec<Utf8DictionaryValue>,
    copied_bytes: u64,
    source_backed_bytes: u64,
}

impl Utf8ChunkDictionary {
    pub(super) fn intern_source(
        &mut self,
        column: &str,
        source: &VarBinViewArray,
        row: usize,
    ) -> Result<u32> {
        // Lookup borrows the provider bytes. Only a new value needs a retained
        // slice, avoiding an atomic owner clone on every duplicate row.
        let bytes = borrowed_bytes(source, row);
        let mut hasher = FxHasher::default();
        hasher.write(bytes);
        self.intern_hashed_with(bytes, hasher.finish(), || {
            BufferString::try_from(source.bytes_at(row))
                .map(Utf8DictionaryValue::source)
                .map_err(|error| invalid_utf8(column, error))
        })
    }

    fn intern_hashed_with(
        &mut self,
        bytes: &[u8],
        hash: u64,
        retain: impl FnOnce() -> Result<Utf8DictionaryValue>,
    ) -> Result<u32> {
        let mut bucket = 0;
        if !self.slots.is_empty() {
            bucket = self.find(bytes, hash);
            if self.slots[bucket].occupied {
                return Ok(self.slots[bucket].id);
            }
        }
        let value = retain()?;
        let id = u32::try_from(self.values.len()).map_err(|_| failed("exceeded u32 entries"))?;
        // Keep at least a quarter of the buckets empty, and grow only on misses.
        if self.values.len() >= self.slots.len() - self.slots.len() / 4 {
            self.grow()?;
            bucket = self.find(bytes, hash);
        }
        self.values
            .try_reserve(1)
            .map_err(|_| failed("could not allocate value directory"))?;
        match &value {
            Utf8DictionaryValue::Owned(_) => self.copied_bytes += value.len() as u64,
            Utf8DictionaryValue::Source { .. } => self.source_backed_bytes += value.len() as u64,
        }
        self.values.push(value);
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

    pub(super) fn into_values(self) -> (Vec<Utf8DictionaryValue>, u64, u64) {
        (self.values, self.copied_bytes, self.source_backed_bytes)
    }
}

fn invalid_utf8(column: &str, error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex aggregate direct UTF-8 column '{column}' had invalid UTF-8: {error}; no fallback execution was attempted"
    ))
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
    use std::sync::Arc;

    impl Utf8ChunkDictionary {
        fn intern(&mut self, column: &str, bytes: &[u8]) -> Result<u32> {
            let mut hasher = FxHasher::default();
            hasher.write(bytes);
            self.intern_hashed(column, bytes, hasher.finish())
        }

        fn intern_hashed(&mut self, column: &str, bytes: &[u8], hash: u64) -> Result<u32> {
            self.intern_hashed_with(bytes, hash, || {
                std::str::from_utf8(bytes)
                    .map(|value| Utf8DictionaryValue::Owned(Arc::from(value)))
                    .map_err(|error| invalid_utf8(column, error))
            })
        }
    }

    #[test]
    fn source_values_keep_exact_ids_under_collisions_without_payload_copies() {
        let expected: Vec<_> = (0..1000)
            .map(|i| format!("shared-prefix-東京-{i}"))
            .collect();
        let source = VarBinViewArray::from_iter_str(expected.iter());
        let mut dictionary = Utf8ChunkDictionary::default();
        for row in (0..expected.len()).chain((0..expected.len()).rev()) {
            let bytes = source.bytes_at(row);
            assert_eq!(
                dictionary
                    .intern_hashed_with(&bytes, 0, || {
                        Ok(Utf8DictionaryValue::source(
                            BufferString::try_from(bytes.clone()).unwrap(),
                        ))
                    })
                    .unwrap() as usize,
                row
            );
        }
        let (values, copied, source_backed) = dictionary.into_values();
        assert_eq!(copied, 0);
        assert_eq!(
            source_backed,
            expected.iter().map(|v| v.len() as u64).sum::<u64>()
        );
        for (row, value) in values.iter().enumerate() {
            assert_eq!(value.as_bytes().as_ptr(), source.bytes_at(row).as_ptr());
        }
        drop(source);
        assert_eq!(
            values.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
            expected
        );
    }

    #[test]
    fn source_duplicates_do_not_retain_another_owner_and_invalid_misses_fail() {
        let mut dictionary = Utf8ChunkDictionary::default();
        let value = BufferString::from("valid");
        dictionary
            .intern_hashed_with(b"valid", 0, || Ok(Utf8DictionaryValue::source(value)))
            .unwrap();
        assert_eq!(
            dictionary
                .intern_hashed_with(b"valid", 0, || panic!("duplicate retained an owner"))
                .unwrap(),
            0
        );
        let invalid = vortex::buffer::ByteBuffer::from(vec![0xff]);
        assert!(
            dictionary
                .intern_hashed_with(&invalid, 0, || {
                    BufferString::try_from(invalid.clone())
                        .map(Utf8DictionaryValue::source)
                        .map_err(|error| invalid_utf8("text", error))
                })
                .unwrap_err()
                .to_string()
                .contains("column 'text' had invalid UTF-8")
        );
        assert_eq!(dictionary.into_values().0.len(), 1);
    }

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
            let (owned, copied, source_backed) = dictionary.into_values();
            assert_eq!(source_backed, 0);
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
        let (owned, copied, source_backed) = dictionary.into_values();
        assert_eq!(source_backed, 0);
        assert_eq!(
            owned.iter().map(AsRef::as_ref).collect::<Vec<&str>>(),
            values
        );
        assert_eq!(copied, values.iter().map(|v| v.len() as u64).sum::<u64>());
    }
}
