// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
// Modified by ShardLoom on 2026-10-21: move existing metadata key validation
// alongside new shared name/locator ownership and pre-allocation admission.

use std::alloc::Layout as AllocationLayout;
use std::ops::Deref;
use std::sync::Arc;

use vortex_array::memory::HostMemoryReservation;
use vortex_array::memory::HostMemorySessionExt;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use super::MAX_METADATA_KEY_BYTES;
use super::MAX_METADATA_SEGMENTS;
use super::SegmentSpec;

/// Shared metadata names and locators, admitted before their payload allocation.
/// Borrowed keys never expose a raw shared allocation without this storage owner.
#[derive(Clone, Debug, Default)]
pub(crate) struct MetadataSegments(Option<Arc<OwnedMetadataSegments>>);

#[derive(Debug)]
struct OwnedMetadataSegments {
    entries: Vec<(String, SegmentSpec)>,
    // Release the vector and its owned names before returning their credit.
    _reservation: HostMemoryReservation,
}

impl MetadataSegments {
    /// Validate borrowed keys, admit exact table/name capacity, then build entries.
    /// The fixed provider key-count limit permits bounded stack references and
    /// allocation-free duplicate checks. Keys are inspected once and retained
    /// as borrowed input until their admitted copies have been constructed.
    pub(crate) fn try_from_fn<'a>(
        count: usize,
        mut key: impl FnMut(usize) -> &'a str,
        mut segment: impl FnMut(usize) -> VortexResult<SegmentSpec>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        if count > MAX_METADATA_SEGMENTS {
            return Err(metadata_count_error(count));
        }
        if count == 0 {
            return Ok(Self::default());
        }
        let layout = AllocationLayout::array::<(String, SegmentSpec)>(count)
            .map_err(|_| vortex_err!("Metadata locator allocation size overflow"))?;
        let mut keys = [""; MAX_METADATA_SEGMENTS];
        let mut bytes = layout.size();
        for index in 0..count {
            let value = key(index);
            validate_metadata_key(value)?;
            if keys[..index].contains(&value) {
                return Err(duplicate_metadata_key_error(value));
            }
            bytes = bytes
                .checked_add(value.len())
                .ok_or_else(|| vortex_err!("Metadata name allocation size overflow"))?;
            keys[index] = value;
        }
        let reservation = session.host_allocator().reserve_external(bytes)?;
        let mut entries = Vec::with_capacity(count);
        for (index, key) in keys[..count].iter().enumerate() {
            let segment = segment(index)?;
            let mut name = String::with_capacity(key.len());
            name.push_str(key);
            debug_assert_eq!(name.capacity(), key.len());
            entries.push((name, segment));
        }
        debug_assert_eq!(entries.capacity(), count);
        Ok(Self(Some(Arc::new(OwnedMetadataSegments {
            entries,
            _reservation: reservation,
        }))))
    }

    /// Validate raw test fixtures as well as normally admitted storage at write.
    pub(crate) fn validate(&self) -> VortexResult<()> {
        if self.len() > MAX_METADATA_SEGMENTS {
            return Err(metadata_count_error(self.len()));
        }
        for (index, (key, _)) in self.iter().enumerate() {
            validate_metadata_key(key)?;
            if self[..index].iter().any(|(prior, _)| prior == key) {
                return Err(duplicate_metadata_key_error(key));
            }
        }
        Ok(())
    }

    /// Construct deliberately malformed caller-owned fixtures for parser tests.
    #[cfg(test)]
    pub(crate) fn from_unchecked(entries: Vec<(String, SegmentSpec)>) -> Self {
        if entries.is_empty() {
            return Self::default();
        }
        Self(Some(Arc::new(OwnedMetadataSegments {
            entries,
            _reservation: HostMemoryReservation::new(()),
        })))
    }
}

impl Deref for MetadataSegments {
    type Target = [(String, SegmentSpec)];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            None => &[],
            Some(owner) => &owner.entries,
        }
    }
}

fn metadata_count_error(metadata_len: usize) -> VortexError {
    vortex_err!(
        "Postscript contains {} metadata segments, but Vortex supports at most {} metadata segments; metadata keys must be non-empty and at most {} bytes",
        metadata_len,
        MAX_METADATA_SEGMENTS,
        MAX_METADATA_KEY_BYTES
    )
}

fn duplicate_metadata_key_error(key: &str) -> VortexError {
    vortex_err!(
        "Postscript contains duplicate metadata key {key}; metadata keys must be unique, non-empty, and at most {} bytes, and files may contain at most {} metadata segments",
        MAX_METADATA_KEY_BYTES,
        MAX_METADATA_SEGMENTS
    )
}

fn validate_metadata_key(key: &str) -> VortexResult<()> {
    if key.is_empty() {
        return Err(vortex_err!(
            "Postscript metadata key must not be empty; metadata keys must be at most {} bytes and files may contain at most {} metadata segments",
            MAX_METADATA_KEY_BYTES,
            MAX_METADATA_SEGMENTS
        ));
    }
    let key_bytes = key.len();
    if key_bytes > MAX_METADATA_KEY_BYTES {
        return Err(vortex_err!(
            "Postscript metadata key {key:?} is {key_bytes} bytes, but metadata keys must be at most {} bytes and files may contain at most {} metadata segments",
            MAX_METADATA_KEY_BYTES,
            MAX_METADATA_SEGMENTS
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use parking_lot::Mutex;
    use vortex_array::memory::DefaultHostAllocator;
    use vortex_array::memory::HostAllocator;
    use vortex_array::memory::WritableHostBuffer;
    use vortex_buffer::Alignment;

    use super::*;

    #[derive(Debug)]
    struct Tables {
        limit: usize,
        live: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<usize>>>,
    }

    struct Credit {
        bytes: usize,
        live: Arc<AtomicUsize>,
    }

    impl Drop for Credit {
        fn drop(&mut self) {
            self.live.fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }

    impl HostAllocator for Tables {
        fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
            DefaultHostAllocator.allocate(bytes, alignment)
        }

        fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
            self.requests.lock().push(bytes);
            let mut current = self.live.load(Ordering::SeqCst);
            loop {
                let total = current
                    .checked_add(bytes)
                    .filter(|total| *total <= self.limit)
                    .ok_or_else(|| vortex_err!("metadata test grant denied"))?;
                match self
                    .live
                    .compare_exchange(current, total, Ordering::SeqCst, Ordering::SeqCst)
                {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
            Ok(HostMemoryReservation::new(Credit {
                bytes,
                live: Arc::clone(&self.live),
            }))
        }
    }

    fn session(limit: usize) -> (VortexSession, Arc<AtomicUsize>, Arc<Mutex<Vec<usize>>>) {
        let live = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let session = VortexSession::empty().with_host_allocator(Arc::new(Tables {
            limit,
            live: Arc::clone(&live),
            requests: Arc::clone(&requests),
        }));
        (session, live, requests)
    }

    fn segment(index: usize) -> SegmentSpec {
        SegmentSpec {
            offset: u64::try_from(index).unwrap(),
            length: 0,
            alignment: Alignment::none(),
        }
    }

    #[derive(Debug)]
    struct BuffersOnly;

    impl HostAllocator for BuffersOnly {
        fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
            DefaultHostAllocator.allocate(bytes, alignment)
        }
    }

    #[test]
    fn empty_metadata_makes_no_callbacks_or_allocator_request() {
        let consumer = VortexSession::empty().with_host_allocator(Arc::new(BuffersOnly));
        let entries = MetadataSegments::try_from_fn(
            0,
            |_| panic!("empty key"),
            |_| panic!("empty segment"),
            &consumer,
        )
        .unwrap();
        assert!(entries.is_empty());
        assert!(entries.0.is_none());
        let error =
            MetadataSegments::try_from_fn(1, |_| "key", |_| panic!("denied segment"), &consumer)
                .unwrap_err();
        assert!(error.to_string().contains("reservation"), "{error}");
    }

    #[test]
    fn metadata_limits_and_invalid_keys_precede_admission_and_segment_production() {
        let (consumer, live, requests) = session(usize::MAX);
        for count in [MAX_METADATA_SEGMENTS + 1, usize::MAX] {
            let error = MetadataSegments::try_from_fn(
                count,
                |_| panic!("excess-count key"),
                |_| panic!("excess-count segment"),
                &consumer,
            )
            .unwrap_err();
            assert!(error.to_string().contains("at most 16 metadata segments"));
        }
        for keys in [
            vec![String::new()],
            vec!["x".repeat(65)],
            vec!["界".repeat(22)],
            vec!["duplicate".to_string(); 2],
        ] {
            assert!(
                MetadataSegments::try_from_fn(
                    keys.len(),
                    |index| keys[index].as_str(),
                    |_| panic!("invalid-key segment"),
                    &consumer
                )
                .is_err()
            );
        }
        assert!(requests.lock().is_empty());
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn metadata_exact_utf8_capacity_and_alias_lifetime_cover_all_keys() {
        let keys = (0..MAX_METADATA_SEGMENTS)
            .map(|index| format!("{}{index:04}", "界".repeat(20)))
            .collect::<Vec<_>>();
        assert!(keys.iter().all(|key| key.len() == MAX_METADATA_KEY_BYTES));
        let expected = keys.len() * size_of::<(String, SegmentSpec)>()
            + keys.iter().map(String::len).sum::<usize>();
        for limit in [expected - 1, expected] {
            let (consumer, live, requests) = session(limit);
            let mut calls = 0;
            let result = MetadataSegments::try_from_fn(
                keys.len(),
                |index| keys[index].as_str(),
                |index| {
                    calls += 1;
                    Ok(segment(index))
                },
                &consumer,
            );
            assert_eq!(requests.lock().as_slice(), &[expected]);
            if limit < expected {
                assert!(result.unwrap_err().to_string().contains("grant denied"));
                assert_eq!(calls, 0);
                assert_eq!(live.load(Ordering::SeqCst), 0);
                continue;
            }
            let entries = result.unwrap();
            assert_eq!(calls, keys.len());
            let owner = entries.0.as_ref().unwrap();
            assert_eq!(owner.entries.capacity(), keys.len());
            for (index, (name, spec)) in entries.iter().enumerate() {
                assert_eq!(name, &keys[index]);
                assert_eq!(name.capacity(), name.len());
                assert_eq!(spec.offset, u64::try_from(index).unwrap());
            }
            let alias = entries.clone();
            assert!(Arc::ptr_eq(owner, alias.0.as_ref().unwrap()));
            let pointers = entries
                .iter()
                .map(|(name, spec)| (name.as_ptr(), std::ptr::from_ref(spec)))
                .collect::<Vec<_>>();
            drop((entries, consumer));
            assert_eq!(live.load(Ordering::SeqCst), expected);
            assert_eq!(
                alias
                    .iter()
                    .map(|(name, spec)| (name.as_ptr(), std::ptr::from_ref(spec)))
                    .collect::<Vec<_>>(),
                pointers
            );
            drop(alias);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn metadata_partial_producer_failure_releases_credit_and_allows_retry() {
        let keys = ["first", "second", "third"];
        let expected = keys.len() * size_of::<(String, SegmentSpec)>()
            + keys.iter().map(|key| key.len()).sum::<usize>();
        for fail_at in [0, 1] {
            let (consumer, live, requests) = session(expected);
            let mut calls = 0;
            let error = MetadataSegments::try_from_fn(
                keys.len(),
                |index| keys[index],
                |index| {
                    calls += 1;
                    assert_eq!(live.load(Ordering::SeqCst), expected);
                    if index == fail_at {
                        Err(vortex_err!("invalid locator"))
                    } else {
                        Ok(segment(index))
                    }
                },
                &consumer,
            )
            .unwrap_err();
            assert!(error.to_string().contains("invalid locator"));
            assert_eq!(calls, fail_at + 1);
            assert_eq!(live.load(Ordering::SeqCst), 0);
            let entries = MetadataSegments::try_from_fn(
                keys.len(),
                |index| keys[index],
                |index| Ok(segment(index)),
                &consumer,
            )
            .unwrap();
            assert_eq!(requests.lock().as_slice(), &[expected, expected]);
            assert_eq!(live.load(Ordering::SeqCst), expected);
            assert_eq!(
                entries
                    .iter()
                    .map(|(key, _)| key.as_str())
                    .collect::<Vec<_>>(),
                keys
            );
            drop(entries);
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }
    }
}
