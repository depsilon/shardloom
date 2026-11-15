// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};

use vortex_array::memory::{DefaultHostAllocator, HostAllocator, WritableHostBuffer};
use vortex_buffer::Alignment;
use vortex_error::vortex_err;

use super::*;
use crate::dfa::FsstMatcher;

#[derive(Debug)]
struct CountingAllocator {
    live: Arc<AtomicUsize>,
    limit: usize,
}

struct Credit {
    live: Arc<AtomicUsize>,
    bytes: usize,
}

impl Drop for Credit {
    fn drop(&mut self) {
        self.live.fetch_sub(self.bytes, Ordering::SeqCst);
    }
}

impl HostAllocator for CountingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        DefaultHostAllocator.allocate(len, alignment)
    }
    fn reserve_external(&self, len: usize) -> VortexResult<HostMemoryReservation> {
        self.live
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(len).filter(|next| *next <= self.limit)
            })
            .map_err(|_| vortex_err!("test reservation denied"))?;
        Ok(HostMemoryReservation::new(Credit {
            live: Arc::clone(&self.live),
            bytes: len,
        }))
    }
}

fn allocator(limit: usize) -> (HostAllocatorRef, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    (
        Arc::new(CountingAllocator {
            live: Arc::clone(&live),
            limit,
        }),
        live,
    )
}

fn symbol(bytes: &[u8]) -> Symbol {
    let mut raw = [0; 8];
    raw[..bytes.len()].copy_from_slice(bytes);
    Symbol::from_slice(&raw)
}

fn matcher(
    reuse: &MatcherReuse,
    allocator: &HostAllocatorRef,
    symbols: &[Symbol],
    lengths: &[u8],
    pattern: &[u8],
    backing: &[u8],
) -> FsstMatcher {
    FsstMatcher::try_new_with_reuse(
        symbols,
        lengths,
        pattern,
        Some(backing),
        Some((reuse, allocator)),
    )
    .unwrap()
    .unwrap()
}

#[test]
fn exact_tables_reuse_and_credit_survives_the_scope_until_the_last_user() {
    let (allocator, live) = allocator(4 * TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let symbols = [symbol(b"a"), symbol(b"b")];
    let table_bytes = table_reservation_bytes(2, 2);
    let first = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%ab%", &[0, 1]);
    let second = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%ab%", &[1, 0, 1]);
    assert!(first.matches(&[0, 1]) && second.matches(&[1, 0, 1]));
    let work = scope.snapshot().unwrap();
    assert_eq!((work.lookups, work.hits, work.constructions), (2, 1, 1));
    assert_eq!(work.retained_table_capacity_bytes, 2 * 256 * 3);
    assert_eq!(
        live.load(Ordering::SeqCst),
        REGISTRY_BYTES + SCOPE_BYTES + table_bytes
    );
    drop(scope);
    assert_eq!(live.load(Ordering::SeqCst), REGISTRY_BYTES + table_bytes);
    assert!(second.matches(&[0, 1]));
    let next_scope = reuse.begin().unwrap();
    let next = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%ab%", &[0, 1]);
    assert_eq!(next_scope.snapshot().unwrap().hits, 0);
    assert_eq!(next_scope.snapshot().unwrap().constructions, 1);
    drop(next_scope);
    drop(next);
    drop(first);
    assert_eq!(live.load(Ordering::SeqCst), REGISTRY_BYTES + table_bytes);
    drop(second);
    assert_eq!(live.load(Ordering::SeqCst), REGISTRY_BYTES);
    drop(reuse);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn whole_backing_rejection_never_becomes_a_reused_table_result() {
    let (allocator, live) = allocator(4 * TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let symbols = [symbol(b"a"), symbol(b"b")];
    let absent = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%b%", &[0]);
    assert!(!absent.matches(&[0]));
    assert_eq!(scope.snapshot().unwrap().lookups, 0);
    let present = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%b%", &[1]);
    assert!(present.matches(&[1]));
    let absent_again = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%b%", &[0, 0]);
    assert!(!absent_again.matches(&[0, 0]));
    let present_again = matcher(&reuse, &allocator, &symbols, &[1, 1], b"%b%", &[0, 1]);
    assert!(present_again.matches(&[0, 1]));
    let work = scope.snapshot().unwrap();
    assert_eq!((work.lookups, work.hits, work.constructions), (2, 1, 1));
    drop((absent, present, absent_again, present_again, scope, reuse));
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn exact_contents_lengths_and_patterns_define_identity_and_other_routes_do_not_lookup() {
    let (allocator, _) = allocator(4 * TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let symbols = [symbol(b"ab"), symbol(b"ba")];
    let _first = matcher(&reuse, &allocator, &symbols, &[2, 2], b"%ab%", &[0, 1]);
    for (table, lengths, pattern) in [
        ([symbol(b"ba"), symbol(b"ab")], [2, 2], b"%ab%".as_slice()),
        (symbols, [1, 2], b"%ab%".as_slice()),
        (symbols, [2, 2], b"%ba%".as_slice()),
    ] {
        let changed = matcher(&reuse, &allocator, &table, &lengths, pattern, &[0, 1]);
        let reference = FsstMatcher::try_new(&table, &lengths, pattern, None)
            .unwrap()
            .unwrap();
        for codes in [
            b"".as_slice(),
            &[0],
            &[1],
            &[0, 1],
            &[255, b'a', 255, b'b'],
            &[255],
        ] {
            assert_eq!(changed.matches(codes), reference.matches(codes));
        }
    }
    assert_eq!(scope.snapshot().unwrap().hits, 0);
    assert_eq!(scope.snapshot().unwrap().constructions, 1);
    let before = scope.snapshot().unwrap();
    for (pattern, backing) in [
        (b"ab%".as_slice(), Some([0, 1].as_slice())),
        (b"%ab%".as_slice(), None),
        (b"%%".as_slice(), Some([0, 1].as_slice())),
    ] {
        let _matcher = FsstMatcher::try_new_with_reuse(
            &symbols,
            &[2, 2],
            pattern,
            backing,
            Some((&reuse, &allocator)),
        )
        .unwrap();
    }
    assert!(
        FsstMatcher::try_new_with_reuse(
            &symbols,
            &[2, 2],
            b"%a_b%",
            Some(&[0]),
            Some((&reuse, &allocator))
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(scope.snapshot().unwrap(), before);
}

#[test]
fn denied_credit_and_a_different_allocator_keep_the_existing_matcher() {
    let (too_small, live) = allocator(REGISTRY_BYTES - 1);
    assert!(MatcherReuse::try_new(&too_small).is_err());
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let (scope_denied, live) = allocator(REGISTRY_BYTES + SCOPE_BYTES - 1);
    let reuse = MatcherReuse::try_new(&scope_denied).unwrap();
    assert!(reuse.begin().is_none());
    assert_eq!(live.load(Ordering::SeqCst), REGISTRY_BYTES);
    drop(reuse);
    assert_eq!(live.load(Ordering::SeqCst), 0);
    let (allocator, live) = allocator(REGISTRY_BYTES + SCOPE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let symbols = [symbol(b"a")];
    let denied = matcher(&reuse, &allocator, &symbols, &[1], b"%a%", &[0]);
    assert!(denied.matches(&[0]));
    assert_eq!(scope.snapshot().unwrap().denied, 1);
    assert_eq!(scope.snapshot().unwrap().constructions, 0);
    let before = scope.snapshot().unwrap();
    let other: HostAllocatorRef = Arc::new(DefaultHostAllocator);
    let independent = matcher(&reuse, &other, &symbols, &[1], b"%a%", &[0]);
    assert!(independent.matches(&[0]));
    assert_eq!(scope.snapshot().unwrap(), before);
    drop((denied, independent, scope, reuse));
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn overlap_poison_and_unwinding_clear_operation_ownership() {
    let (allocator, live) = allocator(4 * TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let first = reuse.begin().unwrap();
    assert!(reuse.begin().is_none());
    assert!(first.snapshot().is_none());
    drop(first);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let scope = reuse.begin().unwrap();
        let _table = matcher(&reuse, &allocator, &[symbol(b"a")], &[1], b"%a%", &[0]);
        let _poison = scope.state.data.lock().unwrap();
        panic!("controlled scope unwind");
    }));
    assert!(result.is_err());
    assert_eq!(live.load(Ordering::SeqCst), REGISTRY_BYTES);
    let next = reuse.begin().unwrap();
    assert_eq!(next.snapshot(), Some(MatcherReuseSnapshot::default()));
    drop((next, reuse));
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn maximum_tables_fit_the_preconstruction_reservation() {
    let (allocator, live) = allocator(REGISTRY_BYTES + SCOPE_BYTES + TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let symbols = [symbol(b"aaaaaaaa"); MAX_SYMBOLS];
    let mut pattern = vec![b'%'];
    pattern.extend_from_slice(&[b'a'; 254]);
    pattern.push(b'%');
    let table = matcher(
        &reuse,
        &allocator,
        &symbols,
        &[8; MAX_SYMBOLS],
        &pattern,
        &[0; 32],
    );
    assert!(table.matches(&[0; 32]));
    assert_eq!(
        scope.snapshot().unwrap().retained_table_capacity_bytes,
        2 * 256 * 255
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        REGISTRY_BYTES + SCOPE_BYTES + table_reservation_bytes(MAX_SYMBOLS, 254)
    );
    drop((scope, table, reuse));
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn installed_scope_is_shared_but_leaves_no_charged_state_between_requests() {
    let (allocator, live) = allocator(4 * TABLE_BYTES);
    let session = VortexSession::empty().with_host_allocator(Arc::clone(&allocator));
    let cloned = session.clone();
    for _ in 0..2 {
        let scope = MatcherReuse::begin_in(&session).unwrap();
        let table = {
            let marker = cloned.get_opt::<MatcherReuse>().unwrap();
            matcher(&marker, &allocator, &[symbol(b"a")], &[1], b"%a%", &[0])
        };
        assert_eq!(scope.snapshot().unwrap().constructions, 1);
        assert_eq!(scope.snapshot().unwrap().hits, 0);
        drop(scope);
        assert!(cloned.get_opt::<MatcherReuse>().unwrap().0.is_none());
        assert_eq!(live.load(Ordering::SeqCst), table_reservation_bytes(1, 1));
        assert!(table.matches(&[0]));
        drop(table);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _scope = MatcherReuse::begin_in(&session).unwrap();
        panic!("controlled installed-scope unwind");
    }));
    assert!(result.is_err());
    assert!(cloned.get_opt::<MatcherReuse>().unwrap().0.is_none());
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn concurrent_constructors_keep_one_entry_and_refund_every_transient_owner() {
    let (allocator, live) = allocator(8 * TABLE_BYTES);
    let reuse = MatcherReuse::try_new(&allocator).unwrap();
    let scope = reuse.begin().unwrap();
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|threads| {
        for _ in 0..4 {
            threads.spawn(|| {
                barrier.wait();
                let table = matcher(&reuse, &allocator, &[symbol(b"ab")], &[2], b"%ab%", &[0]);
                assert!(table.matches(&[0]));
                assert!(!table.matches(&[255, b'x']));
            });
        }
    });
    let work = scope.snapshot().unwrap();
    assert_eq!(work.lookups, 4);
    assert_eq!(work.hits + work.misses, 4);
    assert_eq!(work.hits + work.constructions, 4);
    assert_eq!(work.denied, 0);
    assert_eq!(
        work.retained_reservation_bytes,
        table_reservation_bytes(1, 2)
    );
    assert_eq!(
        live.load(Ordering::SeqCst),
        REGISTRY_BYTES + SCOPE_BYTES + work.retained_reservation_bytes
    );
    drop((scope, reuse));
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn retained_session_snapshot_keeps_its_registry_credit_until_dropped() {
    let (allocator, live) = allocator(4 * TABLE_BYTES);
    let session = VortexSession::empty().with_host_allocator(Arc::clone(&allocator));
    let scope = MatcherReuse::begin_in(&session).unwrap();
    let old_marker = session.get_opt::<MatcherReuse>().unwrap();
    let table = matcher(&old_marker, &allocator, &[symbol(b"a")], &[1], b"%a%", &[0]);
    drop(scope);
    assert!(session.get_opt::<MatcherReuse>().unwrap().0.is_none());
    assert_eq!(
        live.load(Ordering::SeqCst),
        REGISTRY_BYTES + table_reservation_bytes(1, 1)
    );
    drop(old_marker);
    assert_eq!(live.load(Ordering::SeqCst), table_reservation_bytes(1, 1));
    drop(table);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}

#[test]
fn exhaustive_reused_tables_preserve_original_encoded_and_malformed_row_semantics() {
    let symbols = [
        symbol(b"ab"),
        symbol(b"ba"),
        symbol("雪".as_bytes()),
        symbol(b"%_\\"),
    ];
    let lengths = [2, 2, 3, 3];
    let alphabet = [0, 1, 2, 3, 4, 255, b'a', b'b'];
    let mut rows = Vec::new();
    for len in 0..=4_u32 {
        for mut index in 0..alphabet.len().pow(len) {
            let mut bytes = vec![0; len as usize];
            for byte in &mut bytes {
                *byte = alphabet[index % alphabet.len()];
                index /= alphabet.len();
            }
            rows.push(bytes);
        }
    }
    let backing = rows.concat();
    let mut compared = 0;
    for pattern in [
        b"%a%".as_slice(),
        b"%ab%",
        b"%ba%",
        "%雪%".as_bytes(),
        b"%\\%%",
        b"%\\_%",
        b"%\\\\%",
    ] {
        let (allocator, live) = allocator(4 * TABLE_BYTES);
        let reuse = MatcherReuse::try_new(&allocator).unwrap();
        let scope = reuse.begin().unwrap();
        let first = matcher(&reuse, &allocator, &symbols, &lengths, pattern, &backing);
        let shared = matcher(&reuse, &allocator, &symbols, &lengths, pattern, &backing);
        let reference = FsstMatcher::try_new(&symbols, &lengths, pattern, None)
            .unwrap()
            .unwrap();
        for codes in &rows {
            assert_eq!(shared.matches(codes), reference.matches(codes));
            compared += 1;
        }
        assert_eq!(scope.snapshot().unwrap().hits, 1);
        assert_eq!(scope.snapshot().unwrap().constructions, 1);
        drop((first, shared, scope, reuse));
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
    assert_eq!(compared, 32_767);
}
