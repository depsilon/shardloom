use super::*;

#[test]
fn source_segment_cache_empty_call_drop_revokes_pending_copy_admission() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    let epoch = miss(&store, &source, 0, 1);
    // Operation 1 has admitted the table and its retained-copy capacity, but
    // has not published an entry. An unrelated metadata/empty serving call
    // may finish while that copy is outside the cache lock.
    let pending = store
        .reserve_copy(&source, SegmentId::from(0), 1, epoch, 1088)
        .unwrap()
        .unwrap();
    assert!(store.snapshot().table_reserved_bytes > 0);
    store.discard_uncompleted_operation(2);
    assert_eq!(store.snapshot().retained_entries, 0);
    assert_eq!(store.snapshot().table_reserved_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(store.snapshot().retained_capacity_bytes, 1088);
    let stale = store
        .retain(&source, SegmentId::from(0), 1, epoch, &input(1024))
        .unwrap();
    assert!(
        !stale.stored,
        "refunded table ownership must revoke every pending copy admission"
    );
    assert_eq!(stale.copied_bytes, 0);
    drop(pending);
    assert_eq!(store.snapshot().retained_capacity_bytes, 0);
    retain(&store, &source, 0, 3);
    store.complete_operation(3).unwrap();
    let current = store.snapshot();
    assert_eq!(current.retained_entries, 1);
    assert!(current.table_reserved_bytes > 0);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        current.table_reserved_bytes + current.retained_capacity_bytes
    );
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn source() -> Arc<SourceIdentity> {
    Arc::new(
        SourceIdentity::capture(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/local_primitive_struct_five.vortex"),
        )
        .unwrap(),
    )
}

fn input(bytes: usize) -> BufferHandle {
    BufferHandle::new_host(ByteBuffer::copy_from_aligned(
        vec![37; bytes],
        Alignment::of::<u64>(),
    ))
}

fn miss(store: &Store, source: &Arc<SourceIdentity>, segment: u32, operation: u64) -> u64 {
    match store
        .lookup(source, SegmentId::from(segment), operation)
        .unwrap()
    {
        Lookup::Miss { epoch } => epoch,
        _ => panic!("expected a cold segment"),
    }
}

fn retain(store: &Store, source: &Arc<SourceIdentity>, segment: u32, operation: u64) {
    let epoch = miss(store, source, segment, operation);
    let outcome = store
        .retain(
            source,
            SegmentId::from(segment),
            operation,
            epoch,
            &input(1024),
        )
        .unwrap();
    assert!(outcome.stored);
    assert_eq!(outcome.copied_bytes, 1024);
}

#[test]
fn source_segment_cache_requires_completed_call_and_exact_prepared_owner() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    retain(&store, &source, 0, 1);
    miss(&store, &source, 0, 1);
    miss(&store, &source, 0, 2);
    store.complete_operation(1).unwrap();
    miss(&store, &source, 0, 1);
    let independent = self::source();
    miss(&store, &independent, 0, 2);
    let Lookup::Hit { buffer, cross_call } = store.lookup(&source, SegmentId::from(0), 2).unwrap()
    else {
        panic!("completed call must supply the held generation");
    };
    assert!(cross_call);
    assert_eq!(buffer.as_slice(), &[37; 1024]);
    assert_eq!(buffer.alignment(), Alignment::of::<u64>());
    assert!(buffer.alignment().is_ptr_aligned(buffer.as_ptr()));
    store.invalidate_source(&source, true);
    assert_eq!(store.snapshot().retained_entries, 0);
    assert_eq!(store.snapshot().invalidations, 1);
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "held hit keeps full capacity credit"
    );
    drop(buffer);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_pressure_does_not_refund_held_slices_or_overcommit() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    retain(&store, &source, 0, 1);
    store.complete_operation(1).unwrap();
    let Lookup::Hit { buffer, .. } = store.lookup(&source, SegmentId::from(0), 2).unwrap() else {
        panic!()
    };
    let held = buffer.slice(0..1);
    drop(buffer);
    let capacity = store.snapshot().retained_capacity_bytes;
    assert!(capacity > 1024);
    assert!(memory.reserve(8 << 20).is_err());
    let snapshot = store.snapshot();
    assert_eq!(snapshot.retained_entries, 0);
    assert_eq!(snapshot.table_reserved_bytes, 0);
    assert_eq!(snapshot.retained_capacity_bytes, capacity);
    assert_eq!(snapshot.pressure_reclamations, 1);
    assert_eq!(memory.snapshot().reserved_bytes, capacity);
    let mandatory = memory.reserve((8 << 20) - capacity).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 8 << 20);
    assert_eq!(held.as_slice(), &[37]);
    drop((mandatory, held));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_stale_copy_and_failed_call_cannot_publish() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    let epoch = miss(&store, &source, 0, 1);
    store.reclaim_optional_memory();
    let stale = store
        .retain(&source, SegmentId::from(0), 1, epoch, &input(1024))
        .unwrap();
    assert!(!stale.stored);
    assert_eq!(stale.copied_bytes, 0);
    retain(&store, &source, 0, 2);
    store.discard_uncompleted_operation(2);
    assert_eq!(store.snapshot().retained_entries, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    miss(&store, &source, 0, 3);
    retain(&store, &source, 0, 3);
    store.complete_operation(3).unwrap();
    store.discard_uncompleted_operation(3);
    assert_eq!(store.snapshot().retained_entries, 1);
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_bounds_entries_and_evicts_least_recent_segment() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    for id in 0..u32::try_from(MAX_ENTRIES).unwrap() {
        retain(&store, &source, id, 1);
    }
    store.complete_operation(1).unwrap();
    drop(store.lookup(&source, SegmentId::from(0), 2).unwrap());
    retain(&store, &source, u32::try_from(MAX_ENTRIES).unwrap(), 2);
    assert_eq!(store.snapshot().retained_entries, MAX_ENTRIES as u64);
    assert_eq!(store.snapshot().evictions, 1);
    miss(&store, &source, 1, 3);
    assert!(matches!(
        store.lookup(&source, SegmentId::from(0), 3).unwrap(),
        Lookup::Hit { .. }
    ));
    let snapshot = store.snapshot();
    assert!(snapshot.retained_capacity_bytes <= snapshot.retention_limit_bytes);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        snapshot.table_reserved_bytes + snapshot.retained_capacity_bytes
    );
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_concurrent_duplicate_completion_retains_one_copy() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    let epoch = miss(&store, &source, 0, 1);
    let barrier = Arc::new(std::sync::Barrier::new(5));
    let workers = (0..4)
        .map(|_| {
            let (store, source, barrier) = (store.clone(), source.clone(), barrier.clone());
            std::thread::spawn(move || {
                let input = input(1024);
                barrier.wait();
                store
                    .retain(&source, SegmentId::from(0), 1, epoch, &input)
                    .unwrap()
                    .stored
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let stored = workers
        .into_iter()
        .map(|worker| usize::from(worker.join().unwrap()))
        .sum::<usize>();
    assert_eq!(stored, 1);
    let snapshot = store.snapshot();
    assert_eq!(snapshot.retained_entries, 1);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        snapshot.table_reserved_bytes + snapshot.retained_capacity_bytes
    );
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_optional_admission_and_weak_registration_are_bounded() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let first = Store::new(&memory).unwrap();
    let second = Store::new(&memory).unwrap();
    let source = source();
    assert!(first.snapshot().enabled);
    assert!(!second.snapshot().enabled);
    assert!(matches!(
        second.lookup(&source, SegmentId::from(0), 1).unwrap(),
        Lookup::Disabled
    ));
    assert!(
        !second
            .retain(&source, SegmentId::from(0), 1, 0, &input(1024))
            .unwrap()
            .stored
    );
    let epoch = miss(&first, &source, 0, 1);
    let too_large = first
        .retain(
            &source,
            SegmentId::from(0),
            1,
            epoch,
            &input(MAX_SEGMENT_BYTES + 1),
        )
        .unwrap();
    assert!(!too_large.stored);
    let mandatory = memory.reserve((8 << 20) - 1024).unwrap();
    assert!(
        !first
            .retain(&source, SegmentId::from(0), 1, epoch, &input(1024))
            .unwrap()
            .stored
    );
    assert_eq!(memory.snapshot().reserved_bytes, mandatory.bytes());
    drop((mandatory, first));
    let replacement = Store::new(&memory).unwrap();
    assert!(replacement.snapshot().enabled);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_byte_limit_includes_evicted_buffers_held_by_callers() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    let payload = input(192 << 10);
    for id in 0..2_u32 {
        let epoch = miss(&store, &source, id, 1);
        assert!(
            store
                .retain(&source, id.into(), 1, epoch, &payload)
                .unwrap()
                .stored
        );
    }
    store.complete_operation(1).unwrap();
    let Lookup::Hit { buffer: first, .. } = store.lookup(&source, 0.into(), 2).unwrap() else {
        panic!()
    };
    let Lookup::Hit { buffer: second, .. } = store.lookup(&source, 1.into(), 2).unwrap() else {
        panic!()
    };
    let held_capacity = store.snapshot().retained_capacity_bytes;
    let epoch = miss(&store, &source, 2, 3);
    assert!(
        !store
            .retain(&source, 2.into(), 3, epoch, &payload)
            .unwrap()
            .stored
    );
    let snapshot = store.snapshot();
    assert_eq!(snapshot.retained_entries, 0);
    assert_eq!(snapshot.retained_capacity_bytes, held_capacity);
    assert!(snapshot.retained_capacity_bytes <= snapshot.retention_limit_bytes);
    drop((first, second));
    assert_eq!(store.snapshot().retained_capacity_bytes, 0);
    let epoch = miss(&store, &source, 2, 4);
    assert!(
        store
            .retain(&source, 2.into(), 4, epoch, &payload)
            .unwrap()
            .stored
    );
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_allocation_shrink_releases_optional_owners_even_when_they_fit() {
    let memory = LiveMemoryPool::configurable(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    retain(&store, &source, 0, 1);
    store.complete_operation(1).unwrap();
    assert!(memory.snapshot().reserved_bytes < 1 << 20);
    memory.set_limit_bytes(1 << 20).unwrap();
    assert_eq!(store.snapshot().retained_entries, 0);
    assert_eq!(store.snapshot().retention_limit_bytes, 1 << 16);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn source_segment_cache_larger_scans_preserve_a_reusable_bounded_prefix() {
    for (segment_bytes, segments, admitted) in [(1024, 140_u32, 128), (192 << 10, 4, 2)] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let store = Store::new(&memory).unwrap();
        let source = source();
        let input = input(segment_bytes);
        for id in 0..segments {
            let epoch = miss(&store, &source, id, 1);
            let outcome = store.retain(&source, id.into(), 1, epoch, &input).unwrap();
            assert_eq!(outcome.stored, id < admitted);
            if id >= admitted {
                assert_eq!(outcome.copied_bytes, 0);
            }
        }
        store.complete_operation(1).unwrap();
        let mut hits = 0;
        for id in 0..segments {
            match store.lookup(&source, id.into(), 2).unwrap() {
                Lookup::Hit { buffer, .. } => {
                    assert_eq!(buffer.as_slice(), input.as_host().as_slice());
                    hits += 1;
                }
                Lookup::Miss { epoch } => {
                    let outcome = store.retain(&source, id.into(), 2, epoch, &input).unwrap();
                    assert!(!outcome.stored);
                    assert_eq!(outcome.copied_bytes, 0);
                }
                Lookup::Disabled => panic!(),
            }
        }
        assert_eq!(hits, admitted);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.retained_entries, u64::from(admitted));
        assert_eq!(snapshot.evictions, 0);
        assert!(snapshot.retained_capacity_bytes <= snapshot.retention_limit_bytes);
        drop(store);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn source_segment_cache_contended_reclamation_declines_and_overflow_fails_explicitly() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let store = Store::new(&memory).unwrap();
    let source = source();
    retain(&store, &source, 0, 1);
    let guard = store.state.lock().unwrap();
    assert!(memory.reserve(8 << 20).is_err());
    drop(guard);
    let mandatory = memory.reserve(8 << 20).unwrap();
    drop(mandatory);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    store.state.lock().unwrap().clock = u64::MAX;
    assert!(store.lookup(&source, SegmentId::from(0), 2).is_err());
    store.state.lock().unwrap().epoch = u64::MAX;
    store.reclaim_optional_memory();
    assert!(store.validate_evidence().is_err());
}
