use super::*;
use std::collections::BTreeMap;

fn groups(table: &CompactStringCounts) -> BTreeMap<String, u64> {
    table
        .entries()
        .map(|(text, count)| (text.to_owned(), count))
        .collect()
}

#[test]
fn complete_counts_resolve_full_hash_collisions_and_copy_each_unique_string_once() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    let mut expected = BTreeMap::<String, u64>::new();
    for repeat in 0..3 {
        for key in 0..200 {
            let value = format!("renamed-{key}-東京");
            let count = repeat + 1;
            assert_eq!(
                table.update(&value, 0, count, || Ok(())).unwrap(),
                Admission::Ready(())
            );
            *expected.entry(value).or_default() += count;
        }
    }
    for value in ["", "λ", "not a URL", "a\0b"] {
        assert_eq!(
            table.update(value, 0, 7, || Ok(())).unwrap(),
            Admission::Ready(())
        );
        expected.insert(value.to_owned(), 7);
    }
    assert_eq!(groups(&table), expected);
    let work = table.evidence();
    assert_eq!(work.slot_bytes, 24);
    assert_eq!(
        work.payload_bytes_copied,
        expected.keys().map(|key| key.len() as u64).sum::<u64>()
    );
    assert!(work.full_hash_comparisons > work.updates);
    assert!(work.rehashed_slots > 0);
    assert!(work.directory_owner_moves > 0);
    assert_eq!(work.groups, 204);
    assert_eq!(work.rows, expected.values().sum::<u64>());
    assert_eq!(work.owned_bytes, memory.snapshot().reserved_bytes);
    drop(table);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_slice_clones_retain_full_slab_capacity_without_relocation() {
    let memory = LiveMemoryPool::new(8192).unwrap();
    let mut arena = SlabStringArena::new(&memory, 64).unwrap();
    let Admission::Ready(first) = arena.append("stable-東京").unwrap() else {
        panic!("admitted slab")
    };
    let owned = arena.retain(first);
    let pointer = owned.as_str().as_ptr();
    let clone = owned.clone();
    let Admission::Ready(second) = arena.append("next").unwrap() else {
        panic!("admitted slab")
    };
    assert_eq!(arena.slabs.len(), 2, "sharing seals the old slab tail");
    assert_eq!(owned.as_str().as_ptr(), pointer);
    assert_eq!(arena.get(first), "stable-東京");
    assert_eq!(arena.get(second), "next");
    assert_eq!(arena.bytes_copied, "stable-東京next".len() as u64);
    drop(arena);
    let retained = memory.snapshot().reserved_bytes;
    assert_eq!(
        retained,
        (64 + size_of::<Slab>() + 2 * size_of::<usize>()) as u64
    );
    drop(owned);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(clone.as_str(), "stable-東京");
    drop(memory);
    assert_eq!(clone.as_str().as_ptr(), pointer);
    drop(clone);
}

#[test]
fn compact_boundaries_fail_before_copying_and_keep_prior_values_exact() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let mut arena = SlabStringArena::with_address_limit(&memory, 16, 31).unwrap();
    let Admission::Ready(first) = arena.append("abcdefghijklmnop").unwrap() else {
        panic!()
    };
    let Admission::Ready(second) = arena.append("123456789012345").unwrap() else {
        panic!()
    };
    let before = arena.owned_bytes();
    assert_eq!(arena.append("x").unwrap(), Admission::OutsideCompactRange);
    assert_eq!(
        arena.append("over-sixteen-bytes").unwrap(),
        Admission::OutsideCompactRange
    );
    assert_eq!(arena.owned_bytes(), before);
    assert_eq!(arena.bytes_copied, 31);
    assert_eq!(arena.get(first), "abcdefghijklmnop");
    assert_eq!(arena.get(second), "123456789012345");
    drop(arena);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert!(SlabStringArena::new(&memory, 0).is_err());
    assert!(SlabStringArena::new(&memory, 31).is_err());
}

#[test]
fn exact_pressure_refunds_temporary_growth_and_retry_does_not_double_count() {
    let memory = LiveMemoryPool::new(65536).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    for key in 0..8_u64 {
        assert_eq!(
            table.update(&key.to_string(), key, 1, || Ok(())).unwrap(),
            Admission::Ready(())
        );
    }
    let before = groups(&table);
    let live = memory.snapshot().reserved_bytes;
    let blocker = memory.reserve(65536 - live).unwrap();
    assert_eq!(
        table.update("ninth", 9, 5, || Ok(())).unwrap(),
        Admission::Pressure
    );
    assert_eq!(groups(&table), before);
    assert_eq!(table.evidence().rows, 8);
    drop(blocker);
    assert_eq!(memory.snapshot().reserved_bytes, live);
    assert_eq!(
        table.update("ninth", 9, 5, || Ok(())).unwrap(),
        Admission::Ready(())
    );
    assert_eq!(table.evidence().rows, 13);
    let retained = table.retained_value("ninth").unwrap();
    drop(table);
    assert_eq!(retained.as_str(), "ninth");
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cancellation_during_rehash_preserves_committed_state_and_releases_new_table() {
    let memory = LiveMemoryPool::new(65536).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    for key in 0..8_u64 {
        table.update(&key.to_string(), key, 1, || Ok(())).unwrap();
    }
    let before = groups(&table);
    let live = memory.snapshot().reserved_bytes;
    let mut checks = 0;
    assert!(
        table
            .update("trigger-growth", 9, 1, || {
                checks += 1;
                if checks == 2 {
                    Err(failed("test cancellation"))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert_eq!(checks, 2);
    assert_eq!(groups(&table), before);
    assert_eq!(memory.snapshot().reserved_bytes, live);
    drop(table);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cancellation_inside_a_long_collision_chain_keeps_every_committed_key() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    // Seed a valid linear collision chain directly so the fixture does not
    // spend quadratic setup work repeatedly looking up its earlier keys.
    let (mut slots, lease) = allocate::<Slot>(16384, &memory, 0).unwrap().unwrap();
    slots.resize(16384, Slot::default());
    for (key, slot) in slots.iter_mut().take(5000).enumerate() {
        let Admission::Ready(text) = table.arena.append(&key.to_string()).unwrap() else {
            panic!()
        };
        *slot = Slot {
            hash: 0,
            count: 1,
            text,
        };
    }
    table.slots = slots;
    table.slots_lease = lease;
    table.groups = 5000;
    table.work.rows = 5000;
    table.work.updates = 5000;
    let before = groups(&table);
    let live = memory.snapshot().reserved_bytes;
    let mut checks = 0;
    assert!(
        table
            .update("missing", 0, 1, || {
                checks += 1;
                if checks == 2 {
                    Err(failed("collision cancellation"))
                } else {
                    Ok(())
                }
            })
            .is_err()
    );
    assert_eq!(checks, 2);
    assert_eq!(groups(&table), before);
    assert_eq!(table.evidence().rows, 5000);
    assert_eq!(memory.snapshot().reserved_bytes, live);
    drop(table);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn later_slab_denial_keeps_successful_table_growth_owned_without_committing_rows() {
    let memory = LiveMemoryPool::new(65536).unwrap();
    let mut table = CompactStringCounts::new(&memory, 1024).unwrap();
    for key in 0..8_u64 {
        table
            .update(&format!("{key:0128}"), key, 1, || Ok(()))
            .unwrap();
    }
    let before = groups(&table);
    let live = memory.snapshot().reserved_bytes;
    let replacement_bytes = (32 * size_of::<Slot>()) as u64;
    let blocker = memory.reserve(65536 - live - replacement_bytes).unwrap();
    assert_eq!(
        table.update("ninth", 9, 5, || Ok(())).unwrap(),
        Admission::Pressure
    );
    assert_eq!(table.slots.len(), 32, "successful growth remains admitted");
    assert_eq!(groups(&table), before);
    assert_eq!(table.evidence().rows, 8);
    assert_eq!(table.evidence().payload_bytes_copied, 1024);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        table.evidence().owned_bytes + blocker.bytes()
    );
    drop(blocker);
    assert_eq!(
        table.update("ninth", 9, 5, || Ok(())).unwrap(),
        Admission::Ready(())
    );
    assert_eq!(table.evidence().rows, 13);
    assert_eq!(table.evidence().payload_bytes_copied, 1029);
    drop(table);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn zero_weight_and_overflow_are_explicit_without_committing_a_partial_update() {
    let memory = LiveMemoryPool::new(8192).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    assert!(table.update("x", 1, 0, || Ok(())).is_err());
    assert!(table.entries().next().is_none());
    assert_eq!(
        table.update("x", 1, u64::MAX, || Ok(())).unwrap(),
        Admission::Ready(())
    );
    assert!(table.update("x", 1, 1, || Ok(())).is_err());
    assert!(table.update("another", 2, 1, || Ok(())).is_err());
    assert_eq!(groups(&table), BTreeMap::from([("x".to_owned(), u64::MAX)]));
}

#[test]
fn same_dictionary_code_in_unrelated_domains_is_never_a_semantic_string_key() {
    let memory = LiveMemoryPool::new(65536).unwrap();
    let mut table = CompactStringCounts::new(&memory, 64).unwrap();
    let domains = [["left", "right"], ["right", "left"]];
    for domain in domains {
        // The owner resolves codes before calling the table; index zero alone
        // cannot equate the first entries of these different domains.
        for code in [0, 0, 1] {
            table.update(domain[code], 0, 1, || Ok(())).unwrap();
        }
    }
    assert_eq!(
        groups(&table),
        BTreeMap::from([("left".to_owned(), 3), ("right".to_owned(), 3)])
    );
    assert_eq!(table.evidence().payload_bytes_copied, 9);
}
