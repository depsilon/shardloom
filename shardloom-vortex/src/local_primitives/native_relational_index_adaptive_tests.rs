use super::*;

fn hash(row: usize) -> u64 {
    (row as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .rotate_left(19)
}

#[test]
fn adaptive_filter_preserves_every_member_and_exact_hash_collision_checks() {
    let active = CancellationToken::default();
    for count in [0, 1, 127, 128, 129, 4096] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let mut index = RowIndex::new(&memory).unwrap();
        for row in 0..count {
            index
                .insert(hash(row), row, &active, |old| Ok(old == row))
                .unwrap();
        }
        index.seal_join(true, &active).unwrap();
        let before = index.join_work().unwrap();
        assert_eq!(before.runtime_filter.admitted, u64::from(count >= 128));
        assert_eq!(before.runtime_filter.declined_small, u64::from(count < 128));
        for row in 0..count {
            assert_eq!(
                index
                    .find(hash(row), &active, |old| Ok(old == row))
                    .unwrap(),
                Some(row)
            );
        }
        for row in count..count + 4096 {
            assert_eq!(index.find(hash(row), &active, |_| Ok(false)).unwrap(), None);
        }
        let after = index.join_work().unwrap();
        if count >= 128 {
            assert_eq!(after.runtime_filter.probe_rows, (count + 4096) as u64);
            assert!(after.runtime_filter.rejected_rows > 3000);
            assert_eq!(
                after.runtime_filter.index_lookups_avoided,
                after.runtime_filter.rejected_rows
            );
        }
        assert!(
            index
                .insert(hash(9000), 9000, &active, |_| Ok(false))
                .is_err()
        );
        assert!(index.insert_unique(0, 0, &active, |_| Ok(true)).is_err());
        index.seal_join(true, &active).unwrap();
        assert_eq!(index.join_work().unwrap(), after);
        drop(index);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut index = RowIndex::new(&memory).unwrap();
    for row in 0..128 {
        index
            .insert(u64::MAX, row, &active, |old| Ok(old == row))
            .unwrap();
    }
    index.seal_join(true, &active).unwrap();
    for row in 0..128 {
        assert_eq!(
            index.find(u64::MAX, &active, |old| Ok(old == row)).unwrap(),
            Some(row)
        );
    }
    let mut equalities = 0;
    assert_eq!(
        index
            .find(u64::MAX, &active, |_| {
                equalities += 1;
                Ok(false)
            })
            .unwrap(),
        None
    );
    assert_eq!(equalities, 128);
    assert_eq!(index.join_work().unwrap().runtime_filter.rejected_rows, 0);
    drop(index);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn adaptive_skew_preserves_interleaved_duplicate_order_and_counts_consumed_rows() {
    let active = CancellationToken::default();
    for pressure in [false, true] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut index = RowIndex::new(&memory).unwrap();
        let mut keys = Vec::new();
        let mut expected = [Vec::new(), Vec::new(), Vec::new()];
        for round in 0..65 {
            for (key, limit) in [64, 63, 65].into_iter().enumerate() {
                if round >= limit {
                    continue;
                }
                let row = keys.len();
                keys.push(key);
                expected[key].push(row);
                index
                    .insert(key as u64, row, &active, |old| Ok(keys[old] == key))
                    .unwrap();
            }
        }
        let held = pressure.then(|| {
            memory
                .reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes - 32)
                .unwrap()
        });
        let prior = memory.snapshot().reserved_bytes;
        index.seal_join(true, &active).unwrap();
        let work = index.join_work().unwrap();
        assert_eq!((work.skew.hot_groups, work.skew.hot_rows), (2, 129));
        assert_eq!(work.skew.admitted_groups, if pressure { 0 } else { 2 });
        assert_eq!(work.skew.declined_memory, u64::from(pressure));
        if pressure {
            assert_eq!(memory.snapshot().reserved_bytes, prior);
        }
        for (entry, rows) in expected.iter().enumerate() {
            assert_eq!(index.rows(entry).unwrap().collect::<Vec<_>>(), *rows);
        }
        assert_eq!(
            index.rows(0).unwrap().take(7).collect::<Vec<_>>(),
            expected[0][..7]
        );
        let work = index.join_work().unwrap();
        assert_eq!(work.skew.range_visits, if pressure { 0 } else { 3 });
        assert_eq!(work.skew.rows_visited, if pressure { 0 } else { 136 });
        drop((held, index));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn adaptive_admission_refuses_pressure_and_cancelled_build_is_not_published() {
    let active = CancellationToken::default();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut index = RowIndex::new(&memory).unwrap();
    for row in 0..128 {
        index
            .insert(hash(row), row, &active, |old| Ok(old == row))
            .unwrap();
    }
    let before = memory.snapshot().reserved_bytes;
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(index.seal_join(true, &cancelled).is_err());
    assert_eq!(memory.snapshot().reserved_bytes, before);
    assert_eq!(index.join_work().unwrap(), NativeJoinWork::default());
    index
        .insert(hash(128), 128, &active, |_| Ok(false))
        .unwrap();
    let held = memory
        .reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes)
        .unwrap();
    index.seal_join(true, &active).unwrap();
    assert_eq!(index.join_work().unwrap().runtime_filter.declined_memory, 1);
    assert_eq!(
        index
            .find(hash(128), &active, |old| Ok(old == 128))
            .unwrap(),
        Some(128)
    );
    assert!(index.find(hash(128), &cancelled, |_| Ok(true)).is_err());
    drop((held, index));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn adaptive_counter_overflow_cannot_produce_a_completed_join_certificate() {
    let state = State::default();
    let mut work = NativeJoinWork::default();
    work.skew.rows_visited = u64::MAX;
    state.work.set(work);
    state.record_hot_row();
    assert!(state.overflow.get());
    assert_eq!(state.work.get(), work);
}
