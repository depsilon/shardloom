use super::{
    AggregateIntegerKeyPart, ChunkWorkerContext, Insert, Pair, PairPartial, PairSet, Slot, count,
    partitions,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::collections::BTreeMap;
use std::sync::Arc;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{DictArray, PrimitiveArray},
        validity::Validity,
    },
    session::VortexSession,
};

fn worker() -> ChunkWorkerContext {
    ChunkWorkerContext::Inline(CancellationToken::default())
}

fn logical(key: AggregateIntegerKeyPart) -> i128 {
    if key.signed {
        i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
    } else {
        i128::from(key.bits)
    }
}

fn key(group: u64, value: u64) -> Pair {
    Pair::new(
        AggregateIntegerKeyPart {
            bits: group,
            signed: false,
        },
        AggregateIntegerKeyPart {
            bits: value,
            signed: false,
        },
    )
}

fn collect(pairs: &PairSet) -> BTreeMap<(i128, i128), u64> {
    let mut output = BTreeMap::new();
    pairs
        .visit(|pair, weight| {
            assert!(
                output
                    .insert((logical(pair.group()), logical(pair.value())), weight)
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
    output
}

fn collect_partial(partial: &PairPartial) -> BTreeMap<(i128, i128), u64> {
    let mut output = BTreeMap::new();
    partial
        .visit(|pair, weight| {
            assert!(
                output
                    .insert((logical(pair.group()), logical(pair.value())), weight)
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
    output
}

fn integer_fixtures() -> Vec<(ArrayRef, [i128; 5])> {
    macro_rules! fixture {
        ($t:ty) => {{
            let values: [$t; 5] = [<$t>::MIN, 7, <$t>::MAX, 7, <$t>::MAX];
            (
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
                values.map(i128::from),
            )
        }};
    }
    vec![
        fixture!(i8),
        fixture!(i16),
        fixture!(i32),
        fixture!(i64),
        fixture!(u8),
        fixture!(u16),
        fixture!(u32),
        fixture!(u64),
    ]
}

#[test]
fn exact_distinct_partial_all_width_pairs_extrema_and_dictionary_domains() {
    let fixtures = integer_fixtures();
    for (group, groups) in &fixtures {
        for (value, values) in &fixtures {
            for value in [
                value.clone(),
                DictArray::try_new(
                    PrimitiveArray::new(vec![4_u8, 3, 2, 1, 0], Validity::NonNullable).into_array(),
                    value
                        .take(
                            PrimitiveArray::new(vec![4_u8, 3, 2, 1, 0], Validity::NonNullable)
                                .into_array(),
                        )
                        .unwrap(),
                )
                .unwrap()
                .into_array(),
            ] {
                let memory = LiveMemoryPool::new(1 << 20).unwrap();
                let partial = count(
                    group,
                    &value,
                    VortexSession::default().create_execution_ctx(),
                    &worker(),
                    &memory,
                )
                .unwrap();
                let mut expected = BTreeMap::new();
                for pair in groups.iter().copied().zip(values.iter().copied()) {
                    *expected.entry(pair).or_insert(0_u64) += 1;
                }
                assert_eq!(collect_partial(&partial), expected);
                assert_eq!(partial.rows, 5);
                drop(partial);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn exact_distinct_partial_slices_empty_and_rejects_nullable_misaligned_inputs() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let group =
        PrimitiveArray::new(vec![999_i16, -2, -2, 7, 999], Validity::NonNullable).into_array();
    let value =
        PrimitiveArray::new(vec![999_u32, 3, 3, 9, 999], Validity::NonNullable).into_array();
    let session = VortexSession::default();
    let partial = count(
        &group.slice(1..4).unwrap(),
        &value.slice(1..4).unwrap(),
        session.create_execution_ctx(),
        &worker(),
        &memory,
    )
    .unwrap();
    assert_eq!(
        collect_partial(&partial),
        BTreeMap::from([((-2, 3), 2), ((7, 9), 1)])
    );
    drop(partial);
    let partial = count(
        &group.slice(0..0).unwrap(),
        &value.slice(0..0).unwrap(),
        session.create_execution_ctx(),
        &worker(),
        &memory,
    )
    .unwrap();
    assert!(collect_partial(&partial).is_empty());
    drop(partial);
    for bad in [
        PrimitiveArray::from_option_iter([Some(1_i64), None, Some(2)]).into_array(),
        PrimitiveArray::new(vec![1_f64, 2.0, 3.0], Validity::NonNullable).into_array(),
        value.slice(0..2).unwrap(),
    ] {
        let error = count(
            &group.slice(1..4).unwrap(),
            &bad,
            session.create_execution_ctx(),
            &worker(),
            &memory,
        )
        .err()
        .unwrap();
        assert!(
            error
                .to_string()
                .contains("requires aligned nonnullable integer")
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_pair_collisions_entry_denial_and_overflow_preserve_identity() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut pairs = PairSet::new(&memory).unwrap();
    for index in 0..40 {
        assert_eq!(
            pairs
                .insert_hashed(key(index, u64::MAX - index), 0, 1, true, &worker())
                .unwrap(),
            Insert::Applied { new_pair: true }
        );
    }
    assert_eq!(pairs.pairs, 40);
    assert_eq!(
        pairs
            .insert_hashed(key(39, u64::MAX - 39), 0, 4, false, &worker())
            .unwrap(),
        Insert::Applied { new_pair: false }
    );
    assert_eq!(
        pairs
            .insert_hashed(key(100, 100), 0, 1, false, &worker())
            .unwrap(),
        Insert::NeedsEntryCredit
    );
    assert_eq!(pairs.rows, 44);
    let before = collect(&pairs);
    assert!(pairs.insert(key(1, 1), 0, true, &worker()).is_err());
    assert!(pairs.insert(key(1, 1), u64::MAX, true, &worker()).is_err());
    assert_eq!(collect(&pairs), before);
    assert!(pairs.comparisons > 40);
    drop(pairs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_pair_growth_pressure_and_cancel_release_all_capacity() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut pairs = PairSet::new(&memory).unwrap();
    for index in 0..8 {
        assert_eq!(
            pairs.insert(key(index, index), 1, true, &worker()).unwrap(),
            Insert::Applied { new_pair: true }
        );
    }
    let before = collect(&pairs);
    let occupied = memory.snapshot().reserved_bytes;
    let held = memory
        .reserve(memory.snapshot().limit_bytes - occupied)
        .unwrap();
    assert_eq!(
        pairs.insert(key(8, 8), 1, true, &worker()).unwrap(),
        Insert::BytePressure
    );
    assert_eq!(collect(&pairs), before);
    assert_eq!(
        pairs.insert(key(0, 0), 3, false, &worker()).unwrap(),
        Insert::Applied { new_pair: false }
    );
    drop(held);
    let token = CancellationToken::default();
    token.cancel();
    let cancelled = ChunkWorkerContext::Inline(token);
    assert!(pairs.insert(key(8, 8), 1, true, &cancelled).is_err());
    assert_eq!(pairs.pairs, 8);
    assert_eq!(memory.snapshot().reserved_bytes, occupied);
    assert_eq!(
        pairs.insert(key(8, 8), 1, true, &worker()).unwrap(),
        Insert::Applied { new_pair: true }
    );
    assert!(memory.snapshot().peak_reserved_bytes > memory.snapshot().reserved_bytes);
    pairs.clear().unwrap();
    assert_eq!(pairs.rows, 0);
    assert!(collect(&pairs).is_empty());
    drop(pairs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_collision_chain_cancellation_bounds_lookup_and_rehash() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut pairs = PairSet::new(&memory).unwrap();
    let capacity = 16384;
    pairs
        .lease
        .resize((capacity * size_of::<Slot>()) as u64)
        .unwrap();
    pairs.slots = vec![Slot::default(); capacity];
    for index in 0..4097 {
        pairs.slots[index] = Slot {
            pair: key(index as u64, 1),
            hash: 0,
            weight: 1,
        };
    }
    pairs.rows = 4097;
    pairs.pairs = 4097;
    let original = collect(&pairs);
    let live = memory.snapshot().reserved_bytes;
    let mut checks = 0;
    let error = pairs
        .find(key(u64::MAX, 1), 0, &mut || {
            checks += 1;
            if checks == 2 {
                Err(super::failed("injected collision cancellation"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(error.to_string().contains("collision cancellation"));
    assert_eq!(pairs.comparisons, 4096);
    assert_eq!(collect(&pairs), original);
    checks = 0;
    let error = pairs
        .grow(&mut || {
            checks += 1;
            // Slot 0, slot 4096, then that slot's 4096-probe collision chain.
            if checks == 3 {
                Err(super::failed("injected rehash cancellation"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert!(error.to_string().contains("rehash cancellation"));
    assert_eq!(checks, 3);
    assert_eq!(collect(&pairs), original);
    assert_eq!(memory.snapshot().reserved_bytes, live);
    drop(pairs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn pairs_partial(rows: &[(u64, u64)], memory: &LiveMemoryPool) -> PairPartial {
    count(
        &PrimitiveArray::new(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        &PrimitiveArray::new(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            Validity::NonNullable,
        )
        .into_array(),
        VortexSession::default().create_execution_ctx(),
        &worker(),
        memory,
    )
    .unwrap()
}

fn group_counts(groups: &partitions::GroupCounts) -> BTreeMap<i128, u64> {
    let mut result = BTreeMap::new();
    groups
        .visit(|key, count| {
            assert!(result.insert(logical(key), count).is_none());
            Ok(())
        })
        .unwrap();
    result
}

#[test]
fn exact_distinct_complete_partitions_workers_global_winner_cross_chunk_duplicates_and_ties() {
    use super::super::aggregate_chunk_jobs::AggregateChunkJobs;
    for workers in [1, 2, 4] {
        for collisions in [false, true] {
            let memory = LiveMemoryPool::new(8 << 20).unwrap();
            let partitions = partitions::ExactDistinctPartitions::try_new(&memory, 10_000)
                .unwrap()
                .unwrap();
            let mut jobs = AggregateChunkJobs::new(workers, 4, 8 << 20, memory.clone()).unwrap();
            let mut expected = BTreeMap::<u64, std::collections::BTreeSet<u64>>::new();
            let mut rows_seen = 0;
            for batch in 0_u64..4 {
                // Each local winner has 40 distinct values; group 99 loses in
                // every input batch but wins after its full contributions meet.
                let rows = (0..40)
                    .map(|value| (batch + 1, value))
                    .chain((0..30).map(|value| {
                        (
                            99,
                            if value < 10 {
                                value
                            } else {
                                batch * 30 + value
                            },
                        )
                    }))
                    .flat_map(|pair| [pair, pair])
                    .collect::<Vec<_>>();
                rows_seen += rows.len() as u64;
                for &(group, value) in &rows {
                    expected.entry(group).or_default().insert(value);
                }
                let mut partial = pairs_partial(&rows, &memory);
                if collisions {
                    for slot in &mut partial.slots {
                        slot.hash = 0;
                    }
                }
                let owned = Arc::clone(&partitions);
                jobs.submit(256, move |worker, _lease| owned.reduce(partial, worker))
                    .unwrap();
            }
            while let Some(job) = jobs.join_next().unwrap() {
                job.consume(|receipt| {
                    assert!(receipt.deferred.is_none());
                    assert_eq!(receipt.source_rows, 140);
                    assert_eq!(receipt.partial_pairs, 70);
                    Ok(())
                })
                .unwrap();
            }
            let expected = expected
                .into_iter()
                .map(|(group, values)| (i128::from(group), values.len() as u64))
                .collect::<BTreeMap<_, _>>();
            let groups = partitions.finish_groups(100, &worker()).unwrap().unwrap();
            assert_eq!(group_counts(&groups), expected);
            let mut ordered = group_counts(&groups).into_iter().collect::<Vec<_>>();
            ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            assert_eq!(ordered[0], (99, 90));
            assert_eq!(&ordered[1..3], &[(1, 40), (2, 40)]);
            let evidence = partitions.evidence().unwrap();
            assert_eq!(evidence.committed_rows, rows_seen);
            assert_eq!(
                evidence.pairs,
                usize::try_from(expected.values().sum::<u64>()).unwrap()
            );
            assert!(evidence.entry_claims != 0);
            assert_eq!(evidence.entry_claims, evidence.entry_returns);
            drop(groups);
            drop(partitions);
            drop(jobs);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn exact_distinct_pressure_preserves_weighted_complete_pair_prefix_and_suffix() {
    for entry_limit in [1, 7, 1000] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let partitions = partitions::ExactDistinctPartitions::try_new(&memory, entry_limit)
            .unwrap()
            .unwrap();
        let rows = (0_u64..40)
            .flat_map(|value| [(value % 3, value); 2])
            .collect::<Vec<_>>();
        let mut partial = pairs_partial(&rows, &memory);
        for slot in &mut partial.slots {
            slot.hash = 0;
        }
        let held = if entry_limit == 1000 {
            let free = memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes;
            Some(
                memory
                    .reserve(free - (16 * size_of::<Slot>()) as u64)
                    .unwrap(),
            )
        } else {
            None
        };
        let receipt = partitions.reduce(partial, &worker()).unwrap();
        assert!(partitions.pressured());
        let mut replayed = BTreeMap::new();
        partitions
            .replay_and_release(|pair, weight| {
                *replayed
                    .entry((logical(pair.group()), logical(pair.value())))
                    .or_insert(0_u64) += weight;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            replayed.values().sum::<u64>(),
            partitions.evidence().unwrap().committed_rows
        );
        receipt
            .deferred
            .as_ref()
            .unwrap()
            .visit(|pair, weight| {
                *replayed
                    .entry((logical(pair.group()), logical(pair.value())))
                    .or_insert(0) += weight;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            replayed,
            (0_i128..40).map(|value| ((value % 3, value), 2)).collect()
        );
        // Weighted handoff is used only for row-accounting proof. The old
        // distinct reducer unions each identity once, not its duplicate weight.
        let mut distinct = BTreeMap::new();
        for &(group, _) in replayed.keys() {
            *distinct.entry(group).or_insert(0_u64) += 1;
        }
        assert_eq!(distinct, BTreeMap::from([(0, 14), (1, 13), (2, 13)]));
        drop(receipt);
        drop(held);
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_final_group_pressure_preserves_pairs_for_replay_and_retry() {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let partitions = partitions::ExactDistinctPartitions::try_new(&memory, 100)
        .unwrap()
        .unwrap();
    let rows = [(1, 2), (1, 2), (1, 3), (2, 2), (3, 2)];
    assert!(
        partitions
            .reduce(pairs_partial(&rows, &memory), &worker())
            .unwrap()
            .deferred
            .is_none()
    );
    let retained = memory.snapshot().reserved_bytes;
    assert!(partitions.finish_groups(2, &worker()).unwrap().is_none());
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let held = memory
        .reserve(memory.snapshot().limit_bytes - retained)
        .unwrap();
    assert!(partitions.finish_groups(10, &worker()).unwrap().is_none());
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    let groups = partitions.finish_groups(10, &worker()).unwrap().unwrap();
    assert_eq!(
        group_counts(&groups),
        BTreeMap::from([(1, 2), (2, 1), (3, 1)])
    );
    let mut replayed = BTreeMap::new();
    partitions
        .replay_and_release(|pair, weight| {
            replayed.insert((logical(pair.group()), logical(pair.value())), weight);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        replayed,
        BTreeMap::from([((1, 2), 2), ((1, 3), 1), ((2, 2), 1), ((3, 2), 1)])
    );
    drop(partitions);
    assert_eq!(
        group_counts(&groups),
        BTreeMap::from([(1, 2), (2, 1), (3, 1)])
    );
    drop(groups);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_final_selection_is_globally_bounded_and_owns_capacity() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let partitions = partitions::ExactDistinctPartitions::try_new(&memory, 20_000)
        .unwrap()
        .unwrap();
    let rows = (0..2048_u64)
        .flat_map(|group| (0..=(group % 7)).map(move |value| (group, value)))
        .collect::<Vec<_>>();
    assert!(
        partitions
            .reduce(pairs_partial(&rows, &memory), &worker())
            .unwrap()
            .deferred
            .is_none()
    );
    let groups = partitions.finish_groups(3000, &worker()).unwrap().unwrap();
    let mut oracle = group_counts(&groups).into_iter().collect::<Vec<_>>();
    oracle.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let before = memory.snapshot().reserved_bytes;
    let held = memory
        .reserve(memory.snapshot().limit_bytes - before)
        .unwrap();
    assert!(groups.select(19, &worker()).unwrap().is_none());
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, before);
    let selected = groups.select(19, &worker()).unwrap().unwrap();
    let mut actual = Vec::new();
    selected
        .visit(|group, count| {
            actual.push((logical(group), count));
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, oracle[..19]);
    assert_eq!(&actual[16..19], &oracle[16..19]);
    assert_eq!(selected.group_count, 2048);
    assert_eq!(selected.retained_count(), 19);
    drop(groups);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, selected.reserved_bytes());
    drop(selected);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
