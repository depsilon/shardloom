use super::super::aggregate_chunk_jobs::AggregateChunkJobs;
use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::{collections::BTreeMap, sync::mpsc, time::Duration};

struct Collected {
    values: Vec<Candidate>,
    rows: u64,
    groups: usize,
    truncated_partitions: usize,
    // Copied output remains charged after individual task receipts are released.
    lease: MemoryLease,
}

#[test]
fn optional_owner_reservation_declines_before_input_and_refunds_cleanly() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let pressure = memory.reserve(memory.snapshot().limit_bytes).unwrap();
    assert!(Partitions::<i64>::try_new(&memory).unwrap().is_none());
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(pressure);
    let parts = Partitions::<i64>::try_new(&memory).unwrap().unwrap();
    assert_eq!(parts.rows, 0);
    drop(parts);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn collect_next(jobs: &mut AggregateChunkJobs<Selection>, output: &mut Collected, cap: usize) {
    let completed = jobs.join_next().unwrap().unwrap();
    completed
        .consume(|selection| {
            output.rows = output.rows.checked_add(selection.rows).unwrap();
            output.groups += selection.groups;
            output.truncated_partitions += usize::from(selection.groups > cap);
            assert!(selection.retained.len() <= cap);
            assert!(output.values.len() + selection.retained.len() <= output.values.capacity());
            output.values.extend(selection.retained.iter().copied());
            Ok(())
        })
        .unwrap();
}

fn collect(
    mut partitions: NumericPartitions,
    memory: &LiveMemoryPool,
    parallelism: usize,
    cap: usize,
) -> Collected {
    let capacity = PARTITIONS.checked_mul(cap).unwrap();
    let lease = memory
        .reserve(bytes::<Candidate>(capacity).unwrap())
        .unwrap();
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).unwrap();
    assert_eq!(values.capacity(), capacity);
    let mut output = Collected {
        values,
        rows: 0,
        groups: 0,
        truncated_partitions: 0,
        lease,
    };
    let mut jobs = AggregateChunkJobs::new(parallelism, 4, 8 << 20, memory.clone()).unwrap();
    while let Some(part) = partitions.pop() {
        if jobs.is_full() {
            collect_next(&mut jobs, &mut output, cap);
        }
        jobs.submit(
            NumericPartition::output_bytes(cap).unwrap(),
            move |worker, _| part.reduce(cap, worker),
        )
        .unwrap();
    }
    while jobs.outstanding() != 0 {
        collect_next(&mut jobs, &mut output, cap);
    }
    assert_eq!(jobs.submitted(), u64::try_from(PARTITIONS).unwrap());
    assert_eq!(jobs.joined(), jobs.submitted());
    assert!(jobs.peak_outstanding() <= 4);
    drop(jobs);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, output.lease.bytes());
    output
}

fn oracle<K: NumericCountKey>(batches: &[Vec<(K, u64)>]) -> BTreeMap<K, u64> {
    let mut counts = BTreeMap::<K, u64>::new();
    for &(key, count) in batches.iter().flatten() {
        let previous = counts.entry(key).or_default();
        *previous = previous.checked_add(count).unwrap();
    }
    counts
}

fn verify_complete<K: NumericCountKey>(
    batches: &[Vec<(K, u64)>],
    wrap: fn(Partitions<K>) -> NumericPartitions,
) {
    let expected = oracle(batches);
    let expected_bits = expected
        .iter()
        .map(|(&key, &count)| {
            let key = key.aggregate_key();
            ((key.signed, key.bits), count)
        })
        .collect::<BTreeMap<_, _>>();
    let expected_rows = expected.values().copied().sum::<u64>();
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let mut parts = Partitions::<K>::new(&memory).unwrap();
        for batch in batches {
            parts.append(batch, || Ok(())).unwrap();
        }
        assert_eq!(parts.evidence().rows, expected_rows);
        assert_eq!(
            parts.evidence().entries,
            u64::try_from(batches.iter().map(Vec::len).sum::<usize>()).unwrap()
        );
        let output = collect(wrap(parts), &memory, parallelism, expected.len().max(1));
        assert_eq!(output.groups, expected.len());
        assert_eq!(output.rows, expected_rows);
        assert_eq!(output.values.len(), expected.len());
        let actual = output
            .values
            .iter()
            .map(|candidate| ((candidate.key.signed, candidate.key.bits), candidate.count))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected_bits);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn complete_i32_i64_and_u64_weighted_reduction_matches_all_groups_at_p1_and_p4() {
    verify_complete(
        &[
            vec![(i32::MIN, 5), (-1, 2), (0, 7), (i32::MAX, 4)],
            Vec::new(),
            vec![(i32::MAX, 8), (0, 3), (-1, 6), (i32::MIN, 9)],
        ],
        NumericPartitions::Signed32,
    );
    verify_complete(
        &[
            vec![(i64::MIN, 5), (-1, 2), (0, 7), (i64::MAX, 4)],
            Vec::new(),
            vec![(i64::MAX, 8), (0, 3), (-1, 6), (i64::MIN, 9)],
        ],
        NumericPartitions::Signed,
    );
    verify_complete(
        &[
            vec![(0_u64, 5), (1_u64 << 63, 2), (u64::MAX, 7)],
            Vec::new(),
            vec![(u64::MAX, 3), (1_u64 << 63, 6), (0, 9)],
        ],
        NumericPartitions::Unsigned,
    );
}

#[test]
fn empty_input_releases_all_typed_partition_owners() {
    verify_complete::<i32>(&[Vec::new()], NumericPartitions::Signed32);
    verify_complete::<i64>(&[Vec::new()], NumericPartitions::Signed);
    verify_complete::<u64>(&[Vec::new()], NumericPartitions::Unsigned);
}

fn verify_topk<K: NumericCountKey>(
    batches: &[Vec<(K, u64)>],
    wrap: fn(Partitions<K>) -> NumericPartitions,
) {
    let expected = oracle(batches);
    let mut ordered = expected
        .iter()
        .map(|(&key, &count)| (key, count))
        .collect::<Vec<_>>();
    // Independent total ordering: exact count descending, native integer ascending.
    ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let global_winner = ordered[0].0;
    for batch in batches {
        let &(local_key, local_count) = batch.iter().max_by_key(|pair| pair.1).unwrap();
        let global_count_here = batch.iter().find(|pair| pair.0 == global_winner).unwrap().1;
        assert_ne!(local_key, global_winner);
        assert!(local_count > global_count_here);
    }
    let offset = 3;
    let limit = 7;
    let cap = offset + limit;
    let expected_page = ordered
        .iter()
        .skip(offset)
        .take(limit)
        .map(|&(key, count)| {
            let key = key.aggregate_key();
            (key.signed, key.bits, count)
        })
        .collect::<Vec<_>>();
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let mut parts = Partitions::<K>::new(&memory).unwrap();
        for batch in batches {
            parts.append(batch, || Ok(())).unwrap();
        }
        let mut output = collect(wrap(parts), &memory, parallelism, cap);
        assert_eq!(output.groups, expected.len());
        assert_eq!(output.rows, expected.values().copied().sum::<u64>());
        assert!(output.truncated_partitions > 0);
        output.values.sort_by(compare_single_numeric_candidates);
        assert_eq!(output.values[0].key, global_winner.aggregate_key());
        assert_eq!(output.values[0].count, ordered[0].1);
        let page = output
            .values
            .iter()
            .skip(offset)
            .take(limit)
            .map(|candidate| (candidate.key.signed, candidate.key.bits, candidate.count))
            .collect::<Vec<_>>();
        assert_eq!(page, expected_page);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn complete_topk_preserves_cross_chunk_winner_and_integer_ties_before_offset() {
    let signed = (1_i64..=4)
        .map(|local| {
            let mut batch = vec![(local, 10), (0, 9), (i64::MIN, 1), (-1, 1), (i64::MAX, 1)];
            batch.extend((1000..2024).map(|key| (key, 1)));
            batch.reverse();
            batch
        })
        .collect::<Vec<_>>();
    verify_topk(&signed, NumericPartitions::Signed);
    let signed32 = signed
        .iter()
        .map(|batch| {
            batch
                .iter()
                .map(|&(key, count)| {
                    let key = match key {
                        i64::MIN => i32::MIN,
                        i64::MAX => i32::MAX,
                        key => i32::try_from(key).unwrap(),
                    };
                    (key, count)
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    verify_topk(&signed32, NumericPartitions::Signed32);
    let unsigned = signed
        .iter()
        .map(|batch| {
            batch
                .iter()
                .map(|&(key, count)| (u64::from_ne_bytes(key.to_ne_bytes()), count))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    verify_topk(&unsigned, NumericPartitions::Unsigned);
}

#[test]
fn zero_partial_weight_and_complete_weight_overflow_fail_without_wrapping() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let mut parts = Partitions::<i64>::new(&memory).unwrap();
    let error = parts.append(&[(1, 0)], || Ok(())).unwrap_err();
    assert!(error.to_string().contains("zero input weight"));
    let error = parts
        .append(&[(1, u64::MAX), (2, 1)], || Ok(()))
        .unwrap_err();
    assert!(error.to_string().contains("partial weight overflow"));
    assert_eq!(parts.rows, 0);
    assert_eq!(parts.entries, 0);
    assert!(parts.parts.iter().all(|part| part.pairs.is_empty()));
    parts.append(&[(1, u64::MAX)], || Ok(())).unwrap();
    assert_eq!(parts.rows, u64::MAX);
    let error = parts.append(&[(2, 1)], || Ok(())).unwrap_err();
    assert!(error.to_string().contains("complete weight overflow"));
    // An error terminates this committed attempt; no wrapped total is published.
    assert_eq!(parts.rows, u64::MAX);
    drop(parts);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn charged_partition(memory: &LiveMemoryPool, pairs: &[(i64, u64)]) -> NumericPartition {
    let lease = memory
        .reserve(bytes::<(i64, u64)>(pairs.len()).unwrap())
        .unwrap();
    let mut owned = Vec::new();
    owned.try_reserve_exact(pairs.len()).unwrap();
    assert_eq!(owned.capacity(), pairs.len());
    owned.extend_from_slice(pairs);
    NumericPartition::Signed(Partition {
        pairs: owned,
        lease,
    })
}

#[test]
fn reduction_checks_overflow_in_every_group_before_topk_can_discard_it() {
    for (pairs, expected_error) in [
        // Key 0 already fills top-1. The following duplicate group must still
        // fail checked addition instead of wrapping and losing to that winner.
        (vec![(0, u64::MAX), (1, u64::MAX), (1, 1)], "COUNT overflow"),
        (vec![(0, u64::MAX), (1, 1)], "partition weight overflow"),
    ] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let part = charged_partition(&memory, &pairs);
        let mut jobs = AggregateChunkJobs::<Selection>::new(1, 1, 8 << 20, memory.clone()).unwrap();
        let error = jobs
            .submit(
                NumericPartition::output_bytes(1).unwrap(),
                move |worker, _| part.reduce(1, worker),
            )
            .unwrap_err();
        assert!(error.to_string().contains(expected_error));
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn committed_vector_growth_denial_keeps_existing_weights_and_releases_credit() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let mut parts = Partitions::<i64>::new(&memory).unwrap();
    parts.append(&[(5, 1); 64], || Ok(())).unwrap();
    let slot = partition(5_i64);
    assert_eq!(parts.parts[slot].pairs.len(), 64);
    assert_eq!(parts.parts[slot].pairs.capacity(), 64);
    let before = memory.snapshot();
    let pressure = memory
        .reserve(before.limit_bytes - before.reserved_bytes)
        .unwrap();
    let error = parts.append(&[(5, 1)], || Ok(())).unwrap_err();
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(
        memory.snapshot().denied_reservations,
        before.denied_reservations + 1
    );
    assert_eq!(parts.rows, 64);
    assert_eq!(parts.entries, 64);
    assert_eq!(parts.parts[slot].pairs.as_slice(), &[(5, 1); 64]);
    drop(pressure);
    drop(parts);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn cancellation_after_committed_task_admission_joins_and_releases_payloads() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let mut parts = Partitions::<i64>::new(&memory).unwrap();
    let owner_bytes = memory.snapshot().reserved_bytes;
    parts.append(&[(7, 3); 128], || Ok(())).unwrap();
    assert_eq!(parts.rows, 384);
    let committed = std::mem::replace(
        &mut parts.parts[partition(7_i64)],
        Partition {
            pairs: Vec::new(),
            lease: memory.reserve(0).unwrap(),
        },
    );
    let part = NumericPartition::Signed(committed);
    let token = CancellationToken::default();
    let mut jobs = AggregateChunkJobs::<Selection>::with_cancellation(
        4,
        2,
        8 << 20,
        memory.clone(),
        token.clone(),
    )
    .unwrap();
    let (entered_send, entered_receive) = mpsc::channel();
    let (release_send, release_receive) = mpsc::channel();
    jobs.submit(
        NumericPartition::output_bytes(1).unwrap(),
        move |worker, _| {
            entered_send.send(()).unwrap();
            release_receive.recv().unwrap();
            part.reduce(1, worker)
        },
    )
    .unwrap();
    entered_receive
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    assert_eq!(jobs.submitted(), 1);
    assert_eq!(jobs.outstanding(), 1);
    assert!(memory.snapshot().reserved_bytes > owner_bytes);
    token.cancel();
    release_send.send(()).unwrap();
    assert!(jobs.join_next().is_err());
    drop(jobs);
    drop(parts);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
