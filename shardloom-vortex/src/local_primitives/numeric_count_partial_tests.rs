use super::*;
use crate::local_primitives::aggregate_chunk_jobs::AggregateChunkJobs;
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier, mpsc},
};

fn signed_chunks() -> Vec<Vec<i64>> {
    (1..=17)
        .map(|key| {
            // Key 0 wins globally while losing to a different local key in
            // every chunk. Local top-K pruning would lose the correct winner.
            let mut values = vec![key; 18];
            values.extend(std::iter::repeat_n(0, 17));
            values.extend([i64::MIN, i64::MAX, (1_i64 << 60) + key, -key]);
            values.reverse();
            values
        })
        .collect()
}

fn serial_oracle<K: NumericCountKey>(chunks: &[Vec<K>]) -> Vec<(K, u64)> {
    let mut groups = BTreeMap::<K, u64>::new();
    for &key in chunks.iter().flatten() {
        *groups.entry(key).or_default() += 1;
    }
    groups.into_iter().collect()
}

fn verify_workers<K: NumericCountKey>(chunks: &[Vec<K>]) {
    let expected = serial_oracle(chunks);
    let rows = chunks.iter().map(Vec::len).sum::<usize>() as u64;
    for workers in [1, 2, 4, 8, 12] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut jobs = AggregateChunkJobs::new(workers, 3, 1 << 20, memory.clone()).unwrap();
        let mut merged = OwnedNumericCounts::empty(&memory).unwrap();
        let mut ordinal = 0;
        for values in chunks {
            if jobs.is_full() {
                let completed = jobs.join_next().unwrap().unwrap();
                assert_eq!(completed.ordinal(), ordinal);
                ordinal += 1;
                merged = completed
                    .consume(|partial| merge_numeric_counts(&merged, partial, &memory, || Ok(())))
                    .unwrap();
            }
            let admitted = partial_bytes::<K>(values.len()).unwrap();
            // The task owns input across the worker's 'static boundary.
            let values = values.clone();
            jobs.submit(admitted, move |context, lease| {
                count_numeric_values(&values, context, lease)
            })
            .unwrap();
        }
        while let Some(completed) = jobs.join_next().unwrap() {
            assert_eq!(completed.ordinal(), ordinal);
            ordinal += 1;
            merged = completed
                .consume(|partial| merge_numeric_counts(&merged, partial, &memory, || Ok(())))
                .unwrap();
        }
        assert_eq!(merged.pairs(), expected);
        assert_eq!(merged.rows(), rows);
        assert!(merged.reserved_bytes() >= partial_bytes::<K>(expected.len()).unwrap());
        assert_eq!(jobs.submitted(), chunks.len() as u64);
        assert_eq!(jobs.joined(), chunks.len() as u64);
        assert!(jobs.peak_outstanding() <= 3);
        assert_eq!(jobs.outstanding(), 0);
        // The completed global result owns memory after all workers are gone.
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, merged.reserved_bytes());
        drop(merged);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn signed_extrema_skew_and_ties_match_serial_for_every_worker_count() {
    let chunks = signed_chunks();
    let oracle = serial_oracle(&chunks);
    assert_eq!(oracle.iter().max_by_key(|pair| pair.1), Some(&(0, 17 * 17)));
    assert_eq!(oracle.first().unwrap().0, i64::MIN);
    assert_eq!(oracle.last().unwrap().0, i64::MAX);
    verify_workers(&chunks);
}

#[test]
fn unsigned_high_bits_and_empty_chunks_match_serial_for_every_worker_count() {
    let mut chunks = signed_chunks()
        .iter()
        .map(|values| {
            values
                .iter()
                .map(|&value| u64::from_ne_bytes(value.to_ne_bytes()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    chunks.insert(0, Vec::new());
    chunks.push(vec![u64::MAX, 1_u64 << 63, (1_u64 << 63) + 1]);
    verify_workers(&chunks);
}

#[test]
fn delayed_numeric_completion_keeps_source_order_and_exact_counts() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let mut jobs = AggregateChunkJobs::new(4, 2, 4096, memory.clone()).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let first_barrier = Arc::clone(&barrier);
    let (send, receive) = mpsc::channel();
    jobs.submit(partial_bytes::<i64>(4).unwrap(), move |context, lease| {
        first_barrier.wait();
        receive.recv().unwrap();
        count_numeric_values(&[5, 5, i64::MIN, 9], context, lease)
    })
    .unwrap();
    jobs.submit(partial_bytes::<i64>(4).unwrap(), move |context, lease| {
        barrier.wait();
        let result = count_numeric_values(&[5, 9, 9, i64::MAX], context, lease)?;
        send.send(()).unwrap();
        Ok(result)
    })
    .unwrap();
    let mut merged = OwnedNumericCounts::empty(&memory).unwrap();
    for expected_ordinal in 0..2 {
        let completed = jobs.join_next().unwrap().unwrap();
        assert_eq!(completed.ordinal(), expected_ordinal);
        assert_eq!(completed.value().rows(), 4);
        // Output credit was split away from the now-finished task envelope.
        assert_eq!(completed.reserved_bytes(), 0);
        merged = completed
            .consume(|partial| merge_numeric_counts(&merged, partial, &memory, || Ok(())))
            .unwrap();
    }
    assert_eq!(
        merged.pairs(),
        &[(i64::MIN, 1), (5, 3), (9, 3), (i64::MAX, 1)]
    );
    drop((merged, jobs));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn partial<K: NumericCountKey>(memory: &LiveMemoryPool, values: &[K]) -> OwnedNumericCounts<K> {
    let mut lease = memory
        .reserve(partial_bytes::<K>(values.len()).unwrap())
        .unwrap();
    count_numeric_values(
        values,
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap()
}

#[test]
fn denied_merge_preserves_inputs_and_cancelled_work_returns_credits() {
    let memory = LiveMemoryPool::new(64).unwrap();
    let left = partial(&memory, &[i64::MIN, 4]);
    let right = partial(&memory, &[4, i64::MAX]);
    assert!(merge_numeric_counts(&left, &right, &memory, || Ok(())).is_err());
    assert_eq!(left.pairs(), &[(i64::MIN, 1), (4, 1)]);
    assert_eq!(right.pairs(), &[(4, 1), (i64::MAX, 1)]);
    assert_eq!(memory.snapshot().reserved_bytes, 64);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop((left, right));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let mut too_small = memory.reserve(15).unwrap();
    assert!(
        count_numeric_values(
            &[7_i64],
            &ChunkWorkerContext::Inline(CancellationToken::default()),
            &mut too_small,
        )
        .is_err()
    );
    assert_eq!(too_small.bytes(), 15);
    drop(too_small);
    let token = CancellationToken::default();
    token.cancel();
    let mut lease = memory.reserve(16).unwrap();
    assert!(
        count_numeric_values(&[7_i64], &ChunkWorkerContext::Inline(token), &mut lease).is_err()
    );
    assert_eq!(lease.bytes(), 16);
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn overflow_and_foreign_pool_merge_never_expose_wrapped_counts() {
    let memory = LiveMemoryPool::new(1024).unwrap();
    let mut lease = memory.reserve(partial_bytes::<u64>(1).unwrap()).unwrap();
    let full = count_numeric_constant(
        7_u64,
        u64::MAX,
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap();
    let one = partial(&memory, &[7_u64]);
    assert!(merge_numeric_counts(&full, &one, &memory, || Ok(())).is_err());
    assert_eq!(full.pairs(), &[(7, u64::MAX)]);
    assert_eq!(one.pairs(), &[(7, 1)]);
    assert!(checked_count(u64::MAX, 1).is_err());
    assert_eq!(checked_count(u64::MAX, 0).unwrap(), u64::MAX);
    assert!(partial_bytes::<u64>(usize::MAX).is_err());
    let foreign = LiveMemoryPool::new(1024).unwrap();
    assert!(merge_numeric_counts(&full, &one, &foreign, || Ok(())).is_err());
    assert_eq!(foreign.snapshot().reserved_bytes, 0);
    drop((full, one));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn constant_counts_preserve_weight_without_expanding_rows() {
    let memory = LiveMemoryPool::new(16).unwrap();
    let mut lease = memory.reserve(16).unwrap();
    let context = ChunkWorkerContext::Inline(CancellationToken::default());
    let constant = count_numeric_constant(i64::MIN, u64::MAX, &context, &mut lease).unwrap();
    assert_eq!(constant.pairs(), &[(i64::MIN, u64::MAX)]);
    assert_eq!(constant.rows(), u64::MAX);
    assert_eq!(constant.reserved_bytes(), 16);
    assert_eq!(lease.bytes(), 0);
    drop(constant);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let empty = count_numeric_constant(i64::MIN, 0, &context, &mut lease).unwrap();
    assert!(empty.pairs().is_empty());
    assert_eq!(empty.rows(), 0);
    assert_eq!(empty.reserved_bytes(), 0);
}

#[test]
fn cancellation_during_merge_releases_new_capacity_and_preserves_inputs() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let left = partial(&memory, &(0_i64..4096).step_by(2).collect::<Vec<_>>());
    let right = partial(&memory, &(1_i64..4096).step_by(2).collect::<Vec<_>>());
    let retained = memory.snapshot().reserved_bytes;
    let checks = std::cell::Cell::new(0);
    let result = merge_numeric_counts(&left, &right, &memory, || {
        checks.set(checks.get() + 1);
        if checks.get() == 3 {
            Err(failed("injected cancellation during merge"))
        } else {
            Ok(())
        }
    });
    assert!(result.is_err());
    assert_eq!(checks.get(), 3);
    assert!(memory.snapshot().peak_reserved_bytes > retained);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    assert_eq!(left.rows(), 2048);
    assert_eq!(right.rows(), 2048);
    drop((left, right));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn pool_admission_denial_and_early_drop_leave_no_numeric_output_credits() {
    let memory = LiveMemoryPool::new(63).unwrap();
    for workers in [1, 2, 4, 8, 12] {
        let mut jobs = AggregateChunkJobs::new(workers, 2, 63, memory.clone()).unwrap();
        assert!(
            jobs.submit(64, |context, lease| {
                count_numeric_values(&[1_i64, 2, 3, 4], context, lease)
            })
            .is_err()
        );
        jobs.submit(32, |context, lease| {
            count_numeric_values(&[1_i64, 1], context, lease)
        })
        .unwrap();
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
