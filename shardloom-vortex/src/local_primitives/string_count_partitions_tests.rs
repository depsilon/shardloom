use super::super::{aggregate_chunk_jobs::AggregateChunkJobs, string_count_partial};
use super::*;
use shardloom_exec::compute_pool::CancellationToken;
use std::collections::BTreeMap;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{ConstantArray, DictArray, PrimitiveArray, VarBinViewArray},
    validity::Validity,
};

fn strings(values: &[&str]) -> ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}

fn partial(array: &ArrayRef, memory: &LiveMemoryPool) -> StringCountPartial {
    let mut lease = memory
        .reserve(string_count_partial::partial_bytes(array).unwrap())
        .unwrap();
    string_count_partial::count_string_chunk(
        array,
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap()
}

fn selected(partitions: &StringCountPartitions) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::new();
    for index in 0..PARTITIONS {
        let result = partitions
            .select(
                index,
                &ChunkWorkerContext::Inline(CancellationToken::default()),
            )
            .unwrap();
        partitions
            .visit_selection(&result, |value, count| {
                assert!(values.insert(value.to_owned(), count).is_none());
                Ok(())
            })
            .unwrap();
    }
    values
}

fn reduce(
    partitions: &StringCountPartitions,
    partial: StringCountPartial,
    worker: &ChunkWorkerContext,
    memory: &LiveMemoryPool,
) -> Result<PartitionReceipt> {
    let mut lease = memory.reserve(StringCountPartial::deferred_metadata_bytes())?;
    partitions.reduce(partial, worker, &mut lease)
}

#[test]
fn complete_keys_find_winner_outside_every_chunk_topk_at_all_worker_counts() {
    for workers in [1, 2, 4, 8, 12] {
        let memory = LiveMemoryPool::new(8 << 20).unwrap();
        let partitions = StringCountPartitions::try_new(&memory, usize::MAX, 1)
            .unwrap()
            .unwrap();
        let mut jobs = AggregateChunkJobs::new(workers, 3, 8 << 20, memory.clone()).unwrap();
        for winner in ["alpha", "beta", "gamma"] {
            let mut rows = vec![winner; 6];
            rows.extend(["not a URL 東京"; 5]);
            let array = strings(&rows);
            let owned = Arc::clone(&partitions);
            jobs.submit(
                string_count_partial::partial_bytes(&array).unwrap()
                    + StringCountPartial::deferred_metadata_bytes(),
                move |worker, lease| {
                    let partial = string_count_partial::count_string_chunk(
                        &array,
                        vortex::array::legacy_session().create_execution_ctx(),
                        worker,
                        lease,
                    )?;
                    owned.reduce(partial, worker, lease)
                },
            )
            .unwrap();
        }
        while let Some(result) = jobs.join_next().unwrap() {
            result
                .consume(|receipt| {
                    assert!(receipt.deferred.is_none());
                    Ok(())
                })
                .unwrap();
        }
        let mut rows = selected(&partitions).into_iter().collect::<Vec<_>>();
        rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        assert_eq!(rows[0], ("not a URL 東京".to_owned(), 15));
        assert_eq!(partitions.group_count(), 4);
        assert_eq!(partitions.committed_rows.load(Ordering::Acquire), 33);
        drop(jobs);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_dictionary_domains_duplicate_values_and_constants_keep_exact_content_identity() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 100, 20)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let chunks = [
        DictArray::try_new(
            PrimitiveArray::new(vec![0_u8, 1, 2, 0], Validity::NonNullable).into_array(),
            strings(&["tea", "tea", "東京", "unused"]),
        )
        .unwrap()
        .into_array(),
        DictArray::try_new(
            PrimitiveArray::new(vec![0_u8, 1, 0], Validity::NonNullable).into_array(),
            strings(&["東京", "tea", "unused"]),
        )
        .unwrap()
        .into_array(),
        ConstantArray::new("tea", 17).into_array(),
        strings(&["", "λ\"\n", ""]),
    ];
    for array in chunks {
        let receipt = reduce(&partitions, partial(&array, &memory), &worker, &memory).unwrap();
        assert!(receipt.deferred.is_none());
    }
    assert_eq!(
        selected(&partitions),
        BTreeMap::from([
            ("tea".into(), 21),
            ("東京".into(), 3),
            ("".into(), 2),
            ("λ\"\n".into(), 1),
        ])
    );
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn full_hash_collisions_compare_bytes_and_count_overflow_preserves_previous_value() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 100, 10)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut partition = partitions.partitions[0].lock().unwrap();
    for (value, count) in [("alpha", 3), ("beta", 7), ("alpha", 11), ("max", u64::MAX)] {
        assert!(
            partition
                .update(value.as_bytes(), 0, count, &partitions, &worker)
                .unwrap()
        );
    }
    assert!(
        partition
            .update(b"max", 0, 1, &partitions, &worker)
            .unwrap_err()
            .to_string()
            .contains("overflow")
    );
    let index = partition
        .find(b"alpha", 0, &partitions.equality_comparisons)
        .unwrap();
    assert_eq!(partition.slots[index].count, 14);
    let index = partition
        .find(b"beta", 0, &partitions.equality_comparisons)
        .unwrap();
    assert_eq!(partition.slots[index].count, 7);
    let index = partition
        .find(b"max", 0, &partitions.equality_comparisons)
        .unwrap();
    assert_eq!(partition.slots[index].count, u64::MAX);
    drop(partition);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn entry_pressure_handoff_contains_every_weight_exactly_once_after_partial_progress() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 1, 1)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let first = reduce(
        &partitions,
        partial(&strings(&["tea", "coffee", "coffee", "東京"]), &memory),
        &worker,
        &memory,
    )
    .unwrap();
    let second = reduce(
        &partitions,
        partial(&strings(&["tea", "東京", "東京"]), &memory),
        &worker,
        &memory,
    )
    .unwrap();
    assert!(partitions.pressure_requested());
    assert_eq!(partitions.group_count(), 1);
    assert!(partitions.committed_rows.load(Ordering::Acquire) > 0);
    let mut all = BTreeMap::<String, u64>::new();
    partitions
        .replay_and_release(|value, count| {
            *all.entry(value.to_owned()).or_default() += count;
            Ok(())
        })
        .unwrap();
    for receipt in [first, second] {
        if let Some(partial) = receipt.deferred {
            partial
                .for_each_count(|value, count| {
                    *all.entry(value.to_owned()).or_default() += count;
                    Ok(())
                })
                .unwrap();
        }
    }
    assert_eq!(
        all,
        BTreeMap::from([("tea".into(), 2), ("coffee".into(), 2), ("東京".into(), 3)])
    );
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn deferred_suffix_keeps_payload_metadata_credit_after_task_and_partition_drop() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 0, 1)
        .unwrap()
        .unwrap();
    let receipt = reduce(
        &partitions,
        partial(&strings(&["tea", "coffee"]), &memory),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &memory,
    )
    .unwrap();
    let retained = Arc::clone(receipt.deferred.as_ref().unwrap());
    let expected =
        retained.work.partial_capacity_bytes + StringCountPartial::deferred_metadata_bytes();
    drop(receipt);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let mut count = 0;
    retained
        .for_each_count(|_, weight| {
            count += weight;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 2);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn byte_pressure_keeps_committed_table_and_cancellation_releases_owned_storage() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 100, 1)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut partition = partitions.partitions[0].lock().unwrap();
    assert!(
        partition
            .update(b"short", 0, 3, &partitions, &worker)
            .unwrap()
    );
    let snapshot = memory.snapshot();
    let blocker = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
        .unwrap();
    assert!(
        !partition
            .update(&[b'x'; 512], 0, 2, &partitions, &worker)
            .unwrap()
    );
    assert_eq!(partition.groups, 1);
    assert_eq!(partition.slots[0].count, 3);
    drop((blocker, partition));
    let input = partial(&strings(&["not a URL"]), &memory);
    let token = CancellationToken::default();
    token.cancel();
    assert!(
        reduce(
            &partitions,
            input,
            &ChunkWorkerContext::Inline(token),
            &memory
        )
        .is_err()
    );
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
