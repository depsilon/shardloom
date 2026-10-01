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

fn admission(partitions: &StringCountPartitions, requested: usize) -> EntryAdmission<'_> {
    let Claim::Block(block) = partitions
        .entry_credits
        .claim(requested, || Ok(true))
        .unwrap()
    else {
        panic!("test credit block unavailable");
    };
    EntryAdmission {
        block: Some(block),
        exhausted: false,
    }
}

#[test]
fn tagged_directory_checks_encoding_bounds_and_keeps_zero_tag_occupied() {
    assert_eq!(size_of::<DirectoryEntry>(), 8);
    assert!(DirectoryEntry::default().is_empty());
    let largest = usize::try_from(ORDINAL_MASK - 1).unwrap_or(usize::MAX);
    for ordinal in [0, 1, largest] {
        for hash in [0, 1, 0x9876_1234_5678_9abc, u64::MAX] {
            let entry = DirectoryEntry::new(ordinal, hash).unwrap();
            assert!(!entry.is_empty());
            assert_eq!(entry.ordinal(), ordinal);
            assert!(entry.matches_tag(hash));
            assert!(entry.matches_tag(hash ^ ORDINAL_MASK));
            assert!(!entry.matches_tag(hash ^ (1_u64 << 48)));
        }
    }
    if let Ok(too_large) = usize::try_from(ORDINAL_MASK) {
        assert!(DirectoryEntry::new(too_large, 0).is_err());
        assert!(DirectoryEntry::new(usize::MAX, 0).is_err());
    }
}

#[test]
fn tagged_directory_skips_dense_reads_but_equal_tags_still_check_hash_and_bytes() {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 100, 40)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut credit = admission(&partitions, 40);
    let mut expected = BTreeMap::new();
    {
        let mut p = partitions.partitions[0].lock().unwrap();
        for i in 0..32_u64 {
            let key = format!("tag-{i}");
            assert_eq!(
                p.update(
                    (key.as_bytes(), i << 48, 1),
                    &memory,
                    &worker,
                    &mut credit,
                    &mut 0
                )
                .unwrap(),
                Update::Applied
            );
            expected.insert(key, 1_u64);
        }
        for counter in &p.lookup {
            counter.set(0);
        }
        // Same bucket and partition, different high tags: precisely one dense
        // read per hit, despite the deliberately long occupied probe chain.
        for i in 0..32_u64 {
            let bucket = p
                .find(format!("tag-{i}").as_bytes(), i << 48, &mut 0)
                .unwrap();
            assert_eq!(p.records[p.slots[bucket].ordinal()].count, 1);
        }
        assert_eq!(
            p.lookup.each_ref().map(std::cell::Cell::get),
            [528, 32, 496]
        );
        // Equal tag/different full hash, then a full-hash collision with
        // different bytes. Neither can be mistaken for the existing tag-5.
        for (key, hash, count) in [
            ("same-tag", (5_u64 << 48) | (1 << 40), 2),
            ("same-hash", 5_u64 << 48, 3),
            ("same-tag", (5_u64 << 48) | (1 << 40), 7),
        ] {
            assert_eq!(
                p.update(
                    (key.as_bytes(), hash, count),
                    &memory,
                    &worker,
                    &mut credit,
                    &mut 0
                )
                .unwrap(),
                Update::Applied
            );
            *expected.entry(key.to_owned()).or_default() += count;
        }
    }
    drop(credit);
    assert_eq!(selected(&partitions), expected);
    let evidence = partitions.evidence().unwrap();
    assert!(evidence.lookup[2] > 496);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

/// Work attribution only; production timing belongs to the guarded CLI runner.
#[cfg(unix)]
#[test]
#[ignore = "requires an explicitly supplied resident Vortex file and exact reference values"]
fn tagged_directory_real_input_work_diagnostic() {
    use crate::{
        VortexAggregateOrderExpr, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
        VortexSimpleAggregateRequest,
        local_primitives::{
            VortexLocalPrimitiveExecutionPolicy, execute_vortex_local_primitive_with_policy,
        },
    };
    use shardloom_core::{ColumnRef, DatasetUri};
    let source = std::env::var("SHARDLOOM_STRING_DIRECTORY_INPUT").expect("explicit input");
    let reference = std::env::var("SHARDLOOM_STRING_DIRECTORY_REFERENCE").expect("exact reference");
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(reference).unwrap()).unwrap();
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("URL").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "c".into())],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("c", true)]);
    let query =
        VortexQueryPrimitiveRequest::simple_aggregate(DatasetUri::new(source).unwrap(), aggregate)
            .with_source_order_limit(10);
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new(12).unwrap();
    policy.resource_envelope.memory_budget_bytes = 24 << 30;
    let report = execute_vortex_local_primitive_with_policy(&query, policy).unwrap();
    assert!(!report.has_errors());
    assert!(!report.fallback_execution_allowed);
    let payload: serde_json::Value = serde_json::from_str(
        report
            .result_summary
            .as_ref()
            .unwrap()
            .split_once(" values=")
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(payload["values"], expected);
    let count = |name| payload[name].as_u64().unwrap();
    let probes = count("aggregate_workers_test_lookup_probes");
    let reads = count("aggregate_workers_test_lookup_record_reads");
    let rejections = count("aggregate_workers_test_lookup_tag_rejections");
    assert!(rejections > 0 && reads > 0 && probes > reads + rejections);
    eprintln!("string_directory_lookup_diagnostic {payload}");
}

#[test]
fn dense_string_records_keep_exact_collisions_across_page_and_directory_growth() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 3000, 7)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let expected = (0..2065_u64)
        .map(|i| (format!("東京-{i:04}"), i + 1))
        .collect::<BTreeMap<_, _>>();
    let inputs = expected.iter().collect::<Vec<_>>();
    for batch in inputs.chunks(127) {
        let mut admission = admission(&partitions, batch.len());
        let mut p = partitions.partitions[0].lock().unwrap();
        for &(text, count) in batch {
            assert_eq!(
                p.update(
                    (text.as_bytes(), 0, *count),
                    &memory,
                    &worker,
                    &mut admission,
                    &mut 0
                )
                .unwrap(),
                Update::Applied
            );
        }
        assert_eq!(p.records.len(), p.groups);
    }
    assert_eq!(partitions.group_count(), expected.len());
    let mut actual = BTreeMap::new();
    partitions
        .replay_and_release(|text, count| {
            assert!(actual.insert(text.to_owned(), count).is_none());
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, expected);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn vacant_slot_is_invalidated_by_new_hash_mask_but_survives_byte_growth() {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 32, 16)
        .unwrap()
        .unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut credit = admission(&partitions, 16);
    {
        let mut p = partitions.partitions[0].lock().unwrap();
        for i in 0..8 {
            assert_eq!(
                p.update(
                    (format!("seed-{i}").as_bytes(), 16, 1),
                    &memory,
                    &worker,
                    &mut credit,
                    &mut 0
                )
                .unwrap(),
                Update::Applied
            );
        }
        assert_eq!(p.slots.len(), 16);
        let old_vacant = p.find(b"new", 16, &mut 0).unwrap();
        assert_eq!(old_vacant, 8);
        assert_eq!(
            p.update((b"new", 16, 3), &memory, &worker, &mut credit, &mut 0)
                .unwrap(),
            Update::Applied
        );
        assert_eq!(p.slots.len(), 32);
        assert!(p.slots[old_vacant].is_empty());
        assert_eq!(
            p.update((b"new", 16, 5), &memory, &worker, &mut credit, &mut 0)
                .unwrap(),
            Update::Applied
        );
        assert_eq!(p.groups, 9);
        let long = "outlined-new-key".repeat(4096);
        let before = (p.slots.len(), p.bytes.capacity());
        assert_eq!(
            p.update(
                (long.as_bytes(), 16, 7),
                &memory,
                &worker,
                &mut credit,
                &mut 0
            )
            .unwrap(),
            Update::Applied
        );
        assert_eq!(p.slots.len(), before.0);
        assert!(p.bytes.capacity() > before.1);
        assert_eq!(
            p.update(
                (long.as_bytes(), 16, 11),
                &memory,
                &worker,
                &mut credit,
                &mut 0
            )
            .unwrap(),
            Update::Applied
        );
        assert_eq!(p.groups, 10);
    }
    drop(credit);
    let mut result = BTreeMap::new();
    partitions
        .replay_and_release(|value, count| {
            result.insert(value.to_owned(), count);
            Ok(())
        })
        .unwrap();
    assert_eq!(result["new"], 8);
    assert_eq!(result[&"outlined-new-key".repeat(4096)], 18);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn dense_string_growth_denial_does_not_publish_or_lose_a_key() {
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut directory_denied = false;
    let mut page_denied = false;
    for available in [0, 511, 512, 1024, 1279, 1280, 2048, 16384] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let partitions = StringCountPartitions::try_new(&memory, 100, 3)
            .unwrap()
            .unwrap();
        let mut expected = (0..16)
            .map(|i| (format!("seed-{i}"), 1_u64))
            .collect::<BTreeMap<_, _>>();
        let mut seed_credit = admission(&partitions, 16);
        {
            let mut p = partitions.partitions[0].lock().unwrap();
            for text in expected.keys() {
                assert_eq!(
                    p.update(
                        (text.as_bytes(), 0, 1),
                        &memory,
                        &worker,
                        &mut seed_credit,
                        &mut 0
                    )
                    .unwrap(),
                    Update::Applied
                );
            }
        }
        drop(seed_credit);
        let text = "λ".repeat(4096);
        let before = memory.snapshot().reserved_bytes;
        let held = memory.reserve((2 << 20) - before - available).unwrap();
        let mut credit = admission(&partitions, 1);
        {
            let mut p = partitions.partitions[0].lock().unwrap();
            let pages_before = p.records.reserved_bytes();
            let outcome = p
                .update(
                    (text.as_bytes(), 0, 7),
                    &memory,
                    &worker,
                    &mut credit,
                    &mut 0,
                )
                .unwrap();
            if outcome == Update::Pressure {
                directory_denied |= p.slots.len() > 32;
                page_denied |= p.records.reserved_bytes() > pages_before;
                assert_eq!(p.records.len(), 16);
                assert_eq!(p.groups, 16);
                drop(held);
                assert_eq!(
                    p.update(
                        (text.as_bytes(), 0, 7),
                        &memory,
                        &worker,
                        &mut credit,
                        &mut 0
                    )
                    .unwrap(),
                    Update::Applied
                );
            } else {
                assert_eq!(outcome, Update::Applied);
                drop(held);
            }
        }
        drop(credit);
        expected.insert(text, 7);
        let mut actual = BTreeMap::new();
        partitions
            .replay_and_release(|text, count| {
                assert!(actual.insert(text.to_owned(), count).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(partitions.group_count(), 17);
        assert_eq!(
            partitions.evidence().unwrap().entry_credit_reserved_entries,
            0
        );
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(directory_denied && page_denied);
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
            (String::new(), 2),
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
    let mut admission = admission(&partitions, 16);
    let mut comparisons = 0;
    let mut partition = partitions.partitions[0].lock().unwrap();
    for (value, count) in [("alpha", 3), ("beta", 7), ("alpha", 11), ("max", u64::MAX)] {
        assert_eq!(
            partition
                .update(
                    (value.as_bytes(), 0, count),
                    &memory,
                    &worker,
                    &mut admission,
                    &mut comparisons
                )
                .unwrap(),
            Update::Applied
        );
    }
    assert!(
        partition
            .update(
                (b"max", 0, 1),
                &memory,
                &worker,
                &mut admission,
                &mut comparisons
            )
            .unwrap_err()
            .to_string()
            .contains("overflow")
    );
    assert_eq!(comparisons, 7);
    let index = partition.find(b"alpha", 0, &mut comparisons).unwrap();
    assert_eq!(
        partition.records[partition.slots[index].ordinal()].count,
        14
    );
    let index = partition.find(b"beta", 0, &mut comparisons).unwrap();
    assert_eq!(partition.records[partition.slots[index].ordinal()].count, 7);
    let index = partition.find(b"max", 0, &mut comparisons).unwrap();
    assert_eq!(
        partition.records[partition.slots[index].ordinal()].count,
        u64::MAX
    );
    assert_eq!(comparisons, 13);
    drop(partition);
    drop(admission);
    assert_eq!(partitions.group_count(), 3);
    let credits = partitions.entry_credits.evidence().unwrap();
    assert_eq!(credits.refunded_entries, 13);
    assert_eq!(credits.reserved, 0);
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
    assert_eq!(
        partitions.group_count(),
        1,
        "replay preserves committed evidence"
    );
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
    let mut admission = admission(&partitions, 4);
    let mut comparisons = 0;
    let mut partition = partitions.partitions[0].lock().unwrap();
    assert_eq!(
        partition
            .update(
                (b"short", 0, 3),
                &memory,
                &worker,
                &mut admission,
                &mut comparisons
            )
            .unwrap(),
        Update::Applied
    );
    let snapshot = memory.snapshot();
    let blocker = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
        .unwrap();
    assert_eq!(
        partition
            .update(
                (&[b'x'; 512], 0, 2),
                &memory,
                &worker,
                &mut admission,
                &mut comparisons
            )
            .unwrap(),
        Update::Pressure
    );
    assert_eq!(partition.groups, 1);
    assert_eq!(partition.records[partition.slots[0].ordinal()].count, 3);
    drop((blocker, partition));
    drop(admission);
    assert_eq!(partitions.group_count(), 1);
    assert_eq!(
        partitions
            .entry_credits
            .evidence()
            .unwrap()
            .refunded_entries,
        3
    );
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

#[test]
fn waiting_reducer_unlocks_partition_and_rechecks_after_refund_or_exhaustion() {
    for insert_while_waiting in [false, true] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let partitions = StringCountPartitions::try_new(&memory, 1, 1)
            .unwrap()
            .unwrap();
        let input = partial(&strings(&["shared"]), &memory);
        let (_, hash, _) = input.entry(0).unwrap();
        let index = string_count_partial::partition_index::<PARTITIONS>(hash);
        let mut held = admission(&partitions, 1);
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| {
                reduce(
                    &partitions,
                    input,
                    &ChunkWorkerContext::Inline(CancellationToken::default()),
                    &memory,
                )
            });
            partitions.entry_credits.wait_until_blocked();
            let available = partitions.partitions[index].try_lock();
            let lock_was_released = available.is_ok();
            if let Ok(mut partition) = available
                && insert_while_waiting
            {
                assert_eq!(
                    partition
                        .update(
                            (b"shared", hash, 7),
                            &memory,
                            &ChunkWorkerContext::Inline(CancellationToken::default()),
                            &mut held,
                            &mut 0,
                        )
                        .unwrap(),
                    Update::Applied
                );
            }
            drop(held);
            let receipt = waiting.join().unwrap().unwrap();
            assert!(lock_was_released, "credit wait held the partition mutex");
            assert!(receipt.deferred.is_none());
        });
        assert!(!partitions.pressure_requested());
        assert_eq!(partitions.group_count(), 1);
        assert_eq!(
            selected(&partitions),
            BTreeMap::from([("shared".into(), if insert_while_waiting { 8 } else { 1 }),])
        );
        partitions.release_storage().unwrap();
        assert_eq!(partitions.group_count(), 1);
        let evidence = partitions.evidence().unwrap();
        assert_eq!(evidence.groups, 1);
        assert_eq!(evidence.entry_credit_reserved_entries, 0);
        assert!(evidence.entry_credit_wait_calls > 0);
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn waiting_reducer_cancels_or_defers_the_complete_suffix_on_pressure() {
    for cancel in [false, true] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let partitions = StringCountPartitions::try_new(&memory, 1, 1)
            .unwrap()
            .unwrap();
        let input = partial(&strings(&["shared", "shared"]), &memory);
        let held = admission(&partitions, 1);
        let token = CancellationToken::default();
        let worker = ChunkWorkerContext::Inline(token.clone());
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| reduce(&partitions, input, &worker, &memory));
            partitions.entry_credits.wait_until_blocked();
            if cancel {
                token.cancel();
                partitions.entry_credits.wake();
            } else {
                partitions.request_pressure();
            }
            let result = waiting.join().unwrap();
            if cancel {
                assert!(result.is_err());
            } else {
                let receipt = result.unwrap();
                let mut rows = BTreeMap::new();
                receipt
                    .deferred
                    .as_ref()
                    .unwrap()
                    .for_each_count(|key, count| {
                        rows.insert(key.to_owned(), count);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(rows, BTreeMap::from([("shared".into(), 2)]));
            }
        });
        drop(held);
        assert_eq!(partitions.group_count(), 0);
        assert_eq!(partitions.committed_rows.load(Ordering::Acquire), 0);
        assert_eq!(
            partitions.evidence().unwrap().entry_credit_reserved_entries,
            0
        );
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn concurrent_block_limits_replay_all_weights_and_keep_counts_after_release() {
    for workers in [1, 4] {
        for limit in [0, 1, 1023, 1024, 1025] {
            let memory = LiveMemoryPool::new(16 << 20).unwrap();
            let partitions = StringCountPartitions::try_new(&memory, limit, 1)
                .unwrap()
                .unwrap();
            let mut jobs = AggregateChunkJobs::new(workers, 4, 16 << 20, memory.clone()).unwrap();
            let mut oracle = BTreeMap::<String, u64>::new();
            for (start, end) in [(0, 400), (200, 600), (400, 800), (600, 1100)] {
                let rows = (start..end)
                    .map(|key| format!("key-{key}"))
                    .collect::<Vec<_>>();
                for key in &rows {
                    *oracle.entry(key.clone()).or_default() += 1;
                }
                let array = strings(&rows.iter().map(String::as_str).collect::<Vec<_>>());
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
            let mut receipts = Vec::new();
            while let Some(result) = jobs.join_next().unwrap() {
                result
                    .consume(|receipt| {
                        if let Some(partial) = &receipt.deferred {
                            receipts.push(Arc::clone(partial));
                        }
                        Ok(())
                    })
                    .unwrap();
            }
            assert!(partitions.pressure_requested());
            assert_eq!(partitions.group_count(), limit);
            let mut actual = BTreeMap::<String, u64>::new();
            partitions
                .replay_and_release(|key, count| {
                    *actual.entry(key.to_owned()).or_default() += count;
                    Ok(())
                })
                .unwrap();
            for partial in receipts {
                partial
                    .for_each_count(|key, count| {
                        *actual.entry(key.to_owned()).or_default() += count;
                        Ok(())
                    })
                    .unwrap();
            }
            assert_eq!(actual, oracle);
            let evidence = partitions.evidence().unwrap();
            assert_eq!(evidence.groups, limit);
            assert_eq!(evidence.entry_credit_reserved_entries, 0);
            assert_eq!(
                evidence.entry_credit_claim_calls,
                evidence.entry_credit_return_calls
            );
            assert_eq!(
                evidence.entry_credit_granted_entries - evidence.entry_credit_refunded_entries,
                limit as u64
            );
            if limit >= 1023 {
                assert!(evidence.entry_credit_claim_calls < limit as u64 / 2);
            }
            drop(jobs);
            drop(partitions);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn collision_comparisons_publish_per_job_without_per_key_shared_updates() {
    for workers in [1, 2, 4, 8, 12] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let partitions = StringCountPartitions::try_new(&memory, 3, 3)
            .unwrap()
            .unwrap();
        let worker = ChunkWorkerContext::Inline(CancellationToken::default());
        let mut held = admission(&partitions, 3);
        {
            let mut partition = partitions.partitions[0].lock().unwrap();
            for key in ["alpha", "beta", "gamma"] {
                assert_eq!(
                    partition
                        .update((key.as_bytes(), 0, 1), &memory, &worker, &mut held, &mut 0)
                        .unwrap(),
                    Update::Applied
                );
            }
        }
        drop(held);
        let mut jobs = AggregateChunkJobs::new(workers, 4, 1 << 20, memory.clone()).unwrap();
        for _ in 0..4 {
            let owned = Arc::clone(&partitions);
            jobs.submit(0, move |worker, _lease| {
                let mut progress = ReconcileProgress::default();
                let mut admission = EntryAdmission::default();
                let mut partition = owned.partitions[0].lock().unwrap();
                for _ in 0..400 {
                    for key in ["alpha", "beta", "gamma"] {
                        assert_eq!(
                            partition.update(
                                (key.as_bytes(), 0, 1),
                                &owned.memory,
                                worker,
                                &mut admission,
                                &mut progress.comparisons
                            )?,
                            Update::Applied
                        );
                    }
                }
                drop(partition);
                owned.publish_comparisons(&mut progress)
            })
            .unwrap();
        }
        while let Some(result) = jobs.join_next().unwrap() {
            result.consume(|()| Ok(())).unwrap();
        }
        assert_eq!(
            selected(&partitions),
            BTreeMap::from([
                ("alpha".into(), 1601),
                ("beta".into(), 1601),
                ("gamma".into(), 1601),
            ])
        );
        let evidence = partitions.evidence().unwrap();
        assert_eq!(evidence.equality_comparisons, 4 * 400 * (1 + 2 + 3));
        assert_eq!(evidence.comparison_publish_calls, 4);
        assert_eq!(evidence.entry_credit_claim_calls, 1);
        assert_eq!(evidence.groups, 3);
        drop(jobs);
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn reduce_error_publishes_actual_comparisons_without_changing_existing_count() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let partitions = StringCountPartitions::try_new(&memory, 1, 1)
        .unwrap()
        .unwrap();
    let input = partial(&strings(&["full"]), &memory);
    let (_, hash, _) = input.entry(0).unwrap();
    let index = string_count_partial::partition_index::<PARTITIONS>(hash);
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut held = admission(&partitions, 1);
    assert_eq!(
        partitions.partitions[index]
            .lock()
            .unwrap()
            .update(
                (b"full", hash, u64::MAX),
                &memory,
                &worker,
                &mut held,
                &mut 0,
            )
            .unwrap(),
        Update::Applied
    );
    drop(held);
    let error = reduce(&partitions, input, &worker, &memory).err().unwrap();
    assert!(error.to_string().contains("overflow"));
    assert_eq!(
        selected(&partitions),
        BTreeMap::from([("full".into(), u64::MAX)])
    );
    let evidence = partitions.evidence().unwrap();
    assert_eq!(evidence.equality_comparisons, 1);
    assert_eq!(evidence.comparison_publish_calls, 1);
    assert_eq!(evidence.groups, 1);
    assert_eq!(evidence.entry_credit_reserved_entries, 0);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
