use super::{
    GroupedAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    aggregate_chunk_jobs::{AggregateChunkJobs, ChunkWorkerContext},
    compound_count_partial::{self, CompoundPartial, Key},
    compound_count_partitions::{CompoundPartitions, PARTITIONS},
    compound_count_workers::CompoundWorkers,
};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{collections::BTreeMap, sync::Arc};
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray},
    dtype::FieldNames,
    validity::Validity,
};

fn worker() -> ChunkWorkerContext {
    ChunkWorkerContext::Inline(CancellationToken::default())
}
fn strings(values: &[&str]) -> ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}
fn integers(values: &[i64]) -> ArrayRef {
    PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array()
}
fn dictionary(codes: &[u8], values: &[&str]) -> ArrayRef {
    DictArray::try_new(
        PrimitiveArray::new(codes.to_vec(), Validity::NonNullable).into_array(),
        strings(values),
    )
    .unwrap()
    .into_array()
}
fn partial(numeric: &ArrayRef, text: &ArrayRef, memory: &LiveMemoryPool) -> CompoundPartial {
    let bytes = CompoundPartial::bytes(numeric.len()).unwrap()
        + CompoundPartial::dictionary_hash_bytes(text).unwrap();
    let mut lease = memory.reserve(bytes).unwrap();
    compound_count_partial::count(
        numeric,
        text,
        vortex::array::legacy_session().create_execution_ctx(),
        &worker(),
        &mut lease,
    )
    .unwrap()
}
fn logical(key: Key) -> i128 {
    if key.signed {
        i128::from(i64::from_ne_bytes(key.bits.to_ne_bytes()))
    } else {
        i128::from(key.bits)
    }
}
fn collect(partial: &CompoundPartial) -> BTreeMap<(i128, String), u64> {
    let mut result = BTreeMap::new();
    partial
        .visit(|key, value, count| {
            assert!(
                result
                    .insert((logical(key), value.to_owned()), count)
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
    result
}
fn selected(partitions: &CompoundPartitions) -> BTreeMap<(i128, String), u64> {
    let mut result = BTreeMap::new();
    for index in 0..PARTITIONS {
        let selected = partitions.select(index, &worker()).unwrap();
        partitions
            .visit_selected(&selected, |key, value, count| {
                assert!(
                    result
                        .insert((logical(key), value.to_owned()), count)
                        .is_none()
                );
                Ok(())
            })
            .unwrap();
    }
    result
}

#[test]
fn compound_partials_preserve_all_integer_widths_extrema_and_dictionary_domains() {
    macro_rules! signed {
        ($t:ty) => {{
            let raw = [<$t>::MIN, 7, <$t>::MAX, 7];
            (
                PrimitiveArray::new(raw.to_vec(), Validity::NonNullable).into_array(),
                raw.map(i128::from),
            )
        }};
    }
    macro_rules! unsigned {
        ($t:ty) => {{
            let raw = [0 as $t, 7, <$t>::MAX, 7];
            (
                PrimitiveArray::new(raw.to_vec(), Validity::NonNullable).into_array(),
                raw.map(i128::from),
            )
        }};
    }
    for (array, expected) in [
        signed!(i8),
        signed!(i16),
        signed!(i32),
        signed!(i64),
        unsigned!(u8),
        unsigned!(u16),
        unsigned!(u32),
        unsigned!(u64),
    ] {
        for text in [
            strings(&["α", "東京", "", "東京"]),
            dictionary(&[2, 0, 1, 3], &["東京", "", "α", "東京"]),
        ] {
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let counted = partial(&array, &text, &memory);
            let mut reference = BTreeMap::new();
            for (key, value) in expected.into_iter().zip(["α", "東京", "", "東京"]) {
                *reference.entry((key, value.into())).or_insert(0) += 1;
            }
            assert_eq!(collect(&counted), reference);
            assert_eq!(counted.work.rows, 4);
            assert!(memory.snapshot().reserved_bytes >= counted.work.capacity_bytes);
            drop(counted);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn compound_empty_sliced_and_underreserved_inputs_preserve_ownership() {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let counted = partial(&integers(&[]), &strings(&[]), &memory);
    assert!(collect(&counted).is_empty());
    drop(counted);
    let numeric = PrimitiveArray::new(vec![999_i16, -2, -2, 7, 999], Validity::NonNullable)
        .into_array()
        .slice(1..4)
        .unwrap();
    let text = dictionary(&[0, 1, 2, 0, 0], &["tail", "same", "same"])
        .slice(1..4)
        .unwrap();
    let counted = partial(&numeric, &text, &memory);
    drop(numeric);
    drop(text);
    assert_eq!(
        collect(&counted),
        BTreeMap::from([((-2, "same".into()), 2), ((7, "tail".into()), 1)])
    );
    drop(counted);
    let mut lease = memory.reserve(0).unwrap();
    let error = compound_count_partial::count(
        &integers(&[1]),
        &strings(&["x"]),
        vortex::array::legacy_session().create_execution_ctx(),
        &worker(),
        &mut lease,
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("before native execution"));
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn compound_complete_keys_preserve_global_winner_skew_collisions_and_workers() {
    for workers in [1, 2, 4] {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let partitions = CompoundPartitions::try_new(&memory, 1000, 1, true)
            .unwrap()
            .unwrap();
        let mut jobs = AggregateChunkJobs::new(workers, 3, 16 << 20, memory.clone()).unwrap();
        for chunk in 0_i64..3 {
            let numeric = integers(&[vec![chunk; 6], vec![99; 5]].concat());
            // Different codes in every source chunk; the winning string is
            // never that chunk's top1 and all keys are forced into one hash.
            let text = dictionary(
                &[vec![u8::from(chunk != 1); 6], vec![u8::from(chunk == 1); 5]].concat(),
                if chunk == 1 {
                    &["local", "shared"]
                } else {
                    &["shared", "local"]
                },
            );
            let owned = Arc::clone(&partitions);
            jobs.submit(
                CompoundPartial::bytes(11).unwrap()
                    + CompoundPartial::dictionary_hash_bytes(&text).unwrap(),
                move |worker, lease| {
                    let mut partial = compound_count_partial::count(
                        &numeric,
                        &text,
                        vortex::array::legacy_session().create_execution_ctx(),
                        worker,
                        lease,
                    )?;
                    partial.force_collision_hashes();
                    owned.reduce(partial, worker)
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
        let result = selected(&partitions);
        assert_eq!(result, BTreeMap::from([((99, "shared".into()), 15)]));
        let evidence = partitions.evidence().unwrap();
        assert_eq!(evidence.groups, 4);
        assert_eq!(evidence.rows, 33);
        assert!(evidence.comparisons > 0);
        drop(partitions);
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn compound_partition_interns_hot_string_once_and_preserves_tie_order() {
    for numeric_first in [true, false] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let partitions = CompoundPartitions::try_new(&memory, 1000, 2, numeric_first)
            .unwrap()
            .unwrap();
        let mut counted = partial(
            &integers(&[4, 2, 3, 1]),
            &strings(&["a", "z", "a", "z"]),
            &memory,
        );
        counted.force_collision_hashes();
        assert!(
            partitions
                .reduce(counted, &worker())
                .unwrap()
                .deferred
                .is_none()
        );
        let expected = if numeric_first {
            BTreeMap::from([((1, "z".into()), 1), ((2, "z".into()), 1)])
        } else {
            BTreeMap::from([((3, "a".into()), 1), ((4, "a".into()), 1)])
        };
        assert_eq!(selected(&partitions), expected);
        let evidence = partitions.evidence().unwrap();
        assert_eq!(evidence.strings, 2);
        assert_eq!(evidence.string_bytes_copied, 2);
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn compound_entry_and_byte_pressure_replay_exact_prefix_suffix_once() {
    for entry_limit in [0, 1, 7, 1000] {
        let memory = LiveMemoryPool::new(2 << 20).unwrap();
        let partitions = CompoundPartitions::try_new(&memory, entry_limit, 3, true)
            .unwrap()
            .unwrap();
        let keys = (0_i64..40).flat_map(|key| [key, key]).collect::<Vec<_>>();
        let mut counted = partial(
            &integers(&keys),
            &strings(&vec!["same"; keys.len()]),
            &memory,
        );
        counted.force_collision_hashes();
        // Force actual byte denial after initial table state for the last case.
        let held = if entry_limit == 1000 {
            let free = memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes;
            Some(memory.reserve(free - 1400).unwrap())
        } else {
            None
        };
        let receipt = partitions.reduce(counted, &worker()).unwrap();
        assert!(receipt.deferred.is_some());
        assert!(partitions.pressured());
        let mut result = BTreeMap::new();
        partitions
            .replay_and_release(|key, value, count| {
                *result
                    .entry((logical(key), value.to_owned()))
                    .or_insert(0_u64) += count;
                Ok(())
            })
            .unwrap();
        let committed = result.values().sum::<u64>();
        assert_eq!(committed, partitions.evidence().unwrap().rows);
        receipt
            .deferred
            .as_ref()
            .unwrap()
            .visit(|key, value, count| {
                *result.entry((logical(key), value.to_owned())).or_insert(0) += count;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            result,
            (0_i128..40).map(|key| ((key, "same".into()), 2)).collect()
        );
        assert_eq!(result.values().sum::<u64>(), 80);
        drop(receipt);
        drop(partitions);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn request(reverse: bool) -> VortexSimpleAggregateRequest {
    let names = if reverse {
        ["phrase_alias", "actor_alias"]
    } else {
        ["actor_alias", "phrase_alias"]
    };
    VortexSimpleAggregateRequest::grouped(
        names.map(|name| ColumnRef::new(name).unwrap()).to_vec(),
        vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "n_alias".into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("n_alias", true)])
    .with_offset(1)
}
fn chunk(keys: ArrayRef, text: ArrayRef) -> ArrayRef {
    let len = keys.len();
    StructArray::try_new(
        FieldNames::from(["phrase_alias", "actor_alias"]),
        vec![text, keys],
        len,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn compound_workers_renamed_reordered_offset_ties_and_weighted_handoff() {
    for reverse in [false, true] {
        for entry_limit in [2, 1000] {
            let request = request(reverse);
            let columns = request
                .projected_columns()
                .iter()
                .map(|column| column.as_str().to_owned())
                .collect::<Vec<_>>();
            let mut policy = VortexLocalPrimitiveExecutionPolicy::new(2).unwrap();
            policy.resource_envelope.group_state_soft_item_budget = entry_limit;
            let mut states = GroupedAggregateStates::new_with_resource_envelope(
                &request,
                Some(2),
                &columns,
                false,
                false,
                policy.resource_envelope,
            )
            .unwrap();
            let memory = LiveMemoryPool::new(8 << 20).unwrap();
            let chunks = [
                chunk(
                    integers(&[9, 2, 9, 4]),
                    dictionary(&[0, 1, 0, 1], &["a", "z"]),
                ),
                chunk(
                    integers(&[2, 4, 7, 7]),
                    dictionary(&[0, 0, 1, 1], &["z", "a"]),
                ),
            ];
            let mut workers = CompoundWorkers::admit(
                &states,
                chunks[0].dtype(),
                &columns,
                policy,
                vortex::array::legacy_session(),
                &memory,
            )
            .unwrap()
            .unwrap();
            for array in &chunks {
                workers.before_next(&mut states).unwrap();
                if !workers.submit(array, &mut states).unwrap() {
                    states
                        .update_compact_direct_from_chunk(array, &columns, None)
                        .unwrap();
                }
            }
            workers.finish(&mut states).unwrap();
            if states.needs_numeric_utf8_topk_heavy_hitter_second_pass() {
                states
                    .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
                    .unwrap();
                for array in &chunks {
                    assert!(
                        states
                            .update_numeric_utf8_topk_heavy_hitter_exact_from_chunk(array, &columns)
                            .unwrap()
                    );
                }
            }
            let (_, mut summary) = states.result_row_count_and_summary(Some(2)).unwrap();
            workers.annotate_summary(&mut summary).unwrap();
            let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
            let expected = if reverse {
                serde_json::json!([{"actor_alias":9,"phrase_alias":"a","n_alias":2},{"actor_alias":2,"phrase_alias":"z","n_alias":2}])
            } else {
                serde_json::json!([{"actor_alias":4,"phrase_alias":"z","n_alias":2},{"actor_alias":7,"phrase_alias":"a","n_alias":2}])
            };
            assert_eq!(payload["values"], expected);
            assert_eq!(
                payload["aggregate_workers_partition_native_handoffs"],
                u64::from(entry_limit == 2)
            );
            if entry_limit == 1000 {
                assert_eq!(
                    payload["uniqueness_proof_status"],
                    "complete_numeric_utf8_key_partition_all_contributions_before_selection"
                );
                assert_eq!(payload["numeric_utf8_topk_heavy_hitter_second_pass"], false);
            }
            drop(workers);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn compound_rejects_nullable_shapes_and_cancelled_jobs_release_owners() {
    let request = request(false);
    let columns = vec!["actor_alias".into(), "phrase_alias".into()];
    let states = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let array = chunk(
        PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array(),
        strings(&["x", "y"]),
    );
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    assert!(
        CompoundWorkers::admit(
            &states,
            array.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
            vortex::array::legacy_session(),
            &memory
        )
        .unwrap()
        .is_none()
    );
    // Nonnullable children acquire their nullable struct parent's validity
    // through logical projection. Reject before installing worker state.
    let parent_nullable = StructArray::try_new(
        FieldNames::from(["actor_alias", "phrase_alias"]),
        vec![integers(&[1, 2]), strings(&["x", "y"])],
        2,
        Validity::AllValid,
    )
    .unwrap()
    .into_array();
    assert!(parent_nullable.dtype().is_nullable());
    assert!(
        CompoundWorkers::admit(
            &states,
            parent_nullable.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
            vortex::array::legacy_session(),
            &memory,
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let cancel = CancellationToken::default();
    cancel.cancel();
    let mut lease = memory.reserve(CompoundPartial::bytes(2).unwrap()).unwrap();
    assert!(
        compound_count_partial::count(
            &integers(&[1, 2]),
            &strings(&["x", "y"]),
            vortex::array::legacy_session().create_execution_ctx(),
            &ChunkWorkerContext::Inline(cancel),
            &mut lease
        )
        .is_err()
    );
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let partitions = CompoundPartitions::try_new(&memory, 1000, 2, true)
        .unwrap()
        .unwrap();
    let mut jobs = AggregateChunkJobs::new(2, 2, 4 << 20, memory.clone()).unwrap();
    let owned = Arc::clone(&partitions);
    jobs.submit(CompoundPartial::bytes(2).unwrap(), move |worker, lease| {
        let partial = compound_count_partial::count(
            &integers(&[1, 2]),
            &strings(&["x", "y"]),
            vortex::array::legacy_session().create_execution_ctx(),
            worker,
            lease,
        )?;
        owned.reduce(partial, worker)
    })
    .unwrap();
    drop(jobs);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
// Keep successful retry, repeated denial and coincident corruption against one
// shared fixture so exact values and the one-retry bound are compared together.
#[allow(clippy::too_many_lines)]
fn compound_typed_provider_denial_retries_untouched_chunk_once_after_release() {
    use vortex::{
        VortexSessionDefault as _,
        array::memory::MemorySessionExt as _,
        session::{SessionExt as _, VortexSession},
    };
    for (corruption, repeat) in [(false, false), (true, false), (false, true)] {
        let request = request(false);
        let columns = vec!["actor_alias".into(), "phrase_alias".into()];
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // Session clones share their variable store. Configure a fresh store so
        // a concurrently executing test cannot observe this fault or allocator.
        let session = VortexSession::default()
            .with_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
                memory.clone(),
            )))
            .with_some(compound_count_partial::ProviderFault {
                memory: memory.clone(),
                calls: Arc::clone(&calls),
                corruption,
                repeat,
            });
        assert!(
            vortex::array::legacy_session()
                .get_opt::<compound_count_partial::ProviderFault>()
                .is_none()
        );
        let chunks = [
            chunk(
                integers(&[9, 2, 9, 4]),
                dictionary(&[0, 1, 0, 1], &["a", "z"]),
            ),
            chunk(
                integers(&[2, 4, 7, 7]),
                dictionary(&[0, 0, 1, 1], &["z", "a"]),
            ),
        ];
        let mut states =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let mut workers = CompoundWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .unwrap();
        assert!(workers.submit(&chunks[0], &mut states).unwrap());
        workers.drain(&mut states).unwrap();
        assert!(workers.has_committed_groups());
        let submitted = workers.submit(&chunks[1], &mut states);
        if corruption {
            assert!(
                submitted
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("injected compound provider corruption")
            );
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        } else {
            assert!(submitted.unwrap());
            let finished = workers.finish(&mut states);
            if repeat {
                assert!(
                    finished
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("memory reservation denied")
                );
            } else {
                finished.unwrap();
                assert!(states.needs_numeric_utf8_topk_heavy_hitter_second_pass());
                states
                    .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
                    .unwrap();
                for array in &chunks {
                    assert!(
                        states
                            .update_numeric_utf8_topk_heavy_hitter_exact_from_chunk(array, &columns)
                            .unwrap()
                    );
                }
                let (_, mut summary) = states.result_row_count_and_summary(Some(2)).unwrap();
                workers.annotate_summary(&mut summary).unwrap();
                let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
                assert_eq!(
                    payload["values"],
                    serde_json::json!([{"actor_alias":4,"phrase_alias":"z","n_alias":2},{"actor_alias":7,"phrase_alias":"a","n_alias":2}])
                );
                assert_eq!(payload["aggregate_workers_rows"], 8);
                assert_eq!(payload["aggregate_workers_partition_retry_jobs"], 1);
                assert_eq!(payload["aggregate_workers_partition_native_handoffs"], 1);
            }
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        }
        drop(workers);
        drop(states);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn compound_initial_reservation_race_hands_off_untouched_chunk_exactly_once() {
    for parallelism in [1, 2] {
        let request = request(false);
        let columns = vec!["actor_alias".into(), "phrase_alias".into()];
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let chunks = [
            chunk(
                integers(&[9, 2, 9, 4]),
                dictionary(&[0, 1, 0, 1], &["a", "z"]),
            ),
            chunk(
                integers(&[2, 4, 7, 7]),
                dictionary(&[0, 0, 1, 1], &["z", "a"]),
            ),
        ];
        let mut states =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let mut workers = CompoundWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            vortex::array::legacy_session(),
            &memory,
        )
        .unwrap()
        .unwrap();
        assert!(workers.submit(&chunks[0], &mut states).unwrap());
        workers.drain(&mut states).unwrap();
        assert!(workers.has_committed_groups());
        assert_eq!(memory.snapshot().denied_reservations, 0);
        workers.deny_next_initial_reservation_for_test();
        assert!(!workers.submit(&chunks[1], &mut states).unwrap());
        assert_eq!(memory.snapshot().denied_reservations, 1);
        assert!(!workers.has_active_partitions());
        states
            .update_compact_direct_from_chunk(&chunks[1], &columns, None)
            .unwrap();
        workers.finish(&mut states).unwrap();
        assert!(states.needs_numeric_utf8_topk_heavy_hitter_second_pass());
        states
            .prepare_numeric_utf8_topk_heavy_hitter_second_pass()
            .unwrap();
        for array in &chunks {
            assert!(
                states
                    .update_numeric_utf8_topk_heavy_hitter_exact_from_chunk(array, &columns)
                    .unwrap()
            );
        }
        let (_, mut summary) = states.result_row_count_and_summary(Some(2)).unwrap();
        workers.annotate_summary(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            payload["values"],
            serde_json::json!([
                {"actor_alias":4,"phrase_alias":"z","n_alias":2},
                {"actor_alias":7,"phrase_alias":"a","n_alias":2}
            ])
        );
        // Only the first chunk entered a worker. The second remained owned by
        // the caller and contributed once through the native handoff route.
        assert_eq!(payload["aggregate_workers_rows"], 4);
        assert_eq!(payload["aggregate_workers_submitted_chunks"], 1);
        assert_eq!(payload["aggregate_workers_completed_chunks"], 1);
        assert_eq!(payload["aggregate_workers_partition_retry_jobs"], 0);
        assert_eq!(payload["aggregate_workers_partition_native_handoffs"], 1);
        drop(workers);
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[test]
// Full native-file lifecycle, exact public values and both typed source-error
// cases share the same fixture and ownership guard.
#[allow(clippy::too_many_lines)]
fn compound_public_native_query_filter_exact_values_and_typed_source_replay() {
    use super::{
        aggregate_count_workers::{SOURCE_SCAN_TEST_FAULT, SourceScanTestFault},
        execute_vortex_local_primitive_with_policy,
        native_flat_layout::SequentialNativeFlatLayout,
    };
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::{ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use vortex::{
        VortexSessionDefault as _,
        file::WriteOptionsSessionExt as _,
        io::{
            runtime::{BlockingRuntime as _, single::SingleThreadRuntime},
            session::RuntimeSessionExt as _,
        },
        session::VortexSession,
    };
    struct FileGuard(std::path::PathBuf);
    impl Drop for FileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let path = FileGuard(std::env::temp_dir().join(format!(
            "shardloom-compound-native-{}-{}.vortex",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )));
    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let chunks = [
        chunk(
            PrimitiveArray::new(vec![9_u16, 2, 9, 0], Validity::NonNullable).into_array(),
            dictionary(&[0, 1, 0, 1], &["a", "z"]),
        ),
        chunk(
            PrimitiveArray::new(vec![2_u16, 4, 7, 7], Validity::NonNullable).into_array(),
            dictionary(&[0, 0, 1, 1], &["z", "a"]),
        ),
        chunk(
            PrimitiveArray::new(vec![4_u16, 0], Validity::NonNullable).into_array(),
            dictionary(&[0, 1], &["z", "a"]),
        ),
    ];
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path.0)
        .unwrap();
    let mut writer = session
        .write_options()
        .with_strategy(SequentialNativeFlatLayout::strategy(chunks.len()))
        .with_file_statistics(Vec::new())
        .blocking(&runtime)
        .writer(&mut output, chunks[0].dtype().clone());
    for array in chunks {
        writer.push(array).unwrap();
    }
    assert_eq!(writer.finish().unwrap().row_count(), 10);
    drop(output);
    let mut query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.0.display().to_string()).unwrap(),
        request(false),
    )
    .with_source_order_limit(2);
    query.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("actor_alias").unwrap(),
        op: ComparisonOp::GtEq,
        value: StatValue::UInt64(2),
    });
    let expected = serde_json::json!([{"actor_alias":4,"phrase_alias":"z","n_alias":2},{"actor_alias":7,"phrase_alias":"a","n_alias":2}]);
    for fault in [None, Some(SourceScanTestFault::OwnedDenial)] {
        SOURCE_SCAN_TEST_FAULT.with(|current| current.set(fault));
        let report = execute_vortex_local_primitive_with_policy(
            &query,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        )
        .unwrap();
        assert!(!report.fallback_execution_allowed);
        assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
        let (_, json) = report
            .result_summary
            .as_deref()
            .unwrap()
            .rsplit_once(" values=")
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(payload["values"], expected);
        if fault.is_some() {
            assert_eq!(payload["aggregate_workers_partition_source_replays"], 1);
        } else {
            assert_eq!(
                payload["group_output_strategy"],
                "complete_numeric_utf8_key_partition_exact_topk"
            );
            assert_eq!(payload["numeric_utf8_topk_heavy_hitter_second_pass"], false);
        }
    }
    SOURCE_SCAN_TEST_FAULT
        .with(|current| current.set(Some(SourceScanTestFault::CorruptionWithConcurrentDenial)));
    let error = execute_vortex_local_primitive_with_policy(
        &query,
        VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("injected source corruption"));
    assert!(SOURCE_SCAN_TEST_FAULT.with(std::cell::Cell::get).is_none());
}
