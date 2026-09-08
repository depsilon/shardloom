use super::{ExactDistinctWorkers, GroupedAggregateStates, VortexLocalPrimitiveExecutionPolicy};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{DictArray, PrimitiveArray, StructArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    session::{SessionExt as _, VortexSession},
};

pub(super) fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("cohort_alias").unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new("member_alias").unwrap()),
            "uniques_alias".into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("uniques_alias", true)])
    .with_offset(1)
}

fn chunk(groups: &[i16], values: &[u64]) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["member_alias", "cohort_alias"]),
        vec![
            PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
            PrimitiveArray::new(groups.to_vec(), Validity::NonNullable).into_array(),
        ],
        groups.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

#[test]
fn exact_distinct_order_admission_matches_native_integer_ties_and_rejects_other_terms() {
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::DatasetUri;
    let columns = vec!["cohort_alias".into(), "member_alias".into()];
    let chunk = chunk(&[-7, 2, -7, 2], &[u64::MAX, 1, u64::MAX - 1, 2]);
    let session = VortexSession::default();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    for (secondary, admitted) in [
        (None, true),
        (
            Some(VortexAggregateOrderExpr::new("cohort_alias", false)),
            true,
        ),
        (
            Some(VortexAggregateOrderExpr::new("cohort_alias", true)),
            false,
        ),
        (
            Some(VortexAggregateOrderExpr::new("uniques_alias", false)),
            false,
        ),
    ] {
        let mut aggregate = request();
        if let Some(secondary) = secondary {
            aggregate.order_by.push(secondary);
        }
        let query = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new("/absent/admission-only.vortex").unwrap(),
            aggregate.clone(),
        )
        .with_source_order_limit(2);
        assert_eq!(super::request_may_be_admitted(&query), admitted);
        assert_eq!(
            super::request_schema_may_be_admitted(&query, chunk.dtype()),
            admitted
        );
        let states = GroupedAggregateStates::new_with_resource_envelope(
            &aggregate,
            Some(2),
            &columns,
            false,
            false,
            policy.resource_envelope(),
        )
        .unwrap();
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let workers = ExactDistinctWorkers::admit(
            &states,
            chunk.dtype(),
            &columns,
            policy,
            &session,
            &memory,
        )
        .unwrap();
        assert_eq!(workers.is_some(), admitted);
        drop(workers);
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn output(
    workers: &mut ExactDistinctWorkers,
    states: &GroupedAggregateStates<'_>,
) -> serde_json::Value {
    if let Some(result) = workers.take_exact_result() {
        let mut rows = Vec::new();
        result
            .visit(|key, count| {
                assert!(key.signed);
                rows.push((i64::from_ne_bytes(key.bits.to_ne_bytes()), count));
                Ok(())
            })
            .unwrap();
        rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        serde_json::Value::Array(rows.into_iter().skip(1).take(2).map(|(group, count)| serde_json::json!({"cohort_alias": group, "uniques_alias": count})).collect())
    } else {
        let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
        serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
    }
}

#[test]
fn exact_distinct_workers_renamed_reordered_complete_values_and_exact_pressure_sets() {
    for parallelism in [1, 2, 4] {
        for pair_limit in [2, 1000] {
            let request = request();
            let columns = vec!["cohort_alias".into(), "member_alias".into()];
            let memory = LiveMemoryPool::new(4 << 20).unwrap();
            let session = VortexSession::default();
            let mut policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
            policy.resource_envelope.group_state_soft_item_budget = pair_limit;
            let mut states = GroupedAggregateStates::new_with_resource_envelope(
                &request,
                Some(2),
                &columns,
                false,
                false,
                policy.resource_envelope,
            )
            .unwrap();
            let base = 1_u64 << 61;
            let batches = [
                (
                    vec![9_i16, 2, 9, 4, 2, 4],
                    vec![base, base, base + 1, base, base, base + 2],
                ),
                (
                    vec![2_i16, 4, 7, 7, 9, 9],
                    vec![base + 3, base + 2, base, base + 4, base + 1, base + 2],
                ),
            ];
            let chunks = batches
                .iter()
                .map(|(groups, values)| chunk(groups, values))
                .collect::<Vec<_>>();
            let mut oracle = BTreeMap::<i16, BTreeSet<u64>>::new();
            for (groups, values) in &batches {
                for (&group, &value) in groups.iter().zip(values) {
                    oracle.entry(group).or_default().insert(value);
                }
            }
            let mut oracle = oracle
                .into_iter()
                .map(|(key, values)| (key, values.len()))
                .collect::<Vec<_>>();
            oracle.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            let expected = serde_json::Value::Array(oracle.into_iter().skip(1).take(2).map(|(group, count)| serde_json::json!({"cohort_alias": group, "uniques_alias": count})).collect());
            let mut workers = ExactDistinctWorkers::admit(
                &states,
                chunks[0].dtype(),
                &columns,
                policy,
                &session,
                &memory,
            )
            .unwrap()
            .unwrap();
            for chunk in &chunks {
                workers.before_next(&mut states).unwrap();
                if !workers.submit(chunk, &mut states).unwrap() {
                    assert!(
                        states
                            .update_compact_direct_from_chunk(chunk, &columns, None)
                            .unwrap()
                    );
                }
            }
            workers.finish(&mut states).unwrap();
            assert_eq!(workers.handoffs, u64::from(pair_limit == 2));
            assert_eq!(output(&mut workers, &states), expected);
            let mut summary = "{}".to_owned();
            workers.annotate_summary(&mut summary).unwrap();
            let work: serde_json::Value = serde_json::from_str(&summary).unwrap();
            assert_eq!(work["aggregate_workers_outstanding_chunks"], 0);
            assert!(
                work["aggregate_workers_compute_threads"].as_u64().unwrap() < parallelism as u64
            );
            assert_eq!(work["aggregate_workers_provider_background_workers"], 0);
            if pair_limit == 1000 {
                assert_eq!(work["aggregate_workers_rows"], 12);
                assert_eq!(work["aggregate_workers_exact_distinct_complete_pairs"], 9);
                assert_eq!(work["aggregate_workers_distinct_group_reduction_jobs"], 1);
            } else {
                assert!(states.groups.values().all(|group| {
                    match group {
                        super::super::super::GroupedAggregateState::General { states, .. } => {
                            !states.states[0].distinct_values.is_empty()
                        }
                        _ => false,
                    }
                }));
            }
            drop(workers);
            drop(states);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn exact_distinct_workers_retry_untouched_arrays_once_and_never_retry_corruption() {
    for (corruption, repeat) in [(false, false), (true, false), (false, true)] {
        let request = request();
        let columns = vec!["cohort_alias".into(), "member_alias".into()];
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let session = VortexSession::default()
            .with_allocator(Arc::new(crate::owned_buffers::ReservedHostAllocator::new(
                memory.clone(),
            )))
            .with_some(super::super::ProviderFault {
                memory: memory.clone(),
                calls: Arc::clone(&calls),
                corruption,
                repeat,
            });
        assert!(
            vortex::array::legacy_session()
                .get_opt::<super::super::ProviderFault>()
                .is_none()
        );
        let chunks = [
            chunk(&[1, 1, 2, 3], &[9, 9, 8, 7]),
            chunk(&[1, 2, 2, 3], &[10, 8, 6, 7]),
        ];
        let mut states =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let mut workers = ExactDistinctWorkers::admit(
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
        assert_eq!(
            workers
                .partitions
                .as_ref()
                .unwrap()
                .evidence()
                .unwrap()
                .committed_rows,
            4
        );
        let submitted = workers.submit(&chunks[1], &mut states);
        if corruption {
            assert!(
                submitted
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("injected exact distinct provider corruption")
            );
            assert_eq!(calls.load(Ordering::SeqCst), 2);
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
                assert_eq!(workers.retry_jobs, 1);
                assert_eq!(workers.handoffs, 1);
                assert_eq!(workers.rows, 8);
                assert!(workers.take_exact_result().is_none());
                assert_eq!(
                    output(&mut workers, &states),
                    serde_json::json!([{"cohort_alias":2,"uniques_alias":2},{"cohort_alias":3,"uniques_alias":1}])
                );
            }
            assert_eq!(calls.load(Ordering::SeqCst), 3);
        }
        drop(workers);
        drop(states);
        drop(session);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn exact_distinct_workers_native_dictionary_domain_and_cancel_ownership() {
    let request = request();
    let columns = vec!["cohort_alias".into(), "member_alias".into()];
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let session = VortexSession::default();
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let make = |values: Vec<i64>, codes: Vec<u8>| {
        let dict = DictArray::try_new(
            PrimitiveArray::new(codes, Validity::NonNullable).into_array(),
            PrimitiveArray::new(values, Validity::NonNullable).into_array(),
        )
        .unwrap()
        .into_array();
        StructArray::try_new(
            FieldNames::from(["cohort_alias", "member_alias"]),
            vec![
                PrimitiveArray::new(vec![1_i16, 1, 2, 3], Validity::NonNullable).into_array(),
                dict,
            ],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    };
    let chunks = [
        make(vec![i64::MIN, i64::MAX], vec![0, 1, 1, 0]),
        make(vec![i64::MAX, i64::MIN], vec![1, 0, 0, 1]),
    ];
    let mut workers = ExactDistinctWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    for chunk in &chunks {
        assert!(workers.submit(chunk, &mut states).unwrap());
    }
    workers.finish(&mut states).unwrap();
    assert_eq!(workers.rows, 8);
    assert_eq!(
        output(&mut workers, &states),
        serde_json::json!([{"cohort_alias":2,"uniques_alias":1},{"cohort_alias":3,"uniques_alias":1}])
    );
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut workers = ExactDistinctWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&chunks[0], &mut states).unwrap());
    workers.cancel_for_source_replay();
    assert!(workers.finish(&mut states).is_err());
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_workers_reject_nullable_parent_children_and_mixed_measures_before_reservation() {
    let request = request();
    let columns = vec!["cohort_alias".into(), "member_alias".into()];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default();
    let states = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let group = PrimitiveArray::new(vec![1_i16, 2], Validity::NonNullable).into_array();
    for (value, validity) in [
        (
            PrimitiveArray::new(vec![1_u64, 2], Validity::NonNullable).into_array(),
            Validity::AllValid,
        ),
        (
            PrimitiveArray::from_option_iter([Some(1_u64), None]).into_array(),
            Validity::NonNullable,
        ),
    ] {
        let input = StructArray::try_new(
            FieldNames::from(["cohort_alias", "member_alias"]),
            vec![group.clone(), value],
            2,
            validity,
        )
        .unwrap()
        .into_array();
        assert!(
            ExactDistinctWorkers::admit(
                &states,
                input.dtype(),
                &columns,
                VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
                &session,
                &memory
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
    let mut mixed = request.clone();
    mixed.measures.push(VortexSimpleAggregateMeasure::new(
        "count",
        None,
        "rows_alias".into(),
    ));
    let states = GroupedAggregateStates::new(&mixed, Some(2), &columns, false, false).unwrap();
    let input = chunk(&[1, 2], &[7, 8]);
    assert!(
        ExactDistinctWorkers::admit(
            &states,
            input.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
            &session,
            &memory
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_dispatch_installs_explicit_final_counts_with_owned_result_lifetime() {
    use super::super::super::{SimpleAggregateFunction, aggregate_count_workers::CountWorkers};
    let request = request();
    let columns = vec!["cohort_alias".into(), "member_alias".into()];
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let session = VortexSession::default();
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let array = chunk(&[1, 1, 2, 2, 3], &[8, 9, 8, 9, 7]);
    let mut workers = CountWorkers::admit(
        &states,
        array.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(workers, CountWorkers::ExactDistinct(_)));
    assert!(workers.submit(&array, &mut states).unwrap());
    workers.finish(&mut states).unwrap();
    assert!(matches!(
        states.state_template.states[0].function,
        SimpleAggregateFunction::CountDistinct
    ));
    assert!(states.state_template.states[0].distinct_values.is_empty());
    assert!(states.groups.is_empty());
    let reserved = states
        .finalized_distinct_counts
        .as_ref()
        .unwrap()
        .reserved_bytes();
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, reserved);
    let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!([{"cohort_alias":2,"uniques_alias":2},{"cohort_alias":3,"uniques_alias":1}])
    );
    assert!(
        payload["functions"]
            .as_str()
            .unwrap()
            .contains("count_distinct")
    );
    drop(states);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_eof_initial_reservation_denial_preserves_all_pairs_for_exact_handoff() {
    let request = request();
    let columns = vec!["cohort_alias".into(), "member_alias".into()];
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let session = VortexSession::default();
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let array = chunk(&[1, 1, 2, 2, 3], &[8, 9, 8, 9, 7]);
    let mut workers = ExactDistinctWorkers::admit(
        &states,
        array.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&array, &mut states).unwrap());
    workers.drain(&mut states).unwrap();
    let snapshot = memory.snapshot();
    let competing = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes)
        .unwrap();
    workers.finish(&mut states).unwrap();
    assert_eq!(
        memory.snapshot().denied_reservations,
        snapshot.denied_reservations + 1
    );
    assert_eq!(workers.handoffs, 1);
    assert_eq!(workers.group_jobs, 0);
    assert_eq!(workers.rows, 5);
    assert!(workers.take_exact_result().is_none());
    assert_eq!(
        output(&mut workers, &states),
        serde_json::json!([{"cohort_alias":2,"uniques_alias":2},{"cohort_alias":3,"uniques_alias":1}])
    );
    drop(competing);
    drop(workers);
    drop(states);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn exact_distinct_empty_input_and_empty_batches_finalize_without_fake_sets() {
    for submit_empty in [false, true] {
        let request = request();
        let columns = vec!["cohort_alias".into(), "member_alias".into()];
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = VortexSession::default();
        let mut states =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let array = chunk(&[], &[]);
        let mut workers = ExactDistinctWorkers::admit(
            &states,
            array.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(1).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .unwrap();
        if submit_empty {
            assert!(workers.submit(&array, &mut states).unwrap());
        }
        workers.finish(&mut states).unwrap();
        let result = workers.take_exact_result().unwrap();
        assert_eq!(result.group_count(), 0);
        assert_eq!(result.retained_count(), 0);
        assert_eq!(result.result_summary(&states, Some(2)).unwrap().0, 0);
        assert!(states.groups.is_empty());
        drop(result);
        drop(workers);
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
