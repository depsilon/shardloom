use super::super::{aggregate_count_workers, compound_count_roles};
use super::*;
use crate::VortexQueryPrimitiveRequest;
use shardloom_core::DatasetUri;
use std::collections::BTreeSet;
use vortex::array::dtype::{DType, Nullability};

const GROUP: &str = "delivery_region";
const VALUE: &str = "package_number";
const COUNT: &str = "unique_packages";

fn request(offset: usize) -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new(GROUP).unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new(VALUE).unwrap()),
            COUNT.into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new(COUNT, true)])
    .with_offset(offset)
}
#[cfg(all(feature = "vortex-write", unix))]
fn query(path: &std::path::Path, offset: usize, limit: usize) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        request(offset),
    )
    .with_source_order_limit(limit)
}
fn columns() -> Vec<String> {
    vec![GROUP.into(), VALUE.into()]
}
fn chunk(text: ArrayRef, values: ArrayRef) -> ArrayRef {
    let rows = text.len();
    StructArray::try_new(
        FieldNames::from([VALUE, GROUP]),
        vec![values, text],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}
fn state(
    aggregate: &VortexSimpleAggregateRequest,
    limit: usize,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> GroupedAggregateStates<'_> {
    static COLUMNS: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    GroupedAggregateStates::new_with_resource_envelope(
        aggregate,
        Some(limit),
        COLUMNS.get_or_init(columns),
        false,
        false,
        policy.resource_envelope,
    )
    .unwrap()
}
fn selected_distinct(partitions: &CompoundPartitions) -> BTreeMap<String, u64> {
    let mut result = BTreeMap::new();
    for index in 0..PARTITIONS {
        let selected = partitions.select_text_distinct(index, &worker()).unwrap();
        partitions
            .visit_text_distinct(&selected, |key, count| {
                assert!(result.insert(key.to_owned(), count).is_none());
                Ok(())
            })
            .unwrap();
    }
    result
}

#[test]
fn utf8_integer_distinct_all_widths_domains_and_row_weights_remain_exact() {
    macro_rules! array {
        ($t:ty) => {
            PrimitiveArray::new(vec![<$t>::MIN, 7, <$t>::MAX, 7], Validity::NonNullable)
                .into_array()
        };
    }
    for numeric in [
        array!(i8),
        array!(i16),
        array!(i32),
        array!(i64),
        array!(u8),
        array!(u16),
        array!(u32),
        array!(u64),
    ] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let partitions = CompoundPartitions::try_new_text_distinct(&memory, 100, 10)
            .unwrap()
            .unwrap();
        for text in [
            strings(&["東京", "東京", "", "東京"]),
            dictionary(&[2, 0, 1, 2], &["東京", "", "東京"]),
        ] {
            let counted = partial(&numeric, &text, &memory);
            assert!(
                partitions
                    .reduce(counted, &worker())
                    .unwrap()
                    .deferred
                    .is_none()
            );
        }
        let evidence = partitions.evidence().unwrap();
        assert_eq!(evidence.rows, 8);
        assert_eq!(evidence.groups, 3);
        assert_eq!(evidence.strings, 2);
        assert_eq!(
            selected_distinct(&partitions),
            BTreeMap::from([(String::new(), 1), ("東京".into(), 2)])
        );
        drop(partitions);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn utf8_integer_distinct_full_pair_collisions_preserve_same_partition_text_identity() {
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let keys = (0..100_000)
        .map(|index| format!("same-prefix-{index}"))
        .filter(|key| {
            compound_count_partial::partition::<PARTITIONS>(compound_count_partial::string_hash(
                key.as_bytes(),
            )) == 0
        })
        .take(30)
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), 30);
    let text = strings(&keys.iter().map(String::as_str).collect::<Vec<_>>());
    let values = integers(&[i64::MIN; 30]);
    let partitions = CompoundPartitions::try_new_text_distinct(&memory, 100, 100)
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        let mut counted = partial(&values, &text, &memory);
        counted.force_collision_hashes();
        partitions.reduce(counted, &worker()).unwrap();
    }
    assert_eq!(
        selected_distinct(&partitions),
        keys.into_iter().map(|key| (key, 1)).collect()
    );
    assert_eq!(partitions.evidence().unwrap().groups, 30);
    assert_eq!(partitions.evidence().unwrap().rows, 90);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn corpus() -> (Vec<ArrayRef>, BTreeMap<String, BTreeSet<u64>>, usize) {
    let mut oracle = BTreeMap::<String, BTreeSet<u64>>::new();
    let mut chunks = Vec::new();
    let mut rows = 0;
    for batch in 0..12_u64 {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        for row in 0..256_u64 {
            let key = match row % 6 {
                0 => "",
                1 => "東京",
                2 => "é",
                3 => "e\u{301}",
                4 => "\0",
                _ => "heavy",
            };
            let value = if key == "heavy" {
                u64::MAX - batch
            } else {
                (row % 7) + (1_u64 << 54)
            };
            oracle.entry(key.into()).or_default().insert(value);
            keys.push(key);
            values.push(value);
        }
        rows += keys.len();
        chunks.push(chunk(
            strings(&keys),
            PrimitiveArray::new(values, Validity::NonNullable).into_array(),
        ));
    }
    (chunks, oracle, rows)
}
fn expected(
    oracle: &BTreeMap<String, BTreeSet<u64>>,
    offset: usize,
    limit: usize,
) -> serde_json::Value {
    let mut rows = oracle
        .iter()
        .map(|(key, values)| (key, values.len()))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    rows.into_iter()
        .skip(offset)
        .take(limit)
        .map(|(key, count)| serde_json::json!({GROUP:key, COUNT:count}))
        .collect::<Vec<_>>()
        .into()
}

#[test]
fn utf8_integer_distinct_complete_workers_global_winner_offset_and_actual_evidence() {
    let (chunks, oracle, rows) = corpus();
    for parallelism in [1, 2, 4] {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        let aggregate = request(1);
        let mut states = state(&aggregate, 4, policy);
        let mut workers = CompoundWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns(),
            policy,
            vortex::array::legacy_session(),
            &memory,
        )
        .unwrap()
        .unwrap();
        for chunk in &chunks {
            workers.before_next(&mut states).unwrap();
            assert!(workers.submit(chunk, &mut states).unwrap());
        }
        workers.finish(&mut states).unwrap();
        let (_, mut summary) = states.result_row_count_and_summary(Some(4)).unwrap();
        workers.annotate_summary(&mut summary).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(payload["values"], expected(&oracle, 1, 4));
        assert_eq!(payload["candidate_groups"], oracle.len());
        assert_eq!(payload["aggregate_workers_rows"], rows);
        assert_eq!(payload["aggregate_workers_completed_chunks"], chunks.len());
        assert_eq!(payload["aggregate_workers_outstanding_chunks"], 0);
        assert_eq!(
            payload["aggregate_workers_partition_complete_pairs"],
            oracle.values().map(BTreeSet::len).sum::<usize>()
        );
        let completed = payload["aggregate_workers_count_chunks_by_worker"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_u64().unwrap())
            .sum::<u64>();
        assert_eq!(
            completed
                + payload["aggregate_workers_inline_count_chunks"]
                    .as_u64()
                    .unwrap(),
            chunks.len() as u64
        );
        if completed != 0 {
            assert!(
                !payload["aggregate_workers_actual_count_worker_indices"]
                    .as_array()
                    .unwrap()
                    .is_empty()
            );
        }
        drop(workers);
        assert_eq!(
            states
                .finalized_distinct_counts
                .as_ref()
                .unwrap()
                .group_count(),
            oracle.len()
        );
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn admitted(dtype: &DType, aggregate: &VortexSimpleAggregateRequest) -> bool {
    let memory = LiveMemoryPool::new(2 << 20).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let states = state(aggregate, 3, policy);
    let request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("/absent-admission.vortex").unwrap(),
        aggregate.clone(),
    )
    .with_source_order_limit(3);
    let expected = compound_count_roles::admit(&states, dtype, &columns()).is_some();
    assert_eq!(
        !aggregate_count_workers::restore_provider_drivers(&request, dtype),
        expected
    );
    let workers = aggregate_count_workers::CountWorkers::admit(
        &states,
        dtype,
        &columns(),
        policy,
        vortex::array::legacy_session(),
        &memory,
    )
    .unwrap();
    assert_eq!(workers.is_some(), expected);
    drop(workers);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    expected
}

#[test]
fn utf8_integer_distinct_schema_and_order_admission_match_provider_precheck() {
    let good = chunk(strings(&["x"]), integers(&[1]));
    assert!(admitted(good.dtype(), &request(0)));
    for (group, value, root) in [
        (
            Nullability::Nullable,
            Nullability::NonNullable,
            Nullability::NonNullable,
        ),
        (
            Nullability::NonNullable,
            Nullability::Nullable,
            Nullability::NonNullable,
        ),
        (
            Nullability::NonNullable,
            Nullability::NonNullable,
            Nullability::Nullable,
        ),
    ] {
        let dtype = DType::struct_(
            [
                (GROUP, DType::Utf8(group)),
                (
                    VALUE,
                    DType::Primitive(vortex::array::dtype::PType::I64, value),
                ),
            ],
            root,
        );
        assert!(!admitted(&dtype, &request(0)));
    }
    let mut aggregate = request(0);
    aggregate
        .order_by
        .push(VortexAggregateOrderExpr::new(GROUP, false));
    assert!(admitted(good.dtype(), &aggregate));
    aggregate.order_by[1].descending = true;
    // An unsupported order is not a worker-shaped request at all; test actual
    // role admission separately from the shape-dependent provider precheck.
    let states = state(
        &aggregate,
        3,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    );
    assert!(compound_count_roles::admit(&states, good.dtype(), &columns()).is_none());
}

#[test]
fn utf8_integer_distinct_entry_pressure_never_installs_untracked_exact_groups() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.group_state_soft_item_budget = 2;
    let aggregate = request(0);
    let mut states = state(&aggregate, 3, policy);
    let data = chunk(strings(&["x", "x", "x"]), integers(&[1, 2, 3]));
    let mut workers = CompoundWorkers::admit(
        &states,
        data.dtype(),
        &columns(),
        policy,
        vortex::array::legacy_session(),
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&data, &mut states).unwrap());
    let error = workers.finish(&mut states).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("UTF8 DISTINCT committed state pressure")
    );
    assert!(states.groups.is_empty());
    assert!(states.finalized_distinct_counts.is_none());
    drop(workers);
    drop(states);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn utf8_integer_distinct_initial_and_committed_task_capacity_denial_are_distinct() {
    for committed in [false, true] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
        let aggregate = request(0);
        let mut states = state(&aggregate, 3, policy);
        let data = chunk(strings(&["x"]), integers(&[1]));
        let mut workers = CompoundWorkers::admit(
            &states,
            data.dtype(),
            &columns(),
            policy,
            vortex::array::legacy_session(),
            &memory,
        )
        .unwrap()
        .unwrap();
        if committed {
            assert!(workers.submit(&data, &mut states).unwrap());
            workers.drain(&mut states).unwrap();
        }
        workers.deny_next_initial_reservation_for_test();
        let result = workers.submit(&data, &mut states);
        if committed {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("UTF8 DISTINCT committed state pressure")
            );
        } else {
            assert!(!result.unwrap());
            assert!(!workers.has_active_partitions());
        }
        assert!(states.groups.is_empty());
        drop(workers);
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn utf8_integer_distinct_eof_denial_preserves_full_pairs_and_weights() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let partitions = CompoundPartitions::try_new_text_distinct(&memory, 100, 3)
        .unwrap()
        .unwrap();
    let data = partial(&integers(&[1, 1, 2]), &strings(&["x", "x", "x"]), &memory);
    partitions.reduce(data, &worker()).unwrap();
    let pressure = memory
        .reserve(memory.snapshot().limit_bytes - memory.snapshot().reserved_bytes)
        .unwrap();
    let index =
        compound_count_partial::partition::<PARTITIONS>(compound_count_partial::string_hash(b"x"));
    assert!(
        partitions
            .select_text_distinct(index, &worker())
            .err()
            .unwrap()
            .to_string()
            .contains("EOF domain-count reservation denied")
    );
    drop(pressure);
    let mut pairs = BTreeMap::new();
    partitions
        .replay_and_release(|key, text, weight| {
            pairs.insert((logical(key), text.to_owned()), weight);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        pairs,
        BTreeMap::from([((1, "x".into()), 2), ((2, "x".into()), 1)])
    );
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn utf8_integer_distinct_actual_nullable_leaf_and_cancellation_fail_before_publish() {
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut lease = memory.reserve(CompoundPartial::bytes(2).unwrap()).unwrap();
    let nullable = PrimitiveArray::from_option_iter([Some(1_i64), None]).into_array();
    assert!(
        compound_count_partial::count(
            &nullable,
            &strings(&["x", "x"]),
            vortex::array::legacy_session().create_execution_ctx(),
            &worker(),
            &mut lease
        )
        .is_err()
    );
    drop(lease);
    let partitions = CompoundPartitions::try_new_text_distinct(&memory, 100, 3)
        .unwrap()
        .unwrap();
    let data = partial(&integers(&[1]), &strings(&["x"]), &memory);
    let cancel = CancellationToken::default();
    cancel.cancel();
    assert!(
        partitions
            .reduce(data, &ChunkWorkerContext::Inline(cancel))
            .is_err()
    );
    assert_eq!(partitions.evidence().unwrap().rows, 0);
    drop(partitions);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn utf8_integer_distinct_cancel_submitted_jobs_publishes_no_partial_output() {
    let (chunks, _, _) = corpus();
    for parallelism in [1, 4] {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
        let aggregate = request(0);
        let mut states = state(&aggregate, 4, policy);
        let mut workers = CompoundWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns(),
            policy,
            vortex::array::legacy_session(),
            &memory,
        )
        .unwrap()
        .unwrap();
        assert!(workers.submit(&chunks[0], &mut states).unwrap());
        workers.cancel_for_source_replay();
        assert!(workers.finish(&mut states).is_err());
        assert!(states.finalized_distinct_counts.is_none());
        assert!(states.groups.is_empty());
        drop(workers);
        drop(states);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[cfg(all(feature = "vortex-write", unix))]
#[path = "utf8_integer_distinct_native_tests.rs"]
mod native;
