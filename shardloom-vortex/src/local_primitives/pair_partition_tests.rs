//! Exact pair reduction must preserve the existing late-measure result contract.

use super::{
    GroupedAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    pair_partition_workers::PairWorkers,
};
use crate::{
    VortexAggregateOrderExpr, VortexAggregateSpillPolicy, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::ColumnRef;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{PrimitiveArray, StructArray},
    dtype::FieldNames,
    validity::Validity,
};

fn columns() -> Vec<String> {
    ["entity_key", "origin_key", "weight", "span"]
        .map(str::to_owned)
        .into()
}

fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        columns()[..2]
            .iter()
            .map(|name| ColumnRef::new(name).unwrap())
            .collect(),
        vec![
            VortexSimpleAggregateMeasure::new("count", None, "frequency".into()),
            VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("weight").unwrap()),
                "weight_sum".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("span").unwrap()),
                "span_avg".into(),
            ),
        ],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("frequency", true)])
}

fn signed(values: &[i64]) -> ArrayRef {
    PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array()
}

fn chunk(first: ArrayRef, second: ArrayRef, weight: ArrayRef, span: ArrayRef) -> ArrayRef {
    let rows = first.len();
    StructArray::try_new(
        columns().into_iter().collect::<FieldNames>(),
        vec![first, second, weight, span],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn pairs(first: &[i64], second: &[i64]) -> ArrayRef {
    chunk(
        signed(first),
        signed(second),
        signed(&vec![1; first.len()]),
        signed(&vec![10; first.len()]),
    )
}

fn values(states: &GroupedAggregateStates<'_>, limit: usize) -> serde_json::Value {
    let (_, summary) = states.result_row_count_and_summary(Some(limit)).unwrap();
    serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
}

fn complete_late_measures(
    states: &mut GroupedAggregateStates<'_>,
    chunks: &[ArrayRef],
    limit: usize,
) {
    if states.needs_numeric_pair_late_measure_second_pass() {
        states
            .prepare_numeric_pair_late_measure_second_pass(Some(limit))
            .unwrap();
        for chunk in chunks {
            assert!(
                states
                    .update_numeric_pair_late_measure_direct_from_chunk(chunk, &columns())
                    .unwrap()
            );
        }
    }
}

fn serial_values(
    request: &VortexSimpleAggregateRequest,
    chunks: &[ArrayRef],
    limit: usize,
) -> serde_json::Value {
    let columns = columns();
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, true, false).unwrap();
    for chunk in chunks {
        assert!(
            states
                .update_compact_direct_from_chunk(chunk, &columns, None)
                .unwrap()
        );
    }
    complete_late_measures(&mut states, chunks, limit);
    values(&states, limit)
}

fn worker_values(
    request: &VortexSimpleAggregateRequest,
    chunks: &[ArrayRef],
    limit: usize,
    parallelism: usize,
    force: bool,
) -> serde_json::Value {
    let columns = columns();
    let memory = LiveMemoryPool::new(32 << 20).unwrap();
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, true, false).unwrap();
    let mut workers = PairWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
        &memory,
    )
    .unwrap()
    .expect("renamed integer pair and late measures admitted");
    if force {
        workers.force_partitions_for_test();
    }
    for chunk in chunks {
        workers.before_next().unwrap();
        assert!(workers.submit(chunk, &mut states).unwrap());
    }
    workers.finish(&mut states).unwrap();
    assert!(states.numeric_pair_partition_selection.is_some());
    assert!(states.needs_numeric_pair_late_measure_second_pass());
    complete_late_measures(&mut states, chunks, limit);
    assert!(states.numeric_pair_partition_selection.is_none());
    let (_, mut summary) = states.result_row_count_and_summary(Some(limit)).unwrap();
    workers.annotate_summary(&mut summary).unwrap();
    let result = serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone();
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert!(memory.snapshot().peak_reserved_bytes <= memory.snapshot().limit_bytes);
    drop((workers, states));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    result
}

#[test]
fn pair_partitions_keep_a_global_winner_absent_from_every_chunk_top_one() {
    let chunks = (0..3)
        .map(|index| {
            let mut first = vec![100 + index; 6];
            first.extend([7; 5]);
            let mut second = vec![index; 6];
            second.extend([9; 5]);
            pairs(&first, &second)
        })
        .collect::<Vec<_>>();
    let request = request();
    let expected = serial_values(&request, &chunks, 1);
    assert_eq!(expected[0]["entity_key"], 7);
    assert_eq!(expected[0]["origin_key"], 9);
    assert_eq!(expected[0]["frequency"], 15);
    assert_eq!(expected[0]["weight_sum"], 15.0);
    for parallelism in [1, 3] {
        assert_eq!(
            worker_values(&request, &chunks, 1, parallelism, true),
            expected
        );
    }
}

#[test]
fn pair_partitions_preserve_signed_and_unsigned_extrema_in_complete_keys() {
    let chunks = [
        chunk(
            signed(&[i64::MIN, -1, i64::MAX, i64::MIN, 0]),
            PrimitiveArray::new(
                vec![u64::MAX, 0, u64::MAX, u64::MAX, 1 << 63],
                Validity::NonNullable,
            )
            .into_array(),
            signed(&[1, 2, 3, 4, 5]),
            signed(&[10, 20, 30, 40, 50]),
        ),
        chunk(
            signed(&[i64::MAX, -1, i64::MIN]),
            PrimitiveArray::new(vec![u64::MAX, 0, 0], Validity::NonNullable).into_array(),
            signed(&[6, 7, 8]),
            signed(&[60, 70, 80]),
        ),
    ];
    let request = request();
    let expected = serial_values(&request, &chunks, 8);
    assert_eq!(expected[0]["entity_key"], i64::MIN);
    assert_eq!(expected[0]["origin_key"], u64::MAX);
    for parallelism in [1, 3] {
        assert_eq!(
            worker_values(&request, &chunks, 8, parallelism, true),
            expected
        );
    }
}

#[test]
fn pair_partitions_apply_full_pair_ties_before_offset_and_limit() {
    let chunks = [pairs(&[5, -2, -2, 0], &[0, 9, -9, -1])];
    let request = request().with_offset(1);
    let expected = serial_values(&request, &chunks, 2);
    assert_eq!(expected[0]["entity_key"], -2);
    assert_eq!(expected[0]["origin_key"], 9);
    assert_eq!(expected[1]["entity_key"], 0);
    assert_eq!(worker_values(&request, &chunks, 2, 3, true), expected);
    let request = request.with_offset(127);
    assert_eq!(
        worker_values(&request, &chunks, 1, 1, true),
        serde_json::json!([])
    );
}

#[test]
fn pair_partitions_accept_empty_chunks_without_inventing_groups() {
    for chunks in [
        vec![pairs(&[], &[]), pairs(&[], &[])],
        vec![pairs(&[], &[]), pairs(&[1, 1], &[2, 2]), pairs(&[], &[])],
    ] {
        let request = request();
        assert_eq!(
            worker_values(&request, &chunks, 2, 3, true),
            serial_values(&request, &chunks, 2)
        );
    }
}

#[test]
fn pair_partitions_keep_nullable_late_measures_and_original_float_update_order() {
    let nullable = |values: &[Option<f64>]| {
        PrimitiveArray::from_option_iter(values.iter().copied()).into_array()
    };
    let chunks = [
        chunk(
            signed(&[1]),
            signed(&[2]),
            nullable(&[Some(1e16)]),
            nullable(&[Some(1e16)]),
        ),
        chunk(
            signed(&[1]),
            signed(&[2]),
            nullable(&[Some(1.0)]),
            nullable(&[Some(1.0)]),
        ),
        chunk(
            signed(&[1, 1, 7]),
            signed(&[2, 2, 8]),
            nullable(&[Some(-1e16), None, None]),
            nullable(&[Some(-1e16), None, None]),
        ),
    ];
    let request = request();
    let expected = serial_values(&request, &chunks, 2);
    assert_eq!(expected[0]["frequency"], 4);
    assert_eq!(expected[0]["weight_sum"], 0.0);
    assert_eq!(expected[0]["span_avg"], 0.0);
    assert!(expected[1]["weight_sum"].is_null());
    assert!(expected[1]["span_avg"].is_null());
    for parallelism in [1, 3] {
        assert_eq!(
            worker_values(&request, &chunks, 2, parallelism, true),
            expected
        );
    }
}

#[test]
fn pair_partitions_preserve_retained_measure_overflow_errors() {
    let columns = columns();
    let request = request();
    let chunk = chunk(
        signed(&[1, 1]),
        signed(&[2, 2]),
        PrimitiveArray::new(vec![f64::MAX, f64::MAX], Validity::NonNullable).into_array(),
        signed(&[1, 1]),
    );
    for partitioned in [false, true] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, true, false).unwrap();
        let workers = if partitioned {
            let mut workers = PairWorkers::admit(
                &states,
                chunk.dtype(),
                &columns,
                VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
                &memory,
            )
            .unwrap()
            .unwrap();
            workers.force_partitions_for_test();
            assert!(workers.submit(&chunk, &mut states).unwrap());
            workers.finish(&mut states).unwrap();
            Some(workers)
        } else {
            assert!(
                states
                    .update_compact_direct_from_chunk(&chunk, &columns, None)
                    .unwrap()
            );
            None
        };
        states
            .prepare_numeric_pair_late_measure_second_pass(Some(1))
            .unwrap();
        let error = states
            .update_numeric_pair_late_measure_direct_from_chunk(&chunk, &columns)
            .expect_err("the retained sum must reject non-finite accumulation");
        assert!(
            error.to_string().contains("sum became non-finite"),
            "{error}"
        );
        drop((workers, states));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn pair_partitions_reconcile_duplicates_after_a_near_unique_first_sample() {
    let unique = (0..16_384).collect::<Vec<i64>>();
    let chunks = [
        pairs(&[], &[]),
        pairs(&unique, &unique),
        pairs(&[73; 1_000], &[73; 1_000]),
        pairs(&[73; 1_000], &[73; 1_000]),
    ];
    let request = request();
    let expected = serial_values(&request, &chunks, 3);
    assert_eq!(expected[0]["entity_key"], 73);
    assert_eq!(expected[0]["frequency"], 2_001);
    // No force hook: empty chunks cannot make the sampling decision, and
    // subsequent concentrated duplicates cannot invalidate exact reconciliation.
    assert_eq!(worker_values(&request, &chunks, 3, 3, false), expected);
}

#[test]
fn pair_partition_sample_decline_returns_the_untouched_chunk_to_serial_state() {
    let columns = columns();
    let request = request();
    let empty = pairs(&[], &[]);
    let repeated = pairs(&vec![7; 16_384], &vec![9; 16_384]);
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let mut states = GroupedAggregateStates::new(&request, Some(1), &columns, true, false).unwrap();
    let mut workers = PairWorkers::admit(
        &states,
        repeated.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(workers.submit(&empty, &mut states).unwrap());
    assert!(!workers.has_committed_input());
    assert!(!workers.submit(&repeated, &mut states).unwrap());
    assert!(workers.provider_restore_requested());
    assert!(!workers.has_committed_input());
    assert!(states.numeric_pair_partition_selection.is_none());
    assert!(states.numeric_pair_late_measure_count_groups.is_none());
    assert!(
        states
            .numeric_pair_late_measure_near_unique_directory
            .is_none()
    );
    assert!(
        states
            .update_compact_direct_from_chunk(&repeated, &columns, None)
            .unwrap()
    );
    workers.finish(&mut states).unwrap();
    let chunks = [repeated];
    complete_late_measures(&mut states, &chunks, 1);
    assert_eq!(values(&states, 1), serial_values(&request, &chunks, 1));
    drop((workers, states));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn pair_partition_admission_declines_nullable_keys_count_column_and_unbounded_shapes() {
    let columns = columns();
    let ordinary = pairs(&[1], &[2]);
    let nullable_first = chunk(
        PrimitiveArray::from_option_iter([Some(1_i64)]).into_array(),
        signed(&[2]),
        signed(&[3]),
        signed(&[4]),
    );
    let nullable_second = chunk(
        signed(&[1]),
        PrimitiveArray::from_option_iter([Some(2_i64)]).into_array(),
        signed(&[3]),
        signed(&[4]),
    );
    let mut count_column = request();
    count_column.measures[0] = VortexSimpleAggregateMeasure::new(
        "count",
        Some(ColumnRef::new("weight").unwrap()),
        "frequency".into(),
    );
    let spill = request().with_spill(
        VortexAggregateSpillPolicy::new(
            std::env::temp_dir().join("shardloom-pair-admission-only"),
            8 << 20,
            4 << 20,
        )
        .unwrap(),
    );
    let having = request().with_having(vec![crate::VortexAggregateHavingExpr::new(
        "frequency",
        shardloom_core::ComparisonOp::Gt,
        "0",
    )]);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    for (request, limit, chunk) in [
        (request(), Some(1), &nullable_first),
        (request(), Some(1), &nullable_second),
        (count_column, Some(1), &ordinary),
        (spill, Some(1), &ordinary),
        (having, Some(1), &ordinary),
        (request(), None, &ordinary),
        (request(), Some(129), &ordinary),
        (request().with_offset(128), Some(1), &ordinary),
        (request().with_offset(usize::MAX), Some(1), &ordinary),
    ] {
        let states = GroupedAggregateStates::new(&request, limit, &columns, true, false).unwrap();
        assert!(
            PairWorkers::admit(
                &states,
                chunk.dtype(),
                &columns,
                VortexLocalPrimitiveExecutionPolicy::new(3).unwrap(),
                &memory,
            )
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn pair_partition_source_precheck_recognizes_renamed_late_measure_roles() {
    use super::aggregate_count_workers::{request_may_be_admitted, restore_provider_drivers};
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::DatasetUri;

    let chunk = pairs(&[1], &[2]);
    let query = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed-pairs.vortex").unwrap(),
        request(),
    )
    .with_source_order_limit(3);
    assert!(request_may_be_admitted(&query));
    assert!(!restore_provider_drivers(&query, chunk.dtype()));
}

#[test]
fn pair_partition_cancellation_after_committed_input_publishes_no_selection() {
    let columns = columns();
    let request = request();
    let chunk = pairs(&[1, 1], &[2, 2]);
    for parallelism in [1, 3] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, true, false).unwrap();
        let mut workers = PairWorkers::admit(
            &states,
            chunk.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &memory,
        )
        .unwrap()
        .unwrap();
        workers.force_partitions_for_test();
        assert!(workers.submit(&chunk, &mut states).unwrap());
        assert!(workers.has_committed_input());
        assert!(memory.snapshot().reserved_bytes > 0);
        workers.cancel_for_test();
        let error = workers
            .finish(&mut states)
            .expect_err("cancelled input cannot finalize");
        assert!(error.to_string().contains("cancel"), "{error}");
        assert!(states.numeric_pair_partition_selection.is_none());
        assert!(states.numeric_pair_late_measure_retained_keys.is_none());
        drop((workers, states));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn pair_partition_growth_denial_after_committed_input_is_fatal_and_releases_leases() {
    let columns = columns();
    let request = request();
    let first = pairs(&[1], &[2]);
    let growing = pairs(&(0..1_024).collect::<Vec<i64>>(), &vec![2; 1_024]);
    for parallelism in [1, 3] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, true, false).unwrap();
        let mut workers = PairWorkers::admit(
            &states,
            first.dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap(),
            &memory,
        )
        .unwrap()
        .unwrap();
        workers.force_partitions_for_test();
        assert!(workers.submit(&first, &mut states).unwrap());
        assert!(workers.has_committed_input());
        assert_eq!(memory.snapshot().denied_reservations, 0);
        workers.deny_next_growth_for_test();
        let error = match workers.submit(&growing, &mut states) {
            Err(error) => error,
            Ok(true) => workers
                .finish(&mut states)
                .expect_err("growth denial must fail completion"),
            Ok(false) => panic!("committed input cannot be handed to unbounded serial state"),
        };
        assert!(
            error.to_string().contains("memory reservation denied"),
            "{error}"
        );
        assert!(memory.snapshot().denied_reservations > 0);
        assert!(states.numeric_pair_partition_selection.is_none());
        assert!(states.numeric_pair_late_measure_retained_keys.is_none());
        drop((workers, states));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
