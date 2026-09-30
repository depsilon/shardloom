//! Exercise shared serial/worker values, ordered folding and owner retirement.
use super::*;
use crate::local_primitives::{
    aggregate_direct_column_accessors_from_chunk, mixed_distinct_workers::MixedDistinctWorkers,
};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{ArrayRef, arrays::StructArray};

fn chunk(rows: &[Row]) -> ArrayRef {
    StructArray::new(
        ["identity", "width", "bucket", "measure"].into(),
        vec![
            PrimitiveArray::new(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
            PrimitiveArray::new(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                Validity::NonNullable,
            )
            .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .into_array()
}

fn proof(states: &GroupedAggregateStates<'_>, columns: &[String]) -> Report {
    let mut report = admit(states, &dtype(None), columns, 1_000_000, true).unwrap();
    assert!(report.selected_group_bound().is_none());
    report.source_rows = 2;
    report.count_rows = 2;
    report
        .finish_counts(&FxHashMap::from_iter([
            (
                AggregateSingleNumericKey {
                    bits: u64::from_ne_bytes(i64::MIN.to_ne_bytes()),
                    signed: true,
                },
                1,
            ),
            (
                AggregateSingleNumericKey {
                    bits: u64::from_ne_bytes((-1_i64).to_ne_bytes()),
                    signed: true,
                },
                1,
            ),
        ]))
        .unwrap();
    assert_eq!(report.selected_group_bound(), Some(2));
    report
}

fn values(states: &mut GroupedAggregateStates<'_>) -> serde_json::Value {
    let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
    serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
}

#[test]
fn mixed_distinct_workers_preserve_every_row_and_chunk_fold_with_pressure_retirement() {
    let columns = columns();
    let request = request().with_order_by(vec![
        VortexAggregateOrderExpr::new("n", true),
        VortexAggregateOrderExpr::new("bucket", false),
    ]);
    // Chunk order matters for f64 SUM state even though the inputs are integers.
    let chunks = [
        chunk(&[
            (i64::MIN, i64::MAX, 10, u64::MAX),
            (-1, 2, 12, 8),
            (-1, 4, 15, 8),
        ]),
        chunk(
            &std::iter::once((i64::MIN, -i64::MAX, 20, u64::MAX))
                .chain(std::iter::repeat_n((-1, 8, 21, 9), 512))
                .collect::<Vec<_>>(),
        ),
        chunk(&[(i64::MIN, 3, 30, 4), (-1, 16, 33, 9)]),
    ];
    for parallelism in [2, 4] {
        for pressure_after in [None, Some(0), Some(1)] {
            let policy = VortexLocalPrimitiveExecutionPolicy::new(parallelism).unwrap();
            let memory = LiveMemoryPool::new(32 << 20).unwrap();
            let mut serial =
                GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
            let mut state =
                GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
            let report = proof(&state, &columns);
            let mut workers =
                MixedDistinctWorkers::admit(&state, &report, &columns, policy, &memory)
                    .unwrap()
                    .unwrap();
            let mut pressure = None;
            for (index, input) in chunks.iter().enumerate() {
                if pressure_after == Some(index) {
                    let snapshot = memory.snapshot();
                    pressure = Some(
                        memory
                            .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
                            .unwrap(),
                    );
                }
                workers.before_next(&mut state).unwrap();
                if !workers.submit(input, &mut state).unwrap() {
                    assert!(
                        state
                            .update_compact_direct_from_chunk(input, &columns, None)
                            .unwrap()
                    );
                }
                assert!(
                    serial
                        .update_compact_direct_from_chunk(input, &columns, None)
                        .unwrap()
                );
            }
            workers.finish(&mut state).unwrap();
            assert_eq!(values(&mut state), values(&mut serial));
            assert_eq!(workers.retired(), pressure_after.is_some());
            drop(workers);
            drop(pressure);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn mixed_distinct_workers_cancel_and_invalid_proof_fail_without_replaying() {
    let columns = columns();
    let request = request();
    for cancel in [false, true] {
        let memory = LiveMemoryPool::new(16 << 20).unwrap();
        let mut state =
            GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
        let report = proof(&state, &columns);
        let mut workers = MixedDistinctWorkers::admit(
            &state,
            &report,
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
            &memory,
        )
        .unwrap()
        .unwrap();
        let input = chunk(&[(i64::MIN, 1, 2, 3), (-1, 4, 5, 6), (0, 7, 8, 9)]);
        assert!(workers.submit(&input, &mut state).unwrap());
        if cancel {
            workers.cancel();
        }
        assert!(workers.finish(&mut state).is_err());
        assert!(state.groups.is_empty());
        drop(workers);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn mixed_distinct_partial_capacity_and_group_bound_cover_dense_and_sparse_pairs() {
    let columns = columns();
    let request = request();
    let mut state = GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    for rows in [1, 128, 8192] {
        let input = chunk(
            &(0..rows)
                .map(|index| (if index % 2 == 0 { i64::MIN } else { -1 }, 1, 2, index))
                .collect::<Vec<_>>(),
        );
        let accessors = aggregate_direct_column_accessors_from_chunk(
            &input,
            &columns,
            &mut state.native_execution_ctx,
        )
        .unwrap();
        let partial = state
            .prepare_mixed_distinct_partial(&accessors, None, input.len(), Some(2), &|| Ok(()))
            .unwrap()
            .unwrap();
        assert!(partial.retained_capacity_bytes().unwrap() > 0);
        if rows > 1 {
            assert!(
                state
                    .prepare_mixed_distinct_partial(&accessors, None, input.len(), Some(1), &|| Ok(
                        ()
                    ))
                    .is_err()
            );
        }
    }
}
