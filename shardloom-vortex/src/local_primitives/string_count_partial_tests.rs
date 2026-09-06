use super::super::aggregate_chunk_jobs::AggregateChunkJobs;
use super::super::{
    ColumnRef, GroupedAggregateStates, VortexLocalPrimitiveResourceEnvelope,
    VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
};
use super::*;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::collections::BTreeMap;
use vortex::array::{IntoArray as _, VortexSessionExecute as _, arrays::DictArray};

// Mirror the production caller's reservation step before entering the kernel.
fn run_count(
    array: ArrayRef,
    ctx: ExecutionCtx,
    worker: &ChunkWorkerContext,
    lease: &mut MemoryLease,
) -> Result<StringCountPartial> {
    lease.resize(
        lease
            .bytes()
            .checked_add(partial_bytes(&array)?)
            .ok_or_else(|| failed("fixture reservation overflow"))?,
    )?;
    let result = super::count_string_chunk(&array, ctx, worker, lease);
    drop(array);
    result
}

fn strings(values: &[&str]) -> ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}

fn counts(partial: &StringCountPartial) -> BTreeMap<String, u64> {
    let mut result = BTreeMap::new();
    partial
        .for_each_count(|value, count| {
            let previous = result.entry(value.to_owned()).or_insert(0_u64);
            *previous = previous.checked_add(count).unwrap();
            Ok(())
        })
        .unwrap();
    result
}

#[test]
fn chunk_jobs_count_all_keys_at_each_worker_count_without_losing_global_winner() {
    for workers in [1, 2, 4, 8, 12] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let mut jobs = AggregateChunkJobs::new(workers, 3, 1 << 20, memory.clone()).unwrap();
        for local_winner in ["alpha", "beta", "gamma"] {
            let mut rows = vec![local_winner; 6];
            rows.extend(["not a URL"; 5]);
            rows.extend(["東京", "λ\"\n", ""]);
            let array = strings(&rows);
            jobs.submit(64, move |worker, lease| {
                run_count(
                    array,
                    vortex::array::legacy_session().create_execution_ctx(),
                    worker,
                    lease,
                )
            })
            .unwrap();
        }
        let mut merged = BTreeMap::<String, u64>::new();
        let mut rows = 0;
        while let Some(partial) = jobs.join_next().unwrap() {
            partial
                .consume(|partial| {
                    rows += partial.work.rows;
                    partial.for_each_count(|key, count| {
                        let previous = merged.entry(key.to_owned()).or_default();
                        *previous = previous.checked_add(count).unwrap();
                        Ok(())
                    })
                })
                .unwrap();
        }
        assert_eq!(rows, 42);
        assert_eq!(merged.len(), 7);
        assert_eq!(merged["not a URL"], 15);
        assert_eq!(merged["alpha"], 6);
        assert_eq!(merged["東京"], 3);
        assert_eq!(merged.values().sum::<u64>(), 42);
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_dictionary_codes_remain_bound_to_own_values_and_unused_values_are_absent() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut merged = BTreeMap::<String, u64>::new();
    for (values, codes) in [
        (["tea", "coffee", "unused"], vec![0_u8, 1, 1]),
        (["coffee", "tea", "unused"], vec![0_u8, 0, 1]),
    ] {
        let array = DictArray::try_new(
            codes.into_iter().collect::<PrimitiveArray>().into_array(),
            strings(&values),
        )
        .unwrap()
        .into_array();
        let mut lease = memory.reserve(64).unwrap();
        let partial = run_count(
            array,
            vortex::array::legacy_session().create_execution_ctx(),
            &worker,
            &mut lease,
        )
        .unwrap();
        assert!(partial.work.native_dictionary);
        assert_eq!(partial.work.partial_entries, 2);
        partial
            .for_each_count(|key, count| {
                *merged.entry(key.to_owned()).or_default() += count;
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(
        merged,
        BTreeMap::from([("coffee".into(), 4), ("tea".into(), 2)])
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn all_unique_keys_and_hash_bucket_collisions_preserve_exact_string_equality() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let keys = (0..257)
        .map(|index| format!("sku {index} 東京"))
        .collect::<Vec<_>>();
    let refs = keys.iter().map(String::as_str).collect::<Vec<_>>();
    let mut lease = memory.reserve(0).unwrap();
    let partial = run_count(
        strings(&refs),
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap();
    let result = counts(&partial);
    assert_eq!(result.len(), 257);
    assert!(result.values().all(|count| *count == 1));
    assert!(partial.work.partial_capacity_bytes > 0);
    drop(lease);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(partial);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn nullable_admission_pressure_and_cancellation_fail_without_retained_state() {
    let memory = LiveMemoryPool::new(128).unwrap();
    let worker = ChunkWorkerContext::Inline(CancellationToken::default());
    let mut lease = memory.reserve(0).unwrap();
    let nullable = VarBinViewArray::from_iter_nullable_str([Some("tea"), None]).into_array();
    assert!(
        run_count(
            nullable,
            vortex::array::legacy_session().create_execution_ctx(),
            &worker,
            &mut lease
        )
        .is_err()
    );
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut lease = memory.reserve(0).unwrap();
    assert!(
        run_count(
            strings(&["a", "b", "c", "d"]),
            vortex::array::legacy_session().create_execution_ctx(),
            &worker,
            &mut lease
        )
        .is_err()
    );
    assert_eq!(lease.bytes(), 0);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(
        run_count(
            strings(&["a"]),
            vortex::array::legacy_session().create_execution_ctx(),
            &ChunkWorkerContext::Inline(cancelled),
            &mut lease
        )
        .is_err()
    );
    drop(lease);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn ordered_partial_merge_preserves_global_counts_across_native_pressure_transition() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("product_name").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]);
    let columns = vec!["product_name".to_string()];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    for (entry_budget, byte_pressure) in [(0, false), (1, false), (100, false), (100, true)] {
        let mut states = GroupedAggregateStates::new_with_resource_envelope(
            &request,
            Some(3),
            &columns,
            false,
            true,
            VortexLocalPrimitiveResourceEnvelope::new(24, 4).unwrap(),
        )
        .unwrap();
        states
            .enable_string_count_topk_first_pass_exact_histogram()
            .unwrap();
        states.string_count_topk_first_pass_exact_histogram_entry_budget = entry_budget;
        if byte_pressure {
            states.resource_envelope.memory_budget_bytes =
                super::super::STRING_COUNT_TOPK_FIRST_PASS_EXACT_HISTOGRAM_BYTES_PER_ENTRY + 1;
        }
        let mut reducer = StringCountMerge::default();
        for rows in [
            vec!["a", "a"],
            vec!["a", "b", "a", "b", "b"],
            vec!["b", "c"],
        ] {
            let mut lease = memory.reserve(0).unwrap();
            let partial = run_count(
                strings(&rows),
                vortex::array::legacy_session().create_execution_ctx(),
                &ChunkWorkerContext::Inline(CancellationToken::default()),
                &mut lease,
            )
            .unwrap();
            reducer.merge(&mut states, 0, &partial).unwrap();
        }
        assert_eq!(reducer.rows, 9);
        assert_eq!(states.string_count_topk_total_weight, 9);
        assert_eq!(
            reducer.pressure_transitions,
            u64::from(entry_budget < 100 || byte_pressure)
        );
        assert!(
            states
                .promote_string_count_topk_exact_first_pass_if_possible(Some(3))
                .unwrap()
        );
        let (_, summary) = states.result_row_count_and_summary(Some(3)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            value["values"],
            serde_json::json!([
                {"product_name": "a", "n": 4}, {"product_name": "b", "n": 4}, {"product_name": "c", "n": 1},
            ])
        );
    }
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn worker_pressure_sketch_cannot_hide_a_winner_outside_every_local_top_one() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("product_name").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]);
    let columns = vec!["product_name".to_owned()];
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(1),
        &columns,
        false,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(24, 4).unwrap(),
    )
    .unwrap();
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .unwrap();
    states.string_count_topk_first_pass_exact_histogram_entry_budget = 1;
    states.resource_envelope.string_topk_heavy_hitter_capacity = 1;
    let chunks = [
        strings(&["a", "a", "a", "winner", "winner"]),
        strings(&["b", "b", "b", "winner", "winner"]),
        strings(&["c"]),
    ];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut reducer = StringCountMerge::default();
    for array in &chunks {
        let mut lease = memory.reserve(partial_bytes(array).unwrap()).unwrap();
        let partial = super::count_string_chunk(
            array,
            vortex::array::legacy_session().create_execution_ctx(),
            &ChunkWorkerContext::Inline(CancellationToken::default()),
            &mut lease,
        )
        .unwrap();
        reducer.merge(&mut states, 0, &partial).unwrap();
    }
    assert_eq!(states.string_count_topk_total_weight, 11);
    assert_eq!(reducer.pressure_transitions, 1);
    let sketch = states
        .string_count_topk_heavy_hitter_sketch
        .as_ref()
        .unwrap();
    assert!(
        sketch
            .candidate_ids()
            .iter()
            .all(|id| states.string_interner.value(*id).unwrap() != "winner")
    );
    assert!(
        !states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(1))
            .unwrap()
    );
    assert!(
        !states
            .string_count_topk_heavy_hitter_exact_proof_possible(Some(1))
            .unwrap()
    );
    // The production caller's same-source exact refinement is mandatory when
    // the global omitted-key bound cannot certify its candidate result.
    let mut refined =
        GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
    for chunk in &chunks {
        super::super::update_grouped_exact_states_from_chunk(&mut refined, chunk, &columns, None)
            .unwrap();
    }
    let (_, summary) = refined.result_row_count_and_summary(Some(1)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!([{"product_name":"winner","n":4}])
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn ordered_partial_merge_checks_global_count_overflow_and_invalidates_reducer() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("product_name").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]);
    let columns = vec!["product_name".to_string()];
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(1),
        &columns,
        false,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(24, 4).unwrap(),
    )
    .unwrap();
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .unwrap();
    let id = states.string_interner.intern("tea").unwrap();
    states
        .string_count_topk_first_pass_exact_histogram_counts
        .as_mut()
        .unwrap()
        .insert(id, u64::MAX);
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let mut lease = memory.reserve(0).unwrap();
    let partial = run_count(
        strings(&["tea", "tea"]),
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap();
    let mut reducer = StringCountMerge::default();
    let error = reducer
        .merge(&mut states, 0, &partial)
        .unwrap_err()
        .to_string();
    assert!(error.contains("count overflowed"));
    assert!(reducer.merge(&mut states, 0, &partial).is_err());
    assert_eq!(states.string_count_topk_total_weight, 0);
}

#[test]
fn ordinary_string_reducer_preserves_first_key_order_and_constant_dependent_columns() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("product_name").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "literal_one".into(),
            ColumnRef::new("product_name").unwrap(),
            "constant_int",
        )
        .with_argument_offset(1),
    ]);
    let columns = vec!["product_name".to_string()];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    for workers in [1, 2, 4, 8, 12] {
        let mut states =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        assert_eq!(admitted_group_index(&states), Some(0));
        let mut reducer = StringCountMerge::default();
        let mut jobs = AggregateChunkJobs::new(workers, 2, 1 << 20, memory.clone()).unwrap();
        for rows in [["z", "a", "z"], ["b", "a", "c"]] {
            let array = strings(&rows);
            jobs.submit(64, move |context, lease| {
                let mut partial = run_count(
                    array,
                    vortex::array::legacy_session().create_execution_ctx(),
                    context,
                    lease,
                )?;
                partial.preserve_existing_key_order();
                Ok(partial)
            })
            .unwrap();
        }
        while let Some(completed) = jobs.join_next().unwrap() {
            completed
                .consume(|partial| reducer.merge(&mut states, 0, partial))
                .unwrap();
        }
        assert_eq!(reducer.rows, 6);
        let (_, summary) = states.result_row_count_and_summary(None).unwrap();
        let value: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            value["values"],
            serde_json::json!([
                {"product_name": "z", "literal_one": 1, "n": 2},
                {"product_name": "a", "literal_one": 1, "n": 2},
                {"product_name": "b", "literal_one": 1, "n": 1},
                {"product_name": "c", "literal_one": 1, "n": 1},
            ])
        );
        let limited =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        assert_eq!(admitted_group_index(&limited), None);
        drop(jobs);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_constant_strings_keep_one_value_and_count_without_row_expansion() {
    use vortex::array::arrays::ConstantArray;
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut lease = memory.reserve(0).unwrap();
    let partial = run_count(
        ConstantArray::new("東京", 1_000_000_000).into_array(),
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut lease,
    )
    .unwrap();
    assert!(partial.work.native_constant);
    assert_eq!(partial.work.rows, 1_000_000_000);
    assert_eq!(partial.values.len(), 1);
    assert_eq!(partial.work.partial_entries, 1);
    assert_eq!(partial.work.utf8_bytes_hashed, 0);
    assert_eq!(
        counts(&partial),
        BTreeMap::from([("東京".into(), 1_000_000_000)])
    );
    drop((partial, lease));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn queued_string_tables_are_reserved_before_worker_execution_and_stay_with_output() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let mut jobs = AggregateChunkJobs::new(2, 2, 4096, memory.clone()).unwrap();
    let array = strings(&["tea", "coffee", "tea"]);
    let admitted = partial_bytes(&array).unwrap();
    jobs.submit(64 + admitted, move |context, lease| {
        super::count_string_chunk(
            &array,
            vortex::array::legacy_session().create_execution_ctx(),
            context,
            lease,
        )
    })
    .unwrap();
    let completed = jobs.join_next().unwrap().unwrap();
    assert_eq!(completed.reserved_bytes(), 64);
    assert_eq!(completed.value().work.partial_capacity_bytes, admitted);
    assert_eq!(memory.snapshot().reserved_bytes, 64 + admitted);
    completed
        .consume(|partial| {
            assert_eq!(
                counts(partial),
                BTreeMap::from([("tea".into(), 2), ("coffee".into(), 1)])
            );
            Ok(())
        })
        .unwrap();
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let mut missing = memory.reserve(0).unwrap();
    let error = super::count_string_chunk(
        &strings(&["tea"]),
        vortex::array::legacy_session().create_execution_ctx(),
        &ChunkWorkerContext::Inline(CancellationToken::default()),
        &mut missing,
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("pre-reserve"));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
