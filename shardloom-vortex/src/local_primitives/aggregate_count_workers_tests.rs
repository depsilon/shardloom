use super::*;
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use std::{collections::BTreeMap, sync::Arc};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{ConstantArray, DictArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
    },
};

fn request(ordered: bool) -> VortexSimpleAggregateRequest {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("renamed_key").unwrap()],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    );
    if ordered {
        request.with_order_by(vec![VortexAggregateOrderExpr::new("n", true)])
    } else {
        request
    }
}

fn chunk(values: ArrayRef) -> ArrayRef {
    let rows = values.len();
    StructArray::try_new(
        FieldNames::from(["renamed_key"]),
        vec![values],
        rows,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn strings(values: &[&str]) -> ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}

fn run(
    chunks: &[ArrayRef],
    request: &VortexSimpleAggregateRequest,
    limit: Option<usize>,
    workers: usize,
) -> serde_json::Value {
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new(request, limit, &columns, false, false).unwrap();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    let mut jobs = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    for chunk in chunks {
        jobs.before_next(&mut states).unwrap();
        if !jobs.submit(chunk, &mut states).unwrap() {
            assert!(
                states
                    .update_compact_direct_from_chunk(chunk, &columns, None)
                    .unwrap()
            );
        }
    }
    jobs.finish(&mut states).unwrap();
    let CountWorkers::Single(single) = &jobs else {
        panic!("single-key fixture must use single-key workers");
    };
    assert_eq!(single.jobs.outstanding(), 0);
    let (_, mut summary) = states.result_row_count_and_summary(limit).unwrap();
    jobs.annotate_summary(&mut summary).unwrap();
    let value: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(value["aggregate_workers_outstanding_chunks"], 0);
    assert!(value["aggregate_workers_compute_threads"].as_u64().unwrap() < workers as u64);
    assert!(
        value["aggregate_workers_shared_live_peak_bytes"]
            .as_u64()
            .unwrap()
            <= 1 << 20
    );
    drop((jobs, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    value
}

#[test]
fn coordinator_mixed_string_encodings_keep_all_keys_and_original_dictionary_domains() {
    let chunks = vec![
        chunk(strings(&["tea", "not a URL", "東京", "tea"])),
        chunk(
            DictArray::try_new(
                PrimitiveArray::new(vec![0_u8, 1, 0], Validity::NonNullable).into_array(),
                strings(&["東京", "tea", "unused"]),
            )
            .unwrap()
            .into_array(),
        ),
        chunk(ConstantArray::new("not a URL", 5).into_array()),
        chunk(
            DictArray::try_new(
                PrimitiveArray::new(vec![1_u8, 0, 1], Validity::NonNullable).into_array(),
                strings(&["tea", "東京", "unused"]),
            )
            .unwrap()
            .into_array(),
        ),
    ];
    for workers in [1, 2, 4, 8, 12] {
        let result = run(&chunks, &request(false), None, workers);
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_key":"tea", "n":4},
                {"renamed_key":"not a URL", "n":6},
                {"renamed_key":"東京", "n":5},
            ])
        );
        assert_eq!(result["aggregate_workers_native_dictionary_chunks"], 2);
        assert_eq!(result["aggregate_workers_native_constant_chunks"], 1);
        assert_eq!(result["aggregate_workers_rows"], 15);
    }
}

#[test]
fn coordinator_integer_extrema_constants_and_native_dictionary_dispatch_match_complete_oracle() {
    let signed = vec![
        chunk(
            PrimitiveArray::new(vec![i64::MIN, i64::MAX, -7, -7], Validity::NonNullable)
                .into_array(),
        ),
        chunk(
            DictArray::try_new(
                PrimitiveArray::new(vec![0_u8, 0, 1], Validity::NonNullable).into_array(),
                PrimitiveArray::new(vec![i64::MIN, i64::MAX], Validity::NonNullable).into_array(),
            )
            .unwrap()
            .into_array(),
        ),
        chunk(ConstantArray::new(-7_i64, 9).into_array()),
    ];
    let unsigned = vec![
        chunk(
            PrimitiveArray::new(vec![u64::MAX, 1_u64 << 63, 0, 0], Validity::NonNullable)
                .into_array(),
        ),
        chunk(ConstantArray::new(u64::MAX, 8).into_array()),
    ];
    for workers in [1, 2, 4, 8, 12] {
        let result = run(&signed, &request(true), Some(10), workers);
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_key":-7,"n":11}, {"renamed_key":i64::MIN,"n":3}, {"renamed_key":i64::MAX,"n":2},
            ])
        );
        // Dictionary CPU work remained on the pre-existing native route.
        assert_eq!(result["aggregate_workers_submitted_chunks"], 2);
        assert_eq!(result["aggregate_workers_rows"], 13);
        let result = run(&unsigned, &request(true), Some(10), workers);
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_key":u64::MAX,"n":9}, {"renamed_key":0,"n":2}, {"renamed_key":1_u64<<63,"n":1},
            ])
        );
    }
}

#[test]
fn coordinator_chunked_logical_projection_and_nullable_admission_remain_exact() {
    use vortex::array::arrays::ChunkedArray;
    let chunks = vec![chunk(strings(&["c", "a"])), chunk(strings(&["a", "b"]))];
    let logical = ChunkedArray::try_new(chunks.clone(), chunks[0].dtype().clone())
        .unwrap()
        .into_array();
    let result = run(&[logical], &request(false), None, 4);
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"renamed_key":"c","n":1}, {"renamed_key":"a","n":2}, {"renamed_key":"b","n":1},
        ])
    );

    let request = request(false);
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
    let chunks = [
        chunk(VarBinViewArray::from_iter_nullable_str([Some("a"), Some("b")]).into_array()),
        chunk(VarBinViewArray::from_iter_nullable_str([None, Some("a")]).into_array()),
    ];
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    assert!(
        CountWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
            &VortexSession::default(),
            &memory
        )
        .unwrap()
        .is_none()
    );
    for chunk in chunks {
        if !states
            .update_compact_direct_from_chunk(&chunk, &columns, None)
            .unwrap()
        {
            let values = super::super::row_export_columns_from_chunk(&chunk, &columns).unwrap();
            states.update(&values, chunk.len()).unwrap();
        }
    }
    let (_, summary) = states.result_row_count_and_summary(None).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let result = summary["values"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (row["renamed_key"].to_string(), row["n"].as_u64().unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        result,
        BTreeMap::from([("\"a\"".into(), 2), ("\"b\"".into(), 1), ("null".into(), 1)])
    );
}

#[test]
fn coordinator_partial_denial_releases_all_queued_ownership() {
    let request = request(false);
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
    let input = chunk(strings(&["a", "b"]));
    let memory = LiveMemoryPool::new(32).unwrap();
    let mut jobs = CountWorkers::admit(
        &states,
        input.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(4).unwrap(),
        &VortexSession::default(),
        &memory,
    )
    .unwrap()
    .unwrap();
    assert!(jobs.submit(&input, &mut states).is_err());
    assert!(states.groups.is_empty());
    drop(jobs);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn logical_field_selection_preserves_native_dictionary_owner_and_serial_accessor_timing() {
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(vec![0_u8, 1, 0], Validity::NonNullable).into_array(),
        strings(&["tea", "coffee"]),
    )
    .unwrap()
    .into_array();
    let input = chunk(dictionary.clone());
    let selected = super::super::logical_field_from_native_array(&input, "renamed_key").unwrap();
    assert!(ArrayRef::ptr_eq(&selected, &dictionary));
    let request = request(true);
    let columns = vec!["renamed_key".to_owned()];
    let mut states =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).unwrap();
    let mut timing = super::super::aggregate_timing::AggregateFirstPassTiming::default();
    assert!(
        states
            .update_compact_direct_from_chunk_profiled(&input, &columns, None, &mut timing)
            .unwrap()
    );
    assert_eq!(timing.accessor_chunks, 1);
    assert_eq!(timing.accessor_rows, 3);
    assert!(timing.accessor_nanos > 0);
    let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["values"],
        serde_json::json!([
            {"renamed_key":"tea","n":2}, {"renamed_key":"coffee","n":1},
        ])
    );
}

#[test]
fn complete_partition_coordinator_preserves_renamed_topk_offset_and_ties() {
    let chunks = vec![
        chunk(strings(&[
            "alpha", "alpha", "alpha", "winner", "winner", "東京",
        ])),
        chunk(strings(&["beta", "beta", "beta", "winner", "winner", "λ"])),
        chunk(strings(&[
            "gamma", "gamma", "gamma", "winner", "winner", "東京",
        ])),
    ];
    for workers in [1, 2, 4, 8, 12] {
        let request = request(true).with_offset(1);
        let result = run(&chunks, &request, Some(2), workers);
        assert_eq!(
            result["values"],
            serde_json::json!([
                {"renamed_key":"alpha","n":3}, {"renamed_key":"beta","n":3},
            ])
        );
        assert_eq!(result["aggregate_workers_partition_complete_groups"], 6);
        assert_eq!(result["aggregate_workers_partition_committed_rows"], 18);
        assert_eq!(result["aggregate_workers_partition_native_handoffs"], 0);
        assert_eq!(result["aggregate_workers_submitted_chunks"], 3);
        assert_eq!(result["candidate_groups"], 6);
    }
}

#[test]
fn partition_pressure_preserves_native_exact_refinement_and_total_weight() {
    for workers in [1, 4, 12] {
        let request = request(true);
        let columns = vec!["renamed_key".to_owned()];
        let mut states =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, true).unwrap();
        states
            .enable_string_count_topk_first_pass_exact_histogram()
            .unwrap();
        states.string_count_topk_first_pass_exact_histogram_entry_budget = 1;
        states.resource_envelope.string_topk_heavy_hitter_capacity = 1;
        let chunks = ["alpha", "beta", "gamma"].map(|local| {
            let mut rows = vec![local; 6];
            rows.extend(["global winner"; 5]);
            chunk(strings(&rows))
        });
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let session = VortexSession::default().with_allocator(Arc::new(
            crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        ));
        let mut jobs = CountWorkers::admit(
            &states,
            chunks[0].dtype(),
            &columns,
            VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
            &session,
            &memory,
        )
        .unwrap()
        .unwrap();
        for chunk in &chunks {
            jobs.before_next(&mut states).unwrap();
            assert!(jobs.submit(chunk, &mut states).unwrap());
        }
        jobs.finish(&mut states).unwrap();
        let CountWorkers::Single(single) = &jobs else {
            panic!("single-key pressure fixture must use single-key workers");
        };
        assert_eq!(single.partition_handoffs, 1);
        assert_eq!(states.string_count_topk_total_weight, 33);
        assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
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
        // This is the production exact-refinement branch used when the bounded
        // sketch cannot prove the final boundary. The omitted local winner must
        // still be found from every original native chunk.
        let mut exact =
            GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
        for chunk in &chunks {
            super::super::update_grouped_exact_states_from_chunk(&mut exact, chunk, &columns, None)
                .unwrap();
        }
        let (_, summary) = exact.result_row_count_and_summary(Some(1)).unwrap();
        let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            summary["values"],
            serde_json::json!([{"renamed_key":"global winner","n":15}])
        );
        drop((jobs, session));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn complete_partition_proof_does_not_claim_sketch_updates_or_no_eviction() {
    let request = request(true);
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new(&request, Some(1), &columns, false, true).unwrap();
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .unwrap();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = VortexSession::default();
    let input = chunk(strings(&["tea", "tea", "coffee"]));
    let mut jobs = CountWorkers::admit(
        &states,
        input.dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        &session,
        &memory,
    )
    .unwrap()
    .unwrap();
    jobs.submit(&input, &mut states).unwrap();
    jobs.finish(&mut states).unwrap();
    assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
    assert!(!states.needs_string_count_topk_heavy_hitter_second_pass());
    let (rows, summary) = states.result_row_count_and_summary(Some(1)).unwrap();
    let summary: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        summary["uniqueness_proof_status"],
        "complete_key_partition_count_desc_utf8_asc_exact_topk"
    );
    let budget = states.state_budget_report(&request, 3, rows).unwrap();
    assert!(
        budget
            .capillary_work_units
            .contains(&"complete_key_string_partition_reconciliation".to_owned())
    );
    assert!(
        !budget
            .capillary_work_units
            .contains(&"string_heavy_hitter_sketch_update".to_owned())
    );
    assert!(
        !budget
            .pulseweave_pressure_signals
            .contains(&"string_heavy_hitter_no_eviction_exact_proof".to_owned())
    );
}
