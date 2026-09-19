//! Exact counts consume complete native selections without a residual predicate.

use super::{
    GroupedAggregateStates, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveResourceEnvelope, aggregate_count_workers::CountWorkers,
};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::sync::Arc;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{DictArray, FilterArray, PrimitiveArray, StructArray, VarBinViewArray},
        dtype::FieldNames,
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    mask::Mask,
    session::VortexSession,
};

fn filtered_request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("renamed_key").expect("column")],
        vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("n", true)])
}

#[test]
fn filtered_histogram_admission_survives_the_native_embedded_length_rewrite() {
    use super::{
        aggregate_lowering::AggregateLowering, string_count_histogram_selected_input_admitted,
    };
    use crate::VortexQueryPrimitiveRequest;
    use shardloom_core::{ComparisonOp, DatasetUri, PredicateExpr, StatValue};
    use vortex::array::dtype::{DType, Nullability, PType, StructFields};
    let aggregate = filtered_request();
    let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new("renamed.vortex").unwrap(),
        aggregate,
    )
    .with_source_order_limit(2);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("renamed_key").unwrap(),
        op: ComparisonOp::NotEq,
        value: StatValue::Utf8(String::new()),
    });
    let hidden = super::shardloom_utf8_length_derived_column("renamed_key");
    let dtype = DType::Struct(
        StructFields::new(
            FieldNames::from(["renamed_key", hidden.as_str()]),
            vec![
                DType::Utf8(Nullability::NonNullable),
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ],
        ),
        Nullability::NonNullable,
    );
    let lowering = AggregateLowering::new(&request, &dtype).unwrap();
    assert!(
        matches!(lowering.pushdown.as_ref(), Some(PredicateExpr::Compare { column, op: ComparisonOp::Gt, value: StatValue::UInt64(0) }) if column.as_str() == hidden)
    );
    assert!(lowering.residual.is_none());
    assert!(lowering.plan.filter.is_some());
    assert!(string_count_histogram_selected_input_admitted(
        &lowering.rewrite.aggregate,
        request.predicate.as_ref(),
        lowering.residual.as_ref(),
    ));
}

fn filtered_chunk(codes: &[u8], values: &[&str], selected: &[bool]) -> ArrayRef {
    let dictionary = DictArray::try_new(
        PrimitiveArray::new(codes.to_vec(), Validity::NonNullable).into_array(),
        VarBinViewArray::from_iter_str(values.iter().copied()).into_array(),
    )
    .expect("dictionary");
    let filtered = FilterArray::new(
        dictionary.into_array(),
        Mask::from_iter(selected.iter().copied()),
    );
    StructArray::try_new(
        FieldNames::from(["renamed_key"]),
        vec![filtered.into_array()],
        selected.iter().filter(|selected| **selected).count(),
        Validity::NonNullable,
    )
    .expect("struct")
    .into_array()
}

fn run_filtered(chunks: &[ArrayRef], memory_bytes: usize) -> serde_json::Value {
    let request = filtered_request().with_offset(1);
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(2),
        &columns,
        false,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope"),
    )
    .expect("states");
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .expect("histogram");
    let memory = LiveMemoryPool::new(memory_bytes as u64).expect("memory");
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    let mut workers = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(4).expect("policy"),
        &session,
        &memory,
    )
    .expect("admission")
    .expect("workers");
    for chunk in chunks {
        workers.before_next(&mut states).expect("before");
        assert!(workers.submit(chunk, &mut states).expect("submit"));
    }
    workers.finish(&mut states).expect("finish");
    let (_, mut summary) = states
        .result_row_count_and_summary(Some(2))
        .expect("summary");
    workers
        .annotate_summary(&mut summary)
        .expect("worker summary");
    let value: serde_json::Value = serde_json::from_str(&summary).expect("json");
    assert_eq!(
        value["string_count_topk_dictionary_histogram_recount"],
        false
    );
    assert_eq!(
        value["string_count_topk_heavy_hitter_exact_counts_source"],
        "complete_key_partition_topk"
    );
    drop((workers, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    value
}

#[test]
fn filtered_native_utf8_dictionary_histogram_is_exact_across_domains_and_ties() {
    let chunks = vec![
        filtered_chunk(
            &[0, 1, 2, 3],
            &["hot", "warm", "東京", "", "unused"],
            &[true, true, true, false],
        ),
        filtered_chunk(
            &[1, 0, 2, 3],
            &["warm", "hot", "東京", "", "unused"],
            &[true, true, true, false],
        ),
    ];
    let result = run_filtered(&chunks, 1 << 20);
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"renamed_key":"warm", "n":2},
            {"renamed_key":"東京", "n":2},
        ])
    );
    assert_eq!(result["aggregate_workers_rows"], 6);
}

#[test]
fn filtered_exact_histogram_budget_pressure_preserves_exact_refinement() {
    let chunks = vec![
        filtered_chunk(&[0, 1, 2], &["a", "b", "c"], &[true, true, true]),
        filtered_chunk(&[0, 1, 2], &["a", "b", "c"], &[true, true, true]),
    ];
    let request = filtered_request();
    let columns = vec!["renamed_key".to_owned()];
    let mut states = GroupedAggregateStates::new_with_resource_envelope(
        &request,
        Some(2),
        &columns,
        false,
        true,
        VortexLocalPrimitiveResourceEnvelope::new(24, 12).expect("envelope"),
    )
    .expect("states");
    states
        .enable_string_count_topk_first_pass_exact_histogram()
        .expect("histogram");
    states.string_count_topk_first_pass_exact_histogram_entry_budget = 1;
    let memory = LiveMemoryPool::new(1 << 20).expect("memory");
    let session = VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ));
    let mut workers = CountWorkers::admit(
        &states,
        chunks[0].dtype(),
        &columns,
        VortexLocalPrimitiveExecutionPolicy::new(4).expect("policy"),
        &session,
        &memory,
    )
    .expect("admission")
    .expect("workers");
    for chunk in &chunks {
        workers.before_next(&mut states).expect("before");
        assert!(workers.submit(chunk, &mut states).expect("submit"));
    }
    workers.finish(&mut states).expect("finish");
    assert!(states.string_count_topk_first_pass_exact_histogram_disabled);
    assert!(states.string_count_topk_heavy_hitter_sketch.is_some());
    assert_eq!(states.string_count_topk_total_weight, 6);
    assert!(states.needs_string_count_topk_heavy_hitter_second_pass());
    let mut exact =
        GroupedAggregateStates::new(&request, Some(2), &columns, false, false).expect("exact");
    for chunk in &chunks {
        super::update_grouped_exact_states_from_chunk(&mut exact, chunk, &columns, None)
            .expect("refinement");
    }
    let (_, summary) = exact
        .result_row_count_and_summary(Some(2))
        .expect("refined output");
    let summary: serde_json::Value = serde_json::from_str(&summary).expect("json");
    assert_eq!(
        summary["values"],
        serde_json::json!([
            {"renamed_key":"a", "n":2}, {"renamed_key":"b", "n":2},
        ])
    );
    drop((workers, session));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
