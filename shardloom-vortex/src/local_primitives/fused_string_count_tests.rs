use super::super::*;
use super::try_update;
use vortex::array::{
    IntoArray as _,
    arrays::{ChunkedArray, DictArray, PrimitiveArray, StructArray, VarBinViewArray},
    dtype::{DType, FieldNames, Nullability, StructFields},
    validity::Validity,
};

struct Fixture {
    request: VortexSimpleAggregateRequest,
    columns: Vec<String>,
    dtype: DType,
}

impl Fixture {
    fn new(nullable: bool) -> Self {
        Self {
            request: VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("product_name").unwrap()],
                vec![VortexSimpleAggregateMeasure::new("count", None, "n".into())],
            )
            .with_order_by(vec![crate::VortexAggregateOrderExpr::new("n", true)]),
            columns: vec!["product_name".into()],
            dtype: DType::Struct(
                StructFields::new(
                    FieldNames::from(["product_name"]),
                    vec![DType::Utf8(if nullable {
                        Nullability::Nullable
                    } else {
                        Nullability::NonNullable
                    })],
                ),
                Nullability::NonNullable,
            ),
        }
    }

    fn states(&self, limit: usize) -> GroupedAggregateStates<'_> {
        let mut states = GroupedAggregateStates::new_with_resource_envelope(
            &self.request,
            Some(limit),
            &self.columns,
            false,
            aggregate_group_key_dtypes_nonnullable(&self.dtype, &self.request),
            VortexLocalPrimitiveResourceEnvelope::new(24, 12).unwrap(),
        )
        .unwrap();
        states
            .enable_string_count_topk_first_pass_exact_histogram()
            .unwrap();
        states
    }
}

fn utf8(values: &[&str]) -> vortex::array::ArrayRef {
    VarBinViewArray::from_iter_str(values.iter().copied()).into_array()
}

#[test]
fn fused_logical_fields_work_for_chunked_structs_and_lazy_native_projections() {
    let fixture = Fixture::new(false);
    let chunks = [["tea", "coffee"], ["tea", "tea"]].map(|values| {
        StructArray::try_new(
            FieldNames::from(["product_name"]),
            vec![utf8(&values)],
            values.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    });
    let chunked = ChunkedArray::try_new(chunks, fixture.dtype.clone())
        .unwrap()
        .into_array();
    let projection = vortex::expr::select(["product_name"], vortex::expr::root())
        .bind(chunked.dtype())
        .unwrap();
    let projected = chunked.clone().apply_bound(&projection).unwrap();
    for chunk in [chunked, projected] {
        let mut states = fixture.states(2);
        assert!(
            try_update(&mut states, &chunk, &fixture.columns, None)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            exact_values(&mut states, 2)["values"],
            serde_json::json!([
                {"product_name": "tea", "n": 3}, {"product_name": "coffee", "n": 1},
            ])
        );
    }
}

#[test]
fn logical_aggregate_accessors_preserve_nullable_fields_in_chunked_structs() {
    let fixture = Fixture::new(true);
    let chunks = [
        vec![Some("tea"), Some("tea")],
        vec![None, Some("coffee"), None],
    ]
    .map(|values| {
        let rows = values.len();
        StructArray::try_new(
            FieldNames::from(["product_name"]),
            vec![VarBinViewArray::from_iter_nullable_str(values).into_array()],
            rows,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    });
    let chunked = ChunkedArray::try_new(chunks, fixture.dtype.clone())
        .unwrap()
        .into_array();
    let mut states = fixture.states(3);
    let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
    assert!(
        states
            .update_compact_direct_from_chunk_profiled(
                &chunked,
                &fixture.columns,
                None,
                &mut timing
            )
            .unwrap()
    );
    assert_eq!(states.fused_string_count.chunks, 0);
    let (_, summary) = states.result_row_count_and_summary(Some(3)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let rows = payload["values"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    for expected in [
        serde_json::json!({"product_name": null, "n": 2}),
        serde_json::json!({"product_name": "tea", "n": 2}),
        serde_json::json!({"product_name": "coffee", "n": 1}),
    ] {
        assert!(rows.contains(&expected));
    }
    assert_eq!(timing.accessor_rows, 5);
}

#[test]
fn logical_field_fast_path_preserves_the_encoded_dictionary_owner() {
    let dictionary = DictArray::try_new(
        vec![0_u8, 1, 0]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array(),
        utf8(&["tea", "coffee"]),
    )
    .unwrap()
    .into_array();
    let chunk = StructArray::try_new(
        FieldNames::from(["product_name"]),
        vec![dictionary.clone()],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let selected = logical_field_from_native_array(&chunk, "product_name").unwrap();
    assert!(vortex::array::ArrayRef::ptr_eq(&selected, &dictionary));
    let fixture = Fixture::new(false);
    let mut states = fixture.states(2);
    assert!(
        try_update(&mut states, &chunk, &fixture.columns, None)
            .unwrap()
            .is_none()
    );
    assert!(
        states
            .update_compact_direct_from_chunk(&chunk, &fixture.columns, None)
            .unwrap()
    );
    assert!(states.string_count_topk_dictionary_code_reuse);
    assert_eq!(
        exact_values(&mut states, 2)["values"],
        serde_json::json!([
            {"product_name": "tea", "n": 2}, {"product_name": "coffee", "n": 1},
        ])
    );
}

fn exact_values(states: &mut GroupedAggregateStates<'_>, limit: usize) -> serde_json::Value {
    assert!(states.promote_string_count_topk_first_pass_exact_histogram_if_possible());
    let (_, mut summary) = states.result_row_count_and_summary(Some(limit)).unwrap();
    states
        .fused_string_count
        .annotate_summary(&mut summary)
        .unwrap();
    serde_json::from_str(&summary).unwrap()
}

#[test]
fn fused_renamed_non_url_groups_preserve_global_candidates_ties_and_unicode() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(1);
    let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
    for local_winner in ["alpha", "beta"] {
        let mut rows = vec![local_winner; 6];
        rows.extend(["not a URL"; 5]);
        rows.extend(["東京", "λ\"\n", "", "東京"]);
        let chunk = StructArray::try_new(
            FieldNames::from(["product_name"]),
            vec![utf8(&rows)],
            rows.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        assert!(
            states
                .update_compact_direct_from_chunk_profiled(
                    &chunk,
                    &fixture.columns,
                    None,
                    &mut timing
                )
                .unwrap()
        );
    }
    assert_eq!(states.string_interner.len(), 6);
    assert_eq!(
        states
            .string_count_topk_first_pass_exact_histogram_counts
            .as_ref()
            .unwrap()
            .len(),
        6
    );
    assert!(states.string_count_topk_heavy_hitter_sketch.is_none());
    assert!(!states.chunk_dictionary_direct_updates);
    assert!(!states.string_count_topk_dictionary_code_reuse);
    let payload = exact_values(&mut states, 1);
    assert_eq!(
        payload["values"],
        serde_json::json!([{"product_name": "not a URL", "n": 10}])
    );
    assert_eq!(payload["aggregate_fused_string_count_chunks"], 2);
    assert_eq!(payload["aggregate_fused_string_count_rows"], 30);
    assert_eq!(
        payload["aggregate_fused_string_count_new_global_strings"],
        6
    );
    assert_eq!(timing.accessor_rows, 30);
    let (_, all_summary) = states.result_row_count_and_summary(Some(6)).unwrap();
    let all: serde_json::Value = serde_json::from_str(&all_summary).unwrap();
    assert_eq!(
        all["values"][1],
        serde_json::json!({"product_name": "alpha", "n": 6})
    );
    assert_eq!(
        all["values"][2],
        serde_json::json!({"product_name": "beta", "n": 6})
    );
    assert_eq!(
        all["values"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["n"].as_u64().unwrap())
            .sum::<u64>(),
        30
    );
}

#[test]
fn native_dictionary_domains_and_fused_chunks_share_exact_global_identity() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(3);
    assert!(
        try_update(&mut states, &utf8(&["tea", "tea"]), &fixture.columns, None)
            .unwrap()
            .is_some()
    );
    for (values, codes) in [
        (["tea", "coffee", "unused"], vec![0_u8, 1, 1]),
        (["coffee", "tea", "unused"], vec![0_u8, 0, 1]),
    ] {
        let dictionary = DictArray::try_new(
            codes.into_iter().collect::<PrimitiveArray>().into_array(),
            utf8(&values),
        )
        .unwrap()
        .into_array();
        let before = states.string_interner.len();
        assert!(
            try_update(&mut states, &dictionary, &fixture.columns, None)
                .unwrap()
                .is_none()
        );
        assert_eq!(states.string_interner.len(), before);
        assert!(
            states
                .update_compact_direct_from_chunk(&dictionary, &fixture.columns, None)
                .unwrap()
        );
    }
    assert!(
        try_update(
            &mut states,
            &utf8(&["coffee", "water"]),
            &fixture.columns,
            None
        )
        .unwrap()
        .is_some()
    );
    assert_eq!(states.string_interner.id("unused"), None);
    assert!(states.string_count_topk_dictionary_code_reuse);
    let payload = exact_values(&mut states, 3);
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"product_name": "coffee", "n": 5}, {"product_name": "tea", "n": 4}, {"product_name": "water", "n": 1},
        ])
    );
    assert_eq!(payload["aggregate_fused_string_count_rows"], 4);
    assert_eq!(states.string_count_topk_total_weight, 10);
    assert_eq!(
        states.string_count_topk_first_pass_exact_histogram_input_rows,
        10
    );
}

#[test]
fn nullable_schema_never_enters_fused_state_even_before_first_null_chunk() {
    let fixture = Fixture::new(true);
    let mut states = fixture.states(3);
    let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
    for values in [
        vec![Some("tea"), Some("tea")],
        vec![None, Some("coffee"), None],
    ] {
        let chunk = VarBinViewArray::from_iter_nullable_str(values).into_array();
        assert!(
            try_update(&mut states, &chunk, &fixture.columns, None)
                .unwrap()
                .is_none()
        );
        assert!(
            states
                .update_compact_direct_from_chunk_profiled(
                    &chunk,
                    &fixture.columns,
                    None,
                    &mut timing
                )
                .unwrap()
        );
    }
    assert_eq!(states.fused_string_count.chunks, 0);
    assert!(
        states
            .string_count_topk_first_pass_exact_histogram_counts
            .is_none()
    );
    let (_, summary) = states.result_row_count_and_summary(Some(3)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    let rows = payload["values"].as_array().unwrap();
    assert!(
        rows.iter()
            .any(|row| row["product_name"].is_null() && row["n"] == 2)
    );
    assert!(
        rows.iter()
            .any(|row| row["product_name"] == "tea" && row["n"] == 2)
    );
    assert!(
        rows.iter()
            .any(|row| row["product_name"] == "coffee" && row["n"] == 1)
    );
}

#[test]
fn selection_late_measures_and_inactive_exact_state_are_not_admitted() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(1);
    assert!(
        try_update(&mut states, &utf8(&["tea"]), &fixture.columns, Some(&[0]))
            .unwrap()
            .is_none()
    );
    states.string_count_topk_first_pass_exact_histogram_disabled = true;
    assert!(
        try_update(&mut states, &utf8(&["tea"]), &fixture.columns, None)
            .unwrap()
            .is_none()
    );
    assert_eq!(states.string_interner.len(), 0);
    let mut fixture = Fixture::new(false);
    fixture
        .request
        .measures
        .push(VortexSimpleAggregateMeasure::new(
            "count",
            Some(ColumnRef::new("product_name").unwrap()),
            "present".into(),
        ));
    let mut states = fixture.states(1);
    assert!(
        try_update(&mut states, &utf8(&["tea"]), &fixture.columns, None)
            .unwrap()
            .is_none()
    );
    assert_eq!(states.string_interner.len(), 0);
}

#[test]
fn mid_chunk_pressure_replays_consumed_prefix_once_and_continues_exact_native_execution() {
    let fixture = Fixture::new(false);
    for byte_pressure in [false, true] {
        let mut states = fixture.states(3);
        if byte_pressure {
            states.resource_envelope.memory_budget_bytes =
                STRING_COUNT_TOPK_FIRST_PASS_EXACT_HISTOGRAM_BYTES_PER_ENTRY + 1;
        } else {
            states.string_count_topk_first_pass_exact_histogram_entry_budget = 1;
        }
        assert!(
            try_update(&mut states, &utf8(&["a", "a"]), &fixture.columns, None)
                .unwrap()
                .is_some()
        );
        assert!(
            try_update(
                &mut states,
                &utf8(&["a", "b", "a", "b", "b"]),
                &fixture.columns,
                None
            )
            .unwrap()
            .is_some()
        );
        assert!(
            states
                .string_count_topk_first_pass_exact_histogram_counts
                .is_none()
        );
        assert!(states.string_count_topk_first_pass_exact_histogram_disabled);
        assert!(states.string_count_topk_heavy_hitter_sketch.is_some());
        assert_eq!(states.fused_string_count.pressure_prefix_rows, 1);
        assert_eq!(states.fused_string_count.pressure_suffix_rows, 4);
        assert_eq!(
            states
                .fused_string_count
                .pressure_histogram_entries_released,
            1
        );
        assert_eq!(
            states.fused_string_count.pressure_interner_values_retained,
            2
        );
        assert_eq!(states.fused_string_count.utf8_bytes_visited, 7);
        assert_eq!(states.string_count_topk_total_weight, 7);
        assert!(
            states
                .update_compact_direct_from_chunk(&utf8(&["b", "c"]), &fixture.columns, None)
                .unwrap()
        );
        assert_eq!(states.string_count_topk_total_weight, 9);
        assert!(
            states
                .promote_string_count_topk_exact_first_pass_if_possible(Some(3))
                .unwrap()
        );
        let (_, summary) = states.result_row_count_and_summary(Some(3)).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
        assert_eq!(
            payload["values"],
            serde_json::json!([
                {"product_name": "a", "n": 4}, {"product_name": "b", "n": 4}, {"product_name": "c", "n": 1},
            ])
        );
    }
}

#[test]
fn pressure_before_first_row_still_processes_the_whole_unconsumed_suffix_once() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(2);
    states.string_count_topk_first_pass_exact_histogram_entry_budget = 0;
    assert!(
        try_update(&mut states, &utf8(&["a", "b", "a"]), &fixture.columns, None)
            .unwrap()
            .is_some()
    );
    assert_eq!(states.fused_string_count.pressure_prefix_rows, 0);
    assert_eq!(states.fused_string_count.pressure_suffix_rows, 3);
    assert_eq!(states.string_count_topk_total_weight, 3);
    assert!(
        states
            .promote_string_count_topk_exact_first_pass_if_possible(Some(2))
            .unwrap()
    );
    let (_, summary) = states.result_row_count_and_summary(Some(2)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"product_name": "a", "n": 2}, {"product_name": "b", "n": 1},
        ])
    );
}

#[test]
fn pressure_sketch_cannot_hide_global_winner_outside_every_local_top_one() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(1);
    states.string_count_topk_first_pass_exact_histogram_entry_budget = 1;
    states.resource_envelope.string_topk_heavy_hitter_capacity = 1;
    let chunks = [
        utf8(&["a", "a", "a", "winner", "winner"]),
        utf8(&["b", "b", "b", "winner", "winner"]),
        utf8(&["c"]),
    ];
    assert!(
        try_update(&mut states, &chunks[0], &fixture.columns, None)
            .unwrap()
            .is_some()
    );
    for chunk in &chunks[1..] {
        assert!(
            states
                .update_compact_direct_from_chunk(chunk, &fixture.columns, None)
                .unwrap()
        );
    }
    assert_eq!(states.string_count_topk_total_weight, 11);
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
    // The production caller makes this same explicit exact native refinement
    // when the global omitted-key bound cannot prove the candidate result.
    let mut refined =
        GroupedAggregateStates::new(&fixture.request, Some(1), &fixture.columns, false, false)
            .unwrap();
    for chunk in &chunks {
        update_grouped_exact_states_from_chunk(&mut refined, chunk, &fixture.columns, None)
            .unwrap();
    }
    let (_, summary) = refined.result_row_count_and_summary(Some(1)).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(
        payload["values"],
        serde_json::json!([{"product_name": "winner", "n": 4}])
    );
}

#[test]
fn checked_count_overflow_and_lost_nonnull_contract_fail_before_completed_result() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(1);
    let id = states.string_interner.intern("tea").unwrap();
    states
        .string_count_topk_first_pass_exact_histogram_counts
        .as_mut()
        .unwrap()
        .insert(id, u64::MAX);
    let error = try_update(&mut states, &utf8(&["tea"]), &fixture.columns, None)
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("count overflowed"));
    assert!(try_update(&mut states, &utf8(&["tea"]), &fixture.columns, None).is_err());
    assert!(states.result_row_count_and_summary(Some(1)).is_err());
    assert_eq!(
        states
            .string_count_topk_first_pass_exact_histogram_counts
            .as_ref()
            .unwrap()[&id],
        u64::MAX
    );
    let mut states = fixture.states(1);
    let nullable = VarBinViewArray::from_iter_nullable_str([Some("tea"), None]).into_array();
    assert!(try_update(&mut states, &nullable, &fixture.columns, None).is_err());
    assert_eq!(states.string_interner.len(), 0);
}

#[test]
fn count_star_profile_records_accessor_work_even_without_fused_admission() {
    let fixture = Fixture::new(false);
    let mut states = fixture.states(2);
    states.string_count_topk_first_pass_exact_histogram_counts = None;
    states.string_count_topk_first_pass_exact_histogram_enabled = false;
    let mut timing = aggregate_timing::AggregateFirstPassTiming::default();
    assert!(
        states
            .update_compact_direct_from_chunk_profiled(
                &utf8(&["tea", "coffee", "tea"]),
                &fixture.columns,
                None,
                &mut timing
            )
            .unwrap()
    );
    assert_eq!(timing.accessor_chunks, 1);
    assert_eq!(timing.accessor_rows, 3);
    assert!(timing.accessor_nanos > 0);
    assert_eq!(states.fused_string_count.chunks, 0);
}

#[test]
fn fused_constant_dependent_group_key_is_rendered_without_hashing_it_per_row() {
    let mut fixture = Fixture::new(false);
    fixture.request = fixture.request.with_group_expressions(vec![
        crate::VortexAggregateExpression::new(
            "literal_one".into(),
            ColumnRef::new("product_name").unwrap(),
            "constant_int",
        )
        .with_argument_offset(1),
    ]);
    let mut states = fixture.states(2);
    assert_eq!(states.group_key_indices.len(), 1);
    assert_eq!(states.group_columns.len(), 2);
    assert!(
        try_update(
            &mut states,
            &utf8(&["plain", "plain", "東京"]),
            &fixture.columns,
            None
        )
        .unwrap()
        .is_some()
    );
    let payload = exact_values(&mut states, 2);
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"product_name": "plain", "literal_one": 1, "n": 2},
            {"product_name": "東京", "literal_one": 1, "n": 1},
        ])
    );
}
