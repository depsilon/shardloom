use super::GroupedAggregateStates;
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::ColumnRef;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{BoolArray, PrimitiveArray, StructArray},
    dtype::FieldNames,
    validity::Validity,
};

fn count_request(offset: usize) -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("key").unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "count",
            Some(ColumnRef::new("value").unwrap()),
            "nonnull".into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("nonnull", true)])
    .with_offset(offset)
}

fn grouped_chunk(keys: &[i64], values: ArrayRef) -> ArrayRef {
    StructArray::try_new(
        FieldNames::from(["key", "value"]),
        vec![
            PrimitiveArray::from_iter(keys.iter().copied()).into_array(),
            values,
        ],
        keys.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

fn run_count(
    request: &VortexSimpleAggregateRequest,
    chunk: &ArrayRef,
    limit: usize,
) -> serde_json::Value {
    let columns = vec!["key".to_string(), "value".to_string()];
    let mut states =
        GroupedAggregateStates::new(request, Some(limit), &columns, false, false).unwrap();
    assert!(states.compact_measure_specs.is_some());
    assert!(
        states
            .update_compact_direct_from_chunk(chunk, &columns, None)
            .unwrap()
    );
    states.result_row_count_and_payload(Some(limit)).unwrap().1
}

#[test]
fn compact_count_column_handles_nulls_ties_offsets_and_bounded_candidates() {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for key in 0..32 {
        keys.extend([key, key]);
        values.extend([
            (key % 3 > 0).then_some(10_i64),
            (key % 3 == 2).then_some(20),
        ]);
    }
    let values = PrimitiveArray::from_option_iter(values).into_array();
    let chunk = grouped_chunk(&keys, values);
    let request = count_request(2);

    let bounded = run_count(&request, &chunk, 3);
    assert_eq!(bounded["candidate_groups"], 32);
    assert_eq!(bounded["peak_buffered_order_candidates"], 2 * (3 + 2));
    assert_eq!(
        bounded["values"],
        serde_json::json!([
            {"key": 8, "nonnull": 2},
            {"key": 11, "nonnull": 2},
            {"key": 14, "nonnull": 2}
        ])
    );

    let full = run_count(&count_request(0), &chunk, 32);
    assert_eq!(full["candidate_groups"], 32);
    assert_eq!(full["peak_buffered_order_candidates"], 32);
    assert_eq!(
        bounded["values"],
        serde_json::Value::Array(full["values"].as_array().unwrap()[2..5].to_vec())
    );
}

#[test]
fn compact_count_nullable_boolean_keeps_the_all_null_group() {
    let keys = [1, 1, 2, 2, 3, 3, 4, 4];
    let values = BoolArray::from_iter([
        Some(true),
        None,
        None,
        None,
        Some(false),
        Some(true),
        Some(false),
        None,
    ])
    .into_array();
    let chunk = grouped_chunk(&keys, values);
    let request = count_request(0);

    let result = run_count(&request, &chunk, 10);
    assert_eq!(result["candidate_groups"], 4);
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"key": 3, "nonnull": 2},
            {"key": 1, "nonnull": 1},
            {"key": 4, "nonnull": 1},
            {"key": 2, "nonnull": 0}
        ])
    );
}

#[test]
fn general_min_control_preserves_exact_large_unsigned_values_and_nulls() {
    let request = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("key").unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "min",
            Some(ColumnRef::new("value").unwrap()),
            "minimum".into(),
        )],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("minimum", true)]);
    let columns = vec!["key".to_string(), "value".to_string()];
    let chunk = grouped_chunk(
        &[1, 1, 2, 2, 3],
        PrimitiveArray::from_option_iter([
            Some(9_007_199_254_740_993_u64),
            Some(9_007_199_254_740_994),
            None,
            None,
            Some(9_007_199_254_740_992),
        ])
        .into_array(),
    );
    let mut states =
        GroupedAggregateStates::new(&request, Some(10), &columns, false, false).unwrap();
    assert!(
        states
            .update_compact_direct_from_chunk(&chunk, &columns, None)
            .unwrap()
    );
    let payload = states.result_row_count_and_summary(Some(10)).unwrap().1;
    let result: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        result["values"],
        serde_json::json!([
            {"key": 1, "minimum": 9_007_199_254_740_993_u64},
            {"key": 3, "minimum": 9_007_199_254_740_992_u64},
            {"key": 2, "minimum": null}
        ])
    );
}
