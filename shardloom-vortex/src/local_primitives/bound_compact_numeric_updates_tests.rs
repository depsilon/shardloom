use super::*;
use crate::local_primitives::{GroupedAggregateStates, aggregate_direct_column_accessor};
use crate::{
    VortexAggregateHavingExpr, VortexAggregateOrderExpr, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest,
};
use shardloom_core::{ColumnRef, ComparisonOp, StatValue};
use vortex::array::{
    IntoArray as _,
    arrays::{PrimitiveArray, VarBinViewArray},
    dtype::NativePType,
    validity::Validity,
};

fn spec(
    function: SimpleAggregateFunction,
    column_index: Option<usize>,
) -> CompactAggregateMeasureSpec {
    CompactAggregateMeasureSpec {
        function,
        column_index,
        alias: "measure".into(),
        value_transform: AggregateValueTransform::Identity,
    }
}

fn specs() -> Vec<CompactAggregateMeasureSpec> {
    use SimpleAggregateFunction::{Avg, Count, Sum};
    vec![
        spec(Count, None),
        spec(Count, Some(0)),
        spec(Sum, Some(0)),
        spec(Avg, Some(0)),
        spec(Sum, Some(0)),
        spec(Count, None),
    ]
}

fn accessor<T: NativePType>(values: Vec<T>) -> AggregateDirectColumnAccessor {
    aggregate_direct_column_accessor(
        "measure",
        &PrimitiveArray::new(values, Validity::NonNullable).into_array(),
    )
    .unwrap()
}

fn assert_equal(actual: &CompactAggregateMeasures, expected: &CompactAggregateMeasures) {
    assert_eq!(actual.values().len(), expected.values().len());
    for (actual, expected) in actual.values().iter().zip(expected.values()) {
        assert_eq!(actual.count, expected.count);
        assert_eq!(actual.sum.to_bits(), expected.sum.to_bits());
    }
}

#[test]
fn compact_binding_matches_original_widths_validity_and_row_order() {
    let specs = specs();
    let arrays = [
        accessor(vec![0_u8, 1, 2, u8::MAX]),
        accessor(vec![0_u16, 1, 2, u16::MAX]),
        accessor(vec![0_u32, 1, 2, u32::MAX]),
        accessor(vec![0_u64, 1, 2, u64::MAX]),
        accessor(vec![i8::MIN, 0, 1, i8::MAX]),
        accessor(vec![i16::MIN, 0, 1, i16::MAX]),
        accessor(vec![i32::MIN, 0, 1, i32::MAX]),
        accessor(vec![i64::MIN, (1_i64 << 53) + 17, 1, i64::MAX]),
        accessor(vec![-0.0_f32, 1.25, 2.5, -3.75]),
        accessor(vec![-0.0_f64, 1.25, 2.5, -3.75]),
        aggregate_direct_column_accessor(
            "nullable",
            &PrimitiveArray::from_option_iter([Some(5_i32), None, Some(-2), Some(8)]).into_array(),
        )
        .unwrap(),
    ];
    for array in arrays {
        let accessors = [array];
        let bound = BoundCompactNumericUpdates::bind(&specs, &accessors, 4).unwrap();
        let mut actual = CompactAggregateMeasures::new(&specs);
        let mut expected = CompactAggregateMeasures::new(&specs);
        // Repeated, sparse and out-of-order selected rows keep caller order.
        for row in [3, 0, 1, 3, 2] {
            bound.update(&mut actual, row).unwrap();
            expected
                .update_from_direct_row(&specs, &accessors, row)
                .unwrap();
            assert_equal(&actual, &expected);
        }
        assert_eq!(actual.values()[0].count, 5);
        assert_eq!(actual.values()[5].count, 5);
    }
}

#[test]
fn compact_binding_rebinds_owners_and_preserves_empty_and_all_null_measures() {
    let specs = specs();
    let mut actual = CompactAggregateMeasures::new(&specs);
    let mut expected = CompactAggregateMeasures::new(&specs);
    for (array, rows) in [
        (accessor(Vec::<u8>::new()), 0),
        (accessor(vec![10_i16, -20, 30]), 3),
        (
            aggregate_direct_column_accessor(
                "nulls",
                &PrimitiveArray::from_option_iter([None::<u32>, None, None]).into_array(),
            )
            .unwrap(),
            3,
        ),
        (
            aggregate_direct_column_accessor(
                "changed",
                &PrimitiveArray::from_option_iter([Some(1.25_f64), None, Some(2.5)]).into_array(),
            )
            .unwrap(),
            3,
        ),
    ] {
        let accessors = [array];
        let bound = BoundCompactNumericUpdates::bind(&specs, &accessors, rows).unwrap();
        for row in 0..rows {
            bound.update(&mut actual, row).unwrap();
            expected
                .update_from_direct_row(&specs, &accessors, row)
                .unwrap();
            assert_equal(&actual, &expected);
        }
    }
    assert_eq!(actual.values()[0].count, 9);
    assert_eq!(actual.values()[1].count, 5);
    assert_eq!(actual.values()[2].sum.to_bits(), 23.75_f64.to_bits());
}

#[test]
fn compact_binding_preserves_failure_row_measure_order_and_partial_state() {
    let specs = specs();
    for (numeric, overflow_index, seed_sum, row) in [
        (1.0, Some(0), 0.0, 0),
        (1.0, Some(1), 0.0, 0),
        (1.0, Some(2), 0.0, 0),
        (f64::INFINITY, None, 0.0, 0),
        (f64::NAN, None, 0.0, 0),
        (f64::MAX, None, f64::MAX, 0),
        (1.0, None, 0.0, 1),
    ] {
        let accessors = [accessor(vec![numeric])];
        let bound = BoundCompactNumericUpdates::bind(&specs, &accessors, 1).unwrap();
        let mut actual = CompactAggregateMeasures::new(&specs);
        let mut expected = CompactAggregateMeasures::new(&specs);
        for state in [&mut actual, &mut expected] {
            state.values_mut()[2].sum = seed_sum;
            if let Some(index) = overflow_index {
                state.values_mut()[index].count = u64::MAX;
            }
        }
        let actual_error = bound.update(&mut actual, row).unwrap_err();
        let expected_error = expected
            .update_from_direct_row(&specs, &accessors, row)
            .unwrap_err();
        assert_eq!(actual_error.to_string(), expected_error.to_string());
        assert_equal(&actual, &expected);
        // No later measure was visited after the first failing one.
        assert_eq!(actual.values()[5].count, 0);
    }
}

#[test]
fn compact_bound_count_column_counts_nonfinite_values_but_skips_nulls() {
    let specs = [spec(SimpleAggregateFunction::Count, Some(0))];
    let accessors = [aggregate_direct_column_accessor(
        "counted",
        &PrimitiveArray::from_option_iter([Some(f64::NAN), None, Some(f64::INFINITY)]).into_array(),
    )
    .unwrap()];
    let bound = BoundCompactNumericUpdates::bind(&specs, &accessors, 3).unwrap();
    let mut actual = CompactAggregateMeasures::new(&specs);
    for row in 0..3 {
        bound.update(&mut actual, row).unwrap();
    }
    assert_eq!(actual.values()[0].count, 2);
}

#[test]
fn compact_binding_rejects_unsupported_shapes_and_mismatched_owner_lengths() {
    let accessors = [accessor(vec![7_u32])];
    let mut specs = specs();
    assert!(BoundCompactNumericUpdates::bind(&specs, &accessors, 2).is_none());
    for transform in [
        AggregateValueTransform::Length,
        AggregateValueTransform::AddOffset(1),
    ] {
        specs[4].value_transform = transform;
        assert!(BoundCompactNumericUpdates::bind(&specs, &accessors, 1).is_none());
    }
    specs[4].value_transform = AggregateValueTransform::Identity;
    specs[4].function = SimpleAggregateFunction::Min;
    assert!(BoundCompactNumericUpdates::bind(&specs, &accessors, 1).is_none());
    for accessors in [
        vec![AggregateDirectColumnAccessor::Materialized {
            values: vec![StatValue::UInt64(7)],
            blocker: "test",
        }],
        vec![
            aggregate_direct_column_accessor(
                "text",
                &VarBinViewArray::from_iter_str(["seven"]).into_array(),
            )
            .unwrap(),
        ],
    ] {
        assert!(BoundCompactNumericUpdates::bind(&self::specs(), &accessors, 1).is_none());
    }
}

fn request() -> VortexSimpleAggregateRequest {
    VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new("category").unwrap()],
        vec![
            VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
            VortexSimpleAggregateMeasure::new(
                "sum",
                Some(ColumnRef::new("amount").unwrap()),
                "total".into(),
            ),
            VortexSimpleAggregateMeasure::new(
                "avg",
                Some(ColumnRef::new("amount").unwrap()),
                "mean".into(),
            ),
        ],
    )
}

fn result_values(states: &mut GroupedAggregateStates<'_>) -> serde_json::Value {
    let (_, summary) = states.result_row_count_and_summary(Some(1)).unwrap();
    serde_json::from_str::<serde_json::Value>(&summary).unwrap()["values"].clone()
}

#[test]
fn compact_bound_group_updates_preserve_selection_source_order_limit_and_having() {
    let columns = vec!["category".to_owned(), "amount".to_owned()];
    for selected in [None, Some(&[0, 2, 3][..]), Some(&[][..])] {
        for ordered in [false, true] {
            for having in [false, true] {
                let mut request = request();
                if ordered {
                    request = request
                        .with_order_by(vec![VortexAggregateOrderExpr::new("category", false)]);
                }
                if having {
                    request = request.with_having(vec![VortexAggregateHavingExpr::new(
                        "rows",
                        ComparisonOp::GtEq,
                        "3",
                    )]);
                }
                let mut actual =
                    GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
                let mut expected =
                    GroupedAggregateStates::new(&request, Some(1), &columns, false, false).unwrap();
                for values in [
                    [Some(10_u32), None, Some(30), Some(40)],
                    [Some(1), Some(2), None, Some(4)],
                ] {
                    let accessors = [
                        accessor(vec![2_i32, 1, 2, 3]),
                        aggregate_direct_column_accessor(
                            "amount",
                            &PrimitiveArray::from_option_iter(values).into_array(),
                        )
                        .unwrap(),
                    ];
                    assert!(
                        BoundCompactNumericUpdates::bind(
                            actual.compact_measure_specs.as_ref().unwrap(),
                            &accessors,
                            4
                        )
                        .is_some()
                    );
                    assert!(
                        actual
                            .update_compact_direct_from_accessors(&accessors, &columns, selected, 4)
                            .unwrap()
                    );
                    for &row in selected.unwrap_or(&[0, 1, 2, 3]) {
                        expected
                            .update_compact_measure_direct_row(&accessors, row, None)
                            .unwrap();
                    }
                }
                assert_eq!(result_values(&mut actual), result_values(&mut expected));
                if selected.is_none() && having {
                    assert_eq!(
                        result_values(&mut actual),
                        serde_json::json!([
                            {"category": 2, "rows": 4, "total": 41.0, "mean": 41.0 / 3.0}
                        ])
                    );
                }
            }
        }
    }
}
