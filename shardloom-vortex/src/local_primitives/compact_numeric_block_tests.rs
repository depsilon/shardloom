use super::*;
use crate::local_primitives::{GroupedAggregateState, aggregate_direct_column_accessor};
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

fn request(count_first: bool) -> VortexSimpleAggregateRequest {
    let mut measures = vec![
        VortexSimpleAggregateMeasure::new(
            "avg",
            Some(ColumnRef::new("amount").unwrap()),
            "mean".into(),
        ),
        VortexSimpleAggregateMeasure::new("count", None, "rows".into()),
    ];
    if count_first {
        measures.reverse();
    }
    VortexSimpleAggregateRequest::grouped(vec![ColumnRef::new("category").unwrap()], measures)
}

fn accessor<T: NativePType>(values: Vec<T>) -> AggregateDirectColumnAccessor {
    aggregate_direct_column_accessor(
        "amount",
        &PrimitiveArray::new(values, Validity::NonNullable).into_array(),
    )
    .unwrap()
}

fn assert_state_equal(actual: &GroupedAggregateStates<'_>, expected: &GroupedAggregateStates<'_>) {
    assert_eq!(actual.groups.len(), expected.groups.len());
    assert!(actual.group_order == expected.group_order);
    assert_eq!(
        actual.source_order_limited_group_admission,
        expected.source_order_limited_group_admission
    );
    for (key, actual) in &actual.groups {
        let expected = expected.groups.get(key).unwrap();
        assert_eq!(actual.group_values(), expected.group_values());
        let GroupedAggregateState::CompactMeasures {
            measures: actual, ..
        } = actual
        else {
            panic!("expected compact group");
        };
        let GroupedAggregateState::CompactMeasures {
            measures: expected, ..
        } = expected
        else {
            panic!("expected compact reference");
        };
        for (actual, expected) in actual.values().iter().zip(expected.values()) {
            assert_eq!(actual.count, expected.count);
            assert_eq!(actual.sum.to_bits(), expected.sum.to_bits());
        }
    }
}

fn original_rows(
    states: &mut GroupedAggregateStates<'_>,
    accessors: &[AggregateDirectColumnAccessor],
    rows: &[usize],
) -> Result<()> {
    for &row in rows {
        states.update_compact_measure_direct_row(accessors, row, None)?;
    }
    Ok(())
}

#[test]
fn compact_numeric_block_matches_all_widths_nullable_slices_and_selected_order() {
    let columns = vec!["category".to_owned(), "amount".to_owned()];
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
        accessor(vec![-0.0_f64, 1.0e16, 1.0, -1.0e16]),
        aggregate_direct_column_accessor(
            "amount",
            &PrimitiveArray::from_option_iter([Some(5_i32), None, Some(-2), Some(8)]).into_array(),
        )
        .unwrap(),
        aggregate_direct_column_accessor(
            "amount",
            &PrimitiveArray::from_option_iter([None::<u16>, None, None, None]).into_array(),
        )
        .unwrap(),
        aggregate_direct_column_accessor(
            "amount",
            &PrimitiveArray::from_option_iter([
                Some(999_f64),
                Some(1.25),
                None,
                Some(-2.5),
                Some(7.5),
                Some(999.0),
            ])
            .into_array()
            .slice(1..5)
            .unwrap(),
        )
        .unwrap(),
    ];
    for array in arrays {
        let accessors = [
            aggregate_direct_column_accessor(
                "category",
                &PrimitiveArray::from_option_iter([Some(2_i32), None, Some(2), Some(3)])
                    .into_array(),
            )
            .unwrap(),
            array,
        ];
        for count_first in [false, true] {
            for selected in [None, Some(&[3, 0, 1, 3, 2][..]), Some(&[][..])] {
                let request = request(count_first);
                let mut actual =
                    GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
                let mut expected =
                    GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
                assert!(update(&mut actual, &accessors, selected, 4).unwrap());
                original_rows(&mut expected, &accessors, selected.unwrap_or(&[0, 1, 2, 3]))
                    .unwrap();
                assert_state_equal(&actual, &expected);
                assert_eq!(actual.compact_numeric_block_chunks, 1);
                assert_eq!(
                    actual.compact_numeric_block_input_rows,
                    selected.map_or(4, <[usize]>::len) as u64
                );
            }
        }
    }
}

#[test]
fn compact_numeric_block_rebinds_widths_and_preserves_limits_having_and_empty_chunks() {
    let columns = vec!["category".to_owned(), "amount".to_owned()];
    for count_first in [false, true] {
        for ordered in [false, true] {
            for having in [false, true] {
                let mut request = request(count_first);
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
                for (keys, values, rows) in [
                    (accessor(Vec::<u8>::new()), accessor(Vec::<f32>::new()), 0),
                    (
                        accessor(vec![2_u8, 1, 2, 3]),
                        accessor(vec![10_i16, -20, 30, 40]),
                        4,
                    ),
                    (
                        accessor(vec![2_u64, 1, 2, 3]),
                        accessor(vec![1.25_f64, 2.5, 3.75, 4.0]),
                        4,
                    ),
                ] {
                    let accessors = [keys, values];
                    assert!(
                        actual
                            .update_compact_direct_from_accessors(&accessors, &columns, None, rows)
                            .unwrap()
                    );
                    original_rows(&mut expected, &accessors, &(0..rows).collect::<Vec<_>>())
                        .unwrap();
                    assert_state_equal(&actual, &expected);
                }
                let (_, actual_payload) = actual.result_row_count_and_payload(Some(1)).unwrap();
                let (_, expected_payload) = expected.result_row_count_and_payload(Some(1)).unwrap();
                assert_eq!(actual_payload["values"], expected_payload["values"]);
                if having {
                    assert_eq!(
                        actual_payload["values"],
                        serde_json::json!([{"category": 2, "mean": 11.25, "rows": 4}])
                    );
                }
                assert_eq!(actual_payload["aggregate_compact_numeric_block_chunks"], 3);
                assert_eq!(
                    actual_payload["aggregate_compact_numeric_block_input_rows"],
                    8
                );
                assert!(actual.compact_measure_direct_updates);
            }
        }
    }
}

#[test]
fn compact_numeric_block_preserves_error_and_partial_state_in_both_measure_orders() {
    let columns = vec!["category".to_owned(), "amount".to_owned()];
    for count_first in [false, true] {
        for (number, overflow_index, seed_sum, row) in [
            (1.0, Some(0), 0.0, 0),
            (1.0, Some(1), 0.0, 0),
            (f64::INFINITY, None, 0.0, 0),
            (f64::NAN, None, 0.0, 0),
            (f64::MAX, None, f64::MAX, 0),
            (1.0, None, 0.0, 1),
        ] {
            let request = request(count_first);
            let mut actual =
                GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
            let mut expected =
                GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
            let seed = [accessor(vec![2_i32]), accessor(vec![0_f64])];
            for state in [&mut actual, &mut expected] {
                original_rows(state, &seed, &[0]).unwrap();
                let values = state
                    .groups
                    .values_mut()
                    .next()
                    .unwrap()
                    .compact_measures_mut()
                    .unwrap()
                    .values_mut();
                values[usize::from(count_first)].sum = seed_sum;
                if let Some(index) = overflow_index {
                    values[index].count = u64::MAX;
                }
            }
            let accessors = [accessor(vec![2_i32]), accessor(vec![number])];
            let actual_error = update(&mut actual, &accessors, Some(&[row]), 1).unwrap_err();
            let expected_error = original_rows(&mut expected, &accessors, &[row]).unwrap_err();
            assert_eq!(actual_error.to_string(), expected_error.to_string());
            assert_state_equal(&actual, &expected);
            assert_eq!(actual.compact_numeric_block_chunks, 0);
        }
    }
}

#[test]
fn compact_numeric_block_rejects_other_shapes_before_updating_state() {
    let columns = vec!["category".to_owned(), "amount".to_owned()];
    let request = request(false);
    for (keys, values, rows) in [
        (accessor(vec![2_f64]), accessor(vec![7_i32]), 1),
        (accessor(vec![2_i32]), accessor(vec![7_i32]), 2),
        (accessor(vec![2_i32]), accessor(Vec::<i32>::new()), 1),
        (
            accessor(vec![2_i32]),
            AggregateDirectColumnAccessor::Materialized {
                values: vec![StatValue::UInt64(7)],
                blocker: "test",
            },
            1,
        ),
        (
            accessor(vec![2_i32]),
            aggregate_direct_column_accessor(
                "amount",
                &VarBinViewArray::from_iter_str(["seven"]).into_array(),
            )
            .unwrap(),
            1,
        ),
    ] {
        let mut state =
            GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
        assert!(!update(&mut state, &[keys, values], None, rows).unwrap());
        assert!(state.groups.is_empty());
    }
    let accessors = [accessor(vec![2_i32]), accessor(vec![7_i32])];
    let mut state = GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
    for transform in [
        AggregateValueTransform::Length,
        AggregateValueTransform::AddOffset(1),
    ] {
        state.compact_measure_specs.as_mut().unwrap()[0].value_transform = transform;
        assert!(!update(&mut state, &accessors, None, 1).unwrap());
    }
    state.compact_measure_specs.as_mut().unwrap()[0].value_transform =
        AggregateValueTransform::Identity;
    state.compact_measure_specs.as_mut().unwrap()[0].function = SimpleAggregateFunction::Sum;
    assert!(!update(&mut state, &accessors, None, 1).unwrap());
    state.compact_measure_specs.as_mut().unwrap()[0].function = SimpleAggregateFunction::Avg;
    state.compact_measure_specs.as_mut().unwrap()[1].column_index = Some(1);
    assert!(!update(&mut state, &accessors, None, 1).unwrap());
    state.compact_measure_specs.as_mut().unwrap()[1].column_index = None;
    state.group_columns[0].transform = AggregateValueTransform::AddOffset(1);
    assert!(!update(&mut state, &accessors, None, 1).unwrap());
    state.group_columns[0].transform = AggregateValueTransform::Identity;
    state.group_columns[0].extra_column_indices.push(1);
    assert!(!update(&mut state, &accessors, None, 1).unwrap());
    assert!(state.groups.is_empty());
}
