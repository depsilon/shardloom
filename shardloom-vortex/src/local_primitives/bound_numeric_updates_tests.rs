use super::*;
use crate::local_primitives::{
    AggregateDistinctValue, AggregateUtf8DictionarySource, GroupedAggregateStates,
    aggregate_direct_column_accessor,
};
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::{ColumnRef, StatValue};
use vortex::array::{IntoArray as _, arrays::PrimitiveArray, validity::Validity};

fn states(specs: &[(SimpleAggregateFunction, Option<usize>)]) -> SimpleAggregateStates {
    SimpleAggregateStates {
        states: specs
            .iter()
            .enumerate()
            .map(|(index, (function, column_index))| {
                SimpleAggregateState::new(
                    *function,
                    *column_index,
                    format!("m{index}"),
                    None,
                    AggregateValueTransform::Identity,
                )
            })
            .collect(),
        direct_scalar_updates: false,
        direct_distinct_updates: false,
        dictionary_arc_distinct_updates: false,
        dense_integer_distinct_preunion_updates: false,
        fused_numeric_additive_updates: false,
        fused_utf8_dictionary_transform_updates: false,
        lazy_utf8_dictionary_minmax_updates: false,
    }
}

fn numeric_accessor<T>(values: Vec<T>) -> AggregateDirectColumnAccessor
where
    T: vortex::array::dtype::NativePType,
{
    let array = PrimitiveArray::new(values, Validity::NonNullable).into_array();
    aggregate_direct_column_accessor("measure", &array).unwrap()
}

fn assert_states_equal(left: &SimpleAggregateStates, right: &SimpleAggregateStates) {
    assert_eq!(left.states.len(), right.states.len());
    for (left, right) in left.states.iter().zip(&right.states) {
        assert!(left.function == right.function);
        assert_eq!(left.column_index, right.column_index);
        assert_eq!(left.count, right.count);
        assert_eq!(left.sum.to_bits(), right.sum.to_bits());
        assert_eq!(left.min, right.min);
        assert_eq!(left.max, right.max);
        assert_eq!(left.distinct_values, right.distinct_values);
    }
}

fn compare_recipe_to_row_updates(
    template: &SimpleAggregateStates,
    accessors: &[AggregateDirectColumnAccessor],
    rows: usize,
    skipped: usize,
) {
    let recipe = BoundNumericUpdates::bind(template, accessors, skipped, rows)
        .expect("eligible additive recipe");
    let mut actual = template.clone();
    let mut expected = template.clone();
    for row in 0..rows {
        recipe.update(&mut actual, row).unwrap();
        expected
            .update_direct_row_from_accessors_except_state(accessors, row, rows, skipped)
            .unwrap();
        assert_states_equal(&actual, &expected);
    }
}

#[test]
fn bound_numeric_updates_match_row_updates_for_integer_widths_and_large_values() {
    let template = states(&[
        (SimpleAggregateFunction::Count, None),
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::Avg, Some(0)),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
    ]);
    let base = (1_i64 << 53) + 17;
    for accessor in [
        numeric_accessor(vec![0_u8, 1, 2, u8::MAX]),
        numeric_accessor(vec![0_u16, 1, 2, u16::MAX]),
        numeric_accessor(vec![0_u32, 1, 2, u32::MAX]),
        numeric_accessor(vec![0_u64, 1, 2, u64::MAX]),
        numeric_accessor(vec![-7_i8, 0, 11, i8::MAX]),
        numeric_accessor(vec![-7_i16, 0, 11, i16::MAX]),
        numeric_accessor(vec![-7_i32, 0, 11, i32::MAX]),
        numeric_accessor(vec![-7_i64, base, 11, i64::MAX]),
    ] {
        let mut seed = template.clone();
        seed.states[3].count = 41;
        seed.states[3]
            .distinct_values
            .insert(AggregateDistinctValue::Int64(99));
        compare_recipe_to_row_updates(&seed, &[accessor], 4, 3);
    }
}

#[test]
fn bound_numeric_updates_match_nullable_and_floating_row_semantics() {
    let template = states(&[
        (SimpleAggregateFunction::Count, None),
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::Avg, Some(0)),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
    ]);
    let nullable_i32 =
        PrimitiveArray::from_option_iter([Some(5_i32), None, Some(-2), Some(8)]).into_array();
    let nullable_i32 = aggregate_direct_column_accessor("nullable", &nullable_i32).unwrap();
    let mut seed = template.clone();
    seed.states[3].count = 7;
    seed.states[3]
        .distinct_values
        .insert(AggregateDistinctValue::Int64(123));
    compare_recipe_to_row_updates(&seed, &[nullable_i32], 4, 3);

    for accessor in [
        numeric_accessor(vec![-0.0_f32, 1.25, 2.5, -3.75]),
        numeric_accessor(vec![-0.0_f64, 1.25, 2.5, -3.75]),
    ] {
        compare_recipe_to_row_updates(&seed, &[accessor], 4, 3);
    }
}

#[test]
fn bound_numeric_updates_rebinding_observes_new_contents_width_and_validity() {
    let template = states(&[
        (SimpleAggregateFunction::Count, None),
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
    ]);
    let first_accessors = [numeric_accessor(vec![1_i16, 2, 3])];
    let first_recipe = BoundNumericUpdates::bind(&template, &first_accessors, 2, 3);
    assert!(first_recipe.is_some());

    for accessor in [
        numeric_accessor(vec![10_i64, -20, 30]),
        aggregate_direct_column_accessor(
            "changed_validity",
            &PrimitiveArray::from_option_iter([Some(10_i64), None, Some(30)]).into_array(),
        )
        .unwrap(),
    ] {
        let accessors = [accessor];
        let recipe = BoundNumericUpdates::bind(&template, &accessors, 2, 3)
            .expect("fresh accessor binding remains eligible");
        let mut actual = template.clone();
        let mut expected = template.clone();
        for row in 0..3 {
            recipe.update(&mut actual, row).unwrap();
            expected
                .update_direct_row_from_accessors_except_state(&accessors, row, 3, 2)
                .unwrap();
            assert_states_equal(&actual, &expected);
        }
    }
}

#[test]
fn bound_numeric_updates_preserve_errors_and_partial_state_at_the_failing_row() {
    let cases: Vec<(SimpleAggregateStates, AggregateDirectColumnAccessor, usize)> = vec![
        (
            {
                let mut value = states(&[
                    (SimpleAggregateFunction::Count, None),
                    (SimpleAggregateFunction::CountDistinct, Some(0)),
                ]);
                value.states[0].count = u64::MAX;
                value
            },
            numeric_accessor(vec![1_i64]),
            0,
        ),
        (
            {
                let mut value = states(&[
                    (SimpleAggregateFunction::Sum, Some(0)),
                    (SimpleAggregateFunction::CountDistinct, Some(0)),
                ]);
                value.states[0].count = u64::MAX;
                value
            },
            numeric_accessor(vec![1_i64]),
            0,
        ),
        (
            states(&[
                (SimpleAggregateFunction::Sum, Some(0)),
                (SimpleAggregateFunction::CountDistinct, Some(0)),
            ]),
            numeric_accessor(vec![f64::INFINITY]),
            0,
        ),
        (
            {
                let mut value = states(&[
                    (SimpleAggregateFunction::Sum, Some(0)),
                    (SimpleAggregateFunction::CountDistinct, Some(0)),
                ]);
                value.states[0].sum = f64::MAX;
                value
            },
            numeric_accessor(vec![f64::MAX]),
            0,
        ),
    ];
    for (template, accessor, failing_row) in cases {
        let skipped = template.states.len() - 1;
        let recipe =
            BoundNumericUpdates::bind(&template, std::slice::from_ref(&accessor), skipped, 1)
                .expect("row error behavior is exercised only after admission");
        let mut actual = template.clone();
        let mut expected = template.clone();
        for row in 0..=failing_row {
            let recipe_error = recipe.update(&mut actual, row).unwrap_err();
            let reference_error = expected
                .update_direct_row_from_accessors_except_state(
                    std::slice::from_ref(&accessor),
                    row,
                    1,
                    skipped,
                )
                .unwrap_err();
            assert_eq!(recipe_error.to_string(), reference_error.to_string());
            assert_states_equal(&actual, &expected);
        }
    }
}

#[test]
fn bound_numeric_updates_rejects_unsupported_state_recipes() {
    let accessor = numeric_accessor(vec![1_i64, 2]);
    let ordinary = |function| {
        states(&[
            (function, Some(0)),
            (SimpleAggregateFunction::CountDistinct, Some(0)),
        ])
    };
    let mut offset = ordinary(SimpleAggregateFunction::Sum);
    offset.states[0].argument_offset = Some(1);
    assert!(BoundNumericUpdates::bind(&offset, std::slice::from_ref(&accessor), 1, 2).is_none());

    let mut transform = ordinary(SimpleAggregateFunction::Sum);
    transform.states[0].value_transform = AggregateValueTransform::Length;
    assert!(BoundNumericUpdates::bind(&transform, std::slice::from_ref(&accessor), 1, 2).is_none());

    let min = ordinary(SimpleAggregateFunction::Min);
    assert!(BoundNumericUpdates::bind(&min, std::slice::from_ref(&accessor), 1, 2).is_none());

    let text = AggregateDirectColumnAccessor::Utf8Dictionary {
        row_ids: vec![0, 0],
        values: vec![std::sync::Arc::<str>::from("x")]
            .into_iter()
            .map(Into::into)
            .collect(),
        value_nulls: None,
        row_nulls: None,
        source: AggregateUtf8DictionarySource::VortexDictArray,
    };
    assert!(
        BoundNumericUpdates::bind(&ordinary(SimpleAggregateFunction::Sum), &[text], 1, 2).is_none()
    );

    let materialized = AggregateDirectColumnAccessor::Materialized {
        values: vec![StatValue::Int64(1), StatValue::Int64(2)],
        blocker: "test",
    };
    assert!(
        BoundNumericUpdates::bind(
            &ordinary(SimpleAggregateFunction::Sum),
            &[materialized],
            1,
            2
        )
        .is_none()
    );
}

#[test]
fn bound_numeric_updates_keeps_the_existing_row_bounds_diagnostic_and_does_not_preflight() {
    let accessor = numeric_accessor(vec![1.0_f64, f64::INFINITY]);
    let template = states(&[
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
    ]);
    let recipe = BoundNumericUpdates::bind(&template, std::slice::from_ref(&accessor), 1, 2)
        .expect("valid prefix can be bound without reading future rows");
    let mut actual = template.clone();
    recipe.update(&mut actual, 0).unwrap();
    assert_eq!(actual.states[0].sum.to_bits(), 1.0_f64.to_bits());
    assert!(recipe.update(&mut actual, 1).is_err());

    let accessor = numeric_accessor(vec![1_i64]);
    let recipe = BoundNumericUpdates::bind(&template, std::slice::from_ref(&accessor), 1, 1)
        .expect("short owner binds; row checks remain update-time");
    let mut actual = template.clone();
    let mut expected = template;
    let error = recipe.update(&mut actual, 1).unwrap_err();
    let reference = expected
        .update_direct_row_from_accessors_except_state(std::slice::from_ref(&accessor), 1, 1, 1)
        .unwrap_err();
    assert_eq!(error.to_string(), reference.to_string());
}

#[test]
fn bound_numeric_updates_empty_and_all_null_keep_exact_counts() {
    let template = states(&[
        (SimpleAggregateFunction::Count, None),
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
    ]);
    for count in [0, 3] {
        let array = PrimitiveArray::from_option_iter(vec![None::<i64>; count]).into_array();
        let accessors = [aggregate_direct_column_accessor("empty_or_null", &array).unwrap()];
        let recipe = BoundNumericUpdates::bind(&template, &accessors, 2, count).unwrap();
        let mut actual = template.clone();
        for row in 0..count {
            recipe.update(&mut actual, row).unwrap();
        }
        assert_eq!(actual.states[0].count, count as u64);
        assert_eq!(actual.states[1].count, 0);
        assert_eq!(actual.states[1].sum.to_bits(), 0.0_f64.to_bits());
        assert_eq!(actual.states[2].count, 0);
    }
}

#[test]
fn bound_numeric_updates_preserve_measure_order_with_distinct_between_updates() {
    let mut template = states(&[
        (SimpleAggregateFunction::Count, None),
        (SimpleAggregateFunction::CountDistinct, Some(0)),
        (SimpleAggregateFunction::Sum, Some(0)),
        (SimpleAggregateFunction::Sum, Some(1)),
        (SimpleAggregateFunction::Avg, Some(0)),
    ]);
    template.states[1].count = 13;
    template.states[1]
        .distinct_values
        .insert(AggregateDistinctValue::Int64(37));
    let accessors = [
        numeric_accessor(vec![2_i64]),
        numeric_accessor(vec![f64::INFINITY]),
    ];
    let recipe = BoundNumericUpdates::bind(&template, &accessors, 1, 1).unwrap();
    let mut actual = template.clone();
    let mut expected = template.clone();
    let error = recipe.update(&mut actual, 0).unwrap_err();
    let reference = expected
        .update_direct_row_from_accessors_except_state(&accessors, 0, 1, 1)
        .unwrap_err();
    assert_eq!(error.to_string(), reference.to_string());
    assert_states_equal(&actual, &expected);
    assert_eq!(actual.states[0].count, 1);
    assert_eq!(actual.states[1].count, 13);
    assert_eq!(
        actual.states[1].distinct_values,
        template.states[1].distinct_values
    );
    assert_eq!(actual.states[2].count, 1);
    assert_eq!(actual.states[2].sum.to_bits(), 2.0_f64.to_bits());
    for index in [3, 4] {
        assert_eq!(actual.states[index].count, 0);
        assert_eq!(actual.states[index].sum.to_bits(), 0.0_f64.to_bits());
    }
}

#[test]
fn bound_numeric_updates_preserve_pair_preunion_and_every_ordinary_contribution() {
    let request = VortexSimpleAggregateRequest::grouped(
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
            VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("item").unwrap()),
                "unique".into(),
            ),
        ],
    )
    .with_order_by(vec![VortexAggregateOrderExpr::new("category", false)]);
    let columns = vec![
        "category".to_owned(),
        "amount".to_owned(),
        "item".to_owned(),
    ];
    let mut groups = GroupedAggregateStates::new(&request, None, &columns, false, false).unwrap();
    // The same keys appear in successive blocks with different nullable measure values.
    for values in [
        [Some(1_i32), Some(2), None, Some(4)],
        [Some(3), None, Some(8), Some(10)],
    ] {
        let accessors = [
            numeric_accessor(vec![11_i64, 11, 13, 13]),
            aggregate_direct_column_accessor(
                "amount",
                &PrimitiveArray::from_option_iter(values).into_array(),
            )
            .unwrap(),
            numeric_accessor(vec![
                (1_i64 << 60) + 1,
                (1_i64 << 60) + 1,
                i64::MAX,
                i64::MAX,
            ]),
        ];
        groups.observe_aggregate_accessors(&columns, &accessors);
        assert!(
            groups
                .update_general_direct_from_accessors(&accessors, None, 4)
                .unwrap()
        );
    }
    let (rows, summary) = groups.result_row_count_and_summary(None).unwrap();
    let payload: serde_json::Value = serde_json::from_str(&summary).unwrap();
    assert_eq!(rows, 2);
    assert_eq!(payload["aggregate_bound_numeric_recipe_chunks"], 2);
    assert_eq!(
        payload["grouped_count_distinct_pair_preunion_duplicate_rows_elided"],
        4
    );
    assert_eq!(
        payload["values"],
        serde_json::json!([
            {"category":11,"rows":4,"total":6.0,"mean":2.0,"unique":1},
            {"category":13,"rows":4,"total":22.0,"mean":22.0/3.0,"unique":1}
        ])
    );
}
