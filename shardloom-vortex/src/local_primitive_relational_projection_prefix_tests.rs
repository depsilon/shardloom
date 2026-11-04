//! Bounded ordering consumes only projections with a complete output-value proof.

use super::*;
use serde_json::json;

fn ordered(
    input: VortexRelationalPlan,
    key: &str,
    offset: usize,
    count: usize,
    nulls: Option<VortexRelationalNullOrder>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
            input,
            keys: vec![VortexRelationalOrderKey {
                column: ColumnRef::new(key).unwrap(),
                descending: true,
                nulls,
            }],
        })),
        offset,
        count,
    }))
}

#[test]
fn computed_projection_prefix_keeps_complete_results_ties_nulls_and_renamed_chains() {
    let numbers = (0..37_i64)
        .map(|row| (row % 5 != 0).then_some(row % 7))
        .collect::<Vec<_>>();
    let texts = (0..37).map(|row| format!("λİß {row}")).collect::<Vec<_>>();
    let mut expected = (0..37)
        .map(|row| {
            (
                numbers[row].map(|n| n + 1),
                texts[row].to_uppercase().to_lowercase(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort_by_key(|row| std::cmp::Reverse(row.0));
    for chunk in [1, 7, 64] {
        let fixture = Fixture::new(
            StructArray::from_fields(&[
                (
                    "n",
                    PrimitiveArray::from_option_iter(numbers.iter().copied()).into_array(),
                ),
                (
                    "text",
                    VarBinViewArray::from_iter_str(texts.iter().map(String::as_str)).into_array(),
                ),
            ])
            .unwrap()
            .into_array(),
            chunk,
        );
        for (offset, count) in [(0, 1), (3, 7), (30, 7)] {
            let first = project(
                fixture.scan(),
                vec![
                    (
                        "score",
                        binary(col("n"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
                    ),
                    ("label", function("upper", vec![col("text")])),
                ],
            );
            let second = project(
                first,
                vec![
                    ("rank_key", col("score")),
                    ("label", function("lower", vec![col("label")])),
                ],
            );
            let plan = ordered(
                second,
                "rank_key",
                offset,
                count,
                Some(VortexRelationalNullOrder::Last),
            );
            let prepared = prepare_relational(&plan, policy()).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&result),
                expected[offset..offset + count]
                    .iter()
                    .map(|(key, text)| json!({"rank_key": key, "label": text}))
                    .collect::<Vec<_>>()
            );
            window_tests::assert_work(
                &result.execution,
                &[("streaming_ordering_prefix_rows_validated", 37)],
            );
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn computed_projection_borrows_only_after_an_upstream_validated_window() {
    let fixture = window_frame_tests::fixture(2);
    let window = window_frame_tests::one(
        &fixture,
        crate::relational_query::VortexRelationalWindowFunction::RowNumber,
        None,
        Some("id"),
    );
    let computed = project(
        window,
        vec![
            ("borrowed", col("out")),
            (
                "score",
                binary(col("out"), BinaryOp::Add, lit(ScalarValue::Int64(7))),
            ),
        ],
    );
    let prepared = prepare_relational(&ordered(computed, "score", 1, 2, None), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        [
            json!({"borrowed":9,"score":16}),
            json!({"borrowed":8,"score":15})
        ]
    );
    window_tests::assert_work(
        &result.execution,
        &[("streaming_ordering_prefix_rows_validated", 10)],
    );
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn computed_neighbour_does_not_move_unvalidated_borrowed_payload_before_order_key_errors() {
    let fixture = Fixture::new(
        StructArray::from_fields(&[
            ("n", PrimitiveArray::from_iter([0_i64, 1, 2]).into_array()),
            (
                "key",
                PrimitiveArray::from_option_iter([Some(1_i64), Some(2), None]).into_array(),
            ),
            (
                "poison",
                PrimitiveArray::from_iter([f64::NAN, 1.0, 2.0]).into_array(),
            ),
        ])
        .unwrap()
        .into_array(),
        3,
    );
    for count in [0, 1] {
        let projected = project(
            fixture.scan(),
            vec![
                (
                    "computed",
                    binary(col("n"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
                ),
                ("poison", col("poison")),
                ("key", col("key")),
            ],
        );
        let prepared =
            prepare_relational(&ordered(projected, "key", 0, count, None), policy()).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        if count == 0 {
            // Ordinary file LIMIT 0 already skips an input with no completion
            // obligation. A projection proof must not change that contract.
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), [] as [serde_json::Value; 0]);
            drop(result);
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().sort_streaming_prefix_batches, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 1);
            continue;
        }
        let error = prepared
            .collect_jsonl(&CancellationToken::default())
            .err()
            .expect("an unplaced null order key must fail")
            .to_string();
        assert!(
            error.contains("ORDER BY requires explicit NULLS FIRST or NULLS LAST"),
            "{error}"
        );
        #[cfg(debug_assertions)]
        assert_eq!(observation.finish().sort_streaming_prefix_batches, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn computed_projection_prefix_still_evaluates_failing_tail_before_any_delivery() {
    let fixture = Fixture::new(
        single(
            "divisor",
            PrimitiveArray::from_iter([1_i64, 2, 3, 0]).into_array(),
        ),
        1,
    );
    let projected = project(
        fixture.scan(),
        vec![(
            "score",
            binary(
                lit(ScalarValue::Int64(12)),
                BinaryOp::Divide,
                col("divisor"),
            ),
        )],
    );
    let prepared = prepare_relational(&ordered(projected, "score", 0, 1, None), policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .for_each_json_batch(&CancellationToken::default(), 1, 1 << 20, |_| {
            panic!("a failing computed tail cannot deliver a prefix")
        })
        .err()
        .expect("the computed tail must still fail")
        .to_string();
    assert!(error.contains("division by zero"), "{error}");
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
}
