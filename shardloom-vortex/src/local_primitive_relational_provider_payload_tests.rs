//! Equivalent file kernels retain the native sort's payload validation scope.

use super::*;

fn payload_fixture(width: usize, invalid_row: usize) -> Fixture {
    let mut names = vec!["rank".to_owned(), "payload".to_owned()];
    let mut values = [1.0_f64, 2.0];
    values[invalid_row] = f64::NAN;
    let mut arrays = vec![
        PrimitiveArray::from_iter([0_i64, 1]).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
    ];
    for index in 2..width {
        names.push(format!("extra_{index}"));
        arrays.push(PrimitiveArray::from_iter([10_i64, 20]).into_array());
    }
    Fixture::new(
        StructArray::new(
            names.iter().map(String::as_str).collect::<FieldNames>(),
            arrays,
            2,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

#[test]
fn file_sort_kernel_preserves_hidden_and_outside_prefix_payload_errors() {
    let mut outcomes = Vec::new();
    for width in [2, 10] {
        for invalid_row in [0, 1] {
            for hide_payload in [false, true] {
                let fixture = payload_fixture(width, invalid_row);
                let input = range(sort_without_null_order(fixture.scan(), "rank"), 1, 0);
                let plan = if hide_payload {
                    project(input, vec![("rank", column("rank"))])
                } else {
                    input
                };
                for compact in [false, true] {
                    let plan = if compact {
                        plan.clone().compact_streaming()
                    } else {
                        plan.clone()
                    };
                    let mut pair = Vec::new();
                    for compiled in [true, false] {
                        let mut prepared = prepare_relational(&plan, policy()).unwrap();
                        if !compiled {
                            prepared.remove_file_kernels_for_reference().unwrap();
                        }
                        let initial = prepared.snapshot().memory.reserved_bytes;
                        let result = prepared
                            .collect_jsonl(&CancellationToken::default())
                            .map(|result| json_rows(&result))
                            .map_err(|error| error.to_string());
                        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
                        pair.push(result);
                    }
                    outcomes.push((width, invalid_row, hide_payload, compact, pair));
                }
            }
        }
    }
    for (width, invalid_row, hidden, compact, pair) in &outcomes {
        assert!(
            pair[1]
                .as_ref()
                .is_err_and(|error| error.contains("finite declared dtype")),
            "reference validation changed: width={width}, invalid={invalid_row}, hidden={hidden}, compact={compact}"
        );
    }
    let disagreements = outcomes
        .iter()
        .filter(|(_, _, _, _, pair)| pair[0] != pair[1])
        .map(|(width, invalid, hidden, compact, _)| (*width, *invalid, *hidden, *compact))
        .collect::<Vec<_>>();
    assert_eq!(disagreements, [], "file sort changed payload error scope");
}

#[test]
fn projection_before_sort_retains_file_kernel_with_unobserved_source_payload() {
    let fixture = payload_fixture(10, 0);
    let input = project(fixture.scan(), vec![("key", column("rank"))]);
    let plan = project(
        range(sort_without_null_order(input, "key"), 1, 0),
        vec![("chosen", column("key"))],
    );
    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SortRows);
    let result = prepare_relational(&plan, policy())
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), [serde_json::json!({"chosen": 0})]);
}

#[test]
fn text_order_keys_retain_file_kernel_with_integer_payload() {
    let fixture = Fixture::new(
        StructArray::new(
            ["text", "value"].into(),
            vec![
                vortex::array::arrays::VarBinArray::from(vec!["b", "a"]).into_array(),
                PrimitiveArray::from_iter([8_i64, 7]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    for duplicate_key in [false, true] {
        let mut fields = vec![("key", column("text")), ("value", column("value"))];
        if duplicate_key {
            fields.push(("same_text", column("text")));
        }
        let input = project(fixture.scan(), fields);
        let plan = range(sort_without_null_order(input, "key"), 1, 0);
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SortRows);
        let result = prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let expected = if duplicate_key {
            serde_json::json!({"key": "a", "value": 7, "same_text": "a"})
        } else {
            serde_json::json!({"key": "a", "value": 7})
        };
        assert_eq!(json_rows(&result), [expected]);
    }
}

#[test]
fn sort_payload_validation_respects_prior_filter_and_nulls() {
    let fixture = Fixture::new(
        StructArray::new(
            ["rank", "payload"].into(),
            vec![
                PrimitiveArray::from_iter([2_i64, 0, 1]).into_array(),
                PrimitiveArray::from_option_iter([Some(f64::NAN), None, Some(2.0)]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let input = filter(
        fixture.scan(),
        "rank",
        ComparisonOp::Lt,
        ScalarValue::Int64(2),
    );
    let plan = range(sort_without_null_order(input, "rank"), 1, 0);
    for plan in [plan.clone(), plan.compact_streaming()] {
        let mut prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(!prepared.root_has_file_kernel());
        let initial = prepared.snapshot().memory.reserved_bytes;
        for compiled in [true, false] {
            if !compiled {
                prepared.remove_file_kernels_for_reference().unwrap();
            }
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&result),
                [serde_json::json!({"rank": 0, "payload": null})]
            );
            drop(result);
        }
        assert!(prepared.snapshot().memory.reserved_bytes <= initial);
    }
}

#[test]
fn sort_preserves_validation_of_hidden_nested_payload() {
    let payload = single(
        "value",
        PrimitiveArray::from_iter([1.0_f64, f64::NAN]).into_array(),
    );
    let fixture = Fixture::new(
        StructArray::new(
            ["rank", "payload"].into(),
            vec![PrimitiveArray::from_iter([0_i64, 1]).into_array(), payload],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    let plan = project(
        range(sort_without_null_order(fixture.scan(), "rank"), 1, 0),
        vec![("rank", column("rank"))],
    );
    for plan in [plan.clone(), plan.compact_streaming()] {
        let mut prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(!prepared.root_has_file_kernel());
        for compiled in [true, false] {
            if !compiled {
                prepared.remove_file_kernels_for_reference().unwrap();
            }
            let initial = prepared.snapshot().memory.reserved_bytes;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("hidden nested payload must retain validation");
            assert!(error.to_string().contains("finite declared dtype"));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}

#[test]
fn sort_preserves_validation_of_hidden_text_payload() {
    use std::sync::Arc;
    use vortex::array::{
        ArrayParts,
        arrays::{VarBin, VarBinArray},
    };
    let bytes = VarBinArray::from(vec![&b"visible"[..], &b"\xff"[..]]);
    let slots = bytes.slots().iter().cloned().collect();
    let text = VarBinArray::try_from_parts(ArrayParts::new(
        VarBin,
        DType::Utf8(Nullability::NonNullable),
        2,
        bytes.into_data(),
        slots,
    ))
    .unwrap()
    .into_array();
    let fixture = Fixture::with_strategy(
        StructArray::new(
            ["rank", "payload"].into(),
            vec![PrimitiveArray::from_iter([0_i64, 1]).into_array(), text],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        2,
        vec![],
        Arc::new(vortex::layout::layouts::table::TableStrategy::new(
            Arc::new(vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default()),
            Arc::new(vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default()),
        )),
    );
    let plan = project(
        range(sort_without_null_order(fixture.scan(), "rank"), 1, 0),
        vec![("rank", column("rank"))],
    );
    for plan in [plan.clone(), plan.compact_streaming()] {
        for compiled in [true, false] {
            let mut prepared = prepare_relational(&plan, policy()).unwrap();
            if !compiled {
                prepared.remove_file_kernels_for_reference().unwrap();
            }
            let initial = prepared.snapshot().memory.reserved_bytes;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .expect("hidden text must retain validation")
                .to_string();
            // A compiled scan may wrap the upstream error at its own boundary;
            // both forms must preserve the malformed value and row coordinate.
            assert!(error.contains("utf-8") && error.contains("at index 1"));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        }
    }
}
