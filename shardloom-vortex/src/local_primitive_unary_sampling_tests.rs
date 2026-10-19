//! Exact seeded population semantics, observation boundaries and admission scope.

use super::*;
use crate::{
    local_primitives::native_relational_spill::State as Spill,
    relational_query::VortexRelationalSpillPolicy,
};
use serde_json::{Value as Json, json};
use shardloom_core::{ComparisonOp, PredicateExpr};

fn input(rows: &[Json]) -> ArrayRef {
    StructArray::new(
        FieldNames::from(["id", "value", "weight"]),
        vec![
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row["id"].as_i64()))
                .into_array(),
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row["value"].as_i64()))
                .into_array(),
            PrimitiveArray::from_option_iter(rows.iter().map(|row| row["weight"].as_f64()))
                .into_array(),
        ],
        rows.len(),
        Validity::NonNullable,
    )
    .into_array()
}

fn request(options: &Json) -> VortexQueryPrimitiveRequest {
    let mut request = VortexQueryPrimitiveRequest::for_relational_input(
        VortexQueryPrimitiveKind::SampleRows,
        projection(&["id", "value", "weight"]),
    );
    request.source_order_limit = options["n"].as_u64().map(|n| usize::try_from(n).unwrap());
    request.sample_fraction = options["frac"].as_f64();
    request.sample_seed = options["random_state"].as_u64();
    request.sample_with_replacement = options["replace"].as_bool().unwrap_or(false);
    request.sample_weight_column = options["weights"]
        .as_str()
        .map(|name| ColumnRef::new(name).unwrap());
    request
}

fn consume(
    input: &ArrayRef,
    request: &VortexQueryPrimitiveRequest,
    upper: Option<u64>,
    width: usize,
) -> Result<(Vec<Json>, bool)> {
    let fixture = Fixture::new(&[], &[], 1);
    let directory = fixture.0.join("sampling-runs");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"preserve").unwrap();
    let policy = VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)?;
    let session = ResidentVortexSession::new(32 << 20, 1)?;
    let bound = BoundUnary::for_relation(request, input.dtype(), session.memory())?;
    let baseline = session.snapshot().memory.reserved_bytes;
    let mut rows = Vec::new();
    let mut calls = 0;
    let result = session.with_native_execution_context(&CancellationToken::default(), |context| {
        let state = Spill::new(&policy, context)?;
        let mut produce = |consumer: &mut dyn FnMut(ArrayRef) -> Result<()>| {
            calls += 1;
            // Empty input still supplies its prepared schema before completion.
            if input.is_empty() {
                consumer(input.clone())?;
            }
            for start in (0..input.len()).step_by(width) {
                consumer(input.slice(start..input.len().min(start + width)).unwrap())?;
            }
            Ok(())
        };
        let mut consume = |array: ArrayRef| {
            let mut output = runtime::collect::JsonRows::new(context.memory(), 8 << 20, false)?;
            output.append_native(&array, context)?;
            rows.extend(serde_json::from_str::<Vec<Json>>(output.finish()?.value()).unwrap());
            Ok(())
        };
        let admitted = bound.sampling_spill_supported(upper);
        let consumed = if admitted {
            bound
                .consume_relation_sampling(context, &state, width, &mut produce, &mut consume)
                .map(|(usage, report)| {
                    assert_eq!(report.input_rows, input.len() as u64);
                    assert_eq!(report.population_rows, usage.items as u64);
                    assert_eq!(report.output_rows, rows.len() as u64);
                    assert_eq!(usage.all_input_retained, usage.items != 0);
                })
        } else {
            bound
                .consume_relation(context, upper, width, &mut produce, &mut consume)
                .map(|_| ())
        };
        let report = state.finish()?;
        assert!(report.owned_cleanup_completed);
        assert_eq!(
            report.runs_written, 0,
            "small-state control must remain resident"
        );
        consumed.map(|()| admitted)
    });
    assert_eq!(calls, 1);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    drop(bound);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"preserve");
    assert_eq!(fs::read_dir(directory).unwrap().count(), 1);
    result.map(|admitted| (rows, admitted))
}

#[test]
fn sampling_population_matches_full_independent_seed_matrix_and_bound_admission() {
    let fixture: Json = serde_json::from_str(include_str!(
        "../../scripts/native_streaming_unary_reference_fixtures.json"
    ))
    .unwrap();
    let cases = fixture["sampling_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 54);
    for case in cases {
        let rows = case
            .get("rows")
            .unwrap_or(&fixture["sampling_rows"])
            .as_array()
            .unwrap();
        let input = input(rows);
        let request = request(&case["options"]);
        for upper in [None, Some(rows.len() as u64 + 17)] {
            for width in [1, 3, 13] {
                let (actual, admitted) = consume(&input, &request, upper, width)
                    .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
                assert_eq!(
                    actual,
                    *case["expected"].as_array().unwrap(),
                    "{}/{upper:?}/{width}",
                    case["name"]
                );
                assert_eq!(
                    admitted,
                    request.sample_with_replacement
                        || (request.sample_fraction.is_some() && upper.is_none())
                );
            }
        }
    }
}

#[test]
fn sampling_population_empty_fractional_and_replacement_inputs_finish_without_draws() {
    let empty = input(&[]);
    for replace in [false, true] {
        for weighted in [false, true] {
            let request = request(&json!({"frac":0.5, "replace":replace,
                "weights":weighted.then_some("weight")}));
            for upper in [None, Some(0), Some(17)] {
                assert_eq!(
                    consume(&empty, &request, upper, 3).unwrap().0,
                    [] as [Json; 0]
                );
            }
        }
    }
}

#[test]
fn sampling_population_fractional_ties_filter_weights_and_keep_hidden_columns() {
    let tiny = f64::from_bits(1);
    let rows = [
        json!({"id":0,"value":0,"weight":0.0}),
        json!({"id":1,"value":1,"weight":tiny}),
        json!({"id":2,"value":2,"weight":tiny}),
        json!({"id":3,"value":3,"weight":tiny}),
        json!({"id":4,"value":4,"weight":1.0}),
    ];
    let input = input(&rows);
    let mut request = request(&json!({"frac":0.5,"random_state":7,"weights":"weight"}));
    request.projection = projection(&["id", "value"]);
    request.predicate = Some(PredicateExpr::Compare {
        column: ColumnRef::new("id").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::Int64(0),
    });
    for upper in [None, Some(5), Some(20)] {
        for width in [1, 3, 5] {
            assert_eq!(
                consume(&input, &request, upper, width).unwrap().0,
                [json!({"id":1,"value":1}), json!({"id":4,"value":4})]
            );
        }
    }
}

#[test]
fn sampling_population_rounded_cumulative_ties_choose_first_equal_position() {
    // Independent seed-7 draw-zero threshold, with total exactly 1.0. Two tiny
    // positive weights round away so three cumulative sums equal the threshold.
    let first = f64::from_bits(0x3fd6_ba82_24f5_0f05);
    let weights = [first, f64::from_bits(1), f64::from_bits(1), 1.0 - first];
    let rows = weights
        .iter()
        .enumerate()
        .map(|(id, weight)| json!({"id":id,"value":id,"weight":weight}))
        .collect::<Vec<_>>();
    let input = input(&rows);
    let mut request = request(&json!({"n":7,"random_state":7,"replace":true,"weights":"weight"}));
    request.projection = projection(&["id"]);
    let expected = [0, 3, 0, 3, 3, 3, 3].map(|id| json!({"id":id}));
    for upper in [None, Some(4), Some(50)] {
        for width in [1, 2, 4] {
            assert_eq!(consume(&input, &request, upper, width).unwrap().0, expected);
        }
    }
}

#[test]
fn sampling_population_nonempty_minimum_and_full_fraction_preserve_targets() {
    let rows = (0..10)
        .map(|id| json!({"id":id,"value":id,"weight":1.0}))
        .collect::<Vec<_>>();
    let input = input(&rows);
    for fraction in [f64::from_bits(1), 0.1, 1.0] {
        let mut request = request(&json!({"frac":fraction,"random_state":7}));
        request.projection = projection(&["id"]);
        let expected = if fraction.to_bits() == 1.0_f64.to_bits() {
            (0..10).map(|id| json!({"id":id})).collect::<Vec<_>>()
        } else {
            vec![json!({"id":1})]
        };
        assert_eq!(consume(&input, &request, None, 3).unwrap().0, expected);
    }
}

#[test]
fn sampling_population_invalid_weights_and_total_overflow_release_state() {
    for replace in [false, true] {
        for weight in [
            None,
            Some(0.0),
            Some(-1.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
        ] {
            let input = StructArray::new(
                FieldNames::from(["id", "weight"]),
                vec![
                    PrimitiveArray::from_iter([0_i64, 1]).into_array(),
                    PrimitiveArray::from_option_iter([Some(1.0), weight]).into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .into_array();
            let mut request = request(&json!({"frac":0.5,"replace":replace,"weights":"weight"}));
            request.projection = projection(&["id"]);
            let error = consume(&input, &request, None, 1).unwrap_err().to_string();
            assert!(error.contains("weight"), "{error}");
            assert!(error.contains("no fallback"), "{error}");
        }
    }
    let input = StructArray::new(
        FieldNames::from(["id", "weight"]),
        vec![
            PrimitiveArray::from_iter([0_i64, 1]).into_array(),
            PrimitiveArray::from_iter([f64::MAX, f64::MAX]).into_array(),
        ],
        2,
        Validity::NonNullable,
    )
    .into_array();
    let mut request = request(&json!({"n":1,"replace":true,"weights":"weight"}));
    request.projection = projection(&["id"]);
    assert!(
        consume(&input, &request, None, 1)
            .unwrap_err()
            .to_string()
            .contains("total weight must remain finite")
    );
}

#[test]
fn sampling_population_private_floats_are_validated_only_when_selected() {
    for replace in [false, true] {
        for selected_invalid in [false, true] {
            let input = StructArray::new(
                FieldNames::from(["id", "value"]),
                vec![
                    PrimitiveArray::from_iter([0_i64, 1]).into_array(),
                    PrimitiveArray::from_iter(if selected_invalid {
                        [1.0, f64::INFINITY]
                    } else {
                        [f64::INFINITY, 1.0]
                    })
                    .into_array(),
                ],
                2,
                Validity::NonNullable,
            )
            .into_array();
            let mut request = request(&json!({"frac":0.5,"random_state":7,"replace":replace}));
            request.projection = projection(&["id", "value"]);
            let result = consume(&input, &request, None, 1);
            if selected_invalid {
                assert!(result.unwrap_err().to_string().contains("finite"));
            } else {
                assert_eq!(result.unwrap().0, [json!({"id":1,"value":1.0})]);
            }
        }
    }
}
