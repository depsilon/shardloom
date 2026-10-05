use super::*;

pub(super) fn fixture(values: &[Option<i128>], precision: u8, scale: i8, chunk: usize) -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["decimal"]),
            vec![
                DecimalArray::from_option_iter(
                    values.iter().copied(),
                    DecimalDType::new(precision, scale),
                )
                .into_array(),
            ],
            values.len(),
            Validity::NonNullable,
        )
        .into_array(),
        chunk,
    )
}

pub(super) fn rolling(
    aggregate: &str,
    window: usize,
    minimum: usize,
    center: bool,
) -> VortexQueryPrimitiveRequest {
    let mut req = request(Kind::RollingWindowRows, &["decimal"]);
    req.rolling_window = Some(
        VortexRollingWindowRequest::new(
            ColumnRef::new("decimal").unwrap(),
            "value".into(),
            window,
            minimum,
            aggregate.into(),
        )
        .with_center(center),
    );
    req
}

#[test]
fn native_decimal_rolling_matches_independent_windows_across_batches_and_limits() {
    let input = [
        Some(60),
        None,
        Some(-120),
        Some(180),
        Some(0),
        None,
        Some(300),
        Some(-60),
    ];
    for chunk in [1, 3] {
        let fixture = fixture(&input, 8, 2, chunk);
        for window in [1, 2, 3, 4, 9] {
            for minimum in [1, 2].into_iter().filter(|minimum| *minimum <= window) {
                for center in [false, true] {
                    for aggregate in ["sum", "mean", "min", "max", "count"] {
                        let expected = (0..input.len()).filter_map(|row| {
                            let (start, end) = if center {
                                let left = (window - 1) / 2;
                                (row.saturating_sub(left), (row + window - left).min(input.len()))
                            } else {
                                ((row + 1).saturating_sub(window), row + 1)
                            };
                            let values = input[start..end].iter().flatten().copied().collect::<Vec<_>>();
                            if values.len() < minimum { return None; }
                            if aggregate == "count" { return Some(json!({"value":values.len()})); }
                            let total = values.iter().sum::<i128>();
                            let (value, precision, scale) = match aggregate {
                                "sum" => (total, 38, 2),
                                "mean" => { let numerator = total * 10_000; let count = i128::try_from(values.len()).unwrap(); assert_eq!(numerator % count, 0); (numerator / count, 38, 6) },
                                "min" => (*values.iter().min().unwrap(), 8, 2),
                                "max" => (*values.iter().max().unwrap(), 8, 2),
                                _ => unreachable!(),
                            };
                            Some(json!({"value":format!("decimal128({precision},{scale}):{value}")}))
                        }).collect::<Vec<_>>();
                        let req = rolling(aggregate, window, minimum, center);
                        for direct in [false, true] {
                            assert_eq!(
                                rows(&fixture, &req, direct),
                                expected,
                                "{aggregate}/{window}/{minimum}/{center}/{direct}/{chunk}"
                            );
                            let mut limited = req.clone();
                            limited.source_order_limit = Some(2);
                            assert_eq!(
                                rows(&fixture, &limited, direct),
                                expected.iter().take(2).cloned().collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn native_decimal_rolling_wide_totals_exact_averages_and_explicit_failures() {
    let maximum = 10i128.pow(38) - 1;
    for center in [false, true] {
        for (input, aggregate, expected) in [
            (
                vec![Some(maximum), Some(maximum), Some(-maximum)],
                "sum",
                maximum,
            ),
            (vec![Some(maximum), Some(maximum)], "mean", maximum),
        ] {
            let fixture = fixture(&input, 38, 6, 1);
            let req = rolling(aggregate, input.len(), input.len(), center);
            for direct in [false, true] {
                assert_eq!(
                    rows(&fixture, &req, direct),
                    vec![json!({"value":format!("decimal128(38,6):{expected}")})]
                );
            }
        }
        for (input, aggregate, message) in [
            (
                vec![Some(maximum), Some(maximum)],
                "sum",
                "precision overflow",
            ),
            (
                vec![Some(1), Some(0), Some(0)],
                "mean",
                "nonzero fractional digits",
            ),
        ] {
            let fixture = fixture(&input, 38, 6, 1);
            let req = rolling(aggregate, input.len(), input.len(), center);
            for direct in [false, true] {
                assert!(denied(&fixture, &req, direct).contains(message));
            }
        }
    }
}

#[test]
fn native_decimal_rolling_limit_stops_before_unused_inexact_eof_windows() {
    // The first three means are exact; the final shrinking frame is 4/3.
    // Only the first two results become ready before the source ends.
    for chunk in [1, 4] {
        let fixture = fixture(&[Some(0), Some(0), Some(3), Some(1)], 8, 0, chunk);
        let req = rolling("mean", 5, 1, true);
        for direct in [false, true] {
            assert!(denied(&fixture, &req, direct).contains("nonzero fractional digits"));
            let mut limited = req.clone();
            limited.source_order_limit = Some(3);
            assert_eq!(
                rows(&fixture, &limited, direct),
                vec![json!({"value":"decimal128(38,6):1000000"}); 3]
            );
        }
    }
}

#[test]
fn native_decimal_rolling_relational_limits_cap_only_safe_output_prefixes() {
    use crate::relational_query::{VortexRelationalFilter, VortexRelationalProject};
    use shardloom_core::{ExprId, Expression};

    let range = |input, offset, count| {
        VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input,
            offset,
            count,
        }))
    };
    for chunk in [1, 4] {
        let fixture = fixture(&[Some(0), Some(0), Some(3), Some(1)], 8, 0, chunk);
        let request = rolling("mean", 5, 1, true);
        let rolling = || unary(fixture.scan(), request.clone());
        let projected = || {
            VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
                input: rolling(),
                expressions: vec![(
                    "renamed".into(),
                    Expression::column(
                        ExprId::new("alias").unwrap(),
                        ColumnRef::new("value").unwrap(),
                    ),
                )],
            }))
        };
        for (offset, count) in [(0, 0), (0, 3), (1, 2), (2, 1)] {
            assert_eq!(
                collect(&range(projected(), offset, count)),
                vec![json!({"renamed":"decimal128(38,6):1000000"}); count],
            );
        }
        assert_eq!(
            collect(&range(range(projected(), 1, 3), 1, 1)),
            vec![json!({"renamed":"decimal128(38,6):1000000"})],
        );
        let mut already_limited = request.clone();
        already_limited.source_order_limit = Some(2);
        assert_eq!(
            collect(&range(unary(fixture.scan(), already_limited), 1, 3)),
            vec![json!({"value":"decimal128(38,6):1000000"})],
        );
        // These operators need more than a fixed input prefix. A filter that
        // keeps every row is deliberately not special-cased by this rule.
        for barrier in [
            VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input: rolling(),
                predicate: Expression::literal(
                    ExprId::new("all").unwrap(),
                    ScalarValue::Boolean(true),
                ),
            })),
            VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
                input: rolling(),
                keys: vec![VortexRelationalOrderKey {
                    column: ColumnRef::new("value").unwrap(),
                    descending: false,
                    nulls: Some(VortexRelationalNullOrder::Last),
                }],
            })),
        ] {
            let prepared = prepare_relational(&range(barrier, 0, 1), policy()).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let error = prepared
                .collect_jsonl(&CancellationToken::default())
                .err()
                .unwrap();
            assert!(error.to_string().contains("nonzero fractional digits"));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn native_decimal_rolling_empty_and_all_null_results_keep_declared_type() {
    for input in [vec![], vec![None, None]] {
        let fixture = fixture(&input, 38, 38, 1);
        for aggregate in ["sum", "mean", "min", "max"] {
            let req = rolling(aggregate, 3, 1, true);
            let prepared = prepared_composed(&fixture, &req).unwrap();
            assert_eq!(
                prepared
                    .output_dtype()
                    .unwrap()
                    .as_struct_fields_opt()
                    .unwrap()
                    .field("value")
                    .unwrap(),
                DType::Decimal(DecimalDType::new(38, 38), Nullability::NonNullable)
            );
            for direct in [false, true] {
                assert_eq!(rows(&fixture, &req, direct), Vec::<Value>::new());
            }
        }
    }
}
