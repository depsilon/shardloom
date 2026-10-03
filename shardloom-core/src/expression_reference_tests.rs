//! Independent values for the decoded-reference boundary used by native tests.

use super::*;

fn expr(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("reference-conformance").unwrap(), kind)
}

fn literal(value: ScalarValue) -> Expression {
    expr(ExpressionKind::Literal(value))
}

fn call(name: &str, args: Vec<Expression>) -> Expression {
    expr(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}

fn cast(value: ScalarValue, dtype: LogicalDType) -> Expression {
    Expression::cast(ExprId::new("cast").unwrap(), literal(value), dtype)
}

fn decimal(value: i128, precision: u8, scale: u8) -> ScalarValue {
    ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    }
}

fn assert_value(expression: &Expression, value: ScalarValue, dtype: LogicalDType) {
    let report = evaluate_expression(expression, &ExpressionInputRow::new());
    assert_eq!(
        report.status,
        ExpressionEvaluationStatus::Evaluated,
        "{expression:?}: {report:?}"
    );
    assert_eq!(report.value, Some(value), "{expression:?}");
    assert_eq!(report.output_dtype, Some(dtype), "{expression:?}");
    assert!(!report.fallback_attempted && !report.external_engine_invoked);
}

#[test]
fn reference_branches_resolve_types_without_evaluating_unused_values() {
    let bad = || cast(ScalarValue::Utf8("bad".into()), LogicalDType::Int64);
    assert_value(
        &call("coalesce", vec![literal(ScalarValue::Int64(1)), bad()]),
        ScalarValue::Int64(1),
        LogicalDType::Int64,
    );
    assert_value(
        &call(
            "coalesce",
            vec![
                literal(ScalarValue::Null),
                literal(ScalarValue::Null),
                literal(ScalarValue::Int64(3)),
                bad(),
            ],
        ),
        ScalarValue::Int64(3),
        LogicalDType::Int64,
    );
    assert_value(
        &call(
            "coalesce",
            vec![cast(ScalarValue::Null, LogicalDType::UInt64)],
        ),
        ScalarValue::Null,
        LogicalDType::UInt64,
    );
    for count in [0, 129] {
        let report = evaluate_expression(
            &call("coalesce", vec![literal(ScalarValue::Null); count]),
            &ExpressionInputRow::new(),
        );
        assert_eq!(report.status, ExpressionEvaluationStatus::InvalidInput);
    }
    let column = Expression::column(ExprId::new("column").unwrap(), ColumnRef::new("n").unwrap())
        .with_dtype(LogicalDType::Int64);
    let row = ExpressionInputRow::from([("n".into(), ScalarValue::Int64(5))]);
    let skipped = evaluate_expression(
        &call(
            "coalesce",
            vec![literal(ScalarValue::Int64(1)), column.clone()],
        ),
        &row,
    );
    assert_eq!(skipped.value, Some(ScalarValue::Int64(1)));
    assert!(!skipped.data_materialized);
    let selected = evaluate_expression(
        &call("coalesce", vec![literal(ScalarValue::Null), column]),
        &row,
    );
    assert_eq!(selected.value, Some(ScalarValue::Int64(5)));
    assert!(selected.data_materialized);
    for expression in [
        call(
            "coalesce",
            vec![
                literal(ScalarValue::Int64(1)),
                literal(ScalarValue::Utf8("wrong type".into())),
            ],
        ),
        call(
            "case_when",
            vec![
                literal(ScalarValue::Boolean(true)),
                literal(ScalarValue::Int64(1)),
                cast(ScalarValue::Boolean(false), LogicalDType::Int64),
            ],
        ),
    ] {
        assert_eq!(
            evaluate_expression(&expression, &ExpressionInputRow::new()).status,
            ExpressionEvaluationStatus::Unsupported
        );
    }
}

#[test]
fn reference_decimal_branches_preserve_declared_common_domains_and_typed_nulls() {
    for (expression, value, dtype) in [
        (
            call(
                "coalesce",
                vec![
                    cast(ScalarValue::Null, decimal128_dtype(10, 2)),
                    literal(decimal(12, 3, 1)),
                ],
            ),
            decimal(120, 10, 2),
            decimal128_dtype(10, 2),
        ),
        (
            call(
                "coalesce",
                vec![
                    literal(decimal(25, 2, 1)),
                    cast(ScalarValue::Utf8("bad".into()), decimal128_dtype(11, 3)),
                ],
            ),
            decimal(2500, 11, 3),
            decimal128_dtype(11, 3),
        ),
        (
            call(
                "coalesce",
                vec![
                    cast(ScalarValue::Null, decimal128_dtype(5, 4)),
                    literal(decimal(12, 3, 1)),
                    literal(decimal(0, 8, 3)),
                ],
            ),
            decimal(12000, 9, 4),
            decimal128_dtype(9, 4),
        ),
        (
            call(
                "case_when",
                vec![
                    literal(ScalarValue::Boolean(true)),
                    literal(decimal(25, 2, 1)),
                    cast(ScalarValue::Utf8("bad".into()), decimal128_dtype(10, 2)),
                ],
            ),
            decimal(250, 10, 2),
            decimal128_dtype(10, 2),
        ),
        (
            call(
                "case_when",
                vec![
                    literal(ScalarValue::Boolean(true)),
                    cast(ScalarValue::Null, decimal128_dtype(10, 2)),
                    literal(decimal(12, 3, 1)),
                ],
            ),
            ScalarValue::Null,
            decimal128_dtype(10, 2),
        ),
        (
            call(
                "case_when",
                vec![
                    literal(ScalarValue::Null),
                    cast(ScalarValue::Null, decimal128_dtype(10, 2)),
                    cast(ScalarValue::Null, decimal128_dtype(9, 4)),
                ],
            ),
            ScalarValue::Null,
            decimal128_dtype(12, 4),
        ),
    ] {
        assert_value(&expression, value, dtype);
    }
    let column = Expression::column(
        ExprId::new("amount").unwrap(),
        ColumnRef::new("amount").unwrap(),
    )
    .with_dtype(decimal128_dtype(10, 2));
    let expression = call("coalesce", vec![column, literal(decimal(12, 3, 1))]);
    let row = ExpressionInputRow::from([("amount".into(), ScalarValue::Null)]);
    let report = evaluate_expression(&expression, &row);
    assert_eq!(report.value, Some(decimal(120, 10, 2)));
    assert_eq!(report.output_dtype, Some(decimal128_dtype(10, 2)));
    for name in ["coalesce", "case_when"] {
        let mut args = vec![literal(decimal(0, 38, 0)), literal(decimal(0, 38, 38))];
        if name == "case_when" {
            args.insert(0, literal(ScalarValue::Boolean(true)));
        }
        assert!(evaluate_expression(&call(name, args), &ExpressionInputRow::new()).has_errors());
    }
}

#[test]
fn reference_unsigned_numeric_functions_and_negation_keep_exact_domains() {
    for name in [
        "abs",
        "floor",
        "ceil",
        "ceiling",
        "round",
        "numeric_abs",
        "numeric_floor",
        "numeric_ceil",
        "numeric_round",
    ] {
        for value in [0, 1, 9_223_372_036_854_775_808, u64::MAX] {
            assert_value(
                &call(name, vec![literal(ScalarValue::UInt64(value))]),
                ScalarValue::UInt64(value),
                LogicalDType::UInt64,
            );
        }
        assert_value(
            &call(name, vec![cast(ScalarValue::Null, LogicalDType::UInt64)]),
            ScalarValue::Null,
            LogicalDType::UInt64,
        );
    }
    for (value, expected) in [(0, 0), (1, -1), (9_223_372_036_854_775_808, i64::MIN)] {
        assert_value(
            &expr(ExpressionKind::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(literal(ScalarValue::UInt64(value))),
            }),
            ScalarValue::Int64(expected),
            LogicalDType::Int64,
        );
    }
    assert_value(
        &expr(ExpressionKind::Unary {
            op: UnaryOp::Negate,
            expr: Box::new(cast(ScalarValue::Null, LogicalDType::UInt64)),
        }),
        ScalarValue::Null,
        LogicalDType::Int64,
    );
    let overflow = expr(ExpressionKind::Unary {
        op: UnaryOp::Negate,
        expr: Box::new(literal(ScalarValue::UInt64(u64::MAX))),
    });
    assert_eq!(
        evaluate_expression(&overflow, &ExpressionInputRow::new()).status,
        ExpressionEvaluationStatus::InvalidInput
    );
}

#[test]
fn reference_unsigned_arithmetic_checks_overflow_and_typed_nulls() {
    for (left, op, right, expected) in [
        (u64::MAX - 1, BinaryOp::Add, 1, Some(u64::MAX)),
        (u64::MAX, BinaryOp::Subtract, u64::MAX, Some(0)),
        (7, BinaryOp::Multiply, 9, Some(63)),
        (
            u64::MAX,
            BinaryOp::Divide,
            3,
            Some(6_148_914_691_236_517_205),
        ),
        (u64::MAX, BinaryOp::Add, 1, None),
        (0, BinaryOp::Subtract, 1, None),
        (9_223_372_036_854_775_808, BinaryOp::Multiply, 2, None),
        (1, BinaryOp::Divide, 0, None),
    ] {
        let expression = expr(ExpressionKind::Binary {
            left: Box::new(literal(ScalarValue::UInt64(left))),
            op,
            right: Box::new(literal(ScalarValue::UInt64(right))),
        });
        if let Some(expected) = expected {
            assert_value(
                &expression,
                ScalarValue::UInt64(expected),
                LogicalDType::UInt64,
            );
        } else {
            assert_eq!(
                evaluate_expression(&expression, &ExpressionInputRow::new()).status,
                ExpressionEvaluationStatus::InvalidInput,
                "{expression:?}"
            );
        }
    }
    for (left, right, dtype) in [
        (
            cast(ScalarValue::Null, LogicalDType::UInt64),
            literal(ScalarValue::UInt64(7)),
            LogicalDType::UInt64,
        ),
        (
            literal(ScalarValue::Null),
            literal(ScalarValue::UInt64(7)),
            LogicalDType::UInt64,
        ),
        (
            cast(ScalarValue::Null, LogicalDType::Float64),
            literal(ScalarValue::UInt64(7)),
            LogicalDType::Float64,
        ),
    ] {
        assert_value(
            &expr(ExpressionKind::Binary {
                left: Box::new(left),
                op: BinaryOp::Add,
                right: Box::new(right),
            }),
            ScalarValue::Null,
            dtype,
        );
    }
    for (left, right, expected) in [
        (ScalarValue::UInt64(7), ScalarValue::Float64(0.5), 7.5),
        (ScalarValue::Float64(0.5), ScalarValue::UInt64(7), 7.5),
    ] {
        assert_value(
            &expr(ExpressionKind::Binary {
                left: Box::new(literal(left)),
                op: BinaryOp::Add,
                right: Box::new(literal(right)),
            }),
            ScalarValue::Float64(expected),
            LogicalDType::Float64,
        );
    }
}

#[test]
fn reference_unsigned_comparison_and_string_offsets_match_admitted_native_types() {
    for (left, op, right, expected) in [
        (
            ScalarValue::UInt64(u64::MAX),
            ComparisonOp::Gt,
            ScalarValue::Int64(i64::MAX),
            true,
        ),
        (
            ScalarValue::Int64(-1),
            ComparisonOp::Lt,
            ScalarValue::UInt64(0),
            true,
        ),
        (
            ScalarValue::Int64(7),
            ComparisonOp::Eq,
            ScalarValue::UInt64(7),
            true,
        ),
    ] {
        assert_value(
            &expr(ExpressionKind::Compare {
                left: Box::new(literal(left)),
                op,
                right: Box::new(literal(right)),
            }),
            ScalarValue::Boolean(expected),
            LogicalDType::Boolean,
        );
    }
    for (name, args, expected) in [
        (
            "substr",
            vec![
                literal(ScalarValue::Utf8("aé雪z".into())),
                literal(ScalarValue::UInt64(2)),
                literal(ScalarValue::UInt64(2)),
            ],
            "é雪",
        ),
        (
            "left",
            vec![
                literal(ScalarValue::Utf8("aé雪z".into())),
                literal(ScalarValue::UInt64(2)),
            ],
            "aé",
        ),
        (
            "right",
            vec![
                literal(ScalarValue::Utf8("aé雪z".into())),
                literal(ScalarValue::UInt64(u64::MAX)),
            ],
            "aé雪z",
        ),
    ] {
        assert_value(
            &call(name, args),
            ScalarValue::Utf8(expected.into()),
            LogicalDType::Utf8,
        );
    }
}

#[test]
fn reference_try_boolean_cast_distinguishes_value_errors_from_unsupported_sources() {
    for text in ["bad", "TRUE", "", "1"] {
        let expression = Expression::try_cast(
            ExprId::new("try-bool").unwrap(),
            literal(ScalarValue::Utf8(text.into())),
            LogicalDType::Boolean,
        );
        assert_value(&expression, ScalarValue::Null, LogicalDType::Boolean);
    }
}

#[test]
fn reference_branch_type_resolution_checks_bounds_and_nested_metadata() {
    let mut args = vec![literal(ScalarValue::Null); 127];
    args.push(literal(ScalarValue::Int64(3)));
    assert_value(
        &call("coalesce", args),
        ScalarValue::Int64(3),
        LogicalDType::Int64,
    );
    assert_value(
        &call("coalesce", vec![literal(ScalarValue::Null); 128]),
        ScalarValue::Null,
        LogicalDType::Unknown,
    );
    let alias = |value| {
        expr(ExpressionKind::Alias {
            expr: Box::new(value),
            alias: "nested".into(),
        })
    };
    let mut nested = literal(ScalarValue::Int64(3));
    for _ in 0..23 {
        nested = alias(nested);
    }
    assert_value(
        &call("coalesce", vec![nested.clone()]),
        ScalarValue::Int64(3),
        LogicalDType::Int64,
    );
    let too_deep = call("coalesce", vec![alias(nested)]);
    let too_many_nodes = call(
        "coalesce",
        vec![call("coalesce", vec![literal(ScalarValue::Null); 32]); 128],
    );
    for expression in [too_deep, too_many_nodes] {
        let report = evaluate_expression(&expression, &ExpressionInputRow::new());
        assert_eq!(report.status, ExpressionEvaluationStatus::Unsupported);
    }
    for (expression, expected, dtype) in [
        (
            call(
                "coalesce",
                vec![
                    cast(ScalarValue::Null, decimal128_dtype(10, 0)),
                    call("round", vec![literal(decimal(125, 3, 2))]),
                ],
            ),
            decimal(1, 10, 0),
            decimal128_dtype(10, 0),
        ),
        (
            call(
                "case_when",
                vec![
                    literal(ScalarValue::Boolean(true)),
                    literal(ScalarValue::Int64(7)),
                    expr(ExpressionKind::Binary {
                        left: Box::new(literal(ScalarValue::Int64(1))),
                        op: BinaryOp::Divide,
                        right: Box::new(literal(ScalarValue::Int64(0))),
                    }),
                ],
            ),
            ScalarValue::Int64(7),
            LogicalDType::Int64,
        ),
    ] {
        assert_value(&expression, expected, dtype);
    }
    for expression in [
        call(
            "case_when",
            vec![
                cast(ScalarValue::Null, LogicalDType::Int64),
                literal(ScalarValue::Int64(1)),
                literal(ScalarValue::Int64(2)),
            ],
        ),
        expr(ExpressionKind::Binary {
            left: Box::new(cast(ScalarValue::Null, LogicalDType::Float64)),
            op: BinaryOp::Add,
            right: Box::new(literal(decimal(1, 2, 1))),
        }),
        Expression::try_cast(
            ExprId::new("unsupported-null-cast").unwrap(),
            cast(ScalarValue::Null, LogicalDType::Boolean),
            LogicalDType::Int64,
        ),
    ] {
        assert_eq!(
            evaluate_expression(&expression, &ExpressionInputRow::new()).status,
            ExpressionEvaluationStatus::Unsupported
        );
    }
}
