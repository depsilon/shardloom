//! Native and decoded-reference results are both checked against literal oracles.

use super::*;
use shardloom_core::{
    ExpressionEvaluationStatus as Status, ExpressionInputRow, evaluate_expression,
};

fn typed_null(dtype: LogicalDType) -> Expression {
    cast(literal(ScalarValue::Null), dtype, false)
}

fn decimal(value: i128, precision: u8, scale: u8) -> ScalarValue {
    ScalarValue::Decimal128 {
        value,
        precision,
        scale,
    }
}

fn negate(value: Expression) -> Expression {
    expr(ExpressionKind::Unary {
        op: UnaryOp::Negate,
        expr: Box::new(value),
    })
}

fn assert_case(
    fixture: &Fixture,
    expression: &Expression,
    expected: ScalarValue,
    dtype: LogicalDType,
) {
    let reference = evaluate_expression(expression, &ExpressionInputRow::new());
    assert_eq!(
        reference.status,
        Status::Evaluated,
        "{expression:?}: {reference:?}"
    );
    assert_eq!(reference.value, Some(expected.clone()), "{expression:?}");
    assert_eq!(
        reference.output_dtype,
        Some(dtype.clone()),
        "{expression:?}"
    );
    assert!(!reference.fallback_attempted && !reference.external_engine_invoked);
    let expected_json = match expected {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(value) => json!(value),
        ScalarValue::Int64(value) => json!(value),
        ScalarValue::UInt64(value) => json!(value),
        ScalarValue::Float64(value) => json!(value),
        ScalarValue::Utf8(value) => json!(value),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => json!(format!("decimal128({precision},{scale}):{value}")),
        _ => unreachable!("bounded conformance cases"),
    };
    let plan = project(fixture.scan(), vec![("value", expression.clone())]);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![json!({"value": expected_json}); 4],
        "{expression:?}"
    );
    let output_dtype = prepared.output_dtype().unwrap();
    let field = output_dtype
        .as_struct_fields_opt()
        .unwrap()
        .field("value")
        .unwrap();
    let native_dtype = match dtype {
        LogicalDType::Unknown => DType::Null,
        LogicalDType::Boolean => DType::Bool(Nullability::NonNullable),
        LogicalDType::Int64 => DType::Primitive(PType::I64, Nullability::NonNullable),
        LogicalDType::UInt64 => DType::Primitive(PType::U64, Nullability::NonNullable),
        LogicalDType::Float64 => DType::Primitive(PType::F64, Nullability::NonNullable),
        LogicalDType::Utf8 => DType::Utf8(Nullability::NonNullable),
        decimal => {
            let (precision, scale) =
                shardloom_core::expression::decimal128_dtype_parts(&decimal).unwrap();
            DType::Decimal(
                DecimalDType::new(precision, i8::try_from(scale).unwrap()),
                Nullability::NonNullable,
            )
        }
    };
    assert_eq!(field.as_nonnullable(), native_dtype, "{expression:?}");
    let empty_plan = project(empty(fixture.scan()), vec![("value", expression.clone())]);
    let empty_prepared = prepare_relational(&empty_plan, policy()).unwrap();
    assert_eq!(
        empty_prepared.output_dtype(),
        prepared.output_dtype(),
        "{expression:?}"
    );
    assert_eq!(
        json_rows(
            &empty_prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        [] as [Value; 0]
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the literal branch value/type oracle table together.
fn native_typed_expressions_reference_lazy_branch_values_and_declared_types() {
    let fixture = decimals();
    let bad = |dtype| cast(literal(ScalarValue::Utf8("bad".into())), dtype, false);
    for (expression, expected, dtype) in [
        (
            function(
                "coalesce",
                vec![literal(ScalarValue::Int64(1)), bad(LogicalDType::Int64)],
            ),
            ScalarValue::Int64(1),
            LogicalDType::Int64,
        ),
        (
            function(
                "coalesce",
                vec![
                    literal(ScalarValue::Null),
                    literal(ScalarValue::Null),
                    literal(ScalarValue::Int64(3)),
                    bad(LogicalDType::Int64),
                ],
            ),
            ScalarValue::Int64(3),
            LogicalDType::Int64,
        ),
        (
            function("coalesce", vec![typed_null(LogicalDType::UInt64)]),
            ScalarValue::Null,
            LogicalDType::UInt64,
        ),
        (
            function(
                "coalesce",
                vec![
                    typed_null(decimal128_dtype(10, 2)),
                    literal(decimal(12, 3, 1)),
                ],
            ),
            decimal(120, 10, 2),
            decimal128_dtype(10, 2),
        ),
        (
            function(
                "coalesce",
                vec![literal(decimal(25, 2, 1)), bad(decimal128_dtype(11, 3))],
            ),
            decimal(2500, 11, 3),
            decimal128_dtype(11, 3),
        ),
        (
            function(
                "coalesce",
                vec![
                    typed_null(decimal128_dtype(5, 4)),
                    literal(decimal(12, 3, 1)),
                    literal(decimal(0, 8, 3)),
                ],
            ),
            decimal(12000, 9, 4),
            decimal128_dtype(9, 4),
        ),
        (
            function(
                "case_when",
                vec![
                    literal(ScalarValue::Boolean(true)),
                    literal(decimal(25, 2, 1)),
                    bad(decimal128_dtype(10, 2)),
                ],
            ),
            decimal(250, 10, 2),
            decimal128_dtype(10, 2),
        ),
        (
            function(
                "case_when",
                vec![
                    literal(ScalarValue::Boolean(true)),
                    typed_null(decimal128_dtype(10, 2)),
                    literal(decimal(12, 3, 1)),
                ],
            ),
            ScalarValue::Null,
            decimal128_dtype(10, 2),
        ),
        (
            function(
                "case_when",
                vec![
                    literal(ScalarValue::Null),
                    typed_null(decimal128_dtype(10, 2)),
                    typed_null(decimal128_dtype(9, 4)),
                ],
            ),
            ScalarValue::Null,
            decimal128_dtype(12, 4),
        ),
        (
            function(
                "coalesce",
                vec![
                    typed_null(decimal128_dtype(10, 0)),
                    function("round", vec![literal(decimal(125, 3, 2))]),
                ],
            ),
            decimal(1, 10, 0),
            decimal128_dtype(10, 0),
        ),
    ] {
        assert_case(&fixture, &expression, expected, dtype);
    }
}

#[test]
fn native_typed_expressions_reference_unsigned_numeric_domains() {
    use LogicalDType::{Float64 as F64, Int64 as I64, UInt64 as U64};
    use ScalarValue::{Float64 as F, Int64 as I, Null, UInt64 as U};
    let fixture = decimals();
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
            assert_case(
                &fixture,
                &function(name, vec![literal(U(value))]),
                U(value),
                U64,
            );
        }
        assert_case(&fixture, &function(name, vec![typed_null(U64)]), Null, U64);
    }
    for (value, expected) in [(0, 0), (1, -1), (9_223_372_036_854_775_808, i64::MIN)] {
        assert_case(&fixture, &negate(literal(U(value))), I(expected), I64);
    }
    assert_case(&fixture, &negate(typed_null(U64)), Null, I64);
    for (left, op, right, expected) in [
        (u64::MAX - 1, BinaryOp::Add, 1, u64::MAX),
        (u64::MAX, BinaryOp::Subtract, u64::MAX, 0),
        (7, BinaryOp::Multiply, 9, 63),
        (u64::MAX, BinaryOp::Divide, 3, 6_148_914_691_236_517_205),
    ] {
        assert_case(
            &fixture,
            &binary(literal(U(left)), op, literal(U(right))),
            U(expected),
            U64,
        );
    }
    for (left, right, expected, dtype) in [
        (typed_null(U64), literal(U(7)), Null, U64),
        (literal(Null), literal(U(7)), Null, U64),
        (typed_null(F64), literal(U(7)), Null, F64),
        (literal(U(7)), literal(F(0.5)), F(7.5), F64),
        (literal(F(0.5)), literal(U(7)), F(7.5), F64),
    ] {
        assert_case(
            &fixture,
            &binary(left, BinaryOp::Add, right),
            expected,
            dtype,
        );
    }
}

#[test]
fn native_typed_expressions_reference_unsigned_offsets_comparisons_and_try_bool() {
    let fixture = decimals();
    for (left, op, right) in [
        (
            ScalarValue::UInt64(u64::MAX),
            ComparisonOp::Gt,
            ScalarValue::Int64(i64::MAX),
        ),
        (
            ScalarValue::Int64(-1),
            ComparisonOp::Lt,
            ScalarValue::UInt64(0),
        ),
        (
            ScalarValue::Int64(7),
            ComparisonOp::Eq,
            ScalarValue::UInt64(7),
        ),
    ] {
        assert_case(
            &fixture,
            &expr(ExpressionKind::Compare {
                left: Box::new(literal(left)),
                op,
                right: Box::new(literal(right)),
            }),
            ScalarValue::Boolean(true),
            LogicalDType::Boolean,
        );
    }
    for (name, offsets, expected) in [
        ("substr", vec![2, 2], "é雪"),
        ("left", vec![2], "aé"),
        ("right", vec![u64::MAX], "aé雪z"),
    ] {
        let mut args = vec![literal(ScalarValue::Utf8("aé雪z".into()))];
        args.extend(
            offsets
                .into_iter()
                .map(|value| literal(ScalarValue::UInt64(value))),
        );
        assert_case(
            &fixture,
            &function(name, args),
            ScalarValue::Utf8(expected.into()),
            LogicalDType::Utf8,
        );
    }
    for text in ["bad", "TRUE", "", "1"] {
        assert_case(
            &fixture,
            &cast(
                literal(ScalarValue::Utf8(text.into())),
                LogicalDType::Boolean,
                true,
            ),
            ScalarValue::Null,
            LogicalDType::Boolean,
        );
    }
}

#[test]
fn native_typed_expressions_reference_errors_and_empty_type_denials() {
    let fixture = decimals();
    for expression in [
        negate(literal(ScalarValue::UInt64(u64::MAX))),
        binary(
            literal(ScalarValue::UInt64(u64::MAX)),
            BinaryOp::Add,
            literal(ScalarValue::UInt64(1)),
        ),
        binary(
            literal(ScalarValue::UInt64(0)),
            BinaryOp::Subtract,
            literal(ScalarValue::UInt64(1)),
        ),
        binary(
            literal(ScalarValue::UInt64(9_223_372_036_854_775_808)),
            BinaryOp::Multiply,
            literal(ScalarValue::UInt64(2)),
        ),
        binary(
            literal(ScalarValue::UInt64(7)),
            BinaryOp::Divide,
            literal(ScalarValue::UInt64(0)),
        ),
        binary(
            literal(ScalarValue::UInt64(9_007_199_254_740_993)),
            BinaryOp::Add,
            literal(ScalarValue::Float64(0.5)),
        ),
    ] {
        assert!(
            evaluate_expression(&expression, &ExpressionInputRow::new()).has_errors(),
            "{expression:?}"
        );
        let plan = project(fixture.scan(), vec![("value", expression.clone())]);
        assert!(
            prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .is_err(),
            "{expression:?}"
        );
    }
    let invalid = vec![
        function(
            "coalesce",
            vec![literal(decimal(0, 38, 0)), literal(decimal(0, 38, 38))],
        ),
        function(
            "coalesce",
            vec![
                literal(ScalarValue::Int64(1)),
                literal(ScalarValue::Utf8("wrong".into())),
            ],
        ),
        function(
            "case_when",
            vec![
                typed_null(LogicalDType::Int64),
                literal(ScalarValue::Int64(1)),
                literal(ScalarValue::Int64(2)),
            ],
        ),
        binary(
            typed_null(LogicalDType::UInt64),
            BinaryOp::Add,
            literal(ScalarValue::Int64(1)),
        ),
        binary(
            typed_null(LogicalDType::Float64),
            BinaryOp::Add,
            literal(decimal(1, 2, 1)),
        ),
        cast(typed_null(LogicalDType::Boolean), LogicalDType::Int64, true),
        function("coalesce", vec![]),
        function("coalesce", vec![literal(ScalarValue::Null); 129]),
    ];
    for expression in invalid {
        assert!(
            evaluate_expression(&expression, &ExpressionInputRow::new()).has_errors(),
            "{expression:?}"
        );
        for input in [fixture.scan(), empty(fixture.scan())] {
            assert!(
                prepare_relational(
                    &project(input, vec![("value", expression.clone())]),
                    policy()
                )
                .is_err(),
                "{expression:?}"
            );
        }
    }
}
