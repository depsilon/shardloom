use super::*;
use crate::relational_query::VortexRelationalProject;
use shardloom_core::{
    BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, LogicalDType, ScalarValue, UnaryOp,
    decimal128_dtype,
};
use vortex::array::arrays::{ConstantArray, DictArray};

fn expr(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("typed-expression").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expr(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn literal(value: ScalarValue) -> Expression {
    expr(ExpressionKind::Literal(value))
}
fn function(name: &str, args: Vec<Expression>) -> Expression {
    expr(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}
fn cast(value: Expression, target_dtype: LogicalDType, tolerant: bool) -> Expression {
    expr(if tolerant {
        ExpressionKind::TryCast {
            expr: Box::new(value),
            target_dtype,
        }
    } else {
        ExpressionKind::Cast {
            expr: Box::new(value),
            target_dtype,
        }
    })
}
fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expr(ExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn project(
    input: VortexRelationalPlan,
    expressions: Vec<(&str, Expression)>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: expressions
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect(),
    }))
}
fn empty(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset: 0,
        count: 0,
    }))
}
fn decimals() -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["amount", "n", "text"]),
            vec![
                DecimalArray::from_option_iter(
                    [Some(1234i128), Some(-150), None, Some(9999)],
                    DecimalDType::new(10, 2),
                )
                .into_array(),
                PrimitiveArray::from_iter([2i32, 3, 4, 1]).into_array(),
                VarBinArray::from(vec![Some("12.30"), Some("bad"), None, Some("-0.50")])
                    .into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

#[test]
fn native_typed_expressions_promote_decimal_branches_losslessly_before_reading_rows() {
    let fixture = decimals();
    let decimal = |value, precision, scale| {
        literal(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        })
    };
    let expressions = vec![
        (
            "same",
            function("coalesce", vec![col("amount"), decimal(120, 3, 2)]),
        ),
        (
            "finer",
            function("coalesce", vec![col("amount"), decimal(1, 3, 3)]),
        ),
        (
            "wider",
            function("coalesce", vec![col("amount"), decimal(123_456_789, 9, 0)]),
        ),
        (
            "selected",
            function(
                "case_when",
                vec![
                    expr(ExpressionKind::Compare {
                        left: Box::new(col("n")),
                        op: ComparisonOp::Gt,
                        right: Box::new(literal(ScalarValue::Int64(2))),
                    }),
                    col("amount"),
                    decimal(125, 3, 3),
                ],
            ),
        ),
        (
            "lazy",
            function(
                "coalesce",
                vec![
                    decimal(25, 2, 1),
                    cast(
                        literal(ScalarValue::Utf8("bad".into())),
                        decimal128_dtype(11, 3),
                        false,
                    ),
                ],
            ),
        ),
        (
            "missing",
            function(
                "coalesce",
                vec![
                    cast(literal(ScalarValue::Null), decimal128_dtype(10, 2), false),
                    cast(literal(ScalarValue::Null), decimal128_dtype(9, 4), false),
                ],
            ),
        ),
    ];
    let plan = project(fixture.scan(), expressions.clone());
    assert_eq!(
        collect(&plan),
        vec![
            json!({"same":"decimal128(10,2):1234","finer":"decimal128(11,3):12340","wider":"decimal128(11,2):1234","selected":"decimal128(11,3):125","lazy":"decimal128(11,3):2500","missing":null}),
            json!({"same":"decimal128(10,2):-150","finer":"decimal128(11,3):-1500","wider":"decimal128(11,2):-150","selected":"decimal128(11,3):-1500","lazy":"decimal128(11,3):2500","missing":null}),
            json!({"same":"decimal128(10,2):120","finer":"decimal128(11,3):1","wider":"decimal128(11,2):12345678900","selected":null,"lazy":"decimal128(11,3):2500","missing":null}),
            json!({"same":"decimal128(10,2):9999","finer":"decimal128(11,3):99990","wider":"decimal128(11,2):9999","selected":"decimal128(11,3):125","lazy":"decimal128(11,3):2500","missing":null}),
        ]
    );
    let empty_plan = project(empty(fixture.scan()), expressions);
    assert_eq!(collect(&empty_plan), [] as [Value; 0]);
    assert_eq!(
        prepare_relational(&plan, policy()).unwrap().output_dtype(),
        prepare_relational(&empty_plan, policy())
            .unwrap()
            .output_dtype()
    );
    for expression in [
        function("coalesce", vec![decimal(0, 38, 0), decimal(0, 38, 38)]),
        function(
            "case_when",
            vec![
                literal(ScalarValue::Boolean(false)),
                decimal(0, 38, 0),
                decimal(0, 38, 38),
            ],
        ),
        function(
            "coalesce",
            vec![col("amount"), literal(ScalarValue::Int64(1))],
        ),
    ] {
        for input in [fixture.scan(), empty(fixture.scan())] {
            assert!(
                prepare_relational(
                    &project(input, vec![("invalid", expression.clone())]),
                    policy()
                )
                .is_err()
            );
        }
    }
}

#[test]
fn native_typed_expressions_numeric_casts_match_checked_reference_values() {
    use LogicalDType::{Float64 as F64, Int64 as I64, UInt64 as U64};
    use ScalarValue::{Float64 as F, Int64 as I, UInt64 as U, Utf8 as S};
    use shardloom_core::{
        ExpressionEvaluationStatus as Status, ExpressionInputRow, evaluate_expression,
    };
    let fixture = decimals();
    for (source, target, expected) in [
        (S("5".into()), U64, Some(U(5))),
        (S(u64::MAX.to_string()), U64, Some(U(u64::MAX))),
        (S("18446744073709551616".into()), U64, None),
        (S("-1".into()), U64, None),
        (S("bad".into()), U64, None),
        (I(5), U64, Some(U(5))),
        (I(-1), U64, None),
        (U(5), I64, Some(I(5))),
        (U(u64::MAX), I64, None),
        (U(u64::MAX), F64, Some(F(18_446_744_073_709_551_616.0))),
        (F(5.0), U64, Some(U(5))),
        (F(-0.0), U64, Some(U(0))),
        (
            F(18_446_744_073_709_549_568.0),
            U64,
            Some(U(18_446_744_073_709_549_568)),
        ),
        (F(18_446_744_073_709_551_616.0), U64, None),
        (F(-1.0), U64, None),
        (F(1.5), U64, None),
        (F(-9_223_372_036_854_775_808.0), I64, Some(I(i64::MIN))),
        (
            F(9_223_372_036_854_774_784.0),
            I64,
            Some(I(9_223_372_036_854_774_784)),
        ),
        (F(9_223_372_036_854_775_808.0), I64, None),
        (F(-9_223_372_036_854_777_856.0), I64, None),
        (F(1.5), I64, None),
        (ScalarValue::Null, U64, Some(ScalarValue::Null)),
    ] {
        for tolerant in [false, true] {
            let expression = cast(literal(source.clone()), target.clone(), tolerant);
            let reference = evaluate_expression(&expression, &ExpressionInputRow::new());
            let native = prepare_relational(
                &project(fixture.scan(), vec![("value", expression.clone())]),
                policy(),
            )
            .unwrap()
            .collect_jsonl(&CancellationToken::default());
            if expected.is_none() && !tolerant {
                assert_eq!(reference.status, Status::InvalidInput, "{expression:?}");
                assert!(native.is_err(), "{expression:?}");
            } else {
                let expected = expected.clone().unwrap_or(ScalarValue::Null);
                assert_eq!(reference.status, Status::Evaluated, "{expression:?}");
                assert_eq!(reference.value.as_ref(), Some(&expected), "{expression:?}");
                let json = match expected {
                    U(value) => json!(value),
                    I(value) => json!(value),
                    F(value) => json!(value),
                    ScalarValue::Null => Value::Null,
                    _ => unreachable!(),
                };
                let rows = json_rows(&native.unwrap());
                assert_eq!(rows, vec![json!({"value":json}); 4], "{expression:?}");
            }
            assert!(!reference.fallback_attempted && !reference.external_engine_invoked);
        }
    }
}

#[test]
fn native_typed_expressions_nonfinite_sources_keep_admission_failure() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5, 0.0])
                .into_array(),
        ),
        5,
    );
    for target in [
        LogicalDType::UInt64,
        LogicalDType::Int64,
        LogicalDType::Float64,
    ] {
        for tolerant in [false, true] {
            let plan = project(
                fixture.scan(),
                vec![("converted", cast(col("value"), target.clone(), tolerant))],
            );
            let error = prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .err()
                .unwrap();
            assert!(
                error
                    .to_string()
                    .contains("nonfinite scalar values are not admitted")
            );
        }
        // Nonfinite literals retain their earlier binding denial, even for TRY_CAST.
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let expression = cast(literal(ScalarValue::Float64(value)), target.clone(), true);
            assert!(
                prepare_relational(
                    &project(empty(fixture.scan()), vec![("invalid", expression)]),
                    policy()
                )
                .is_err()
            );
        }
    }
}

#[test]
fn native_typed_expressions_decimal_arithmetic_and_rounding() {
    let fixture = decimals();
    let expressions = vec![
        ("add", binary(col("amount"), BinaryOp::Add, col("n"))),
        ("sub", binary(col("amount"), BinaryOp::Subtract, col("n"))),
        ("mul", binary(col("amount"), BinaryOp::Multiply, col("n"))),
        ("div", binary(col("amount"), BinaryOp::Divide, col("n"))),
        (
            "neg",
            expr(ExpressionKind::Unary {
                op: UnaryOp::Negate,
                expr: Box::new(col("amount")),
            }),
        ),
        ("abs", function("abs", vec![col("amount")])),
        ("floor", function("floor", vec![col("amount")])),
        ("ceil", function("ceil", vec![col("amount")])),
        ("round", function("round", vec![col("amount")])),
    ];
    let plan = project(fixture.scan(), expressions.clone());
    let rows = collect(&plan);
    assert_eq!(
        rows,
        vec![
            json!({"add":"decimal128(13,2):1434","sub":"decimal128(13,2):1034","mul":"decimal128(20,2):2468","div":"decimal128(38,6):6170000","neg":"decimal128(10,2):-1234","abs":"decimal128(10,2):1234","floor":"decimal128(9,0):12","ceil":"decimal128(9,0):13","round":"decimal128(9,0):12"}),
            json!({"add":"decimal128(13,2):150","sub":"decimal128(13,2):-450","mul":"decimal128(20,2):-450","div":"decimal128(38,6):-500000","neg":"decimal128(10,2):150","abs":"decimal128(10,2):150","floor":"decimal128(9,0):-2","ceil":"decimal128(9,0):-1","round":"decimal128(9,0):-2"}),
            json!({"add":null,"sub":null,"mul":null,"div":null,"neg":null,"abs":null,"floor":null,"ceil":null,"round":null}),
            json!({"add":"decimal128(13,2):10099","sub":"decimal128(13,2):9899","mul":"decimal128(20,2):9999","div":"decimal128(38,6):99990000","neg":"decimal128(10,2):-9999","abs":"decimal128(10,2):9999","floor":"decimal128(9,0):99","ceil":"decimal128(9,0):100","round":"decimal128(9,0):100"}),
        ]
    );
    let empty = project(empty(fixture.scan()), expressions);
    assert_eq!(collect(&empty), [] as [Value; 0]);
    assert_eq!(
        prepare_relational(&empty, policy()).unwrap().output_dtype(),
        prepare_relational(&plan, policy()).unwrap().output_dtype()
    );
}

#[test]
fn native_typed_expressions_cast_values_and_tolerant_failures() {
    let fixture = decimals();
    let plan = project(
        fixture.scan(),
        vec![
            ("parsed", cast(col("text"), decimal128_dtype(6, 2), true)),
            ("integral", cast(col("amount"), LogicalDType::Int64, true)),
            ("string", cast(col("amount"), LogicalDType::Utf8, false)),
            (
                "rescaled",
                cast(
                    literal(ScalarValue::Decimal128 {
                        value: -12300,
                        precision: 5,
                        scale: 3,
                    }),
                    decimal128_dtype(4, 2),
                    false,
                ),
            ),
            ("float", cast(col("amount"), LogicalDType::Float64, false)),
            ("binary", cast(col("amount"), LogicalDType::Binary, false)),
        ],
    );
    let rows = collect(&plan);
    for (row, parsed, string, float, bytes) in [
        (
            0,
            json!("decimal128(6,2):1230"),
            json!("12.34"),
            json!(12.34),
            json!("31322e3334"),
        ),
        (
            1,
            Value::Null,
            json!("-1.50"),
            json!(-1.5),
            json!("2d312e3530"),
        ),
        (2, Value::Null, Value::Null, Value::Null, Value::Null),
        (
            3,
            json!("decimal128(6,2):-50"),
            json!("99.99"),
            json!(99.99),
            json!("39392e3939"),
        ),
    ] {
        assert_eq!(
            rows[row],
            json!({"parsed":parsed,"integral":null,"string":string,"rescaled":"decimal128(4,2):-1230","float":float,"binary":bytes})
        );
    }
    for value in ["12.301", "1e-3", "1e1000000000", "not-a-decimal"] {
        let plan = project(
            fixture.scan(),
            vec![(
                "invalid",
                cast(
                    literal(ScalarValue::Utf8(value.into())),
                    decimal128_dtype(6, 2),
                    false,
                ),
            )],
        );
        assert!(
            prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
    }
}

#[test]
fn native_typed_expressions_binary_and_calendar_columns() {
    let fixture = fixture();
    let plan = project(
        fixture.scan(),
        vec![
            ("bytes", function("octet_length", vec![col("payload")])),
            ("text", cast(col("payload"), LogicalDType::Utf8, true)),
            ("year", function("year", vec![col("day")])),
            (
                "next",
                function(
                    "date_add_days",
                    vec![col("day"), literal(ScalarValue::Int64(1))],
                ),
            ),
            ("date", cast(col("instant"), LogicalDType::Date32, false)),
            (
                "midnight",
                cast(col("day"), LogicalDType::TimestampMicros, false),
            ),
            ("second", function("timestamp_second", vec![col("instant")])),
            (
                "hex",
                function("unhex", vec![literal(ScalarValue::Utf8("00ffC3A9".into()))]),
            ),
            (
                "base64",
                function(
                    "from_base64",
                    vec![literal(ScalarValue::Utf8("AP/DqQ==".into()))],
                ),
            ),
        ],
    );
    assert_eq!(
        collect(&plan),
        vec![
            json!({"bytes":3,"text":null,"year":1969,"next":0,"date":-1,"midnight":-86_400_000_000i64,"second":59,"hex":"00ffc3a9","base64":"00ffc3a9"}),
            json!({"bytes":0,"text":"","year":2024,"next":20001,"date":19675,"midnight":1_728_000_000_000_000i64,"second":20,"hex":"00ffc3a9","base64":"00ffc3a9"}),
            json!({"bytes":null,"text":null,"year":null,"next":null,"date":null,"midnight":null,"second":null,"hex":"00ffc3a9","base64":"00ffc3a9"}),
            json!({"bytes":2,"text":"é","year":1970,"next":1,"date":0,"midnight":0,"second":0,"hex":"00ffc3a9","base64":"00ffc3a9"}),
        ]
    );
}

#[test]
fn native_typed_expressions_literals_lazy_selection_and_empty_denials() {
    let fixture = decimals();
    let decimal = |value, precision, scale| {
        literal(ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        })
    };
    let invalid = cast(
        literal(ScalarValue::Utf8("bad".into())),
        decimal128_dtype(6, 2),
        false,
    );
    let selected = function(
        "case_when",
        vec![
            literal(ScalarValue::Boolean(false)),
            invalid,
            decimal(123, 6, 2),
        ],
    );
    let plan = project(
        fixture.scan(),
        vec![
            ("decimal", selected),
            ("binary", literal(ScalarValue::Binary(vec![0, 255]))),
            ("date", literal(ScalarValue::Date32(i32::MAX))),
            ("timestamp", literal(ScalarValue::TimestampMicros(i64::MIN))),
            (
                "safe",
                cast(
                    literal(ScalarValue::Date32(i32::MAX)),
                    LogicalDType::TimestampMicros,
                    true,
                ),
            ),
        ],
    );
    assert_eq!(
        collect(&plan),
        vec![
            json!({"decimal":"decimal128(6,2):123","binary":"00ff","date":i32::MAX,"timestamp":i64::MIN,"safe":null});
            4
        ]
    );
    for value in [
        decimal(i128::MIN, 38, 0),
        decimal(100, 2, 0),
        decimal(0, 0, 0),
        decimal(0, 39, 0),
        binary(decimal(0, 38, 2), BinaryOp::Add, decimal(0, 38, 2)),
        binary(decimal(0, 10, 2), BinaryOp::Multiply, decimal(0, 10, 3)),
        binary(
            col("amount"),
            BinaryOp::Add,
            literal(ScalarValue::Float64(1.0)),
        ),
        cast(literal(ScalarValue::Date32(0)), LogicalDType::Int64, true),
        cast(col("amount"), LogicalDType::Boolean, true),
        function("abs", vec![literal(ScalarValue::Binary(vec![]))]),
        expr(ExpressionKind::Compare {
            left: Box::new(col("amount")),
            op: ComparisonOp::Eq,
            right: Box::new(col("n")),
        }),
    ] {
        assert!(
            prepare_relational(
                &project(empty(fixture.scan()), vec![("invalid", value)]),
                policy()
            )
            .is_err()
        );
    }
}

#[test]
fn native_typed_expressions_encoded_arrays_and_final_output_credit_ownership() {
    let codes = PrimitiveArray::from_iter([1u8, 0, 2, 1]).into_array();
    let decimal =
        DecimalArray::from_option_iter([Some(-150i128), Some(1234), None], DecimalDType::new(6, 2))
            .into_array();
    let values = DictArray::try_new(codes, decimal).unwrap().into_array();
    let input = StructArray::new(
        FieldNames::from(["amount", "increment", "missing"]),
        vec![
            values,
            ConstantArray::new(2i32, 4).into_array(),
            DecimalArray::from_option_iter([None::<i128>; 4], DecimalDType::new(6, 2)).into_array(),
        ],
        4,
        Validity::NonNullable,
    )
    .into_array();
    let fixture = Fixture::new(input.clone(), 4);
    let prepared = prepare_relational(
        &project(
            fixture.scan(),
            vec![
                (
                    "added",
                    binary(col("amount"), BinaryOp::Add, col("increment")),
                ),
                ("rounded", function("round", vec![col("amount")])),
                ("missing", function("floor", vec![col("missing")])),
                (
                    "promoted",
                    function(
                        "coalesce",
                        vec![
                            col("amount"),
                            literal(ScalarValue::Decimal128 {
                                value: 1,
                                precision: 3,
                                scale: 3,
                            }),
                        ],
                    ),
                ),
            ],
        ),
        policy(),
    )
    .unwrap();
    let memory = prepared.session.memory().clone();
    let PreparedRoot::Bound(root) = &prepared.root else {
        panic!("static test plan")
    };
    let NodeKind::Project { expressions, .. } = &root.kind else {
        panic!("project test plan")
    };
    let mut output = Vec::new();
    for bound in expressions {
        output.push(
            prepared
                .session
                .with_native_execution_context(&CancellationToken::default(), |context| {
                    bound.evaluate(&input, context)
                })
                .unwrap(),
        );
    }
    drop((input, prepared));
    assert!(memory.snapshot().reserved_bytes > 0);
    let native = VortexSession::default();
    let mut context = native.create_execution_ctx();
    for (array, expected) in output.iter().zip([
        vec![
            json!("decimal128(13,2):1434"),
            json!("decimal128(13,2):50"),
            Value::Null,
            json!("decimal128(13,2):1434"),
        ],
        vec![
            json!("decimal128(5,0):12"),
            json!("decimal128(5,0):-2"),
            Value::Null,
            json!("decimal128(5,0):12"),
        ],
        vec![Value::Null; 4],
        vec![
            json!("decimal128(7,3):12340"),
            json!("decimal128(7,3):-1500"),
            json!("decimal128(7,3):1"),
            json!("decimal128(7,3):12340"),
        ],
    ]) {
        for (row, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                result_batch::scalar_value(array, row, &mut context)
                    .unwrap()
                    .into_json()
                    .unwrap(),
                expected
            );
        }
    }
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_typed_expressions_try_cast_memory_denial_cancellation_and_retry() {
    let text = "1".repeat(500_000);
    let fixture = Fixture::new(
        single("text", VarBinArray::from(vec![text.as_str()]).into_array()),
        1,
    );
    let plan = project(
        fixture.scan(),
        vec![("parsed", cast(col("text"), decimal128_dtype(6, 2), true))],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let block = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - (2 << 20))
        .unwrap();
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("budget") || error.to_string().contains("reservation"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(block);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    let token = CancellationToken::default();
    token.cancel();
    assert!(prepared.collect_jsonl(&token).is_err());
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![json!({"parsed":null})]
    );
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    assert_eq!(prepared.snapshot().completed_executions, 1);
}

#[test]
fn native_typed_expressions_lazy_coalesce_and_strict_errors_release_state() {
    let fixture = decimals();
    let safe = cast(
        literal(ScalarValue::Utf8("1.20".into())),
        decimal128_dtype(4, 2),
        false,
    );
    let invalid = cast(
        literal(ScalarValue::Utf8("invalid".into())),
        decimal128_dtype(4, 2),
        false,
    );
    assert_eq!(
        collect(&project(
            fixture.scan(),
            vec![
                (
                    "selected",
                    function("coalesce", vec![safe, invalid.clone()])
                ),
                (
                    "nullable",
                    function("nullif", vec![col("amount"), literal(ScalarValue::Null)])
                ),
            ]
        ))[0],
        json!({"selected":"decimal128(4,2):120","nullable":"decimal128(10,2):1234"})
    );
    let prepared = prepare_relational(
        &project(fixture.scan(), vec![("invalid", invalid)]),
        policy(),
    )
    .unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    for _ in 0..2 {
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn native_typed_expressions_calendar_extremes_and_checked_offsets() {
    let fixture = decimals();
    let date = |value| literal(ScalarValue::Date32(value));
    let timestamp = |value| literal(ScalarValue::TimestampMicros(value));
    let plan = project(
        fixture.scan(),
        vec![
            (
                "date",
                function(
                    "date_add_days",
                    vec![
                        date(i32::MIN),
                        literal(ScalarValue::Int64(i64::from(u32::MAX))),
                    ],
                ),
            ),
            (
                "days",
                function("date_diff_days", vec![date(i32::MAX), date(i32::MIN)]),
            ),
            (
                "seconds",
                function(
                    "timestamp_diff_seconds",
                    vec![timestamp(i64::MAX), timestamp(i64::MIN)],
                ),
            ),
            (
                "shifted",
                function(
                    "timestamp_add_seconds",
                    vec![
                        timestamp(i64::MIN),
                        literal(ScalarValue::Int64(18_446_744_073_709)),
                    ],
                ),
            ),
            ("negative", cast(timestamp(-1), LogicalDType::Date32, false)),
            ("text", cast(date(i32::MIN), LogicalDType::Utf8, false)),
        ],
    );
    assert_eq!(
        collect(&plan)[0],
        json!({"date":i32::MAX,"days":u32::MAX,"seconds":18_446_744_073_709i64,"shifted":9_223_372_036_854_224_192i64,"negative":-1,"text":"-5877641-06-23"})
    );
    for expression in [
        function(
            "date_add_days",
            vec![date(i32::MAX), literal(ScalarValue::Int64(1))],
        ),
        function(
            "timestamp_sub_seconds",
            vec![timestamp(i64::MIN), literal(ScalarValue::Int64(1))],
        ),
        cast(date(i32::MIN), LogicalDType::TimestampMicros, false),
        function("unhex", vec![literal(ScalarValue::Utf8("0x".into()))]),
        function(
            "from_base64",
            vec![literal(ScalarValue::Utf8("AB==".into()))],
        ),
    ] {
        assert!(
            prepare_relational(
                &project(fixture.scan(), vec![("invalid", expression)]),
                policy()
            )
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .is_err()
        );
    }
}

#[cfg(feature = "universal-format-io")]
fn computed_expression_oracle(count: u32) -> (Vec<serde_json::Value>, String) {
    use std::fmt::Write as _;
    let mut expected = Vec::new();
    let mut csv = String::from("id,payload,amount,day,instant\n");
    for id in 0..count {
        let payload = id
            .to_string()
            .bytes()
            .fold(String::new(), |mut text, byte| {
                write!(&mut text, "{byte:02x}").unwrap();
                text
            });
        let amount = format!("decimal128(8,2):{}", id * 100);
        let instant = i64::from(id) * 1_000_000;
        writeln!(
            &mut csv,
            "{id},\"\"\"{payload}\"\"\",\"\"\"{amount}\"\"\",{id},{instant}"
        )
        .unwrap();
        expected
            .push(json!({"id":id,"payload":payload,"amount":amount,"day":id,"instant":instant}));
    }
    (expected, csv)
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_typed_expressions_write_complete_computed_values_above_collection_limits() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
    let (expected, csv) = computed_expression_oracle(65_537);
    let fixture = Fixture::new(
        single("id", PrimitiveArray::from_iter(0..65_537u32).into_array()),
        4096,
    );
    let plan = project(
        fixture.scan(),
        vec![
            ("id", col("id")),
            ("payload", cast(col("id"), LogicalDType::Binary, false)),
            ("amount", cast(col("id"), decimal128_dtype(8, 2), false)),
            (
                "day",
                function(
                    "date_add_days",
                    vec![literal(ScalarValue::Date32(0)), col("id")],
                ),
            ),
            (
                "instant",
                function(
                    "timestamp_add_seconds",
                    vec![literal(ScalarValue::TimestampMicros(0)), col("id")],
                ),
            ),
        ],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    let token = CancellationToken::default();
    let mut delivered = 0;
    assert!(
        prepared
            .for_each_batch(&token, |_, _| {
                delivered += 1;
                token.cancel();
                Ok(())
            })
            .is_err()
    );
    assert_eq!(delivered, 1);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    for format in [
        Format::Vortex,
        Format::Parquet,
        Format::ArrowIpc,
        Format::Avro,
        Format::Json,
        Format::Jsonl,
        Format::Csv,
    ] {
        let path = fixture.0.join(format!("computed.{}", format.as_str()));
        let report = prepared.write(&path, format, false).unwrap();
        assert_eq!(report.output.rows_written, 65_537);
        assert_eq!(report.execution.output_rows, 65_537);
        assert!(report.execution.max_output_batch_rows <= BATCH_ROWS);
        assert!(report.execution.native_io_certificate.is_certified());
        if format == Format::Csv {
            assert_eq!(fs::read_to_string(path).unwrap(), csv);
        } else {
            assert_eq!(
                super::super::writer_tests::read_rows(
                    &path,
                    format,
                    &prepared.output_dtype().unwrap()
                ),
                expected,
                "{format:?}"
            );
        }
        // Execution proof metadata remains charged while its report is held.
        assert!(prepared.session.memory().snapshot().reserved_bytes > baseline);
        drop(report);
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline,
            "{format:?}"
        );
    }
    let path = fixture.0.join("computed-denied.orc");
    assert!(prepared.write(&path, Format::Orc, false).is_err());
    assert!(!path.exists());
    assert_eq!(prepared.snapshot().completed_executions, 7);
    let memory = prepared.session.memory().clone();
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
