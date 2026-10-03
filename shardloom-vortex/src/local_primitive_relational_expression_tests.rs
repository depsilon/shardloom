use super::*;
use crate::relational_query::{
    VortexRelationalFilter, VortexRelationalLimit, VortexRelationalNullOrder,
    VortexRelationalOrderKey, VortexRelationalProject, VortexRelationalSort,
};
use shardloom_core::{
    BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue, UnaryOp,
};
use vortex::array::arrays::{BoolArray, VarBinViewArray};

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("test").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn lit(value: ScalarValue) -> Expression {
    expression(ExpressionKind::Literal(value))
}
fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expression(ExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn compare(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    expression(ExpressionKind::Compare {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}
fn unary(op: UnaryOp, expr: Expression) -> Expression {
    expression(ExpressionKind::Unary {
        op,
        expr: Box::new(expr),
    })
}
fn function(name: &str, args: Vec<Expression>) -> Expression {
    expression(ExpressionKind::FunctionCall {
        name: name.to_owned(),
        args,
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
            .map(|(name, expr)| (name.to_owned(), expr))
            .collect(),
    }))
}
fn collected(plan: &VortexRelationalPlan) -> Vec<serde_json::Value> {
    json_rows(
        &prepare_relational(plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    )
}

#[test]
fn native_relational_expression_three_valued_booleans_and_filter_preserve_unknown() {
    let a = [
        Some(true),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(false),
        None,
        None,
        None,
    ];
    let b = [
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
        Some(true),
        Some(false),
        None,
    ];
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["a", "b"]),
            vec![
                BoolArray::from_iter(a).into_array(),
                BoolArray::from_iter(b).into_array(),
            ],
            9,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    );
    let plan = project(
        fixture.scan(),
        vec![
            ("and", binary(col("a"), BinaryOp::And, col("b"))),
            ("or", binary(col("a"), BinaryOp::Or, col("b"))),
            ("not", unary(UnaryOp::Not, col("a"))),
        ],
    );
    assert_eq!(
        serde_json::json!(collected(&plan)),
        serde_json::json!([
            {"and":true,"or":true,"not":false},{"and":false,"or":true,"not":false},{"and":null,"or":true,"not":false},
            {"and":false,"or":true,"not":true},{"and":false,"or":false,"not":true},{"and":false,"or":null,"not":true},
            {"and":null,"or":true,"not":null},{"and":false,"or":null,"not":null},{"and":null,"or":null,"not":null}
        ])
    );
    let filter = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: plan,
        predicate: col("and"),
    }));
    assert_eq!(
        collected(&filter),
        vec![serde_json::json!({"and":true,"or":true,"not":false})]
    );
}

#[test]
fn native_relational_expression_exact_integers_arithmetic_and_lazy_conditional() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["signed", "unsigned", "denominator"]),
            vec![
                PrimitiveArray::from_iter([-1_i64, 0, i64::MAX]).into_array(),
                PrimitiveArray::from_iter([u64::MAX, 0, i64::MAX.cast_unsigned()]).into_array(),
                PrimitiveArray::from_iter([0_i64, 2, -2]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let divided = function(
        "case_when",
        vec![
            compare(
                col("denominator"),
                ComparisonOp::Eq,
                lit(ScalarValue::Int64(0)),
            ),
            lit(ScalarValue::Int64(99)),
            binary(
                lit(ScalarValue::Int64(8)),
                BinaryOp::Divide,
                col("denominator"),
            ),
        ],
    );
    let plan = project(
        fixture.scan(),
        vec![
            (
                "eq",
                compare(col("signed"), ComparisonOp::Eq, col("unsigned")),
            ),
            (
                "lt",
                compare(col("signed"), ComparisonOp::Lt, col("unsigned")),
            ),
            ("division", divided),
            ("same", col("unsigned")),
        ],
    );
    assert_eq!(
        serde_json::json!(collected(&plan)),
        serde_json::json!([
            {"eq":false,"lt":true,"division":99,"same":u64::MAX}, {"eq":true,"lt":false,"division":4,"same":0}, {"eq":true,"lt":false,"division":-4,"same":i64::MAX}
        ])
    );
    for expression in [
        binary(col("signed"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
        binary(
            lit(ScalarValue::Int64(8)),
            BinaryOp::Divide,
            col("denominator"),
        ),
        binary(col("signed"), BinaryOp::Add, lit(ScalarValue::Float64(0.5))),
    ] {
        let prepared = prepare_relational(
            &project(fixture.scan(), vec![("fail", expression)]),
            policy(),
        )
        .unwrap();
        let before = prepared.session.memory().snapshot().reserved_bytes;
        assert!(
            prepared
                .collect_jsonl(&CancellationToken::default())
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.session.memory().snapshot().reserved_bytes, before);
    }
}

#[test]
fn native_relational_expression_coalesce_nullif_and_nullable_conditional() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["word", "value", "choose"]),
            vec![
                VarBinViewArray::from_iter_nullable_str([None, Some("東京\0"), Some("")])
                    .into_array(),
                PrimitiveArray::from_option_iter([None, Some(7_i32), Some(0)]).into_array(),
                BoolArray::from_iter([None, Some(true), Some(false)]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        1,
    );
    let plan = project(
        fixture.scan(),
        vec![
            (
                "text",
                function(
                    "coalesce",
                    vec![col("word"), lit(ScalarValue::Utf8("absent".to_owned()))],
                ),
            ),
            (
                "number",
                function("coalesce", vec![col("value"), lit(ScalarValue::Int64(9))]),
            ),
            (
                "nullif",
                function("nullif", vec![col("value"), lit(ScalarValue::Int64(0))]),
            ),
            (
                "case",
                function(
                    "case_when",
                    vec![col("choose"), lit(ScalarValue::Null), col("word")],
                ),
            ),
        ],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let dtype = prepared.output_dtype().unwrap();
    let fields = dtype.as_struct_fields_opt().unwrap();
    assert_eq!(
        fields.field("number").unwrap(),
        DType::Primitive(PType::I64, Nullability::NonNullable)
    );
    assert_eq!(
        fields.field("text").unwrap(),
        DType::Utf8(Nullability::NonNullable)
    );
    assert_eq!(
        serde_json::json!(collected(&plan)),
        serde_json::json!([
            {"text":"absent","number":9,"nullif":null,"case":null}, {"text":"東京\0","number":7,"nullif":7,"case":null}, {"text":"","number":0,"nullif":null,"case":""}
        ])
    );
    let short_circuit = project(
        fixture.scan(),
        vec![(
            "value",
            function(
                "coalesce",
                vec![
                    lit(ScalarValue::Int64(7)),
                    binary(
                        lit(ScalarValue::Int64(1)),
                        BinaryOp::Divide,
                        lit(ScalarValue::Int64(0)),
                    ),
                ],
            ),
        )],
    );
    assert_eq!(
        collected(&short_circuit),
        vec![serde_json::json!({"value":7}); 3]
    );
}

#[test]
#[allow(clippy::too_many_lines)] // One literal Unicode/numeric matrix shares the same native input.
fn native_relational_scalar_functions_preserve_unicode_nulls_and_numeric_domains() {
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["word", "number"]),
            vec![
                VarBinViewArray::from_iter_nullable_str([
                    Some("  StraßE 東京\0  "),
                    None,
                    Some("ΟΣ"),
                    Some(""),
                ])
                .into_array(),
                PrimitiveArray::from_option_iter([Some(-1.5_f64), None, Some(2.5), Some(-0.0)])
                    .into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    );
    let plan = project(
        fixture.scan(),
        vec![
            ("lower", function("lower", vec![col("word")])),
            ("upper", function("upper", vec![col("word")])),
            ("trim", function("trim", vec![col("word")])),
            ("length", function("length", vec![col("word")])),
            (
                "contains",
                function(
                    "contains",
                    vec![col("word"), lit(ScalarValue::Utf8("東京".into()))],
                ),
            ),
            (
                "regex",
                function(
                    "regex_match",
                    vec![col("word"), lit(ScalarValue::Utf8("^Ο.$".into()))],
                ),
            ),
            (
                "substr",
                function(
                    "substr",
                    vec![
                        col("word"),
                        lit(ScalarValue::Int64(3)),
                        lit(ScalarValue::Int64(6)),
                    ],
                ),
            ),
            (
                "left",
                function("left", vec![col("word"), lit(ScalarValue::Int64(2))]),
            ),
            (
                "right",
                function("right", vec![col("word"), lit(ScalarValue::Int64(3))]),
            ),
            (
                "replace",
                function(
                    "replace",
                    vec![
                        col("word"),
                        lit(ScalarValue::Utf8("StraßE".into())),
                        lit(ScalarValue::Utf8("done".into())),
                    ],
                ),
            ),
            ("abs", function("abs", vec![col("number")])),
            ("floor", function("floor", vec![col("number")])),
            ("ceil", function("ceil", vec![col("number")])),
            ("round", function("round", vec![col("number")])),
        ],
    );
    let rows = collected(&plan);
    assert_eq!(
        rows[0],
        serde_json::json!({"lower":"  straße 東京\0  ","upper":"  STRASSE 東京\0  ","trim":"StraßE 東京\0","length":14,"contains":true,"regex":false,"substr":"StraßE","left":"  ","right":"\0  ","replace":"  done 東京\0  ","abs":1.5,"floor":-2.0,"ceil":-1.0,"round":-2.0})
    );
    assert!(
        rows[1]
            .as_object()
            .unwrap()
            .values()
            .all(serde_json::Value::is_null)
    );
    assert_eq!(rows[2]["lower"], "ος");
    assert_eq!(rows[2]["regex"], true);
    assert_eq!(rows[2]["right"], "ΟΣ");
    assert_eq!(rows[2]["round"], 3.0);
    assert_eq!(rows[3]["length"], 0);
    let concat = project(
        fixture.scan(),
        vec![(
            "joined",
            function(
                "concat",
                vec![
                    lit(ScalarValue::Utf8("[".into())),
                    col("word"),
                    lit(ScalarValue::Utf8("]".into())),
                ],
            ),
        )],
    );
    assert_eq!(
        serde_json::json!(collected(&concat)),
        serde_json::json!([{"joined":"[  StraßE 東京\0  ]"},{"joined":null},{"joined":"[ΟΣ]"},{"joined":"[]"}])
    );
}

#[test]
fn native_relational_casts_reject_overflow_and_try_cast_preserves_nullable_types() {
    use shardloom_core::LogicalDType;
    let fixture = Fixture::new(
        single(
            "text",
            VarBinViewArray::from_iter_nullable_str([
                Some("9223372036854775807"),
                Some("9223372036854775808"),
                Some("bad"),
                None,
                Some("42"),
            ])
            .into_array(),
        ),
        2,
    );
    let to_int = expression(ExpressionKind::TryCast {
        expr: Box::new(col("text")),
        target_dtype: LogicalDType::Int64,
    });
    let plan = project(fixture.scan(), vec![("integer", to_int)]);
    assert_eq!(
        serde_json::json!(collected(&plan)),
        serde_json::json!([{"integer":i64::MAX},{"integer":null},{"integer":null},{"integer":null},{"integer":42}])
    );
    let strict = project(
        fixture.scan(),
        vec![(
            "integer",
            expression(ExpressionKind::Cast {
                expr: Box::new(col("text")),
                target_dtype: LogicalDType::Int64,
            }),
        )],
    );
    assert!(
        prepare_relational(&strict, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    let values = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([
                9_223_372_036_854_774_784.0_f64,
                9_223_372_036_854_775_808.0,
                -9_223_372_036_854_775_808.0,
                -9_223_372_036_854_777_856.0,
                1.25,
            ])
            .into_array(),
        ),
        3,
    );
    let plan = project(
        values.scan(),
        vec![(
            "integer",
            expression(ExpressionKind::TryCast {
                expr: Box::new(col("value")),
                target_dtype: LogicalDType::Int64,
            }),
        )],
    );
    assert_eq!(
        serde_json::json!(collected(&plan)),
        serde_json::json!([{"integer":9_223_372_036_854_774_784_i64},{"integer":null},{"integer":i64::MIN},{"integer":null},{"integer":null}])
    );
    let values = Fixture::new(
        single("value", PrimitiveArray::from_iter([u64::MAX]).into_array()),
        1,
    );
    let plan = project(
        values.scan(),
        vec![(
            "text",
            expression(ExpressionKind::Cast {
                expr: Box::new(col("value")),
                target_dtype: LogicalDType::Utf8,
            }),
        )],
    );
    assert_eq!(
        collected(&plan),
        vec![serde_json::json!({"text":"18446744073709551615"})]
    );
}

#[test]
fn native_relational_expression_binding_is_schema_driven_for_empty_inputs_and_denies_invalid_types()
{
    let empty = Fixture::new(keyed(&[], &[]), 1);
    let plan = project(
        empty.scan(),
        vec![(
            "empty",
            compare(col("amount"), ComparisonOp::Eq, lit(ScalarValue::Int64(0))),
        )],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let owned = prepared.execute_owned().unwrap();
    assert_eq!(
        owned.result.dtype(),
        &DType::struct_(
            [(String::from("empty"), DType::Bool(Nullability::NonNullable))],
            Nullability::NonNullable
        )
    );
    assert_eq!(owned.execution.output_rows, 0);
    for expression in [
        binary(
            col("amount"),
            BinaryOp::And,
            lit(ScalarValue::Boolean(true)),
        ),
        function("length", vec![col("amount")]),
        function(
            "regex_match",
            vec![
                lit(ScalarValue::Utf8(String::new())),
                lit(ScalarValue::Utf8("[".into())),
            ],
        ),
    ] {
        assert!(
            prepare_relational(
                &project(empty.scan(), vec![("invalid", expression)]),
                policy()
            )
            .is_err()
        );
    }
}

#[test]
fn native_relational_sort_range_preserves_stable_ties_null_placement_and_original_width() {
    let fixture = Fixture::new(
        keyed(
            &[Some(u64::MAX), None, Some(2), Some(2), Some(1)],
            &[0, 1, 2, 3, 4],
        ),
        2,
    );
    for (nulls, descending, expected) in [
        (VortexRelationalNullOrder::First, false, vec![1, 4, 2, 3, 0]),
        (VortexRelationalNullOrder::Last, true, vec![0, 2, 3, 4, 1]),
    ] {
        let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
            input: fixture.scan(),
            keys: vec![VortexRelationalOrderKey {
                column: ColumnRef::new("entity").unwrap(),
                descending,
                nulls: Some(nulls),
            }],
        }));
        let rows = collected(&sorted);
        assert_eq!(
            rows.iter()
                .map(|row| row["amount"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        for (offset, count) in [(1, 3), (0, 0), (9, 1), (0, usize::MAX)] {
            let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: sorted.clone(),
                offset,
                count,
            }));
            let selected = collected(&limited);
            let end = rows.len().min(offset.saturating_add(count));
            assert_eq!(selected, rows[offset.min(rows.len())..end]);
            let prepared = prepare_relational(&limited, policy()).unwrap();
            assert_eq!(
                prepared.output_dtype().unwrap(),
                DType::struct_(
                    [
                        (
                            String::from("entity"),
                            DType::Primitive(PType::U64, Nullability::Nullable)
                        ),
                        (
                            String::from("amount"),
                            DType::Primitive(PType::U32, Nullability::NonNullable)
                        )
                    ],
                    Nullability::NonNullable
                )
            );
        }
    }
    let missing_null_order = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: fixture.scan(),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("entity").unwrap(),
            descending: false,
            nulls: None,
        }],
    }));
    let prepared = prepare_relational(&missing_null_order, policy()).unwrap();
    let mut delivered = false;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                delivered = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!delivered);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_join_filter_expression_order_and_range_roundtrip_all_writers() {
    let left = Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1);
    let right = Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1);
    let filtered = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: join(&left, &right, JoinKind::Full),
        predicate: unary(UnaryOp::IsNotNull, col("credit")),
    }));
    let projected = project(
        filtered,
        vec![
            (
                "debit",
                function("coalesce", vec![col("debit"), lit(ScalarValue::UInt64(0))]),
            ),
            ("credit", col("credit")),
            (
                "total",
                binary(
                    function("coalesce", vec![col("debit"), lit(ScalarValue::UInt64(0))]),
                    BinaryOp::Add,
                    col("credit"),
                ),
            ),
        ],
    );
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: projected,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("total").unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: sorted,
        offset: 1,
        count: 2,
    }));
    super::writer_tests::verify_writers(
        &left,
        &plan,
        "transforms",
        &[
            serde_json::json!({"debit":10,"credit":20,"total":30}),
            serde_json::json!({"debit":0,"credit":22,"total":22}),
        ],
        "debit,credit,total\n10,20,30\n0,22,22\n",
    );
}
