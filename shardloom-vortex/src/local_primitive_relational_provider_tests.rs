use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalLimit,
        VortexRelationalOrderKey, VortexRelationalProject, VortexRelationalSort,
    },
};
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};

#[path = "local_primitive_relational_provider_normalization_tests.rs"]
mod normalization_tests;

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}
fn measure(function: &str, name: Option<&str>, alias: &str) -> Measure {
    Measure::new(
        function,
        name.map(|name| ColumnRef::new(name).unwrap()),
        alias.into(),
    )
}
fn aggregate(
    input: VortexRelationalPlan,
    groups: &[&str],
    measures: Vec<Measure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
        grouping_sets: None,
        measures,
    }))
}
fn filter(
    input: VortexRelationalPlan,
    name: &str,
    op: ComparisonOp,
    literal: ScalarValue,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input,
        predicate: Expression::new(
            ExprId::new("predicate").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column(name)),
                op,
                right: Box::new(Expression::literal(
                    ExprId::new("literal").unwrap(),
                    literal,
                )),
            },
        ),
    }))
}
fn sort(input: VortexRelationalPlan, name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new(name).unwrap(),
            descending: false,
            nulls: Some(crate::relational_query::VortexRelationalNullOrder::First),
        }],
    }))
}

fn sort_without_null_order(input: VortexRelationalPlan, name: &str) -> VortexRelationalPlan {
    let mut plan = sort(input, name);
    let VortexRelationalPlan::Sort(sort) = &mut plan else {
        unreachable!()
    };
    sort.keys[0].nulls = None;
    plan
}
fn range(input: VortexRelationalPlan, count: usize, offset: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}
fn fixture() -> Fixture {
    Fixture::new(
        keyed(&[Some(2), None, Some(1), Some(2), None], &[7, 8, 9, 10, 11]),
        2,
    )
}

fn literal(value: ScalarValue) -> Expression {
    Expression::literal(ExprId::new("literal").unwrap(), value)
}

fn call(name: &str, args: Vec<Expression>) -> Expression {
    Expression::new(
        ExprId::new(name).unwrap(),
        ExpressionKind::FunctionCall {
            name: name.into(),
            args,
        },
    )
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

fn subtract(name: &str, offset: i64) -> Expression {
    Expression::new(
        ExprId::new("shift").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(column(name)),
            op: shardloom_core::BinaryOp::Subtract,
            right: Box::new(literal(ScalarValue::Int64(offset))),
        },
    )
}

fn assert_provider_matches_general(plan: &VortexRelationalPlan, kind: VortexQueryPrimitiveKind) {
    let optimized = prepare_relational(plan, policy()).unwrap();
    assert!(optimized.provider.is_some(), "expected {kind:?}");
    let mut general = prepare_relational(plan, policy()).unwrap();
    general.provider = None;
    let expected = general
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let baseline = optimized.snapshot().memory.reserved_bytes;
    for completed in 1..=2 {
        let result = optimized
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), json_rows(&expected));
        assert_eq!(
            result.result_schema_json.value(),
            expected.result_schema_json.value()
        );
        let report = result.execution.primitive_execution.as_ref().unwrap();
        assert_eq!(report.primitive_kind, kind);
        assert!(!report.fallback_execution_allowed);
        assert_eq!(result.execution.runtime.prepared_source_opens, 1);
        assert_eq!(result.execution.runtime.completed_executions, completed);
        drop(result);
        assert_eq!(optimized.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn shared_provider_float_groups_merge_signed_zero_and_preserve_selected_bits() {
    let fixture = Fixture::new(
        single(
            "zero",
            PrimitiveArray::from_iter([-0.0_f64, 0.0, -0.0]).into_array(),
        ),
        1,
    );
    let plan = aggregate(
        fixture.scan(),
        &["zero"],
        vec![
            measure("count", None, "rows"),
            measure("count_distinct", Some("zero"), "distinct"),
            measure("min", Some("zero"), "first"),
        ],
    );
    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![serde_json::json!({"zero":-0.0,"rows":3,"distinct":1,"first":-0.0})]
    );
    prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            for name in ["zero", "first"] {
                let value = crate::local_primitives::logical_field_from_native_array(&array, name)?;
                let scalar = crate::local_primitives::result_batch::scalar_value(
                    &value,
                    0,
                    &mut context.native_session().create_execution_ctx(),
                )?;
                let crate::local_primitives::result_batch::Value::Float(value) = scalar else {
                    panic!("floating output type changed")
                };
                assert_eq!(value.to_bits(), (-0.0_f64).to_bits());
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn shared_provider_count_aggregate_sort_alias_and_offset_match_complete_typed_semantics() {
    let fixture = fixture();
    let count = aggregate(fixture.scan(), &[], vec![measure("count", None, "total")]);
    assert_provider_matches_general(&count, VortexQueryPrimitiveKind::CountAll);
    let filtered = filter(
        fixture.scan(),
        "entity",
        ComparisonOp::Eq,
        ScalarValue::UInt64(2),
    );
    assert_provider_matches_general(
        &aggregate(
            filtered.clone(),
            &[],
            vec![measure("count", None, "present")],
        ),
        VortexQueryPrimitiveKind::CountWhere,
    );
    assert_provider_matches_general(
        &range(sort(filtered, "amount"), 1, 1),
        VortexQueryPrimitiveKind::SortRows,
    );
    let grouped = aggregate(
        fixture.scan(),
        &["entity"],
        vec![
            measure("count", None, "rows"),
            measure("sum", Some("amount"), "sum"),
            measure("min", Some("amount"), "min"),
            measure("count_distinct", Some("amount"), "distinct"),
        ],
    );
    let grouped = range(
        sort(
            filter(grouped, "rows", ComparisonOp::Gt, ScalarValue::UInt64(1)),
            "entity",
        ),
        2,
        0,
    );
    let grouped = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: grouped,
        expressions: vec![
            ("total".into(), column("sum")),
            ("key".into(), column("entity")),
            ("minimum".into(), column("min")),
            ("distinct".into(), column("distinct")),
        ],
    }));
    assert_provider_matches_general(&grouped, VortexQueryPrimitiveKind::SimpleAggregate);
}

#[test]
fn shared_provider_preserves_operator_order_and_does_not_hide_invalid_projection() {
    let fixture = fixture();
    let limited_then_filtered = range(
        sort(
            filter(
                range(fixture.scan(), 2, 1),
                "entity",
                ComparisonOp::Eq,
                ScalarValue::UInt64(1),
            ),
            "amount",
        ),
        1,
        0,
    );
    assert!(
        prepare_relational(&limited_then_filtered, policy())
            .unwrap()
            .provider
            .is_none()
    );
    let projected = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: fixture.scan(),
        expressions: vec![
            ("bad".into(), column("absent")),
            ("entity".into(), column("entity")),
        ],
    }));
    assert!(
        prepare_relational(
            &aggregate(projected, &["entity"], vec![measure("count", None, "rows")]),
            policy()
        )
        .is_err()
    );
    let zero = range(sort(fixture.scan(), "amount"), 0, 2);
    let prepared = prepare_relational(&zero, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert_eq!(prepared.execute_owned().unwrap().execution.output_rows, 0);
}

#[test]
fn shared_provider_final_consumer_cancellation_and_generation_failure_are_terminal() {
    for count in [true, false] {
        let fixture = fixture();
        let plan = if count {
            aggregate(fixture.scan(), &[], vec![measure("count", None, "rows")])
        } else {
            range(sort(fixture.scan(), "amount"), 2, 0)
        };
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(prepared.provider.is_some());
        let token = CancellationToken::default();
        assert!(
            prepared
                .for_each_batch(&token, |_, _| {
                    token.cancel();
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert!(
            prepared
                .for_each_batch(&CancellationToken::default(), |_, _| {
                    fixture.replace();
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
}

#[test]
fn shared_provider_derived_unsigned_signed_groups_and_measures_keep_exact_domains() {
    let fixture = Fixture::new(
        single(
            "value",
            PrimitiveArray::from_iter([0_u32, 1, u32::MAX, 1]).into_array(),
        ),
        2,
    );
    let projection = project(
        fixture.scan(),
        vec![
            ("original", column("value")),
            ("shifted", subtract("value", 3)),
            ("constant", literal(ScalarValue::Int64(7))),
        ],
    );
    assert_provider_matches_general(
        &aggregate(
            projection,
            &["constant", "original", "shifted"],
            vec![measure("count", None, "n")],
        ),
        VortexQueryPrimitiveKind::SimpleAggregate,
    );
    let projection = project(fixture.scan(), vec![("shifted", subtract("value", 3))]);
    assert_provider_matches_general(
        &aggregate(
            projection,
            &[],
            vec![
                measure("sum", Some("shifted"), "sum"),
                measure("avg", Some("shifted"), "mean"),
            ],
        ),
        VortexQueryPrimitiveKind::SimpleAggregate,
    );

    let wide = Fixture::new(
        single("value", PrimitiveArray::from_iter([i64::MIN]).into_array()),
        1,
    );
    let plan = aggregate(
        project(wide.scan(), vec![("shifted", subtract("value", 1))]),
        &[],
        vec![measure("sum", Some("shifted"), "sum")],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert!(prepared.execute_owned().is_err());
}

#[test]
fn shared_provider_temporal_and_domain_functions_match_general_in_compositions() {
    use vortex::array::arrays::VarBinViewArray;
    let time = Fixture::new(
        single(
            "seconds",
            PrimitiveArray::from_option_iter([Some(-61_i64), Some(-1), Some(0), Some(60), None])
                .into_array(),
        ),
        2,
    );
    for function in ["extract_minute", "date_trunc_minute"] {
        let plan = aggregate(
            project(
                time.scan(),
                vec![("key", call(function, vec![column("seconds")]))],
            ),
            &["key"],
            vec![measure("count", None, "n")],
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    }
    let domain = Fixture::new(
        single(
            "address",
            VarBinViewArray::from_iter_nullable_str([
                Some("https://www.example.com:80/path"),
                Some("http://no-path"),
                Some("http://é.test/a"),
                Some("https://www.example.com:80/other"),
                Some("http://a/x\ny"),
                None,
            ])
            .into_array(),
        ),
        2,
    );
    for function in ["url_domain", "regex_domain"] {
        let plan = aggregate(
            project(
                domain.scan(),
                vec![("key", call(function, vec![column("address")]))],
            ),
            &["key"],
            vec![measure("count", None, "n")],
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    }
    let replacement = call(
        "regexp_replace",
        vec![
            column("address"),
            literal(ScalarValue::Utf8(r"^https?://(?:www\.)?([^/]+)/.*$".into())),
            literal(ScalarValue::Utf8(r"\1".into())),
        ],
    );
    assert_provider_matches_general(
        &aggregate(
            project(domain.scan(), vec![("key", replacement)]),
            &["key"],
            vec![measure("count", None, "n")],
        ),
        VortexQueryPrimitiveKind::SimpleAggregate,
    );
    let unsupported = project(
        domain.scan(),
        vec![(
            "key",
            call(
                "regexp_replace",
                vec![
                    column("address"),
                    literal(ScalarValue::Utf8(".*".into())),
                    literal(ScalarValue::Utf8("x".into())),
                ],
            ),
        )],
    );
    assert!(prepare_relational(&unsupported, policy()).is_err());

    let overflow = Fixture::new(
        single(
            "seconds",
            PrimitiveArray::from_iter([i64::MIN]).into_array(),
        ),
        1,
    );
    let plan = aggregate(
        project(
            overflow.scan(),
            vec![("key", call("date_trunc_minute", vec![column("seconds")]))],
        ),
        &["key"],
        vec![measure("count", None, "n")],
    );
    let optimized = prepare_relational(&plan, policy()).unwrap();
    let mut general = prepare_relational(&plan, policy()).unwrap();
    general.provider = None;
    assert!(optimized.execute_owned().is_err());
    assert!(general.execute_owned().is_err());
}

#[test]
fn shared_provider_does_not_confuse_character_and_byte_length() {
    use vortex::array::arrays::VarBinViewArray;
    let text = Fixture::new(
        single(
            "text",
            VarBinViewArray::from_iter_nullable_str([Some("é"), Some("a"), None]).into_array(),
        ),
        2,
    );
    let bytes = aggregate(
        project(
            text.scan(),
            vec![("size", call("octet_length", vec![column("text")]))],
        ),
        &[],
        vec![measure("avg", Some("size"), "mean")],
    );
    assert_provider_matches_general(&bytes, VortexQueryPrimitiveKind::SimpleAggregate);
    let chars = aggregate(
        project(
            text.scan(),
            vec![("size", call("length", vec![column("text")]))],
        ),
        &[],
        vec![measure("avg", Some("size"), "mean")],
    );
    let prepared = prepare_relational(&chars, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![serde_json::json!({"mean":1.0})]
    );
}

#[test]
fn shared_provider_conditional_groups_use_expression_roles_and_preserve_nullable_ordering() {
    use vortex::array::arrays::VarBinViewArray;
    let fixture = Fixture::new(
        StructArray::try_new(
            FieldNames::from(["a", "b", "text"]),
            vec![
                PrimitiveArray::from_iter([0_u8, 1, 0, 0]).into_array(),
                PrimitiveArray::from_iter([0_i16, 0, 0, 2]).into_array(),
                VarBinViewArray::from_iter_str(["chosen", "unused", "chosen", "unused"])
                    .into_array(),
            ],
            4,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    );
    let zero = |name: &str| {
        Expression::new(
            ExprId::new("zero").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(column(name)),
                op: ComparisonOp::Eq,
                right: Box::new(literal(ScalarValue::Int64(0))),
            },
        )
    };
    let condition = Expression::new(
        ExprId::new("and").unwrap(),
        ExpressionKind::Binary {
            left: Box::new(zero("a")),
            op: shardloom_core::BinaryOp::And,
            right: Box::new(zero("b")),
        },
    );
    let plan = aggregate(
        project(
            fixture.scan(),
            vec![(
                "key",
                call(
                    "case_when",
                    vec![
                        condition,
                        column("text"),
                        literal(ScalarValue::Utf8(String::new())),
                    ],
                ),
            )],
        ),
        &["key"],
        vec![measure("count", None, "n")],
    );
    assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);

    let fixture = super::provider_tests::fixture();
    let mut sorted = sort(
        aggregate(
            fixture.scan(),
            &["entity"],
            vec![measure("count", None, "n")],
        ),
        "entity",
    );
    let VortexRelationalPlan::Sort(sort) = &mut sorted else {
        unreachable!()
    };
    sort.keys[0].nulls = None;
    let prepared = prepare_relational(&sorted, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert!(prepared.execute_owned().is_err());
}

#[test]
fn shared_provider_grouped_nonnull_measures_do_not_require_null_ordering() {
    let fixture = fixture();
    for function in ["sum", "avg", "min", "max"] {
        let plan = range(
            sort_without_null_order(
                aggregate(
                    fixture.scan(),
                    &["entity"],
                    vec![measure(function, Some("amount"), "value")],
                ),
                "value",
            ),
            3,
            0,
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    }

    let text = Fixture::new(
        single(
            "text",
            vortex::array::arrays::VarBinViewArray::from_iter_str(["a", "é", "a"]).into_array(),
        ),
        1,
    );
    let grouped = aggregate(
        project(
            text.scan(),
            vec![
                ("key", column("text")),
                ("bytes", call("octet_length", vec![column("text")])),
            ],
        ),
        &["key"],
        vec![
            measure("avg", Some("bytes"), "mean"),
            measure("count", None, "rows"),
        ],
    );
    assert_provider_matches_general(
        &range(
            sort_without_null_order(
                filter(grouped, "rows", ComparisonOp::Gt, ScalarValue::UInt64(0)),
                "mean",
            ),
            2,
            0,
        ),
        VortexQueryPrimitiveKind::SimpleAggregate,
    );
}

#[test]
fn shared_provider_does_not_assume_nullable_or_empty_measures_have_values() {
    let fixture = fixture();
    let nullable = sort_without_null_order(
        aggregate(
            fixture.scan(),
            &["amount"],
            vec![measure("avg", Some("entity"), "mean")],
        ),
        "mean",
    );
    let empty_scalar = sort_without_null_order(
        aggregate(
            filter(
                fixture.scan(),
                "amount",
                ComparisonOp::Lt,
                ScalarValue::Int64(0),
            ),
            &[],
            vec![measure("avg", Some("amount"), "mean")],
        ),
        "mean",
    );
    for plan in [nullable, empty_scalar] {
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(prepared.provider.is_none());
        assert!(prepared.execute_owned().is_err());
    }
}

fn having_fixture() -> Fixture {
    Fixture::new(
        keyed(
            &[None, None, Some(0), None, Some(2), Some(1), None],
            &[1, 1, 2, 2, 2, 3, 4],
        ),
        2,
    )
}

fn assert_typed_having_rows(plan: &VortexRelationalPlan, expected: &[serde_json::Value]) {
    let prepared = prepare_relational(plan, policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for completed in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected);
        assert!(prepared.provider.is_none());
        assert!(result.execution.primitive_execution.is_none());
        assert_eq!(result.execution.runtime.completed_executions, completed);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

fn numeric_having_fixture(values: ArrayRef) -> Fixture {
    let len = values.len();
    Fixture::new(
        StructArray::try_new(
            FieldNames::from(["group", "value"]),
            vec![
                PrimitiveArray::from_iter(0..u64::try_from(len).unwrap()).into_array(),
                values,
            ],
            len,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array(),
        2,
    )
}

fn assert_exact_numeric_having(
    fixture: &Fixture,
    function: &str,
    threshold: &ScalarValue,
    expected_order: &[std::cmp::Ordering],
) {
    use std::cmp::Ordering::{Equal, Greater, Less};
    for op in [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ] {
        let plan = project(
            sort(
                filter(
                    aggregate(
                        fixture.scan(),
                        &["group"],
                        vec![measure(function, Some("value"), "reduced")],
                    ),
                    "reduced",
                    op,
                    threshold.clone(),
                ),
                "group",
            ),
            vec![("group", column("group"))],
        );
        let expected = expected_order
            .iter()
            .enumerate()
            .filter(|(_, order)| match op {
                ComparisonOp::Eq => **order == Equal,
                ComparisonOp::NotEq => **order != Equal,
                ComparisonOp::Lt => **order == Less,
                ComparisonOp::LtEq => **order != Greater,
                ComparisonOp::Gt => **order == Greater,
                ComparisonOp::GtEq => **order != Less,
            })
            .map(|(group, _)| serde_json::json!({"group":group}))
            .collect::<Vec<_>>();
        assert_typed_having_rows(&plan, &expected);
    }
}

#[test]
fn shared_provider_having_float_reductions_keep_exact_large_integer_comparisons() {
    use std::cmp::Ordering::{Greater, Less};
    let fixture = numeric_having_fixture(
        PrimitiveArray::from_iter([
            -9_007_199_254_740_994.0_f64,
            -9_007_199_254_740_992.0,
            0.0,
            9_007_199_254_740_992.0,
            9_007_199_254_740_994.0,
            9_223_372_036_854_775_808.0,
            18_446_744_073_709_551_616.0,
        ])
        .into_array(),
    );
    for function in ["sum", "avg", "min", "max"] {
        for (threshold, expected) in [
            (
                ScalarValue::Int64(9_007_199_254_740_993),
                [Less, Less, Less, Less, Greater, Greater, Greater],
            ),
            (
                ScalarValue::Int64(-9_007_199_254_740_993),
                [Less, Greater, Greater, Greater, Greater, Greater, Greater],
            ),
            (
                ScalarValue::Int64(i64::MAX),
                [Less, Less, Less, Less, Less, Greater, Greater],
            ),
            (
                ScalarValue::UInt64(u64::MAX),
                [Less, Less, Less, Less, Less, Less, Greater],
            ),
        ] {
            assert_exact_numeric_having(&fixture, function, &threshold, &expected);
        }
    }
}

#[test]
fn shared_provider_having_integer_reductions_preserve_floating_literal_identity() {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let unsigned = numeric_having_fixture(
        PrimitiveArray::from_iter([
            9_007_199_254_740_991_u64,
            9_007_199_254_740_992,
            9_007_199_254_740_993,
            9_223_372_036_854_775_808,
            9_223_372_036_854_775_809,
            u64::MAX,
        ])
        .into_array(),
    );
    let signed = numeric_having_fixture(
        PrimitiveArray::from_iter([
            i64::MIN,
            -9_007_199_254_740_993,
            -9_007_199_254_740_992,
            -9_007_199_254_740_991,
            0,
            i64::MAX,
        ])
        .into_array(),
    );
    for function in ["min", "max"] {
        for (fixture, threshold, expected) in [
            (
                &unsigned,
                9_223_372_036_854_775_808.0,
                [Less, Less, Less, Equal, Greater, Greater],
            ),
            (
                &unsigned,
                9_007_199_254_740_992.0,
                [Less, Equal, Greater, Greater, Greater, Greater],
            ),
            (&unsigned, 18_446_744_073_709_551_616.0, [Less; 6]),
            (
                &signed,
                -9_007_199_254_740_992.0,
                [Less, Less, Equal, Greater, Greater, Greater],
            ),
            (
                &signed,
                -9_223_372_036_854_775_808.0,
                [Equal, Greater, Greater, Greater, Greater, Greater],
            ),
            (&signed, 9_223_372_036_854_775_808.0, [Less; 6]),
        ] {
            assert_exact_numeric_having(
                fixture,
                function,
                &ScalarValue::Float64(threshold),
                &expected,
            );
        }
    }
}

#[test]
fn shared_provider_having_nullable_measures_preserve_unknown_and_hidden_fields() {
    let fixture = having_fixture();
    for function in ["min", "max", "sum", "avg"] {
        for (op, retains_nonnull) in [
            (ComparisonOp::Lt, true),
            (ComparisonOp::LtEq, true),
            (ComparisonOp::NotEq, true),
            (ComparisonOp::Eq, false),
            (ComparisonOp::Gt, false),
            (ComparisonOp::GtEq, false),
        ] {
            let grouped = aggregate(
                fixture.scan(),
                &["amount"],
                vec![
                    measure(function, Some("entity"), "reduced"),
                    measure("count", None, "rows"),
                ],
            );
            let mut filtered = filter(grouped.clone(), "reduced", op, ScalarValue::UInt64(10));
            // Also cover conjunctions containing a non-null COUNT comparison.
            if matches!(function, "sum" | "avg") {
                let VortexRelationalPlan::Filter(count) =
                    filter(grouped, "rows", ComparisonOp::Gt, ScalarValue::UInt64(0))
                else {
                    unreachable!()
                };
                let VortexRelationalPlan::Filter(value) = &mut filtered else {
                    unreachable!()
                };
                value.predicate = Expression::new(
                    ExprId::new("having_and").unwrap(),
                    ExpressionKind::Binary {
                        left: Box::new(value.predicate.clone()),
                        op: shardloom_core::BinaryOp::And,
                        right: Box::new(count.predicate),
                    },
                );
            }
            // The nullable HAVING operand is absent from the final schema.
            let plan = project(sort(filtered, "amount"), vec![("group", column("amount"))]);
            let expected = if retains_nonnull {
                vec![
                    serde_json::json!({"group":2}),
                    serde_json::json!({"group":3}),
                ]
            } else {
                vec![]
            };
            assert_typed_having_rows(&plan, &expected);
        }
    }
}

#[test]
fn shared_provider_having_nullable_keys_and_empty_scalar_reductions_keep_typed_filter() {
    let fixture = having_fixture();
    let cases = [
        (ComparisonOp::Lt, &[0_u64][..]),
        (ComparisonOp::LtEq, &[0, 1][..]),
        (ComparisonOp::NotEq, &[0, 2][..]),
        (ComparisonOp::Eq, &[1][..]),
        (ComparisonOp::Gt, &[2][..]),
        (ComparisonOp::GtEq, &[1, 2][..]),
    ];
    for (op, keys) in cases {
        let plan = project(
            sort(
                filter(
                    aggregate(
                        fixture.scan(),
                        &["entity"],
                        vec![measure("count", None, "rows")],
                    ),
                    "entity",
                    op,
                    ScalarValue::UInt64(1),
                ),
                "entity",
            ),
            vec![("key", column("entity")), ("rows", column("rows"))],
        );
        let expected = keys
            .iter()
            .map(|key| serde_json::json!({"key":key,"rows":1}))
            .collect::<Vec<_>>();
        assert_typed_having_rows(&plan, &expected);

        for function in ["min", "max", "sum", "avg"] {
            let empty = filter(
                fixture.scan(),
                "amount",
                ComparisonOp::Lt,
                ScalarValue::UInt64(0),
            );
            let plan = filter(
                aggregate(empty, &[], vec![measure(function, Some("amount"), "value")]),
                "value",
                op,
                ScalarValue::UInt64(1),
            );
            assert_typed_having_rows(&plan, &[]);
        }
    }
}

#[test]
fn shared_provider_having_proven_nonnull_reductions_and_counts_keep_provider() {
    let fixture = having_fixture();
    for function in ["min", "max", "sum", "avg"] {
        let plan = sort(
            filter(
                aggregate(
                    fixture.scan(),
                    &["entity"],
                    vec![measure(function, Some("amount"), "value")],
                ),
                "value",
                ComparisonOp::Gt,
                ScalarValue::UInt64(0),
            ),
            "entity",
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    }
    for function in ["count", "count_distinct"] {
        let empty = filter(
            fixture.scan(),
            "amount",
            ComparisonOp::Lt,
            ScalarValue::UInt64(0),
        );
        let plan = filter(
            aggregate(empty, &[], vec![measure(function, Some("entity"), "value")]),
            "value",
            ComparisonOp::Lt,
            ScalarValue::UInt64(1),
        );
        assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
    }
}

#[test]
fn shared_provider_having_exact_mixed_numeric_ranges_keep_provider() {
    for values in [
        PrimitiveArray::from_iter([-0.0_f64, 0.0, 9_007_199_254_740_992.0]).into_array(),
        PrimitiveArray::from_iter([0_u64, 1, 9_007_199_254_740_993, u64::MAX]).into_array(),
        PrimitiveArray::from_iter([i64::MIN, -9_007_199_254_740_993, 0, 1, i64::MAX]).into_array(),
    ] {
        let fixture = numeric_having_fixture(values);
        for function in ["min", "max", "sum", "avg"] {
            for threshold in [
                ScalarValue::Int64(-9_007_199_254_740_992),
                ScalarValue::UInt64(9_007_199_254_740_992),
                ScalarValue::Float64(-9_007_199_254_740_991.0),
                ScalarValue::Float64(9_007_199_254_740_991.0),
                ScalarValue::Float64(-1.5),
                ScalarValue::Float64(-0.0),
                ScalarValue::Float64(0.0),
                ScalarValue::Float64(1.5),
            ] {
                let plan = sort(
                    filter(
                        aggregate(
                            fixture.scan(),
                            &["group"],
                            vec![measure(function, Some("value"), "reduced")],
                        ),
                        "reduced",
                        ComparisonOp::LtEq,
                        threshold,
                    ),
                    "group",
                );
                assert_provider_matches_general(&plan, VortexQueryPrimitiveKind::SimpleAggregate);
            }
        }
    }
}

#[test]
fn shared_provider_file_filter_and_alias_projection_use_native_scan() {
    let fixture = fixture();
    let plan = filter(
        project(fixture.scan(), vec![("key", column("entity"))]),
        "key",
        ComparisonOp::Eq,
        ScalarValue::UInt64(2),
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for completed in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), vec![serde_json::json!({"key":2}); 2]);
        assert_eq!(result.execution.scan_rows_delivered, 2);
        assert_eq!(result.execution.runtime.completed_executions, completed);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let PreparedRoot::Bound(root) = &prepared.root else {
        unreachable!()
    };
    let NodeKind::Project { input, .. } = &root.kind else {
        panic!("alias must remain an encoded projection")
    };
    let NodeKind::Scan { plan, columns, .. } = &input.kind else {
        panic!("filter should use the existing native scan")
    };
    assert!(plan.filter.is_some());
    assert_eq!(columns, &["entity"]);
    assert_eq!(input.fields.len(), 1);

    let ranged = filter(
        range(fixture.scan(), 1, 1),
        "entity",
        ComparisonOp::Eq,
        ScalarValue::UInt64(2),
    );
    let prepared = prepare_relational(&ranged, policy()).unwrap();
    let PreparedRoot::Bound(root) = &prepared.root else {
        unreachable!()
    };
    assert!(matches!(root.kind, NodeKind::Filter { .. }));
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        Vec::<serde_json::Value>::new()
    );
}

#[test]
fn shared_provider_predicates_preserve_typed_null_literal_and_numeric_domains() {
    let fixture = fixture();
    let eq = |value| {
        let VortexRelationalPlan::Filter(filter) =
            filter(fixture.scan(), "entity", ComparisonOp::Eq, value)
        else {
            unreachable!()
        };
        filter.predicate
    };
    let plan = aggregate(
        VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input: fixture.scan(),
            predicate: Expression::new(
                ExprId::new("or").unwrap(),
                ExpressionKind::Binary {
                    left: Box::new(eq(ScalarValue::UInt64(1))),
                    op: shardloom_core::BinaryOp::Or,
                    right: Box::new(eq(ScalarValue::Null)),
                },
            ),
        })),
        &[],
        vec![measure("count", None, "rows")],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert_eq!(
        json_rows(
            &prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        ),
        vec![serde_json::json!({"rows":1})]
    );

    let narrow = Fixture::new(
        single("n", PrimitiveArray::from_iter([0_u8, 255]).into_array()),
        1,
    );
    for (op, value, count) in [
        (ComparisonOp::Lt, ScalarValue::UInt64(256), 2),
        (ComparisonOp::Gt, ScalarValue::Int64(-1), 2),
        (ComparisonOp::Lt, ScalarValue::Float64(0.5), 1),
    ] {
        let plan = aggregate(
            filter(narrow.scan(), "n", op, value),
            &[],
            vec![measure("count", None, "rows")],
        );
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert!(prepared.provider.is_none());
        assert_eq!(
            json_rows(
                &prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            ),
            vec![serde_json::json!({"rows":count})]
        );
    }

    let nonfinite = Fixture::new(
        single(
            "n",
            PrimitiveArray::from_iter([1.0_f64, f64::NAN]).into_array(),
        ),
        1,
    );
    let plan = aggregate(
        filter(
            nonfinite.scan(),
            "n",
            ComparisonOp::Eq,
            ScalarValue::Float64(1.0),
        ),
        &[],
        vec![measure("count", None, "rows")],
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    assert!(prepared.provider.is_none());
    assert!(prepared.execute_owned().is_err());
}
