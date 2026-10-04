use super::*;
use crate::{
    query_primitive::{
        VortexDuplicateKeepPolicy as Keep, VortexExpressionProjectionRequest,
        VortexExpressionRewrite as Rewrite, VortexMeltProjectionRequest,
        VortexQueryPrimitiveKind as Kind, VortexSimpleAggregateMeasure as Measure,
    },
    relational_query::{VortexRelationalAggregate, VortexRelationalProject, VortexRelationalUnary},
};
use serde_json::json;
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue, UnaryOp};
use shardloom_plan::ProjectionRequest;

fn request(kind: Kind, names: &[&str]) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::for_relational_input(
        kind,
        ProjectionRequest::columns(names.iter().map(|name| column(name)).collect()),
    )
}
fn unary(
    input: VortexRelationalPlan,
    request: VortexQueryPrimitiveRequest,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary { input, request }))
}
fn twice(fixture: &Fixture) -> VortexRelationalPlan {
    VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: fixture.scan(),
        right: fixture.scan(),
        kind: SetKind::UnionAll,
    }))
}
fn expr(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("nested").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expr(ExpressionKind::Column(column(name)))
}
fn function(name: &str, args: Vec<Expression>) -> Expression {
    expr(ExpressionKind::FunctionCall {
        name: name.into(),
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
            .map(|(name, expr)| (name.into(), expr))
            .collect(),
    }))
}
fn aggregate(
    input: VortexRelationalPlan,
    groups: &[&str],
    measures: &[(&str, Option<&str>, &str)],
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups.iter().map(|name| column(name)).collect(),
        measures: measures
            .iter()
            .map(|(function, name, alias)| {
                Measure::new(*function, name.map(column), (*alias).into())
            })
            .collect(),
    }))
}

#[test]
fn native_nested_keys_group_count_distinct_and_extrema_cross_batches() {
    let fixture = fixture();
    assert_eq!(
        collect(&aggregate(
            twice(&fixture),
            &[],
            &[
                ("count", Some("items"), "count"),
                ("count_distinct", Some("items"), "distinct"),
                ("min", Some("items"), "min"),
                ("max", Some("items"), "max"),
            ]
        )),
        vec![json!({"count":6,"distinct":3,"min":[],"max":[9,null]})]
    );
    assert_eq!(
        collect(&aggregate(
            twice(&fixture),
            &["items"],
            &[("count", None, "rows"), ("count", Some("items"), "valid")]
        )),
        vec![
            json!({"items":[9,null],"rows":2,"valid":2}),
            json!({"items":[],"rows":2,"valid":2}),
            json!({"items":null,"rows":2,"valid":0}),
            json!({"items":[-4],"rows":2,"valid":2}),
        ]
    );
}

#[test]
fn native_nested_keys_selected_expressions_preserve_parent_and_child_nulls() {
    let fixture = fixture();
    let plan = project(
        fixture.scan(),
        vec![
            (
                "equal",
                expr(ExpressionKind::Compare {
                    left: Box::new(col("items")),
                    op: ComparisonOp::Eq,
                    right: Box::new(col("items")),
                }),
            ),
            (
                "missing",
                expr(ExpressionKind::Unary {
                    op: UnaryOp::IsNull,
                    expr: Box::new(col("items")),
                }),
            ),
            ("nil", function("nullif", vec![col("items"), col("items")])),
            (
                "kept",
                function(
                    "coalesce",
                    vec![
                        expr(ExpressionKind::Literal(ScalarValue::Null)),
                        col("items"),
                    ],
                ),
            ),
        ],
    );
    assert_eq!(
        collect(&plan),
        vec![
            json!({"equal":true,"missing":false,"nil":null,"kept":[9,null]}),
            json!({"equal":true,"missing":false,"nil":null,"kept":[]}),
            json!({"equal":null,"missing":true,"nil":null,"kept":null}),
            json!({"equal":true,"missing":false,"nil":null,"kept":[-4]}),
        ]
    );
}

#[test]
fn native_nested_state_tail_and_all_duplicate_policies_keep_complete_values() {
    let fixture = fixture();
    let expected = vec![
        json!({"id":1,"items":[9,null]}),
        json!({"id":2,"items":[]}),
        json!({"id":3,"items":null}),
        json!({"id":4,"items":[-4]}),
    ];
    for keep in [Keep::First, Keep::Last, Keep::AllDuplicates] {
        let mut req = request(Kind::DropDuplicateRows, &["id", "items"]);
        req.deduplicate_key_projection = Some(ProjectionRequest::columns(vec![column("items")]));
        req.duplicate_keep = keep;
        assert_eq!(
            collect(&unary(twice(&fixture), req)),
            if keep == Keep::AllDuplicates {
                vec![]
            } else {
                expected.clone()
            }
        );
    }
    let mut req = request(Kind::TailRows, &["id", "items"]);
    req.source_order_limit = Some(2);
    assert_eq!(collect(&unary(twice(&fixture), req)), expected[2..]);
    assert_eq!(
        collect(&unary(
            twice(&fixture),
            request(Kind::DistinctRows, &["items"])
        )),
        expected
            .iter()
            .map(|row| json!({"items":row["items"]}))
            .collect::<Vec<_>>()
    );
}

#[test]
fn native_nested_state_forward_fill_and_melt_keep_arrays() {
    let fixture = fixture();
    let mut req = request(Kind::ExpressionProjectRows, &["id", "items"]);
    req.expression_projection = Some(VortexExpressionProjectionRequest::new(vec![
        Rewrite::ForwardFillNull {
            target_column: column("items"),
            limit: Some(1),
        },
        Rewrite::RowNumber {
            target_column: column("ordinal"),
            start: 8,
        },
    ]));
    let filled = unary(fixture.scan(), req);
    assert_eq!(
        collect(&filled),
        vec![
            json!({"id":1,"items":[9,null],"ordinal":8}),
            json!({"id":2,"items":[],"ordinal":9}),
            json!({"id":3,"items":[],"ordinal":10}),
            json!({"id":4,"items":[-4],"ordinal":11})
        ]
    );
    let source = project(
        fixture.scan(),
        vec![("id", col("id")), ("a", col("items")), ("b", col("items"))],
    );
    let mut melt = request(Kind::MeltRows, &["id", "a", "b"]);
    melt.melt_projection = Some(VortexMeltProjectionRequest::new(
        vec![column("id")],
        vec![column("a"), column("b")],
        "name".into(),
        "value".into(),
    ));
    let expected = [json!([9, null]), json!([]), json!(null), json!([-4])]
        .iter()
        .enumerate()
        .flat_map(|(index, value)| {
            [
                json!({"id":index+1,"name":"a","value":value}),
                json!({"id":index+1,"name":"b","value":value}),
            ]
        })
        .collect::<Vec<_>>();
    assert_eq!(collect(&unary(source, melt)), expected);
}

#[test]
fn native_nested_keys_do_not_admit_scalar_kernels_on_empty_input() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["items"]),
            vec![lists().slice(0..0).unwrap()],
            0,
            Validity::NonNullable,
        )
        .into_array(),
        1,
    );
    for value in [
        expr(ExpressionKind::Unary {
            op: UnaryOp::Negate,
            expr: Box::new(col("items")),
        }),
        function("lower", vec![col("items")]),
        function("byte_length", vec![col("items")]),
    ] {
        let error = prepare_relational(&project(fixture.scan(), vec![("bad", value)]), policy())
            .err()
            .unwrap();
        assert!(error.to_string().contains("operated scalar"), "{error}");
    }
    for function in ["sum", "avg"] {
        let error = prepare_relational(
            &aggregate(fixture.scan(), &[], &[(function, Some("items"), "bad")]),
            policy(),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("numeric"), "{error}");
    }
}

#[test]
fn native_nested_keys_joins_and_sets_keep_parent_null_policies() {
    let fixture = fixture();
    for (kind, pairs) in [
        (
            JoinKind::Inner,
            vec![(Some(1), Some(1)), (Some(2), Some(2)), (Some(4), Some(4))],
        ),
        (
            JoinKind::Full,
            vec![
                (Some(1), Some(1)),
                (Some(2), Some(2)),
                (Some(3), None),
                (Some(4), Some(4)),
                (None, Some(3)),
            ],
        ),
    ] {
        let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fixture.scan(),
            right: fixture.scan(),
            kind,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: column("items"),
                right: column("items"),
            }],
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: column("id"),
                    output_column: "left".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: column("id"),
                    output_column: "right".into(),
                },
            ],
        }));
        assert_eq!(
            collect(&plan),
            pairs
                .into_iter()
                .map(|(left, right)| json!({"left":left,"right":right}))
                .collect::<Vec<_>>()
        );
    }
    let left = project(twice(&fixture), vec![("items", col("items"))]);
    let right =
        VortexRelationalPlan::Limit(Box::new(crate::relational_query::VortexRelationalLimit {
            input: project(fixture.scan(), vec![("items", col("items"))]),
            offset: 0,
            count: 2,
        }));
    for (kind, values) in [
        (
            SetKind::UnionDistinct,
            vec![json!([9, null]), json!([]), json!(null), json!([-4])],
        ),
        (SetKind::Intersect, vec![json!([9, null]), json!([])]),
        (SetKind::Except, vec![json!(null), json!([-4])]),
    ] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: left.clone(),
            right: right.clone(),
            kind,
        }));
        assert_eq!(
            collect(&plan),
            values
                .into_iter()
                .map(|value| json!({"items":value}))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn native_nested_keys_order_rank_and_partition_identity_are_consistent() {
    use crate::relational_query::{
        VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey as OrderKey,
        VortexRelationalSort, VortexRelationalWindow,
        VortexRelationalWindowExpression as WindowExpression,
        VortexRelationalWindowFunction as WindowFunction,
    };
    let fixture = fixture();
    for (descending, nulls, ids) in [
        (false, NullOrder::First, [3, 2, 4, 1]),
        (false, NullOrder::Last, [2, 4, 1, 3]),
        (true, NullOrder::First, [3, 1, 4, 2]),
        (true, NullOrder::Last, [1, 4, 2, 3]),
    ] {
        let plan = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
            input: fixture.scan(),
            keys: vec![OrderKey {
                column: column("items"),
                descending,
                nulls: Some(nulls),
            }],
        }));
        let output = collect(&plan);
        assert_eq!(
            output
                .iter()
                .map(|row| row["id"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            ids
        );
    }
    let plan = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
        input: twice(&fixture),
        columns: vec![column("id"), column("items")],
        expressions: vec![
            WindowExpression {
                output_column: "rank".into(),
                function: WindowFunction::Rank,
                partition_by: vec![],
                order_by: vec![OrderKey {
                    column: column("items"),
                    descending: false,
                    nulls: Some(NullOrder::First),
                }],
            },
            WindowExpression {
                output_column: "within".into(),
                function: WindowFunction::RowNumber,
                partition_by: vec![column("items")],
                order_by: vec![OrderKey {
                    column: column("id"),
                    descending: false,
                    nulls: None,
                }],
            },
        ],
    }));
    let payload = [json!([9, null]), json!([]), json!(null), json!([-4])];
    let ranks = [7, 3, 1, 5];
    let expected = (0..8)
        .map(
            |row| json!({"id":row%4+1,"items":payload[row%4],"rank":ranks[row%4],"within":row/4+1}),
        )
        .collect::<Vec<_>>();
    assert_eq!(collect(&plan), expected);
}

#[test]
fn native_nested_keys_membership_quantifiers_and_correlation_preserve_sql_nulls() {
    use crate::relational_query::{
        VortexRelationalQuantifier as Quantifier, VortexRelationalSubquery,
        VortexRelationalSubqueryKind as SubqueryKind,
    };
    let fixture = fixture();
    let pair = || VortexRelationalJoinKey {
        left: column("items"),
        right: column("items"),
    };
    let first_two =
        VortexRelationalPlan::Limit(Box::new(crate::relational_query::VortexRelationalLimit {
            input: fixture.scan(),
            offset: 0,
            count: 2,
        }));
    for (kind, correlation, expected) in [
        (
            SubqueryKind::In {
                columns: vec![pair()],
            },
            vec![],
            json!([true, true, null, false]),
        ),
        (
            SubqueryKind::Quantified {
                columns: pair(),
                comparison: ComparisonOp::Gt,
                quantifier: Quantifier::Any,
            },
            vec![],
            json!([true, false, null, true]),
        ),
        (
            SubqueryKind::Quantified {
                columns: pair(),
                comparison: ComparisonOp::Gt,
                quantifier: Quantifier::All,
            },
            vec![],
            json!([false, false, null, false]),
        ),
        (
            SubqueryKind::Exists,
            vec![pair()],
            json!([true, true, false, false]),
        ),
    ] {
        let plan = VortexRelationalPlan::Subquery(Box::new(VortexRelationalSubquery {
            input: fixture.scan(),
            relation: first_two.clone(),
            kind,
            correlation,
            negated: false,
            output_column: "matches".into(),
        }));
        let values = collect(&plan)
            .into_iter()
            .map(|row| row["matches"].clone())
            .collect::<Vec<_>>();
        assert_eq!(json!(values), expected);
    }
}

#[test]
fn native_nested_state_duplicate_masks_sampling_and_rolling_count_keep_existing_rules() {
    let fixture = fixture();
    for (keep, mask) in [
        (
            Keep::First,
            vec![false, false, false, false, true, true, true, true],
        ),
        (
            Keep::Last,
            vec![true, true, true, true, false, false, false, false],
        ),
        (Keep::AllDuplicates, vec![true; 8]),
    ] {
        let mut req = request(Kind::DuplicateMaskRows, &["items"]);
        req.duplicate_keep = keep;
        assert_eq!(
            collect(&unary(twice(&fixture), req)),
            mask.into_iter()
                .map(|value| json!({"duplicated":value}))
                .collect::<Vec<_>>()
        );
    }
    let mut req = request(Kind::SampleRows, &["id", "items"]);
    req.source_order_limit = Some(4);
    req.sample_seed = Some(7);
    assert_eq!(
        collect(&unary(fixture.scan(), req)),
        collect(&fixture.scan())
    );
    let mut req = request(Kind::RollingWindowRows, &["items"]);
    req.rolling_window = Some(crate::query_primitive::VortexRollingWindowRequest::new(
        column("items"),
        "n".into(),
        3,
        1,
        "count".into(),
    ));
    assert_eq!(
        collect(&unary(fixture.scan(), req)),
        [1, 2, 2, 2].map(|n| json!({"n":n}))
    );
}
