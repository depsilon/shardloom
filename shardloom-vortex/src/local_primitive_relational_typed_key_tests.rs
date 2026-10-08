use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure as Measure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalProject, VortexRelationalQuantifier,
        VortexRelationalSubquery, VortexRelationalSubqueryKind, VortexRelationalWindow,
        VortexRelationalWindowExpression, VortexRelationalWindowFunction,
    },
};
use shardloom_core::{
    ComparisonOp, ExprId, Expression, ExpressionKind, LogicalDType, ScalarValue, UnaryOp,
};

const FIELDS: [&str; 4] = ["payload", "amount", "day", "instant"];
const DESCENDING: [[usize; 4]; 4] = [[3, 0, 1, 2], [0, 3, 1, 2], [1, 3, 0, 2], [1, 3, 0, 2]];

fn column(name: &str) -> ColumnRef {
    ColumnRef::new(name).unwrap()
}
fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("typed-key").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expression(ExpressionKind::Column(column(name)))
}
fn function(name: &str, args: Vec<Expression>) -> Expression {
    expression(ExpressionKind::FunctionCall {
        name: name.into(),
        args,
    })
}
fn comparison(left: Expression, right: Expression, op: ComparisonOp) -> Expression {
    expression(ExpressionKind::Compare {
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
fn aggregate(
    input: VortexRelationalPlan,
    groups: &[&str],
    measures: Vec<Measure>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: groups.iter().map(|name| column(name)).collect(),
        measures,
    }))
}
fn measure(function: &str, name: Option<&str>, output: &str) -> Measure {
    Measure::new(function, name.map(column), output.into())
}
fn duplicate(input: VortexRelationalPlan) -> VortexRelationalPlan {
    VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: input.clone(),
        right: input,
        kind: SetKind::UnionAll,
    }))
}

#[test]
fn native_typed_keys_spill_merge_preserves_domains_stable_ties_and_cleanup() {
    use crate::relational_query::VortexRelationalSpillPolicy;

    let ids = (0..24_013u32).rev().collect::<Vec<_>>();
    let binary = [Some(&b"\xff\0"[..]), Some(&b""[..]), Some(&b"\0"[..]), None];
    let decimal = [Some(-DECIMAL_EDGE), Some(0), Some(DECIMAL_EDGE), None];
    let day = [Some(i32::MIN), Some(-1), Some(i32::MAX), None];
    let instant = [Some(i64::MIN), Some(-1), Some(i64::MAX), None];
    let category = |id: u32| usize::try_from(id % 4).unwrap();
    let array = StructArray::new(
        FieldNames::from(["id", "payload", "amount", "day", "instant"]),
        vec![
            PrimitiveArray::from_iter(ids.iter().copied()).into_array(),
            VarBinArray::from(
                ids.iter()
                    .map(|id| binary[category(*id)])
                    .collect::<Vec<_>>(),
            )
            .into_array(),
            DecimalArray::from_option_iter(
                ids.iter().map(|id| decimal[category(*id)]),
                DecimalDType::new(38, 6),
            )
            .into_array(),
            ExtensionArray::new(
                Date::new(TimeUnit::Days, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter(ids.iter().map(|id| day[category(*id)]))
                    .into_array(),
            )
            .into_array(),
            ExtensionArray::new(
                Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
                PrimitiveArray::from_option_iter(ids.iter().map(|id| instant[category(*id)]))
                    .into_array(),
            )
            .into_array(),
        ],
        ids.len(),
        Validity::NonNullable,
    )
    .into_array();
    let dtype = array.dtype().clone();
    let fixture = Fixture::new(array, 512);
    for name in FIELDS {
        let prepared = prepare_relational(&sort(fixture.scan(), name), policy())
            .unwrap()
            .with_spill(VortexRelationalSpillPolicy::new(&fixture.0, 64 << 20, 1 << 20).unwrap())
            .unwrap();
        assert_eq!(prepared.output_dtype(), Some(dtype.clone()));
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut expected_ids = ids.clone();
        // Independently specified descending category ranks, with nulls last.
        let ranks = if name == "payload" {
            [0, 2, 1, 3]
        } else {
            [2, 1, 0, 3]
        };
        expected_ids.sort_by_key(|id| ranks[category(*id)]);
        let expected = expected_ids
            .into_iter()
            .map(|id| {
                let index = category(id);
                let payload = [Some("ff00"), Some(""), Some("00"), None][index];
                json!({"id":id,"payload":payload,
                "amount":decimal[index].map(|value| format!("decimal128(38,6):{value}")),
                "day":day[index],"instant":instant[index]})
            })
            .collect::<Vec<_>>();
        let collected = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&collected), expected);
        let report = collected.execution.spill.as_ref().unwrap();
        assert!(report.runs_written >= 3);
        assert!(report.merge_passes >= 1);
        assert_eq!(report.max_open_runs, 2);
        assert!(report.peak_disk_bytes <= report.quota_bytes);
        assert!(report.owned_cleanup_completed);
        assert!(
            collected
                .execution
                .native_io_certificate
                .side_effects
                .spill_io_performed
        );
        drop(collected);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read_dir(&fixture.0)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            [std::ffi::OsString::from("input.vortex")]
        );
    }
}

#[test]
fn native_typed_keys_order_and_set_semantics_preserve_full_values() {
    let fixture = fixture();
    let expected = expected();
    for (name, permutation) in FIELDS.into_iter().zip(DESCENDING) {
        assert_eq!(
            collect(&sort(fixture.scan(), name)),
            permutation.map(|row| expected[row].clone())
        );
    }
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let plan = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
            left: duplicate(fixture.scan()),
            right: fixture.scan(),
            kind,
        }));
        assert_eq!(
            collect(&plan),
            if kind == SetKind::Except {
                vec![]
            } else {
                expected.clone()
            }
        );
    }
}

#[test]
fn native_typed_keys_join_does_not_match_nulls_and_retains_logical_types() {
    let fixture = fixture();
    for name in FIELDS {
        let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: fixture.scan(),
            right: fixture.scan(),
            kind: JoinKind::Full,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: column(name),
                right: column(name),
            }],
            columns: [
                (Side::Left, "id", "id"),
                (Side::Right, "id", "matched"),
                (Side::Left, name, name),
            ]
            .into_iter()
            .map(|(side, field, output)| VortexRelationalJoinColumn {
                side,
                column: column(field),
                output_column: output.into(),
            })
            .collect(),
        }));
        let mut rows = expected().into_iter().map(|row| {
            json!({"id":row["id"],"matched":if row[name].is_null() { Value::Null } else { row["id"].clone() },name:row[name]})
        }).collect::<Vec<_>>();
        rows.push(json!({"id":null,"matched":3,name:null}));
        assert_eq!(collect(&plan), rows);
        super::super::join_spill_tests::assert_ordered(&fixture, &plan, &rows);
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let dtype = prepared.output_dtype().unwrap();
        let source = prepare_relational(&fixture.scan(), policy())
            .unwrap()
            .output_dtype()
            .unwrap()
            .clone();
        assert_eq!(
            dtype.as_struct_fields_opt().unwrap().field(name).unwrap(),
            source
                .as_struct_fields_opt()
                .unwrap()
                .field(name)
                .unwrap()
                .as_nullable()
        );
    }
}

#[test]
fn native_typed_keys_grouping_count_distinct_and_extrema_keep_exact_values() {
    let fixture = fixture();
    let source = expected();
    for (name, permutation) in FIELDS.into_iter().zip(DESCENDING) {
        let measures = || {
            vec![
                measure("count", None, "rows"),
                measure("count", Some(name), "present"),
                measure("count_distinct", Some(name), "unique"),
                measure("min", Some(name), "low"),
                measure("max", Some(name), "high"),
            ]
        };
        let grouped = aggregate(duplicate(fixture.scan()), &[name], measures());
        let rows = source
            .iter()
            .map(|row| {
                json!({name:row[name],"rows":2,"present":if row[name].is_null(){0}else{2},
            "unique":i32::from(!row[name].is_null()),"low":row[name],"high":row[name]})
            })
            .collect::<Vec<_>>();
        assert_eq!(collect(&grouped), rows);
        let scalar = aggregate(duplicate(fixture.scan()), &[], measures());
        assert_eq!(
            collect(&scalar),
            vec![json!({"rows":8,"present":6,"unique":3,
            "low":source[permutation[2]][name],"high":source[permutation[0]][name]})]
        );
        let nulls = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: fixture.scan(),
            offset: 2,
            count: 1,
        }));
        assert_eq!(
            collect(&aggregate(nulls, &[], measures())),
            vec![json!({"rows":1,"present":0,"unique":0,"low":null,"high":null})]
        );
        let empty = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: fixture.scan(),
            offset: 0,
            count: 0,
        }));
        assert_eq!(
            collect(&aggregate(empty, &[], measures())),
            vec![json!({"rows":0,"present":0,"unique":0,"low":null,"high":null})]
        );
    }
}

#[test]
fn native_typed_keys_window_partition_and_order_share_exact_domains() {
    let fixture = fixture();
    for (name, permutation) in FIELDS.into_iter().zip(DESCENDING) {
        let plan = VortexRelationalPlan::Window(Box::new(VortexRelationalWindow {
            input: duplicate(fixture.scan()),
            columns: vec![column("id"), column(name)],
            expressions: vec![
                VortexRelationalWindowExpression {
                    output_column: "rank".into(),
                    function: VortexRelationalWindowFunction::Rank,
                    partition_by: vec![],
                    order_by: vec![VortexRelationalOrderKey {
                        column: column(name),
                        descending: true,
                        nulls: Some(VortexRelationalNullOrder::Last),
                    }],
                    frame: None,
                },
                VortexRelationalWindowExpression {
                    output_column: "rn".into(),
                    function: VortexRelationalWindowFunction::RowNumber,
                    partition_by: vec![column(name)],
                    order_by: vec![VortexRelationalOrderKey {
                        column: column("id"),
                        descending: false,
                        nulls: None,
                    }],
                    frame: None,
                },
            ],
        }));
        let mut rows = Vec::new();
        for repeat in 1..=2 {
            for (index, row) in expected().into_iter().enumerate() {
                let rank = permutation
                    .iter()
                    .position(|value| *value == index)
                    .unwrap()
                    * 2
                    + 1;
                rows.push(json!({"id":row["id"],name:row[name],"rank":rank,"rn":repeat}));
            }
        }
        assert_eq!(collect(&plan), rows);
    }
}

#[test]
fn native_typed_keys_membership_quantifiers_and_correlation_keep_null_rules() {
    let fixture = fixture();
    for (name, permutation) in FIELDS.into_iter().zip(DESCENDING) {
        let columns = VortexRelationalJoinKey {
            left: column(name),
            right: column(name),
        };
        for (kind, correlation, wanted) in [
            (
                VortexRelationalSubqueryKind::In {
                    columns: vec![columns.clone()],
                },
                vec![],
                json!([true, true, null, true]),
            ),
            (
                VortexRelationalSubqueryKind::Exists,
                vec![columns.clone()],
                json!([true, true, false, true]),
            ),
            (
                VortexRelationalSubqueryKind::Quantified {
                    columns,
                    comparison: ComparisonOp::Gt,
                    quantifier: VortexRelationalQuantifier::Any,
                },
                vec![],
                json!(
                    (0..4)
                        .map(|row| if row == 2 || row == permutation[2] {
                            Value::Null
                        } else {
                            json!(true)
                        })
                        .collect::<Vec<_>>()
                ),
            ),
        ] {
            let plan = VortexRelationalPlan::Subquery(Box::new(VortexRelationalSubquery {
                input: fixture.scan(),
                relation: fixture.scan(),
                kind,
                correlation,
                output_column: "matched".into(),
                evaluation_guard: None,
                negated: false,
            }));
            let actual = collect(&plan)
                .into_iter()
                .map(|row| row["matched"].clone())
                .collect::<Vec<_>>();
            assert_eq!(json!(actual), wanted, "{name}");
        }
    }
}

#[test]
fn native_typed_keys_null_selection_and_comparison_expressions_preserve_types() {
    let fixture = fixture();
    for name in FIELDS {
        let nullable = expression(ExpressionKind::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(col(name)),
        });
        let plan = project(
            fixture.scan(),
            vec![
                ("id", col("id")),
                (name, col(name)),
                ("missing", nullable.clone()),
                ("equal", comparison(col(name), col(name), ComparisonOp::Eq)),
                ("erased", function("nullif", vec![col(name), col(name)])),
                (
                    "selected",
                    function("case_when", vec![nullable, col(name), col(name)]),
                ),
                ("filled", function("coalesce", vec![col(name), col(name)])),
            ],
        );
        let rows = expected().into_iter().map(|row| json!({"id":row["id"],name:row[name],
            "missing":row[name].is_null(),"equal":if row[name].is_null(){Value::Null}else{json!(true)},
            "erased":null,"selected":row[name],"filled":row[name]})).collect::<Vec<_>>();
        assert_eq!(collect(&plan), rows);
    }
}

#[test]
fn native_typed_keys_keep_incompatible_numeric_and_mixed_type_denials_on_empty_inputs() {
    let fixture = fixture();
    let empty = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: fixture.scan(),
        offset: 0,
        count: 0,
    }));
    for name in FIELDS {
        for value in [
            function("lower", vec![col(name)]),
            comparison(
                col(name),
                expression(ExpressionKind::Literal(ScalarValue::Int64(0))),
                ComparisonOp::Eq,
            ),
        ] {
            assert!(
                prepare_relational(&project(empty.clone(), vec![("invalid", value)]), policy())
                    .is_err(),
                "{name}"
            );
        }
        assert!(
            prepare_relational(
                &project(
                    empty.clone(),
                    vec![(
                        "text",
                        expression(ExpressionKind::Cast {
                            expr: Box::new(col(name)),
                            target_dtype: LogicalDType::Utf8,
                        })
                    )]
                ),
                policy()
            )
            .is_ok()
        );
        for value in [
            function("abs", vec![col(name)]),
            expression(ExpressionKind::TryCast {
                expr: Box::new(col(name)),
                target_dtype: LogicalDType::Float64,
            }),
        ] {
            assert_eq!(
                prepare_relational(&project(empty.clone(), vec![("numeric", value)]), policy())
                    .is_ok(),
                name == "amount"
            );
        }
        for function in ["sum", "avg"] {
            assert_eq!(
                prepare_relational(
                    &aggregate(
                        empty.clone(),
                        &[],
                        vec![measure(function, Some(name), "invalid")]
                    ),
                    policy()
                )
                .is_ok(),
                name == "amount"
            );
        }
    }
    assert!(
        prepare_relational(
            &project(
                empty,
                vec![(
                    "invalid",
                    comparison(col("day"), col("instant"), ComparisonOp::Eq)
                )]
            ),
            policy()
        )
        .is_err()
    );
}
