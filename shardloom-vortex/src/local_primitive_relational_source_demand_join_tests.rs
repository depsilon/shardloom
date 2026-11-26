//! Parent demand removes unused join payload only for admitted owned sources.

use super::*;
#[cfg(debug_assertions)]
use crate::local_primitives::native_relational_observation::Session as Observation;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalJoin, VortexRelationalJoinColumn,
        VortexRelationalJoinKey, VortexRelationalJoinKind as JoinKind,
        VortexRelationalSide as Side,
    },
};
use serde_json::json;

#[path = "local_primitive_relational_source_demand_join_lifecycle_tests.rs"]
mod lifecycle_tests;

fn joined(left: &ParquetFixture, right: &ParquetFixture, kind: JoinKind) -> VortexRelationalPlan {
    let mut columns = Vec::new();
    for (side, prefix) in [(Side::Left, "left"), (Side::Right, "right")] {
        if side == Side::Right && matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            continue;
        }
        for name in ["n", "other", "text"] {
            columns.push(VortexRelationalJoinColumn {
                side,
                column: ColumnRef::new(name).unwrap(),
                output_column: format!("{prefix}_{name}"),
            });
        }
    }
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: left.scan(&[], None),
        right: right.scan(&[], None),
        kind,
        keys: if kind == JoinKind::Cross {
            Vec::new()
        } else {
            vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            }]
        },
        condition: None,
        columns,
    }))
}

fn prepare(fixtures: &[&ParquetFixture], plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        for fixture in fixtures {
            let source = LocalColumnarSource::admit(
                &fixture.path(),
                "parquet",
                &preparation.shared_memory_pool(),
                &preparation.cancellation_token(),
            )?;
            preparation.register_local_source(
                DatasetUri::new(fixture.path().display().to_string())?,
                source,
            )?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

fn aggregate(input: VortexRelationalPlan, mixed: bool) -> VortexRelationalPlan {
    let mut measures = vec![VortexSimpleAggregateMeasure::new(
        "count",
        None,
        "rows".into(),
    )];
    if mixed {
        measures.extend(["sum", "min", "max"].map(|function| {
            VortexSimpleAggregateMeasure::new(
                function,
                Some(ColumnRef::new("left_other").unwrap()),
                function.into(),
            )
        }));
    }
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: Vec::new(),
        grouping_sets: None,
        measures,
    }))
}

#[test]
fn parquet_join_parent_projection_retains_hidden_keys_and_omits_unused_payload() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    left.corrupt_chunk(3, 1);
    let plan = project(
        joined(&left, &right, JoinKind::Left),
        &[("right_text", "label"), ("right_text", "duplicate")],
    );
    let prepared = prepare(&[&left, &right], &plan);
    let initial = prepared.snapshot().memory.reserved_bytes;
    #[cfg(debug_assertions)]
    let observation = Observation::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..16)
            .map(|n| {
                let text = (n % 4 != 0).then(|| format!("row-{n}-λ"));
                json!({"label":text,"duplicate":text})
            })
            .collect::<Vec<_>>()
    );
    #[cfg(debug_assertions)]
    {
        let counts = observation.finish();
        assert_eq!(
            (counts.join_output_rows, counts.join_output_cells),
            (16, 16)
        );
    }
    let input = result.execution.input.as_ref().unwrap();
    assert_eq!(
        (
            input.columnar_projected_columns,
            input.columnar_available_columns
        ),
        (3, 6)
    );
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .source_capability_report
            .fallback_attempted
    );
    drop(result);
    prepared
        .session
        .shared_memory_pool()
        .reclaim_optional_memory();
    assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
}

#[test]
fn parquet_join_count_and_mixed_measures_construct_only_consumed_columns() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    for kind in [
        JoinKind::Inner,
        JoinKind::Left,
        JoinKind::Right,
        JoinKind::Full,
        JoinKind::LeftSemi,
        JoinKind::LeftAnti,
        JoinKind::Cross,
    ] {
        for mixed in [false, true] {
            let prepared = prepare(
                &[&left, &right],
                &aggregate(joined(&left, &right, kind), mixed),
            );
            #[cfg(debug_assertions)]
            let observation = Observation::start();
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let rows = match kind {
                JoinKind::LeftAnti => 0,
                JoinKind::Cross => 256,
                _ => 16,
            };
            let expected = if mixed {
                if rows == 0 {
                    json!({"rows":0,"sum":null,"min":null,"max":null})
                } else {
                    json!({"rows":rows,"sum":if rows == 256 {27_520.0} else {1720.0},"min":100,"max":115})
                }
            } else {
                json!({"rows":rows})
            };
            assert_eq!(
                json_rows(&result),
                vec![expected],
                "kind={kind:?}, mixed={mixed}"
            );
            let direct = kind == JoinKind::Inner;
            let work = result
                .execution
                .native_io_certificate
                .join_work
                .direct_aggregate;
            assert_eq!(work.aggregate_rows, if direct { rows } else { 0 });
            if direct {
                // Owned-source demand already removed unused fields. The one
                // declared match column is validated without a joined copy.
                assert_eq!((work.match_rows, work.validated_cells), (rows, rows));
            }
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                let copied_rows = if direct { 0 } else { rows };
                assert_eq!(counts.join_output_rows, copied_rows);
                assert_eq!(
                    counts.join_output_cells, copied_rows,
                    "kind={kind:?}, mixed={mixed}"
                );
            }
        }
    }
}

fn key_fixture(keys: &[Option<i64>]) -> ParquetFixture {
    ParquetFixture::with_batch_rows(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(keys.to_vec())),
                Arc::new(Int64Array::from_iter_values(
                    0..i64::try_from(keys.len()).unwrap(),
                )),
                Arc::new(StringArray::from_iter(
                    (0..keys.len()).map(|n| Some(format!("λ-{n}"))),
                )),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
        2,
    )
}

fn expected_pairs(
    left: &[Option<i64>],
    right: &[Option<i64>],
    kind: JoinKind,
) -> Vec<serde_json::Value> {
    let mut rows = Vec::new();
    let mut matched = vec![false; right.len()];
    for (l, key) in left.iter().enumerate() {
        let mut found = false;
        for (r, other) in right.iter().enumerate() {
            if kind == JoinKind::Cross || (key.is_some() && key == other) {
                matched[r] = true;
                found = true;
                if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                    rows.push(json!({"l":l,"r":r}));
                }
            }
        }
        match kind {
            JoinKind::LeftSemi if found => rows.push(json!({"l":l})),
            JoinKind::LeftAnti if !found => rows.push(json!({"l":l})),
            JoinKind::Left | JoinKind::Full if !found => rows.push(json!({"l":l,"r":null})),
            _ => {}
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (r, matched) in matched.into_iter().enumerate() {
            if !matched {
                rows.push(json!({"l":null,"r":r}));
            }
        }
    }
    rows
}

#[test]
fn parquet_join_projection_preserves_sparse_null_duplicate_outer_and_empty_inputs() {
    let keys = [
        Some(i64::MIN),
        Some(3),
        None,
        Some(3),
        Some(i64::MAX),
        Some(-71),
    ];
    let others = [None, Some(3), Some(3), Some(i64::MAX), Some(19)];
    for (left_keys, right_keys) in [
        (&keys[..], &others[..]),
        (&keys[..0], &others[..]),
        (&keys[..], &others[..0]),
        (&keys[..0], &others[..0]),
    ] {
        let left = key_fixture(left_keys);
        let right = key_fixture(right_keys);
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::LeftSemi,
            JoinKind::LeftAnti,
            JoinKind::Cross,
        ] {
            let names = if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                vec![("left_other", "l")]
            } else {
                vec![("left_other", "l"), ("right_other", "r")]
            };
            let plan = project(joined(&left, &right, kind), &names);
            let prepared = prepare(&[&left, &right], &plan);
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&result),
                expected_pairs(left_keys, right_keys, kind),
                "kind={kind:?}, left={left_keys:?}, right={right_keys:?}"
            );
            let input = result.execution.input.as_ref().unwrap();
            assert!(input.end_of_input_observed && input.output_ownership_detached);
        }
    }
}

#[test]
fn parquet_join_on_and_computed_dependencies_are_never_removed() {
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    right.corrupt_chunk(3, 2);
    let VortexRelationalPlan::Join(mut join) = joined(&left, &right, JoinKind::Inner) else {
        unreachable!()
    };
    join.condition = Some(Expression::new(
        ExprId::new("on").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(column("left.other")),
            op: ComparisonOp::Eq,
            right: Box::new(column("right.other")),
        },
    ));
    let plan = aggregate(VortexRelationalPlan::Join(join), false);
    let prepared = prepare(&[&left, &right], &plan);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"rows":16})]);
    assert_eq!(
        result
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_projected_columns,
        4
    );
    drop(result);
    right.corrupt_chunk(3, 1);
    assert!(
        prepare(&[&left, &right], &plan)
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );

    let left = key_fixture(&[Some(0), Some(i64::MAX)]);
    let right = key_fixture(&[Some(0)]);
    let VortexRelationalPlan::Join(mut join) = joined(&left, &right, JoinKind::Inner) else {
        unreachable!()
    };
    join.left = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input: left.scan(&[], None),
        expressions: vec![
            ("n".into(), column("n")),
            ("other".into(), column("other")),
            (
                "text".into(),
                Expression::new(
                    ExprId::new("late").unwrap(),
                    ExpressionKind::Binary {
                        left: Box::new(column("n")),
                        op: shardloom_core::BinaryOp::Add,
                        right: Box::new(Expression::literal(
                            ExprId::new("one").unwrap(),
                            ScalarValue::Int64(1),
                        )),
                    },
                ),
            ),
        ],
    }));
    let plan = aggregate(VortexRelationalPlan::Join(join), false);
    let prepared = prepare(&[&left, &right], &plan);
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap();
    assert!(error.to_string().contains("overflow"), "{error}");
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn parquet_join_nested_count_and_cross_projection_keep_native_row_records_and_spill() {
    use crate::relational_query::VortexRelationalSpillPolicy;
    let left = ParquetFixture::new();
    let right = ParquetFixture::new();
    for cross in [false, true] {
        let inner = joined(&left, &right, JoinKind::Inner);
        let plan = aggregate(
            VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                left: inner,
                right: project(right.scan(&[], None), &[("n", "n"), ("other", "other")]),
                kind: if cross {
                    JoinKind::Cross
                } else {
                    JoinKind::Inner
                },
                keys: if cross {
                    vec![]
                } else {
                    vec![VortexRelationalJoinKey {
                        left: ColumnRef::new("left_n").unwrap(),
                        right: ColumnRef::new("n").unwrap(),
                    }]
                },
                condition: None,
                columns: vec![
                    VortexRelationalJoinColumn {
                        side: Side::Left,
                        column: ColumnRef::new("left_other").unwrap(),
                        output_column: "left_other".into(),
                    },
                    VortexRelationalJoinColumn {
                        side: Side::Right,
                        column: ColumnRef::new("other").unwrap(),
                        output_column: "unused".into(),
                    },
                ],
            })),
            false,
        );
        for spill in [false, true] {
            let mut prepared = prepare(&[&left, &right], &plan);
            let runs = left.0.join("join-runs");
            if spill {
                fs::create_dir_all(&runs).unwrap();
                prepared = prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(runs.clone(), 128 << 20, 1 << 20).unwrap(),
                    )
                    .unwrap();
            }
            let initial = prepared.snapshot().memory.reserved_bytes;
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&result),
                vec![json!({"rows":if cross {256} else {16}})]
            );
            assert_eq!(
                result.execution.ordered_join_stages,
                if spill { 2 } else { 0 }
            );
            drop(result);
            prepared
                .session
                .shared_memory_pool()
                .reclaim_optional_memory();
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            if spill {
                assert_eq!(fs::read_dir(&runs).unwrap().count(), 0);
            }
        }
    }
}

fn cmp_i64(
    column: &str,
    op: shardloom_core::ComparisonOp,
    value: i64,
) -> shardloom_core::Expression {
    shardloom_core::Expression::new(
        shardloom_core::ExprId::new("cmp").unwrap(),
        shardloom_core::ExpressionKind::Compare {
            left: Box::new(super::column(column)),
            op,
            right: Box::new(shardloom_core::Expression::literal(
                shardloom_core::ExprId::new("lit").unwrap(),
                shardloom_core::ScalarValue::Int64(value),
            )),
        },
    )
}

fn and_expr(
    left: shardloom_core::Expression,
    right: shardloom_core::Expression,
) -> shardloom_core::Expression {
    shardloom_core::Expression::new(
        shardloom_core::ExprId::new("and").unwrap(),
        shardloom_core::ExpressionKind::Binary {
            left: Box::new(left),
            op: shardloom_core::BinaryOp::And,
            right: Box::new(right),
        },
    )
}

#[test]
fn parquet_post_join_filter_pushdown_prunes_row_groups_on_inner_and_three_way_joins() {
    use shardloom_core::ComparisonOp;
    let a = ParquetFixture::new();
    let b = ParquetFixture::new();
    let c = ParquetFixture::new();

    // Two-way INNER JOIN with side-specific AND conjuncts pushes both into Parquet row-group pruning.
    let two_way = project(
        VortexRelationalPlan::Filter(Box::new(crate::relational_query::VortexRelationalFilter {
            input: joined(&a, &b, JoinKind::Inner),
            predicate: and_expr(
                cmp_i64("left_n", ComparisonOp::GtEq, 12),
                cmp_i64("right_other", ComparisonOp::GtEq, 112),
            ),
        })),
        &[("left_n", "n"), ("right_other", "other")],
    );
    let prepared = prepare(&[&a, &b], &two_way);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            json!({"n": 12, "other": 112}),
            json!({"n": 13, "other": 113}),
            json!({"n": 14, "other": 114}),
            json!({"n": 15, "other": 115}),
        ]
    );
    let report = result.execution.input.as_ref().unwrap();
    // Each fixture has 4 row groups; pushdown reads only the final row group of each table.
    assert_eq!(report.columnar_row_groups_total, 8);
    assert_eq!(report.columnar_row_groups_read, 2);

    // Three-way INNER JOIN pushes filters recursively down through both join boundaries.
    let inner_ab = joined(&a, &b, JoinKind::Inner);
    let abc = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: inner_ab,
        right: c.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("left_n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("left_n").unwrap(),
                output_column: "left_n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("right_other").unwrap(),
                output_column: "b_other".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("other").unwrap(),
                output_column: "c_other".into(),
            },
        ],
    }));
    let three_way = project(
        VortexRelationalPlan::Filter(Box::new(crate::relational_query::VortexRelationalFilter {
            input: abc,
            predicate: and_expr(
                and_expr(
                    cmp_i64("left_n", ComparisonOp::GtEq, 12),
                    cmp_i64("b_other", ComparisonOp::GtEq, 114),
                ),
                cmp_i64("c_other", ComparisonOp::GtEq, 114),
            ),
        })),
        &[("left_n", "n"), ("c_other", "c_other")],
    );
    let prepared_three = prepare(&[&a, &b, &c], &three_way);
    let result_three = prepared_three
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result_three),
        vec![
            json!({"n": 14, "c_other": 114}),
            json!({"n": 15, "c_other": 115}),
        ]
    );
    let report_three = result_three.execution.input.as_ref().unwrap();
    assert_eq!(report_three.columnar_row_groups_total, 12);
    assert_eq!(report_three.columnar_row_groups_read, 3);
}

#[test]
fn post_join_filter_pushdown_respects_outer_join_and_fallible_expression_contracts() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::file::properties::EnabledStatistics;
    use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, LogicalDType, UnaryOp};

    let left = ParquetFixture::new();
    let right_sparse = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values([0, 1, 12, 13])),
                Arc::new(Int64Array::from_iter_values([100, 101, 112, 113])),
                Arc::new(arrow_array::StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );

    // LEFT JOIN: left_n >= 12 pushes to left scan; right_n IS NULL MUST stay above LEFT JOIN.
    let right_is_null = Expression::new(
        ExprId::new("isnull").unwrap(),
        ExpressionKind::Unary {
            op: UnaryOp::IsNull,
            expr: Box::new(super::column("right_n")),
        },
    );
    let anti_via_left = project(
        VortexRelationalPlan::Filter(Box::new(crate::relational_query::VortexRelationalFilter {
            input: joined(&left, &right_sparse, JoinKind::Left),
            predicate: and_expr(cmp_i64("left_n", ComparisonOp::GtEq, 12), right_is_null),
        })),
        &[("left_n", "n"), ("right_n", "r")],
    );
    let prepared_left = prepare(&[&left, &right_sparse], &anti_via_left);
    let result_left = prepared_left
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result_left),
        vec![json!({"n": 14, "r": null}), json!({"n": 15, "r": null}),]
    );
    assert_eq!(
        result_left
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_row_groups_read,
        2 // left pruned 3 row groups + right read its 1 row group
    );

    // Fallible post-join predicate (CAST(left_text AS INT64) = 1) MUST NOT evaluate before Inner join
    // drops all unmatched rows when right_n >= 999 prunes right to empty!
    let cast_eq = Expression::new(
        ExprId::new("cmp").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(Expression::cast(
                ExprId::new("fallible_cast").unwrap(),
                super::column("left_text"),
                LogicalDType::Int64,
            )),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::literal(
                ExprId::new("one").unwrap(),
                shardloom_core::ScalarValue::Int64(1),
            )),
        },
    );
    let empty_join = project(
        VortexRelationalPlan::Filter(Box::new(crate::relational_query::VortexRelationalFilter {
            input: joined(&left, &right_sparse, JoinKind::Inner),
            predicate: and_expr(cast_eq, cmp_i64("right_n", ComparisonOp::GtEq, 999)),
        })),
        &[("left_n", "n")],
    );
    let prepared_fallible = prepare(&[&left, &right_sparse], &empty_join);
    let result_fallible = prepared_fallible
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result_fallible), Vec::<serde_json::Value>::new());
}

#[test]
fn vortex_multifile_inner_join_prunes_probe_files_via_runtime_filter() {
    use super::super::{Fixture, keyed};
    use crate::relational_query::{VortexRelationalSet, VortexRelationalSetKind};
    use vortex::array::expr::stats::Stat;

    let stats = vec![Stat::Min, Stat::Max, Stat::NullCount];
    let strategy = |rows: usize| {
        crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
            rows.max(1),
        )
    };
    let part_low = Fixture::with_strategy_statistics(
        keyed(&[Some(1), Some(2), Some(3)], &[10, 20, 30]),
        3,
        vec![],
        strategy(1),
        stats.clone(),
    );
    let part_high = Fixture::with_strategy_statistics(
        keyed(&[Some(100), Some(101), Some(102)], &[110, 111, 112]),
        3,
        vec![],
        strategy(1),
        stats.clone(),
    );
    let build = Fixture::with_strategy_statistics(
        keyed(&[Some(101)], &[777]),
        1,
        vec![],
        strategy(1),
        stats,
    );
    let union_probe = VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
        left: part_low.scan(),
        right: part_high.scan(),
        kind: VortexRelationalSetKind::UnionAll,
    }));
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: union_probe,
        right: build.scan(),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("entity").unwrap(),
            right: ColumnRef::new("entity").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("entity").unwrap(),
                output_column: "k".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("amount").unwrap(),
                output_column: "v".into(),
            },
        ],
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), vec![json!({"k": 101, "v": 777})]);
    let scans = &result.execution.native_io_certificate.file_scans;
    assert_eq!(scans.contracts.len(), 3);
    assert!(
        scans
            .contracts
            .iter()
            .any(|contract| contract.whole_file_exclusions == 1 && contract.delivered_rows == 0)
    );
}

#[test]
fn owned_array_source_inner_join_prunes_probe_splits_via_runtime_filter() {
    use crate::owned_array_source::OwnedArraySourceBounds;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    let session =
        ResidentVortexSession::new(policy().resource_envelope.memory_budget_bytes, 1).unwrap();
    let fact_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(0..20_000)),
            Arc::new(Int64Array::from_iter_values((0..20_000).map(|n| n * 10))),
        ],
    )
    .unwrap();
    let dim_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from_iter_values([18_000, 18_500]))],
    )
    .unwrap();
    let make_owned = |batch: RecordBatch| {
        let columns: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        let column_refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        let mem = session
            .with_native_execution_context(&CancellationToken::default(), |ctx| {
                ResidentMemorySource::from_local_arrow_with_ownership(
                    &session,
                    batch,
                    ctx,
                    crate::vortex_ingest::arrow_ownership::ArrowOwnershipPolicy::ValidatedCopy,
                )
            })
            .unwrap();
        mem.prepare_projection(&column_refs, None, None)
            .unwrap()
            .execute_arrays()
            .unwrap()
            .into_owned_array_source(
                OwnedArraySourceBounds::default(),
                &CancellationToken::default(),
            )
            .unwrap()
    };
    let fact = make_owned(fact_batch);
    let dim = make_owned(dim_batch);
    let fact_uri = DatasetUri::new("memory://rf-owned-fact").unwrap();
    let dim_uri = DatasetUri::new("memory://rf-owned-dim").unwrap();
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: fact_uri.clone(),
            projection: ProjectionRequest::All,
            predicate: None,
        }),
        right: VortexRelationalPlan::Scan(VortexRelationalScan {
            source_uri: dim_uri.clone(),
            projection: ProjectionRequest::All,
            predicate: None,
        }),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("k").unwrap(),
            right: ColumnRef::new("k").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("k").unwrap(),
                output_column: "k".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("v").unwrap(),
                output_column: "v".into(),
            },
        ],
    }));
    let prepared = prepare_relational_with_schema_in_session(policy(), &session, |schemas| {
        schemas.register_owned_array_source(fact_uri.clone(), fact.clone())?;
        schemas.register_owned_array_source(dim_uri.clone(), dim.clone())?;
        Ok(plan.clone())
    })
    .unwrap();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            json!({"k": 18_000, "v": 180_000}),
            json!({"k": 18_500, "v": 185_000}),
        ]
    );
    assert!(fact.pruned_splits() >= 2);
}

#[test]
#[allow(clippy::too_many_lines)]
fn parquet_runtime_filters_prune_row_groups_across_small_large_and_star_joins() {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::file::properties::EnabledStatistics;

    let batch_xy = |k1: Vec<i64>, k2: Vec<i64>, val: Vec<i64>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, false),
                Field::new("k2", DataType::Int64, false),
                Field::new("other", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(k1)),
                Arc::new(Int64Array::from_iter_values(k2)),
                Arc::new(Int64Array::from_iter_values(val)),
            ],
        )
        .unwrap()
    };
    let fact = ParquetFixture::with_batch_rows(
        batch_xy(
            (0..32).collect(),
            (100..132).collect(),
            (200..232).collect(),
        ),
        EnabledStatistics::Chunk,
        4,
    );
    let dim_small = ParquetFixture::with_batch_rows(
        batch_xy(vec![20, 22], vec![0, 0], vec![1, 2]),
        EnabledStatistics::Chunk,
        4,
    );
    let dim_k2 = ParquetFixture::with_batch_rows(
        batch_xy(vec![0], vec![122], vec![9]),
        EnabledStatistics::Chunk,
        4,
    );
    let star_left = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact.scan(&[], None),
        right: dim_small.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("k2").unwrap(),
                output_column: "k2".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("other").unwrap(),
                output_column: "other".into(),
            },
        ],
    }));
    let star = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: star_left,
        right: dim_k2.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("k2").unwrap(),
            right: ColumnRef::new("k2").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("other").unwrap(),
                output_column: "other".into(),
            },
        ],
    }));
    let res_star = prepare(&[&fact, &dim_small, &dim_k2], &star)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&res_star), vec![json!({"n": 22, "other": 222})]);
    let rpt_star = res_star.execution.input.as_ref().unwrap();
    assert_eq!(rpt_star.columnar_row_groups_total, 10);
    assert_eq!(rpt_star.columnar_row_groups_read, 3);

    // >64 build keys exercise range predicate + 3-bit Bloom on Parquet.
    let fact_big = ParquetFixture::with_batch_rows(
        batch_xy((0..256).collect(), vec![0; 256], (0..256).collect()),
        EnabledStatistics::Chunk,
        16,
    );
    let dim_big = ParquetFixture::with_batch_rows(
        batch_xy(
            (128..208).filter(|v| v % 2 == 0).chain(208..250).collect(),
            vec![0; 82],
            vec![0; 82],
        ),
        EnabledStatistics::Chunk,
        100,
    );
    let big_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: fact_big.scan(&[], None),
        right: dim_big.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("n").unwrap(),
                output_column: "n".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("other").unwrap(),
                output_column: "other".into(),
            },
        ],
    }));
    let res_big = prepare(&[&fact_big, &dim_big], &big_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let rpt_big = res_big.execution.input.as_ref().unwrap();
    assert_eq!(rpt_big.columnar_row_groups_total, 17);
    assert!(rpt_big.columnar_row_groups_read < rpt_big.columnar_row_groups_total);
    assert_eq!(json_rows(&res_big).len(), 82);
}

#[test]
fn empty_build_short_circuits_probe_scans_and_preserves_outer_semantics() {
    use super::super::{Fixture, keyed};
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::file::properties::EnabledStatistics;

    let fact_pq =
        ParquetFixture::with_batch_rows(ParquetFixture::batch(), EnabledStatistics::Chunk, 4);
    let empty_pq = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("other", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![None::<i64>, None])),
                Arc::new(Int64Array::from_iter_values([1, 2])),
                Arc::new(arrow_array::StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    let res_empty_pq = prepare(
        &[&fact_pq, &empty_pq],
        &joined(&fact_pq, &empty_pq, JoinKind::Inner),
    )
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    assert_eq!(json_rows(&res_empty_pq), [] as [serde_json::Value; 0]);
    assert_eq!(
        res_empty_pq
            .execution
            .input
            .as_ref()
            .unwrap()
            .columnar_row_groups_read,
        1
    );

    // Non-inner join kinds preserve unmatched left rows against an all-null build.
    for (kind, expected_rows) in [
        (JoinKind::Left, 16),
        (JoinKind::Full, 18),
        (JoinKind::LeftAnti, 16),
    ] {
        let rows = prepare(&[&fact_pq, &empty_pq], &joined(&fact_pq, &empty_pq, kind))
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&rows).len(), expected_rows, "{kind:?}");
    }

    // Vortex empty build short-circuit keeps valid FileScan contract and zero probe reads.
    let v_probe = Fixture::new(keyed(&[Some(1), Some(2), Some(3)], &[10, 20, 30]), 1);
    let v_empty = Fixture::new(keyed(&[None, None], &[1, 2]), 1);
    let v_res = prepare_relational(
        &super::super::join(&v_probe, &v_empty, JoinKind::Inner),
        policy(),
    )
    .unwrap()
    .collect_jsonl(&CancellationToken::default())
    .unwrap();
    assert_eq!(json_rows(&v_res), [] as [serde_json::Value; 0]);
    assert!(
        v_res
            .execution
            .native_io_certificate
            .file_scans
            .contracts
            .iter()
            .any(|contract| contract.whole_file_exclusions == 1 && contract.delivered_rows == 0)
    );
}

#[test]
fn mixed_width_i32_probe_vs_u64_max_build_stays_exact() {
    use arrow_array::{Int32Array, Int64Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::file::properties::EnabledStatistics;

    // Mixed i32 probe vs UInt64 max build stays exact without literal overflow errors.
    let probe_i32 = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int32, false),
                Field::new("v", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int32Array::from_iter_values([-5, 1, 10])),
                Arc::new(Int64Array::from_iter_values([50, 100, 200])),
            ],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    let build_u64 = ParquetFixture::with_batch(
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from_iter_values([1, u64::MAX]))],
        )
        .unwrap(),
        EnabledStatistics::Chunk,
    );
    let mixed_plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: probe_i32.scan(&[], None),
        right: build_u64.scan(&[], None),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: ColumnRef::new("v").unwrap(),
            output_column: "v".into(),
        }],
    }));
    let mixed_res = prepare(&[&probe_i32, &build_u64], &mixed_plan)
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&mixed_res), vec![json!({"v": 100})]);
}
