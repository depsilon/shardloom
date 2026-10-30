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
            #[cfg(debug_assertions)]
            {
                let counts = observation.finish();
                assert_eq!(counts.join_output_rows, rows);
                assert_eq!(
                    counts.join_output_cells, rows,
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
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            if spill {
                assert_eq!(fs::read_dir(&runs).unwrap().count(), 0);
            }
        }
    }
}
