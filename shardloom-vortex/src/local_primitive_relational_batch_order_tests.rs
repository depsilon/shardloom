//! Complete-source order/range semantics and private input-owner lifetimes.

use super::*;
use crate::relational_query::{
    VortexRelationalLimit, VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey,
    VortexRelationalSort,
};
use std::cmp::Ordering as Comparison;

#[path = "local_primitive_relational_batch_order_spill_tests.rs"]
mod spill_tests;

fn key(name: &str, descending: bool, nulls: NullOrder) -> VortexRelationalOrderKey {
    VortexRelationalOrderKey {
        column: ColumnRef::new(name).unwrap(),
        descending,
        nulls: Some(nulls),
    }
}

fn sorted(
    input: VortexRelationalPlan,
    keys: Vec<VortexRelationalOrderKey>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Sort(Box::new(VortexRelationalSort { input, keys }))
}

fn limited(input: VortexRelationalPlan, offset: usize, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        offset,
        count,
    }))
}

#[derive(Clone, Debug)]
struct Row {
    n: Option<i64>,
    s: Option<&'static str>,
    id: i64,
}

impl Row {
    fn json(&self) -> serde_json::Value {
        serde_json::json!({"n":self.n,"s":self.s,"id":self.id})
    }
}

fn rows_source(session: &ResidentVortexSession, rows: &[Row]) -> Result<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(
                    &rows.iter().map(|row| row.n).collect::<Vec<_>>(),
                ),
            },
            MemoryColumn {
                name: "s",
                values: MemoryColumnValues::Utf8(&rows.iter().map(|row| row.s).collect::<Vec<_>>()),
            },
            MemoryColumn {
                name: "id",
                values: MemoryColumnValues::Int64(
                    &rows.iter().map(|row| Some(row.id)).collect::<Vec<_>>(),
                ),
            },
        ],
    )
}

fn prepare_rows(plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |preparation| {
        preparation.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            rows_source(session, &[])
        })?;
        Ok(plan.clone())
    })
    .unwrap()
}

fn reference<T: Ord>(a: Option<T>, b: Option<T>, descending: bool, nulls: NullOrder) -> Comparison {
    match (a, b) {
        (None, None) => Comparison::Equal,
        (None, Some(_)) => {
            if nulls == NullOrder::First {
                Comparison::Less
            } else {
                Comparison::Greater
            }
        }
        (Some(_), None) => {
            if nulls == NullOrder::First {
                Comparison::Greater
            } else {
                Comparison::Less
            }
        }
        (Some(a), Some(b)) => {
            if descending {
                b.cmp(&a)
            } else {
                a.cmp(&b)
            }
        }
    }
}

fn reference_sort(rows: &mut [Row], keys: &[VortexRelationalOrderKey]) {
    rows.sort_by(|left, right| {
        for key in keys {
            let nulls = key.nulls.unwrap();
            let order = match key.column.as_str() {
                "n" => reference(left.n, right.n, key.descending, nulls),
                "s" => reference(left.s, right.s, key.descending, nulls),
                _ => panic!("unsupported test key"),
            };
            if order != Comparison::Equal {
                return order;
            }
        }
        Comparison::Equal
    });
}

fn fixture_rows() -> Vec<Row> {
    (0..131)
        .map(|id| Row {
            n: [
                None,
                Some(i64::MIN),
                Some(i64::MAX),
                Some(-1),
                Some(0),
                Some((1 << 53) + 1),
            ][id % 6],
            s: [Some("東京"), None, Some(""), Some("λ\"\n"), Some("a\0z")][id % 5],
            id: i64::try_from(id).unwrap(),
        })
        .collect()
}

#[test]
fn streamed_ordering_cross_batch_multikey_and_nested_stability_detach_input_owners() {
    let rows = fixture_rows();
    for (descending, nulls, nested) in [
        (false, NullOrder::First, false),
        (true, NullOrder::Last, false),
        (false, NullOrder::Last, true),
        (true, NullOrder::First, true),
    ] {
        let keys = vec![
            key("n", descending, nulls),
            key("s", !descending, NullOrder::Last),
        ];
        let mut expected = rows.clone();
        let plan = if nested {
            reference_sort(&mut expected, &keys[1..]);
            reference_sort(&mut expected, &keys[..1]);
            sorted(sorted(scan(), keys[1..].to_vec()), keys[..1].to_vec())
        } else {
            reference_sort(&mut expected, &keys);
            sorted(scan(), keys)
        };
        let prepared = prepare_rows(&plan);
        let before = prepared.snapshot().memory.reserved_bytes;
        let mut chunks = std::iter::once(&rows[..0]).chain(rows.chunks(17));
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            calls += 1;
            let Some(rows) = chunks.next() else {
                return Ok(None);
            };
            let source = rows_source(session, rows)?;
            prior = Some(source.batch_release_witness()?);
            Ok(Some(source))
        };
        let mut actual = Vec::new();
        let mut retained = Vec::new();
        let execution = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                let mut sink = crate::local_primitives::collect::JsonRows::new(
                    context.memory(),
                    1 << 20,
                    false,
                )?;
                sink.append_native(&array, context)?;
                actual.extend(
                    serde_json::from_str::<Vec<serde_json::Value>>(sink.finish()?.value()).unwrap(),
                );
                retained.push(array.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected.iter().map(Row::json).collect::<Vec<_>>());
        assert_eq!(calls, rows.len().div_ceil(17) + 2);
        let input = execution.input.as_ref().unwrap();
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert_eq!(input.rows, rows.len() as u64);
        assert_eq!(
            input.ordering_rows_detached,
            rows.len() as u64 * if nested { 2 } else { 1 }
        );
        assert_eq!(input.max_retained_input_batches, 1);
        assert!(execution.native_io_certificate.is_certified());
        drop(execution);
        assert!(prepared.snapshot().memory.reserved_bytes > before);
        drop(retained);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    }
}

#[test]
fn streamed_ordering_filters_and_projects_on_both_sides_preserve_complete_values() {
    let rows = fixture_rows();
    let filtered = |input, name: &str| {
        VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
            input,
            predicate: Expression::new(
                ExprId::new("positive").unwrap(),
                ExpressionKind::Compare {
                    left: Box::new(col(name)),
                    op: shardloom_core::ComparisonOp::Gt,
                    right: Box::new(Expression::new(
                        ExprId::new("zero").unwrap(),
                        ExpressionKind::Literal(shardloom_core::ScalarValue::Int64(0)),
                    )),
                },
            ),
        }))
    };
    let projected = |input| {
        VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input,
            expressions: vec![
                ("value".into(), col("n")),
                ("label".into(), col("s")),
                ("ordinal".into(), col("id")),
            ],
        }))
    };
    let plan = filtered(
        sorted(
            projected(filtered(scan(), "id")),
            vec![key("value", false, NullOrder::First)],
        ),
        "value",
    );
    let prepared = prepare_rows(&plan);
    let mut chunks = rows.chunks(19);
    let mut calls = 0;
    let mut input = |session: &ResidentVortexSession| {
        calls += 1;
        chunks
            .next()
            .map(|rows| rows_source(session, rows))
            .transpose()
    };
    let mut actual = Vec::new();
    let execution = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 7, 1 << 20, |batch| {
            assert!(batch.rows <= 7);
            actual.extend(
                serde_json::from_str::<Vec<serde_json::Value>>(batch.values_json.value()).unwrap(),
            );
            Ok(())
        })
        .unwrap();
    let mut expected = rows
        .iter()
        .filter(|row| row.id > 0 && row.n.is_some_and(|n| n > 0))
        .cloned()
        .collect::<Vec<_>>();
    expected.sort_by_key(|row| row.n);
    assert_eq!(
        actual,
        expected
            .iter()
            .map(|row| serde_json::json!({"value":row.n,"label":row.s,"ordinal":row.id}))
            .collect::<Vec<_>>()
    );
    assert_eq!(calls, rows.len().div_ceil(19) + 1);
    assert!(execution.input.as_ref().unwrap().end_of_input_observed);
}

#[test]
fn streamed_ordering_limits_and_offsets_apply_once_and_drain_every_input_batch() {
    let rows = fixture_rows();
    for stage in ["range", "before", "after", "both"] {
        for (offset, count) in [(0, 0), (16, 24), (130, 4), (200, 2)] {
            let mut expected = rows.clone();
            let mut plan = scan();
            let keys = vec![key("n", true, NullOrder::Last)];
            if matches!(stage, "after" | "both") {
                plan = sorted(plan, keys.clone());
                reference_sort(&mut expected, &keys);
            }
            plan = limited(plan, offset, count);
            expected = expected.into_iter().skip(offset).take(count).collect();
            if matches!(stage, "before" | "both") {
                let keys = vec![key("s", false, NullOrder::First)];
                plan = sorted(plan, keys.clone());
                reference_sort(&mut expected, &keys);
            }
            let prepared = prepare_rows(&plan);
            let before = prepared.snapshot().memory.reserved_bytes;
            let mut chunks = rows.chunks(17);
            let mut calls = 0;
            let mut input = |session: &ResidentVortexSession| {
                calls += 1;
                chunks
                    .next()
                    .map(|rows| rows_source(session, rows))
                    .transpose()
            };
            let collected = prepared
                .with_batch_input(&mut input)
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(
                json_rows(&collected),
                expected.iter().map(Row::json).collect::<Vec<_>>(),
                "{stage} {offset} {count}"
            );
            assert_eq!(calls, rows.len().div_ceil(17) + 1);
            let input = collected.execution.input.as_ref().unwrap();
            assert!(input.end_of_input_observed);
            assert_eq!(input.rows, rows.len() as u64);
            drop(collected);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
        }
    }
}

#[test]
fn streamed_ordering_empty_all_null_and_filtered_inputs_observe_the_end() {
    for mode in ["empty", "empty-batches", "all-null", "filtered"] {
        let mut plan = scan();
        if mode == "filtered" {
            plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
                input: plan,
                predicate: Expression::new(
                    ExprId::new("false").unwrap(),
                    ExpressionKind::Literal(shardloom_core::ScalarValue::Boolean(false)),
                ),
            }));
        }
        plan = sorted(plan, vec![key("n", true, NullOrder::First)]);
        let prepared = prepare(&plan, 2 << 20).unwrap();
        let before = prepared.snapshot().memory.reserved_bytes;
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if mode == "empty" || calls == 4 {
                return Ok(None);
            }
            if mode == "empty-batches" {
                return Ok(Some(source(session, &[], &[])?));
            }
            Ok(Some(source(session, &[None], &[None])?))
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, if mode == "empty" { 1 } else { 4 });
        assert_eq!(
            result.execution.output_rows,
            if mode == "all-null" { 3 } else { 0 }
        );
        assert!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .end_of_input_observed
        );
        assert_eq!(result.execution.output_columns, vec!["n", "s"]);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    }
}

#[test]
fn streamed_ordering_late_failures_still_fail_after_a_limit_even_when_zero() {
    for count in [0, 1] {
        for order in [false, true] {
            for failure in ["producer", "schema", "expression"] {
                let mut plan = scan();
                if failure == "expression" {
                    plan = VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
                        input: plan,
                        expressions: vec![(
                            "n".into(),
                            Expression::new(
                                ExprId::new("increment").unwrap(),
                                ExpressionKind::Binary {
                                    left: Box::new(col("n")),
                                    op: shardloom_core::BinaryOp::Add,
                                    right: Box::new(Expression::new(
                                        ExprId::new("one").unwrap(),
                                        ExpressionKind::Literal(
                                            shardloom_core::ScalarValue::Int64(1),
                                        ),
                                    )),
                                },
                            ),
                        )],
                    }));
                }
                if order {
                    plan = sorted(plan, vec![key("n", false, NullOrder::Last)]);
                }
                let prepared = prepare(&limited(plan, 0, count), 2 << 20).unwrap();
                let before = prepared.snapshot().memory.reserved_bytes;
                let mut calls = 0;
                let mut input = |session: &ResidentVortexSession| {
                    calls += 1;
                    if calls == 2 {
                        if failure == "producer" {
                            return Err(batch_input::failed("late limit producer error"));
                        }
                        if failure == "schema" {
                            return Ok(Some(ResidentMemorySource::from_batch_columns(
                                session,
                                &[MemoryColumn {
                                    name: "wrong",
                                    values: MemoryColumnValues::Int64(&[Some(2)]),
                                }],
                            )?));
                        }
                    }
                    Ok(Some(source(
                        session,
                        &[Some(if calls == 1 { 3 } else { i64::MAX })],
                        &[None],
                    )?))
                };
                let mut emitted = 0;
                let error = prepared
                    .with_batch_input(&mut input)
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |array, _| {
                        emitted += array.len();
                        Ok(())
                    })
                    .err()
                    .expect("the limit must not hide a late upstream failure")
                    .to_string();
                assert!(
                    error.contains(match failure {
                        "producer" => "late limit producer error",
                        "schema" => "declared",
                        _ => "overflow",
                    }),
                    "{error}"
                );
                assert_eq!(calls, 2);
                assert_eq!(emitted, if order { 0 } else { count });
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
            }
        }
    }
}
