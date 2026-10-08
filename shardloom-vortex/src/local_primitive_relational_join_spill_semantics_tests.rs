//! Exact ON candidate evaluation and mixed-domain equality in both strategies.

use super::*;
use shardloom_core::{BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};
use vortex::array::arrays::VarBinViewArray;

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("join-semantics").unwrap(), kind)
}
fn col(name: &str) -> Expression {
    expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}
fn literal(value: ScalarValue) -> Expression {
    expression(ExpressionKind::Literal(value))
}

#[test]
fn ordered_join_on_evaluates_a_complete_candidate_batch_before_semi_anti_short_circuit() {
    let left = fixture(&[(Some(1), 10)], 1);
    let count = 32_777;
    let right = Fixture::new(
        StructArray::new(
            FieldNames::from(["entity", "amount"]),
            vec![
                PrimitiveArray::from_iter(std::iter::repeat_n(1u64, count)).into_array(),
                PrimitiveArray::from_iter(
                    (0..count).map(|row| if row == 1030 { i64::MAX } else { 0 }),
                )
                .into_array(),
            ],
            count,
            Validity::NonNullable,
        )
        .into_array(),
        509,
    );
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        let VortexRelationalPlan::Join(mut join) = plan(&left, &right, kind) else {
            unreachable!()
        };
        join.condition = Some(expression(ExpressionKind::Compare {
            left: Box::new(expression(ExpressionKind::Binary {
                left: Box::new(col("right.amount")),
                op: BinaryOp::Add,
                right: Box::new(literal(ScalarValue::Int64(1))),
            })),
            op: ComparisonOp::Gt,
            right: Box::new(literal(ScalarValue::Int64(0))),
        }));
        let plan = VortexRelationalPlan::Join(join);
        for spilling in [false, true] {
            let prepared = if spilling {
                ordered(&left, &plan)
            } else {
                prepare_relational(&plan, policy()).unwrap()
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            for batch_rows in [1024, 2048] {
                let mut rows = Vec::new();
                let result = prepared.for_each_json_batch(
                    &CancellationToken::default(),
                    batch_rows,
                    1 << 20,
                    |batch| {
                        rows.extend(
                            serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap(),
                        );
                        Ok(())
                    },
                );
                if batch_rows == 1024 {
                    let report = result.unwrap();
                    assert_eq!(
                        rows,
                        if kind == JoinKind::LeftSemi {
                            vec![json!({"debit":10})]
                        } else {
                            vec![]
                        }
                    );
                    if spilling {
                        assert!(report.spill.as_ref().unwrap().runs_written > 0);
                    }
                } else {
                    let error = result
                        .err()
                        .expect("the second native block is in the same ON batch");
                    assert!(error.to_string().contains("overflow"), "{error}");
                    assert_eq!(rows.len(), 0);
                }
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if spilling {
                    assert_eq!(fs::read_dir(left.0.join("join-runs")).unwrap().count(), 0);
                }
            }
            assert_eq!(prepared.snapshot().completed_executions, 1);
        }
    }
}

#[test]
fn ordered_join_constant_on_supports_empty_private_payload_and_unknown_semi_anti() {
    let left = fixture(&[(None, 10), (Some(1), 11)], 1);
    let right = fixture(&[(None, 20), (Some(2), 21)], 2);
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        for value in [
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
            ScalarValue::Null,
        ] {
            let VortexRelationalPlan::Join(mut join) = plan(&left, &right, kind) else {
                unreachable!()
            };
            join.keys.clear();
            let matched = value == ScalarValue::Boolean(true);
            join.condition = Some(literal(value));
            let expected = if (kind == JoinKind::LeftSemi) == matched {
                vec![json!({"debit":10}), json!({"debit":11})]
            } else {
                vec![]
            };
            assert_ordered(&left, &VortexRelationalPlan::Join(join), &expected);
        }
    }
    for matched in [false, true] {
        let VortexRelationalPlan::Join(mut join) = plan(&left, &right, JoinKind::Full) else {
            unreachable!()
        };
        join.keys.clear();
        join.columns.truncate(1);
        join.condition = Some(literal(ScalarValue::Boolean(matched)));
        let expected = if matched {
            vec![
                json!({"debit":10}),
                json!({"debit":10}),
                json!({"debit":11}),
                json!({"debit":11}),
            ]
        } else {
            vec![
                json!({"debit":10}),
                json!({"debit":11}),
                json!({"debit":null}),
                json!({"debit":null}),
            ]
        };
        assert_ordered(&left, &VortexRelationalPlan::Join(join), &expected);
    }
    let rows = (0..65_537u32)
        .map(|id| (Some(u64::from(id)), id))
        .collect::<Vec<_>>();
    let right = fixture(&rows, 509);
    let VortexRelationalPlan::Join(mut join) = plan(&left, &right, JoinKind::LeftSemi) else {
        unreachable!()
    };
    join.keys.clear();
    join.condition = Some(literal(ScalarValue::Boolean(true)));
    let prepared = ordered(&left, &VortexRelationalPlan::Join(join));
    let (rows, report) = complete(&prepared, 2048);
    assert_eq!(rows, vec![json!({"debit":10}), json!({"debit":11})]);
    assert!(report.spill.as_ref().unwrap().runs_written > 0);
}

#[test]
fn ordered_join_compound_integer_domains_nulls_and_exact_float_boundaries() {
    let array = |keys: ArrayRef, tags: &[Option<&str>], ids: &[u32]| {
        StructArray::new(
            FieldNames::from(["entity", "tag", "amount"]),
            vec![
                keys,
                VarBinViewArray::from_iter_nullable_str(tags.iter().copied()).into_array(),
                PrimitiveArray::from_iter(ids.iter().copied()).into_array(),
            ],
            ids.len(),
            Validity::NonNullable,
        )
        .into_array()
    };
    let left = Fixture::new(
        array(
            PrimitiveArray::from_iter([-1i64, 0, i64::MAX, 0]).into_array(),
            &[Some("x"), Some("same"), Some("same"), None],
            &[10, 11, 12, 13],
        ),
        2,
    );
    let right = Fixture::new(
        array(
            PrimitiveArray::from_iter([u64::MAX, 0, i64::MAX as u64, 0, 0]).into_array(),
            &[Some("x"), Some("same"), Some("same"), Some("other"), None],
            &[20, 21, 22, 23, 24],
        ),
        3,
    );
    let VortexRelationalPlan::Join(mut join) = plan(&left, &right, JoinKind::Full) else {
        unreachable!()
    };
    join.keys.push(VortexRelationalJoinKey {
        left: ColumnRef::new("tag").unwrap(),
        right: ColumnRef::new("tag").unwrap(),
    });
    assert_ordered(
        &left,
        &VortexRelationalPlan::Join(join),
        &[
            json!({"debit":10,"credit":null}),
            json!({"debit":11,"credit":21}),
            json!({"debit":12,"credit":22}),
            json!({"debit":13,"credit":null}),
            json!({"debit":null,"credit":20}),
            json!({"debit":null,"credit":23}),
            json!({"debit":null,"credit":24}),
        ],
    );
    let left = Fixture::new(
        array(
            PrimitiveArray::from_iter([9_007_199_254_740_992i64, 9_007_199_254_740_993, -1])
                .into_array(),
            &[Some("x"); 3],
            &[10, 11, 12],
        ),
        2,
    );
    let right = Fixture::new(
        array(
            PrimitiveArray::from_iter([9_007_199_254_740_992f64, -1.0]).into_array(),
            &[Some("x"); 2],
            &[20, 21],
        ),
        1,
    );
    assert_ordered(
        &left,
        &plan(&left, &right, JoinKind::Full),
        &[
            json!({"debit":10,"credit":20}),
            json!({"debit":11,"credit":null}),
            json!({"debit":12,"credit":21}),
        ],
    );
}
