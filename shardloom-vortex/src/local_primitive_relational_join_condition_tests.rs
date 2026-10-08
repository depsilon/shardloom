use super::*;
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};

fn compare(left: &str, op: ComparisonOp, right: &str) -> Expression {
    Expression::new(
        ExprId::new("on").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(Expression::column(
                ExprId::new("left").unwrap(),
                ColumnRef::new(left).unwrap(),
            )),
            op,
            right: Box::new(Expression::column(
                ExprId::new("right").unwrap(),
                ColumnRef::new(right).unwrap(),
            )),
        },
    )
}

fn conditional(
    left: &Fixture,
    right: &Fixture,
    kind: JoinKind,
    keyed: bool,
    condition: Expression,
) -> VortexRelationalPlan {
    let VortexRelationalPlan::Join(mut plan) = join(left, right, kind) else {
        unreachable!()
    };
    plan.condition = Some(condition);
    if !keyed {
        plan.keys.clear();
    }
    if matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
        plan.columns.truncate(1);
    }
    VortexRelationalPlan::Join(plan)
}

#[test]
fn native_relational_on_precedes_null_extension_for_every_join_kind() {
    let left = Fixture::new(
        keyed(&[Some(1), Some(1), Some(2), None], &[10, 30, 40, 50]),
        2,
    );
    let right = Fixture::new(
        keyed(&[Some(1), Some(1), Some(2), None], &[20, 5, 35, 60]),
        1,
    );
    for (kind, expected) in [
        (
            JoinKind::Inner,
            serde_json::json!([{"debit":10,"credit":20}]),
        ),
        (
            JoinKind::Left,
            serde_json::json!([
                {"debit":10,"credit":20},{"debit":30,"credit":null},
                {"debit":40,"credit":null},{"debit":50,"credit":null}
            ]),
        ),
        (
            JoinKind::Right,
            serde_json::json!([
                {"debit":10,"credit":20},{"debit":null,"credit":5},
                {"debit":null,"credit":35},{"debit":null,"credit":60}
            ]),
        ),
        (
            JoinKind::Full,
            serde_json::json!([
                {"debit":10,"credit":20},{"debit":30,"credit":null},
                {"debit":40,"credit":null},{"debit":50,"credit":null},
                {"debit":null,"credit":5},{"debit":null,"credit":35},{"debit":null,"credit":60}
            ]),
        ),
        (JoinKind::LeftSemi, serde_json::json!([{"debit":10}])),
        (
            JoinKind::LeftAnti,
            serde_json::json!([{"debit":30},{"debit":40},{"debit":50}]),
        ),
    ] {
        let plan = conditional(
            &left,
            &right,
            kind,
            true,
            compare("left.amount", ComparisonOp::Lt, "right.amount"),
        );
        let prepared = prepare_relational(&plan, policy()).unwrap();
        let baseline = prepared.session.memory().snapshot().reserved_bytes;
        for call in 1..=2 {
            let result = prepared
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(serde_json::json!(json_rows(&result)), expected, "{kind:?}");
            assert_eq!(result.execution.runtime.completed_executions, call);
            assert_eq!(result.execution.runtime.prepared_source_opens, 2);
            drop(result);
            assert_eq!(
                prepared.session.memory().snapshot().reserved_bytes,
                baseline
            );
        }
        super::join_spill_tests::assert_ordered(&left, &plan, expected.as_array().unwrap());
    }
}

#[test]
fn native_relational_non_equi_predicate_keeps_unknown_distinct_from_true() {
    let left = Fixture::new(
        keyed(&[Some(1), Some(1), Some(2), None], &[10, 30, 40, 50]),
        2,
    );
    let right = Fixture::new(
        keyed(&[Some(1), Some(1), Some(2), None], &[20, 5, 35, 60]),
        1,
    );
    let plan = conditional(
        &left,
        &right,
        JoinKind::Full,
        false,
        compare("left.entity", ComparisonOp::Lt, "right.entity"),
    );
    assert_eq!(
        serde_json::json!(json_rows(
            &prepare_relational(&plan, policy())
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap()
        )),
        serde_json::json!([
            {"debit":10,"credit":35},{"debit":30,"credit":35},
            {"debit":40,"credit":null},{"debit":50,"credit":null},
            {"debit":null,"credit":20},{"debit":null,"credit":5},{"debit":null,"credit":60}
        ])
    );
}

#[test]
fn native_relational_constant_on_and_empty_relations_preserve_outer_semantics() {
    let left = Fixture::new(keyed(&[None], &[10]), 1);
    let right = Fixture::new(keyed(&[None], &[20]), 1);
    let empty = Fixture::new(keyed(&[], &[]), 1);
    for (value, expected) in [
        (
            ScalarValue::Boolean(true),
            serde_json::json!([{"debit":10,"credit":20}]),
        ),
        (
            ScalarValue::Boolean(false),
            serde_json::json!([{"debit":10,"credit":null},{"debit":null,"credit":20}]),
        ),
        (
            ScalarValue::Null,
            serde_json::json!([{"debit":10,"credit":null},{"debit":null,"credit":20}]),
        ),
    ] {
        let condition = Expression::literal(ExprId::new("constant").unwrap(), value);
        let plan = conditional(&left, &right, JoinKind::Full, false, condition.clone());
        let prepared = prepare_relational(&plan, policy()).unwrap();
        assert_eq!(
            serde_json::json!(json_rows(
                &prepared
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap()
            )),
            expected
        );
        super::join_spill_tests::assert_ordered(&left, &plan, expected.as_array().unwrap());
        for (left, right, expected) in [
            (
                &left,
                &empty,
                serde_json::json!([{"debit":10,"credit":null}]),
            ),
            (
                &empty,
                &right,
                serde_json::json!([{"debit":null,"credit":20}]),
            ),
            (&empty, &empty, serde_json::json!([])),
        ] {
            let plan = conditional(left, right, JoinKind::Full, false, condition.clone());
            assert_eq!(
                serde_json::json!(json_rows(
                    &prepare_relational(&plan, policy())
                        .unwrap()
                        .collect_jsonl(&CancellationToken::default())
                        .unwrap()
                )),
                expected
            );
            super::join_spill_tests::assert_ordered(left, &plan, expected.as_array().unwrap());
        }
    }
}

#[cfg(feature = "universal-format-io")]
#[test]
fn native_relational_on_and_post_join_where_have_distinct_results_through_all_writers() {
    use crate::relational_query::VortexRelationalFilter;
    let left = Fixture::new(keyed(&[Some(1), Some(2)], &[10, 30]), 1);
    let right = Fixture::new(keyed(&[Some(1), Some(2)], &[20, 5]), 1);
    let plan = conditional(
        &left,
        &right,
        JoinKind::Full,
        true,
        compare("left.amount", ComparisonOp::Lt, "right.amount"),
    );
    super::writer_tests::verify_writers(
        &left,
        &plan,
        "join-on",
        &[
            serde_json::json!({"debit":10,"credit":20}),
            serde_json::json!({"debit":30,"credit":null}),
            serde_json::json!({"debit":null,"credit":5}),
        ],
        "debit,credit\n10,20\n30,\n,5\n",
    );
    let plan = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: plan,
        predicate: compare("debit", ComparisonOp::Lt, "credit"),
    }));
    super::writer_tests::verify_writers(
        &left,
        &plan,
        "join-where",
        &[serde_json::json!({"debit":10,"credit":20})],
        "debit,credit\n10,20\n",
    );
}

#[test]
fn native_relational_on_candidates_cross_batches_and_release_after_cancellation_and_pressure() {
    let left = Fixture::new(keyed(&[Some(1)], &[0]), 1);
    let right = Fixture::new(
        keyed(&vec![Some(1); 5001], &(0..5001_u32).collect::<Vec<_>>()),
        100,
    );
    let plan = conditional(
        &left,
        &right,
        JoinKind::Inner,
        true,
        compare("left.amount", ComparisonOp::Lt, "right.amount"),
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
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
    let block = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - 128 * 1024)
        .unwrap();
    assert!(
        prepared
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    drop(block);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let expected = (1..5001_u32)
        .map(|credit| serde_json::json!({"debit":0,"credit":credit}))
        .collect::<Vec<_>>();
    assert_eq!(json_rows(&result), expected);
    assert_eq!(result.execution.runtime.completed_executions, 1);
    assert_eq!(result.execution.runtime.prepared_source_opens, 2);
}
