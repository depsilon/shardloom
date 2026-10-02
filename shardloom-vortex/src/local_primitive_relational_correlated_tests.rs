use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalFilter, VortexRelationalLimit,
        VortexRelationalNullOrder, VortexRelationalOrderKey, VortexRelationalSort,
        VortexRelationalSubquery, VortexRelationalSubqueryKind as Kind,
    },
};
use shardloom_core::{ComparisonOp, ExprId, Expression, ExpressionKind, ScalarValue};

fn column(name: &str) -> Expression {
    Expression::column(ExprId::new(name).unwrap(), ColumnRef::new(name).unwrap())
}

fn compare(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    Expression::new(
        ExprId::new("comparison").unwrap(),
        ExpressionKind::Compare {
            left: Box::new(left),
            op,
            right: Box::new(right),
        },
    )
}

fn filtered_inner(inner: &Fixture) -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: inner.scan(),
        right: VortexRelationalPlan::Outer,
        kind: JoinKind::Inner,
        keys: vec![],
        condition: Some(compare(
            column("left.entity"),
            ComparisonOp::LtEq,
            column("right.entity"),
        )),
        columns: ["entity", "amount"]
            .into_iter()
            .map(|name| VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new(name).unwrap(),
                output_column: name.into(),
            })
            .collect(),
    }))
}

fn correlated(
    input: VortexRelationalPlan,
    relation: VortexRelationalPlan,
    kind: Kind,
    output: &str,
) -> VortexRelationalPlan {
    VortexRelationalPlan::CorrelatedSubquery(Box::new(VortexRelationalSubquery {
        input,
        relation,
        kind,
        correlation: vec![],
        output_column: output.into(),
        negated: false,
    }))
}

fn membership(left: &str, right: &str) -> Kind {
    Kind::In {
        columns: vec![VortexRelationalJoinKey {
            left: ColumnRef::new(left).unwrap(),
            right: ColumnRef::new(right).unwrap(),
        }],
    }
}

fn count(input: VortexRelationalPlan, grouped: bool) -> VortexRelationalPlan {
    VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input,
        group_by: if grouped {
            vec![ColumnRef::new("entity").unwrap()]
        } else {
            vec![]
        },
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "count".into(),
        )],
    }))
}

fn fixtures() -> (Fixture, Fixture) {
    (
        Fixture::new(
            keyed(
                &[Some(2), None, Some(1), Some(1), Some(3)],
                &[3, 0, 2, 2, 3],
            ),
            2,
        ),
        Fixture::new(
            keyed(
                &[Some(1), Some(1), Some(2), Some(3), None],
                &[10, 10, 30, 40, 50],
            ),
            2,
        ),
    )
}

#[test]
fn native_relational_parameterized_scalar_aggregate_sees_each_outer_row_and_empty_count() {
    let (left, right) = fixtures();
    let plan = correlated(
        left.scan(),
        count(filtered_inner(&right), false),
        membership("amount", "count"),
        "predicate",
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    let expected = serde_json::json!([
        {"entity":2,"amount":3,"predicate":true},
        {"entity":null,"amount":0,"predicate":true},
        {"entity":1,"amount":2,"predicate":true},
        {"entity":1,"amount":2,"predicate":true},
        {"entity":3,"amount":3,"predicate":false}
    ]);
    for call in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(serde_json::json!(json_rows(&result)), expected);
        assert_eq!(result.execution.runtime.prepared_source_opens, 2);
        assert_eq!(result.execution.runtime.completed_executions, call);
        // One scan of five outer rows plus a separately parameterized inner scan per row.
        assert_eq!(result.execution.scan_rows_delivered, 30);
        drop(result);
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
    }
    #[cfg(feature = "universal-format-io")]
    super::writer_tests::verify_writers(
        &left,
        &plan,
        "correlated-count",
        expected.as_array().unwrap(),
        "entity,amount,predicate\n2,3,true\n,0,true\n1,2,true\n1,2,true\n3,3,false\n",
    );
}

#[test]
fn native_relational_correlated_group_having_order_and_limit_apply_before_membership() {
    let (left, right) = fixtures();
    let grouped = count(filtered_inner(&right), true);
    let having = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: grouped,
        predicate: compare(
            column("count"),
            ComparisonOp::Gt,
            Expression::literal(ExprId::new("one").unwrap(), ScalarValue::UInt64(1)),
        ),
    }));
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: having,
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("entity").unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: sorted,
        offset: 0,
        count: 1,
    }));
    let plan = correlated(
        left.scan(),
        limited,
        membership("entity", "entity"),
        "predicate",
    );
    let rows = json_rows(
        &prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert_eq!(
        serde_json::json!(rows.iter().map(|row| &row["predicate"]).collect::<Vec<_>>()),
        serde_json::json!([false, false, true, true, false])
    );
}

#[test]
fn native_relational_correlated_top_one_is_chosen_after_each_parameter_filter() {
    let (left, right) = fixtures();
    let sorted = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: filtered_inner(&right),
        keys: vec![VortexRelationalOrderKey {
            column: ColumnRef::new("entity").unwrap(),
            descending: true,
            nulls: Some(VortexRelationalNullOrder::Last),
        }],
    }));
    let limited = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: sorted,
        offset: 0,
        count: 1,
    }));
    let plan = correlated(
        left.scan(),
        limited,
        membership("entity", "entity"),
        "predicate",
    );
    let rows = json_rows(
        &prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert_eq!(
        serde_json::json!(rows.iter().map(|row| &row["predicate"]).collect::<Vec<_>>()),
        serde_json::json!([true, false, true, true, true])
    );
}

#[test]
fn native_relational_nested_parameters_use_the_nearest_scope_and_reject_unbound_outer_sources() {
    let (left, right) = fixtures();
    assert!(prepare_relational(&VortexRelationalPlan::Outer, policy()).is_err());
    // The nested parameter is the right input row; referring to the original
    // outer row here would produce false results for several right-side keys.
    let nested = correlated(
        right.scan(),
        VortexRelationalPlan::Outer,
        membership("entity", "entity"),
        "nested",
    );
    let nested = VortexRelationalPlan::Filter(Box::new(VortexRelationalFilter {
        input: nested,
        predicate: column("nested"),
    }));
    let plan = correlated(
        left.scan(),
        count(nested, false),
        membership("amount", "count"),
        "predicate",
    );
    let rows = json_rows(
        &prepare_relational(&plan, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert_eq!(
        serde_json::json!(rows.iter().map(|row| &row["predicate"]).collect::<Vec<_>>()),
        serde_json::json!([false, false, false, false, false])
    );
    // A subsequent sibling must recover the enclosing parameter after nested work.
    let nested = correlated(
        VortexRelationalPlan::Outer,
        VortexRelationalPlan::Outer,
        Kind::Exists,
        "nested",
    );
    let outer = correlated(
        left.scan(),
        nested,
        membership("amount", "amount"),
        "predicate",
    );
    let rows = json_rows(
        &prepare_relational(&outer, policy())
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    assert!(rows.iter().all(|row| row["predicate"] == true));
}

#[test]
fn native_relational_correlated_cancellation_pressure_and_source_invalidation_never_publish_success()
 {
    let (left, right) = fixtures();
    let plan = correlated(
        left.scan(),
        count(filtered_inner(&right), false),
        membership("amount", "count"),
        "predicate",
    );
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
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
    prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(prepared.snapshot().completed_executions, 1);
    let mut changed = false;
    assert!(
        prepared
            .for_each_batch(&CancellationToken::default(), |_, _| {
                if !changed {
                    right.replace();
                    changed = true;
                }
                Ok(())
            })
            .is_err()
    );
    assert!(changed);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(
        prepared.session.memory().snapshot().reserved_bytes,
        baseline
    );
}
