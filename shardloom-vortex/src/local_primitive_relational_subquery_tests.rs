use super::*;
use crate::relational_query::{
    VortexRelationalQuantifier as Quantifier, VortexRelationalSubquery as Subquery,
    VortexRelationalSubqueryKind as Kind,
};
use shardloom_core::ComparisonOp;

fn key(left: &str, right: &str) -> VortexRelationalJoinKey {
    VortexRelationalJoinKey {
        left: ColumnRef::new(left).unwrap(),
        right: ColumnRef::new(right).unwrap(),
    }
}

fn plan(
    left: &Fixture,
    right: &Fixture,
    kind: Kind,
    negated: bool,
    correlation: Vec<VortexRelationalJoinKey>,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Subquery(Box::new(Subquery {
        input: left.scan(),
        relation: right.scan(),
        kind,
        correlation,
        negated,
        output_column: "predicate".into(),
    }))
}

fn predicates(plan: &VortexRelationalPlan) -> Vec<serde_json::Value> {
    let prepared = prepare_relational(plan, policy()).unwrap();
    let rows = json_rows(
        &prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap(),
    );
    rows.into_iter()
        .map(|row| row["predicate"].clone())
        .collect()
}

#[test]
fn native_relational_subquery_in_not_in_any_all_and_exists_preserve_null_and_empty_set_rules() {
    let left = Fixture::new(
        single(
            "v",
            PrimitiveArray::from_option_iter([None, Some(1i32), Some(2), Some(3)]).into_array(),
        ),
        1,
    );
    let with_null = Fixture::new(
        single(
            "w",
            PrimitiveArray::from_option_iter([Some(2i64), None, Some(2)]).into_array(),
        ),
        1,
    );
    let no_null = Fixture::new(
        single(
            "w",
            PrimitiveArray::from_option_iter([Some(2i64)]).into_array(),
        ),
        1,
    );
    let empty = Fixture::new(
        single(
            "w",
            PrimitiveArray::from_option_iter(Vec::<Option<i64>>::new()).into_array(),
        ),
        1,
    );
    for (right, expected_in, expected_not, expected_gt_any, expected_gt_all, exists) in [
        (
            &with_null,
            serde_json::json!([null, null, true, null]),
            serde_json::json!([null, null, false, null]),
            serde_json::json!([null, null, null, true]),
            serde_json::json!([null, false, false, null]),
            true,
        ),
        (
            &no_null,
            serde_json::json!([null, false, true, false]),
            serde_json::json!([null, true, false, true]),
            serde_json::json!([null, false, false, true]),
            serde_json::json!([null, false, false, true]),
            true,
        ),
        (
            &empty,
            serde_json::json!([false, false, false, false]),
            serde_json::json!([true, true, true, true]),
            serde_json::json!([false, false, false, false]),
            serde_json::json!([true, true, true, true]),
            false,
        ),
    ] {
        for (kind, negated, expected) in [
            (
                Kind::In {
                    columns: vec![key("v", "w")],
                },
                false,
                expected_in,
            ),
            (
                Kind::In {
                    columns: vec![key("v", "w")],
                },
                true,
                expected_not,
            ),
            (
                Kind::Quantified {
                    columns: key("v", "w"),
                    comparison: ComparisonOp::Gt,
                    quantifier: Quantifier::Any,
                },
                false,
                expected_gt_any,
            ),
            (
                Kind::Quantified {
                    columns: key("v", "w"),
                    comparison: ComparisonOp::Gt,
                    quantifier: Quantifier::All,
                },
                false,
                expected_gt_all,
            ),
            (Kind::Exists, false, serde_json::json!(vec![exists; 4])),
            (Kind::Exists, true, serde_json::json!(vec![!exists; 4])),
        ] {
            assert_eq!(
                serde_json::json!(predicates(&plan(&left, right, kind, negated, vec![]))),
                expected
            );
        }
    }
}

fn tuple(a: &[Option<i64>], b: &[Option<u64>]) -> ArrayRef {
    StructArray::new(
        FieldNames::from(["a", "b"]),
        vec![
            PrimitiveArray::from_option_iter(a.iter().copied()).into_array(),
            PrimitiveArray::from_option_iter(b.iter().copied()).into_array(),
        ],
        a.len(),
        Validity::NonNullable,
    )
    .into_array()
}

#[test]
fn native_relational_row_subquery_distinguishes_unknown_from_a_definite_mismatching_tuple() {
    let left = Fixture::new(
        tuple(
            &[Some(1), Some(1), Some(2), None, Some(3)],
            &[Some(9), None, Some(7), Some(8), None],
        ),
        2,
    );
    let right = Fixture::new(tuple(&[Some(1), Some(2)], &[None, Some(8)]), 1);
    let in_plan = plan(
        &left,
        &right,
        Kind::In {
            columns: vec![key("a", "a"), key("b", "b")],
        },
        false,
        vec![],
    );
    assert_eq!(
        serde_json::json!(predicates(&in_plan)),
        serde_json::json!([null, null, false, null, false])
    );
}

#[test]
fn native_relational_correlated_membership_and_exists_treat_null_correlation_as_empty() {
    let left = Fixture::new(
        tuple(
            &[Some(1), Some(2), Some(3), None, Some(1)],
            &[Some(9), Some(8), None, Some(9), Some(7)],
        ),
        2,
    );
    let right = Fixture::new(
        tuple(
            &[Some(1), Some(1), Some(2), None],
            &[Some(9), None, Some(8), Some(9)],
        ),
        1,
    );
    let columns = vec![key("b", "b")];
    assert_eq!(
        serde_json::json!(predicates(&plan(
            &left,
            &right,
            Kind::In { columns },
            false,
            vec![key("a", "a")]
        ))),
        serde_json::json!([true, true, false, false, null])
    );
    assert_eq!(
        serde_json::json!(predicates(&plan(
            &left,
            &right,
            Kind::Exists,
            false,
            vec![key("a", "a")]
        ))),
        serde_json::json!([true, true, false, false, true])
    );
    assert_eq!(
        serde_json::json!(predicates(&plan(
            &left,
            &right,
            Kind::Quantified {
                columns: key("b", "b"),
                comparison: ComparisonOp::Eq,
                quantifier: Quantifier::All
            },
            false,
            vec![key("a", "a")]
        ))),
        serde_json::json!([null, true, true, true, false])
    );
}

#[test]
fn native_relational_subqueries_preserve_every_outer_row_and_can_be_reexecuted_without_source_reopen()
 {
    let left = Fixture::new(
        keyed(&[Some(2), Some(2), Some(3), None], &[10, 11, 12, 13]),
        1,
    );
    let right = Fixture::new(keyed(&[Some(2), Some(2), None], &[20, 21, 22]), 1);
    let prepared = prepare_relational(
        &plan(
            &left,
            &right,
            Kind::In {
                columns: vec![key("entity", "entity")],
            },
            false,
            vec![],
        ),
        policy(),
    )
    .unwrap();
    let baseline = prepared.session.memory().snapshot().reserved_bytes;
    for call in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                serde_json::json!({"entity":2,"amount":10,"predicate":true}),
                serde_json::json!({"entity":2,"amount":11,"predicate":true}),
                serde_json::json!({"entity":3,"amount":12,"predicate":null}),
                serde_json::json!({"entity":null,"amount":13,"predicate":null}),
            ]
        );
        assert_eq!(result.execution.runtime.completed_executions, call);
        assert_eq!(result.execution.runtime.prepared_source_opens, 2);
        drop(result);
        assert_eq!(
            prepared.session.memory().snapshot().reserved_bytes,
            baseline
        );
    }
}
