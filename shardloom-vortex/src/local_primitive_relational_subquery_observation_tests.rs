//! Exact numerics and selected-key observation are shared across predicate state.

use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;
use shardloom_core::{ExprId, Expression, ScalarValue};
use vortex::array::validity::Validity;

#[test]
fn subquery_membership_mixed_exact_numeric_keys_never_round_integer_identity() {
    let right = Fixture::new(
        single(
            "v",
            PrimitiveArray::from_iter([
                -1_f64,
                -0.0,
                9_007_199_254_740_992.0,
                9_223_372_036_854_775_808.0,
                18_446_744_073_709_551_616.0,
            ])
            .into_array(),
        ),
        2,
    );
    let signed = Fixture::new(
        single(
            "v",
            PrimitiveArray::from_iter([
                -1_i64,
                0,
                9_007_199_254_740_992,
                9_007_199_254_740_993,
                i64::MAX,
            ])
            .into_array(),
        ),
        2,
    );
    let unsigned = Fixture::new(
        single(
            "v",
            PrimitiveArray::from_iter([u64::MAX, 1_u64 << 63, 9_007_199_254_740_993, 0])
                .into_array(),
        ),
        2,
    );
    for (left, expected) in [
        (&signed, vec![true, true, true, false, false]),
        (&unsigned, vec![false, true, false, true]),
    ] {
        for negated in [false, true] {
            assert_eq!(
                predicates(&plan(
                    left,
                    &right,
                    Kind::In {
                        columns: vec![key("v", "v")]
                    },
                    negated,
                    vec![]
                )),
                expected
                    .iter()
                    .map(|value| serde_json::json!(*value ^ negated))
                    .collect::<Vec<_>>()
            );
        }
    }
}

fn array(groups: &[Option<i64>], values: &[f64], unused: bool) -> ArrayRef {
    let mut names = vec!["g", "v"];
    let mut columns = vec![
        PrimitiveArray::from_option_iter(groups.iter().copied()).into_array(),
        PrimitiveArray::from_iter(values.iter().copied()).into_array(),
    ];
    if unused {
        names.push("unused");
        columns.push(PrimitiveArray::from_iter(vec![f64::NAN; values.len()]).into_array());
    }
    StructArray::new(names.into(), columns, values.len(), Validity::NonNullable).into_array()
}

#[test]
fn subquery_membership_validates_late_keys_before_null_prefix_elision_and_empty_groups() {
    let left = Fixture::new(array(&[Some(7)], &[42.0], false), 1);
    let right = Fixture::new(
        array(&[Some(7), None, Some(7)], &[42.0, f64::NAN, 43.0], false),
        1,
    );
    let invalid_left = Fixture::new(array(&[None], &[f64::INFINITY], false), 1);
    let empty = Fixture::new(array(&[], &[], false), 1);
    let unused = Fixture::new(array(&[Some(7)], &[42.0], true), 1);
    for spill in [false, true] {
        for (outer, inner, guarded, denied) in [
            (&left, &right, false, true),
            (&left, &right, true, false),
            (&invalid_left, &empty, false, true),
            (&left, &unused, false, false),
        ] {
            let mut query = Subquery {
                input: outer.scan(),
                relation: inner.scan(),
                kind: Kind::In {
                    columns: vec![key("v", "v")],
                },
                evaluation_guard: None,
                correlation: vec![key("g", "g")],
                negated: false,
                output_column: "predicate".into(),
            };
            if guarded {
                query.evaluation_guard = Some(Expression::literal(
                    ExprId::new("guard").unwrap(),
                    ScalarValue::Boolean(false),
                ));
            }
            let directory = left.0.join(format!(
                "observation-{}",
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&directory).unwrap();
            let mut prepared =
                prepare_relational(&VortexRelationalPlan::Subquery(Box::new(query)), policy())
                    .unwrap();
            if spill {
                prepared = prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(&directory, 64 << 20, 1 << 20).unwrap(),
                    )
                    .unwrap();
            }
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let result = prepared.collect_jsonl(&CancellationToken::default());
            if denied {
                let error = result.err().unwrap().to_string();
                assert!(error.contains("finite"), "{error}");
                assert_eq!(prepared.snapshot().completed_executions, 0);
            } else {
                let result = result.unwrap();
                assert_eq!(
                    json_rows(&result),
                    vec![serde_json::json!({
                        "g":7, "v":42.0, "predicate": if guarded {serde_json::Value::Null} else {serde_json::json!(true)},
                    })]
                );
                assert_eq!(
                    result.execution.spillable_subquery_stages,
                    u64::from(spill && !guarded)
                );
                drop(result);
            }
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        }
    }
}
