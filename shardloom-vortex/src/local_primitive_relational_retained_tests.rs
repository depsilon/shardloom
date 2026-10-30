//! Independently specified retained-result and frontier semantics.

use super::*;
use crate::relational_query::{
    VortexRelationalFilter as Filter, VortexRelationalLimit as Limit, VortexRelationalPlan as Plan,
    VortexRelationalRecursionId as RecursionId, VortexRelationalRecursive as Recursive,
    VortexRelationalRecursiveInput as RecursiveInput, VortexRelationalSetKind as SetKind,
};
use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues, MemorySourceBounds};
use serde_json::json;
use shardloom_core::{
    BinaryOp, ComparisonOp, ExprId, Expression, ExpressionKind, FloatPolicy, RecursionLimit,
    ScalarValue,
};
use std::sync::Arc;

const SOURCE: &str = "memory://recursive-seed";

#[path = "local_primitive_relational_retained_stream_tests.rs"]
mod stream_tests;

fn expression(kind: ExpressionKind) -> Expression {
    Expression::new(ExprId::new("retained_test").unwrap(), kind)
}

fn col(name: &str) -> Expression {
    expression(ExpressionKind::Column(ColumnRef::new(name).unwrap()))
}

fn lit(value: ScalarValue) -> Expression {
    expression(ExpressionKind::Literal(value))
}

fn binary(left: Expression, op: BinaryOp, right: Expression) -> Expression {
    expression(ExpressionKind::Binary {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn compare(left: Expression, op: ComparisonOp, right: Expression) -> Expression {
    expression(ExpressionKind::Compare {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

fn project(input: Plan, expressions: Vec<(&str, Expression)>) -> Plan {
    Plan::Project(Box::new(crate::relational_query::VortexRelationalProject {
        input,
        expressions: expressions
            .into_iter()
            .map(|(name, value)| (name.into(), value))
            .collect(),
    }))
}

fn scan() -> Plan {
    Plan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(SOURCE).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn input(identity: &RecursionId) -> Plan {
    Plan::RecursiveInput(RecursiveInput {
        identity: identity.clone(),
        columns: vec!["n".into()],
    })
}

fn prepare(plan: &Plan, values: &[Option<i64>], limit: u64) -> Result<PreparedVortexRelational> {
    let resources = policy();
    let session = ResidentVortexSession::new_with_policies(
        resources.resource_envelope.memory_budget_bytes,
        resources.max_parallelism,
        FloatPolicy::Strict,
        RecursionLimit::new(limit)?,
    )?;
    prepare_relational_with_schema_in_session(resources, &session, |schemas| {
        schemas.register_memory_source(DatasetUri::new(SOURCE)?, |session| {
            ResidentMemorySource::from_columns(
                session,
                &[MemoryColumn {
                    name: "n",
                    values: MemoryColumnValues::Int64(values),
                }],
                MemorySourceBounds::default(),
            )
        })?;
        Ok(plan.clone())
    })
}

fn recursive(maximum: i64, distinct: bool) -> Plan {
    let identity = RecursionId::default();
    let selected = Plan::Filter(Box::new(Filter {
        input: input(&identity),
        predicate: compare(col("n"), ComparisonOp::Lt, lit(ScalarValue::Int64(maximum))),
    }));
    Plan::Recursive(Arc::new(Recursive {
        anchor: scan(),
        step: project(
            selected,
            vec![(
                "n",
                binary(col("n"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
            )],
        ),
        identity,
        distinct,
    }))
}

#[test]
fn materialized_aliases_retain_complete_rows_once_and_recompute_each_action() {
    let definition = Arc::new(scan());
    let plan = Plan::Set(Box::new(VortexRelationalSet {
        left: Plan::Limit(Box::new(Limit {
            input: Plan::Materialized(Arc::clone(&definition)),
            offset: 0,
            count: 1,
        })),
        right: Plan::Materialized(definition),
        kind: SetKind::UnionAll,
    }));
    let prepared = prepare(&plan, &[Some(3), None, Some(3), Some(-1)], 10).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                json!({"n":3}),
                json!({"n":3}),
                json!({"n":null}),
                json!({"n":3}),
                json!({"n":-1})
            ]
        );
        assert_eq!(result.execution.composition.materialized_relations, 1);
        assert_eq!(result.execution.composition.materialized_reuses, 1);
        assert_eq!(result.execution.composition.materialized_rows, 4);
        assert_eq!(result.execution.scan_rows_delivered, 4);
        assert!(!result.execution.native_io_certificate.fallback_attempted);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let separate = Plan::Set(Box::new(VortexRelationalSet {
        left: Plan::Materialized(Arc::new(scan())),
        right: Plan::Materialized(Arc::new(scan())),
        kind: SetKind::UnionAll,
    }));
    let result = prepare(&separate, &[Some(1)], 10)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(result.execution.composition.materialized_relations, 2);
    assert_eq!(result.execution.composition.materialized_reuses, 0);
}

#[test]
fn recursive_chain_runs_beyond_plan_depth_and_obeys_exact_iteration_boundary() {
    let plan = recursive(37, false);
    let prepared = prepare(&plan, &[Some(0)], 38).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for _ in 0..2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            (0..=37).map(|n| json!({"n":n})).collect::<Vec<_>>()
        );
        assert_eq!(result.execution.composition.recursive_relations, 1);
        assert_eq!(result.execution.composition.recursive_iterations, 38);
        assert_eq!(result.execution.composition.recursive_output_rows, 38);
        assert_eq!(result.execution.composition.recursive_peak_frontier_rows, 1);
        assert_eq!(result.execution.recursion_limit.get(), 38);
        for (name, value) in result.execution.composition.counters() {
            assert!(
                result
                    .execution
                    .native_io_certificate
                    .source_pushdown_report
                    .proof_basis
                    .split(|ch: char| ch.is_whitespace() || ch == ';')
                    .any(|field| field == format!("{name}={value}")),
                "missing {name}={value} in {}",
                result
                    .execution
                    .native_io_certificate
                    .source_pushdown_report
                    .proof_basis
            );
        }
        assert!(
            result
                .execution
                .native_io_certificate
                .materialization_boundaries
                .iter()
                .any(
                    |boundary| boundary.boundary_id == "native_relational.recursive_retention"
                        && boundary.rows_materialized == 38
                )
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let limited = Plan::Limit(Box::new(Limit {
        input: plan,
        offset: 0,
        count: 1,
    }));
    let prepared = prepare(&limited, &[Some(0)], 37).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let error = prepared
        .for_each_json_batch(&CancellationToken::default(), 1, 1024, |_| {
            delivered += 1;
            Ok(())
        })
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("SL_RECURSION_LIMIT_EXCEEDED"),
        "{error}"
    );
    assert_eq!(
        delivered, 0,
        "an outer LIMIT cannot certify a partial iteration result"
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn recursive_union_distinct_terminates_duplicate_cycles_and_keeps_null_identity() {
    for spill in [false, true] {
        let identity = RecursionId::default();
        let plan = Plan::Recursive(Arc::new(Recursive {
            anchor: scan(),
            step: input(&identity),
            identity,
            distinct: true,
        }));
        let prepared = prepare(&plan, &[Some(1), Some(1), None, None, Some(2)], 1).unwrap();
        let directory = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let runs = directory.0.join("recursive-runs");
        fs::create_dir(&runs).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    crate::relational_query::VortexRelationalSpillPolicy::new(
                        runs.clone(),
                        64 << 20,
                        1 << 20,
                    )
                    .unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![json!({"n":1}), json!({"n":null}), json!({"n":2})]
        );
        assert_eq!(result.execution.composition.recursive_iterations, 1);
        assert_eq!(result.execution.composition.recursive_output_rows, 3);
        if spill {
            assert!(
                result
                    .execution
                    .spill
                    .as_ref()
                    .unwrap()
                    .owned_cleanup_completed
            );
        }
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(runs).unwrap().count(), 0);
    }
}

#[test]
fn recursive_union_all_keeps_duplicates_and_empty_anchor_never_evaluates_step() {
    let plan = recursive(2, false);
    let result = prepare(&plan, &[Some(0), Some(0), None], 3)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        vec![
            json!({"n":0}),
            json!({"n":0}),
            json!({"n":null}),
            json!({"n":1}),
            json!({"n":1}),
            json!({"n":2}),
            json!({"n":2})
        ]
    );
    assert_eq!(result.execution.composition.recursive_iterations, 3);
    let empty = prepare(&plan, &[], 1)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&empty), Vec::<serde_json::Value>::new());
    assert_eq!(empty.execution.composition.recursive_iterations, 0);
    assert_eq!(empty.execution.composition.recursive_relations, 1);
}

#[test]
fn recursive_binding_rejects_foreign_nonlinear_and_type_changing_frontiers() {
    let identity = RecursionId::default();
    assert!(
        prepare(&input(&identity), &[Some(0)], 10)
            .err()
            .unwrap()
            .to_string()
            .contains("enclosing recursive step")
    );
    let wrong_type = Plan::Recursive(Arc::new(Recursive {
        anchor: scan(),
        step: project(
            input(&identity),
            vec![("n", lit(ScalarValue::Utf8("changed".into())))],
        ),
        identity: identity.clone(),
        distinct: false,
    }));
    assert!(
        prepare(&wrong_type, &[Some(0)], 10)
            .err()
            .unwrap()
            .to_string()
            .contains("exact value domains")
    );
    let foreign = Plan::Recursive(Arc::new(Recursive {
        anchor: scan(),
        step: input(&RecursionId::default()),
        identity: identity.clone(),
        distinct: false,
    }));
    assert!(
        prepare(&foreign, &[Some(0)], 10)
            .err()
            .unwrap()
            .to_string()
            .contains("self-reference")
    );
    let nonlinear = Plan::Recursive(Arc::new(Recursive {
        anchor: scan(),
        step: Plan::Set(Box::new(VortexRelationalSet {
            left: input(&identity),
            right: input(&identity),
            kind: SetKind::UnionAll,
        })),
        identity,
        distinct: false,
    }));
    assert!(prepare(&nonlinear, &[Some(0)], 10).is_err());
}

#[test]
fn retained_result_failure_releases_state_and_fresh_action_succeeds() {
    let prepared = prepare(&recursive(4, false), &[Some(0)], 5).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .for_each_json_batch(&CancellationToken::default(), 1, 1024, |_| {
            Err(failed("intentional retained consumer failure"))
        })
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("intentional retained consumer failure")
    );
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(prepared.collect_jsonl(&cancellation).is_err());
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (0..=4).map(|n| json!({"n":n})).collect::<Vec<_>>()
    );
    assert_eq!(result.execution.composition.recursive_iterations, 5);
}

#[test]
fn recursive_step_replays_one_producer_shared_with_its_anchor_without_reopening() {
    let identity = RecursionId::default();
    let joined = Plan::Join(Box::new(VortexRelationalJoin {
        left: input(&identity),
        right: scan(),
        kind: JoinKind::Inner,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("n").unwrap(),
            right: ColumnRef::new("n").unwrap(),
        }],
        condition: None,
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: ColumnRef::new("n").unwrap(),
            output_column: "n".into(),
        }],
    }));
    let plan = Plan::Recursive(Arc::new(Recursive {
        identity,
        anchor: Plan::Filter(Box::new(Filter {
            input: scan(),
            predicate: compare(col("n"), ComparisonOp::Eq, lit(ScalarValue::Int64(0))),
        })),
        step: project(
            joined,
            vec![(
                "n",
                binary(col("n"), BinaryOp::Add, lit(ScalarValue::Int64(1))),
            )],
        ),
        distinct: false,
    }));
    let batch = |session: &ResidentVortexSession, values: &[Option<i64>]| {
        ResidentMemorySource::from_batch_columns(
            session,
            &[MemoryColumn {
                name: "n",
                values: MemoryColumnValues::Int64(values),
            }],
        )
    };
    let prepared = prepare_relational_with_schema(policy(), |schemas| {
        schemas.register_batch_source(DatasetUri::new(SOURCE)?, |session| batch(session, &[]))?;
        Ok(plan)
    })
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for count in [3, 0, 2, 3] {
        let mut calls = 0;
        let mut witness = None;
        let mut producer = |session: &ResidentVortexSession| {
            assert!(
                witness
                    .as_ref()
                    .is_none_or(|owner: &std::sync::Weak<MemoryLease>| owner.strong_count() == 0)
            );
            let index = calls;
            calls += 1;
            assert!(index <= count, "iteration reopened its source");
            if index == count {
                return Ok(None);
            }
            let value = batch(session, &[Some(i64::from(index))])?;
            witness = Some(value.batch_release_witness()?);
            Ok(Some(value))
        };
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(calls, count + 1);
        let expected = if count == 0 {
            vec![]
        } else {
            (0..=count).map(|n| json!({"n":n})).collect()
        };
        assert_eq!(json_rows(&result), expected);
        assert_eq!(
            result.execution.composition.recursive_iterations,
            if count == 0 {
                0
            } else {
                u64::try_from(count).unwrap() + 1
            }
        );
        assert_eq!(
            result
                .execution
                .input
                .as_ref()
                .unwrap()
                .replay_prefilled_sources,
            1
        );
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn retained_output_keeps_its_own_credits_after_action_and_preparation_release() {
    let prepared = prepare(&recursive(2, false), &[Some(0)], 3).unwrap();
    let memory = prepared.session.memory().clone();
    let mut retained = Vec::new();
    let report = prepared
        .for_each_batch(&CancellationToken::default(), |array, _| {
            retained.push(array);
            Ok(())
        })
        .unwrap();
    drop(report);
    drop(prepared);
    assert_eq!(retained.iter().map(ArrayRef::len).sum::<usize>(), 3);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
