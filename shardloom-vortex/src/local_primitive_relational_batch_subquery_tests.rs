//! Exact subquery results, nested producer ownership and completion-only drains.

use super::*;
use crate::{
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalLimit, VortexRelationalQuantifier as Quantifier,
        VortexRelationalSpillPolicy, VortexRelationalSubquery,
        VortexRelationalSubqueryKind as Kind,
    },
};
use serde_json::{Value, json};
use shardloom_core::{BinaryOp, ComparisonOp, ScalarValue};

#[path = "local_primitive_relational_batch_subquery_pressure_tests.rs"]
mod pressure_tests;

#[path = "local_primitive_relational_batch_subquery_fault_tests.rs"]
mod fault_tests;

type Row = (Option<i64>, Option<&'static str>);
const URIS: [&str; 2] = ["memory://stream", "memory://inner"];
const LEFT: [Row; 4] = [
    (Some(1), Some("a")),
    (Some(2), Some("b")),
    (Some(3), Some("c")),
    (None, Some("z")),
];
const RIGHT: [Row; 3] = [
    (Some(2), Some("b")),
    (None, Some("q")),
    (Some(2), Some("b")),
];

fn input_scan(index: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(URIS[index]).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn project(input: VortexRelationalPlan, name: &str) -> VortexRelationalPlan {
    VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
        input,
        expressions: vec![(name.into(), col(name))],
    }))
}

fn limit(input: VortexRelationalPlan, count: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input,
        count,
        offset: 0,
    }))
}

fn key(name: &str) -> VortexRelationalJoinKey {
    VortexRelationalJoinKey {
        left: ColumnRef::new(name).unwrap(),
        right: ColumnRef::new(name).unwrap(),
    }
}

fn query(
    input: VortexRelationalPlan,
    relation: VortexRelationalPlan,
    kind: Kind,
) -> VortexRelationalSubquery {
    VortexRelationalSubquery {
        input,
        relation,
        kind,
        evaluation_guard: None,
        correlation: vec![],
        output_column: "answer".into(),
        negated: false,
    }
}

fn prepare_sources(
    plan: &VortexRelationalPlan,
    count: usize,
    bytes: u64,
) -> Result<PreparedVortexRelational> {
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = bytes;
    prepare_relational_with_schema(resources, |schemas| {
        for uri in &URIS[..count] {
            schemas.register_batch_source(DatasetUri::new(*uri)?, |session| {
                source(session, &[], &[])
            })?;
        }
        Ok(plan.clone())
    })
}

struct Inputs<'a> {
    rows: [&'a [Row]; 2],
    width: usize,
    calls: [u64; 2],
    prior: Option<Weak<MemoryLease>>,
}

impl<'a> Inputs<'a> {
    fn new(left: &'a [Row], right: &'a [Row], width: usize) -> Self {
        Self {
            rows: [left, right],
            width,
            calls: [0; 2],
            prior: None,
        }
    }

    fn next(
        &mut self,
        uri: &DatasetUri,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0),
            "the original outer batch must be released before inner demand"
        );
        let index = URIS
            .iter()
            .position(|candidate| *candidate == uri.as_str())
            .unwrap();
        assert_eq!(
            ordinal, self.calls[index],
            "producer reopened or ordinal changed"
        );
        self.calls[index] += 1;
        let start = usize::try_from(ordinal.saturating_sub(1)).unwrap() * self.width;
        let rows = if ordinal == 0 {
            &[][..]
        } else if start >= self.rows[index].len() {
            return Ok(None);
        } else {
            &self.rows[index][start..(start + self.width).min(self.rows[index].len())]
        };
        let numbers = rows.iter().map(|row| row.0).collect::<Vec<_>>();
        let strings = rows.iter().map(|row| row.1).collect::<Vec<_>>();
        let batch = source(session, &numbers, &strings)?;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

fn expected(answers: &[Value]) -> Vec<Value> {
    LEFT.iter()
        .zip(answers)
        .map(|(row, answer)| json!({"n":row.0,"s":row.1,"answer":answer}))
        .collect()
}

fn quantified(left: Option<i64>, op: ComparisonOp, quantifier: Quantifier) -> Value {
    let mut unknown = false;
    let any = quantifier == Quantifier::Any;
    for right in RIGHT {
        let Some((left, right)) = left.zip(right.0) else {
            unknown = true;
            continue;
        };
        let comparison = match op {
            ComparisonOp::Eq => left == right,
            ComparisonOp::NotEq => left != right,
            ComparisonOp::Lt => left < right,
            ComparisonOp::LtEq => left <= right,
            ComparisonOp::Gt => left > right,
            ComparisonOp::GtEq => left >= right,
        };
        if comparison == any {
            return json!(any);
        }
    }
    if unknown { Value::Null } else { json!(!any) }
}

#[test]
#[allow(clippy::too_many_lines)] // All predicate families share one ownership/result matrix.
fn streaming_subqueries_predicates_preserve_nulls_negation_correlations_and_batch_ownership() {
    let mut cases = vec![
        (
            "in".into(),
            Kind::In {
                columns: vec![key("n")],
            },
            false,
            vec![Value::Null, json!(true), Value::Null, Value::Null],
        ),
        (
            "not-in".into(),
            Kind::In {
                columns: vec![key("n")],
            },
            true,
            vec![Value::Null, json!(false), Value::Null, Value::Null],
        ),
        (
            "row-in".into(),
            Kind::In {
                columns: vec![key("n"), key("s")],
            },
            false,
            vec![json!(false), json!(true), json!(false), json!(false)],
        ),
        (
            "row-not-in".into(),
            Kind::In {
                columns: vec![key("n"), key("s")],
            },
            true,
            vec![json!(true), json!(false), json!(true), json!(true)],
        ),
        ("exists".into(), Kind::Exists, false, vec![json!(true); 4]),
        (
            "not-exists".into(),
            Kind::Exists,
            true,
            vec![json!(false); 4],
        ),
    ];
    for comparison in [
        ComparisonOp::Eq,
        ComparisonOp::NotEq,
        ComparisonOp::Lt,
        ComparisonOp::LtEq,
        ComparisonOp::Gt,
        ComparisonOp::GtEq,
    ] {
        for quantifier in [Quantifier::Any, Quantifier::All] {
            cases.push((
                format!("{comparison:?}-{quantifier:?}"),
                Kind::Quantified {
                    columns: key("n"),
                    comparison,
                    quantifier,
                },
                false,
                LEFT.iter()
                    .map(|row| quantified(row.0, comparison, quantifier))
                    .collect(),
            ));
        }
    }
    for (name, kind, negated, answers) in cases {
        for width in [1, 3] {
            let mut declaration = query(input_scan(0), input_scan(1), kind.clone());
            declaration.negated = negated;
            let plan = VortexRelationalPlan::Subquery(Box::new(declaration));
            let prepared = prepare_sources(&plan, 2, 8 << 20).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            for execution in 1..=2 {
                let mut inputs = Inputs::new(&LEFT, &RIGHT, width);
                let mut held = Vec::new();
                let mut actual = Vec::new();
                let result = prepared
                    .with_batch_inputs(&mut |uri, ordinal, session| {
                        inputs.next(uri, ordinal, session)
                    })
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |array, context| {
                        actual.extend(values(&array, context)?);
                        held.push(array);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(actual, expected(&answers), "{name} width={width}");
                assert_eq!(
                    inputs.calls,
                    [
                        (LEFT.len().div_ceil(width) + 2) as u64,
                        (RIGHT.len().div_ceil(width) + 2) as u64
                    ]
                );
                let input = result.input.as_ref().unwrap();
                assert_eq!(input.sources, 2);
                assert_eq!(input.rows, 7);
                assert_eq!(input.max_retained_input_batches, 1);
                assert_eq!(input.subquery_build_rows_detached, 3);
                assert_eq!(input.subquery_outer_rows_detached, 0);
                assert_eq!(input.subquery_validation_sources, 0);
                assert_eq!(input.replay_sources, 0);
                assert!(input.end_of_input_observed && input.output_ownership_detached);
                assert!(result.native_io_certificate.is_certified());
                assert!(!result.native_io_certificate.side_effects.fallback_attempted);
                assert_eq!(result.runtime.completed_executions, execution);
                drop(result);
                assert!(prepared.snapshot().memory.reserved_bytes > baseline);
                drop(held);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
    for kind in [
        Kind::Exists,
        Kind::In {
            columns: vec![key("n")],
        },
    ] {
        let mut declaration = query(input_scan(0), input_scan(1), kind);
        declaration.correlation = vec![key("s")];
        let prepared = prepare_sources(
            &VortexRelationalPlan::Subquery(Box::new(declaration)),
            2,
            8 << 20,
        )
        .unwrap();
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected(&[json!(false), json!(true), json!(false), json!(false)])
        );
    }
}

fn parameterized_inner() -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: input_scan(1),
        right: VortexRelationalPlan::Outer,
        kind: JoinKind::Inner,
        keys: vec![],
        condition: Some(Expression::new(
            ExprId::new("bounded").unwrap(),
            ExpressionKind::Compare {
                left: Box::new(col("left.n")),
                op: ComparisonOp::LtEq,
                right: Box::new(col("right.n")),
            },
        )),
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: ColumnRef::new("n").unwrap(),
            output_column: "n".into(),
        }],
    }))
}

#[test]
fn streaming_subqueries_scalar_and_parameterized_inner_replay_preserve_each_outer_binding() {
    let aggregate = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: parameterized_inner(),
        group_by: vec![],
        measures: vec![VortexSimpleAggregateMeasure::new(
            "max",
            Some(ColumnRef::new("n").unwrap()),
            "best".into(),
        )],
    }));
    let cases = [
        (
            false,
            limit(project(input_scan(1), "n"), 1),
            vec![json!(2); 4],
        ),
        (
            false,
            limit(project(input_scan(1), "n"), 0),
            vec![Value::Null; 4],
        ),
        (
            true,
            aggregate,
            vec![Value::Null, json!(2), json!(2), Value::Null],
        ),
    ];
    for (parameterized, relation, answers) in cases {
        let declaration = Box::new(query(input_scan(0), relation, Kind::Scalar));
        let plan = if parameterized {
            VortexRelationalPlan::CorrelatedSubquery(declaration)
        } else {
            VortexRelationalPlan::Subquery(declaration)
        };
        let prepared = prepare_sources(&plan, 2, 8 << 20).unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
            let result = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), expected(&answers));
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.subquery_outer_rows_detached, 4);
            assert_eq!(input.replay_sources, u64::from(parameterized));
            assert_eq!(input.replay_passes, if parameterized { 3 } else { 0 });
            assert_eq!(input.replay_rows, if parameterized { 9 } else { 0 });
            assert_eq!(input.replay_prefilled_sources, 0);
            assert_eq!(input.subquery_validation_sources, 0);
            assert_eq!(result.execution.runtime.completed_executions, execution);
            assert_eq!(inputs.calls, [4, 4]);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

fn skipped_scalar(empty_outer: bool, divide: bool) -> VortexRelationalPlan {
    let relation = if divide {
        VortexRelationalPlan::Project(Box::new(VortexRelationalProject {
            input: input_scan(1),
            expressions: vec![(
                "bad".into(),
                Expression::new(
                    ExprId::new("divide").unwrap(),
                    ExpressionKind::Binary {
                        left: Box::new(Expression::literal(
                            ExprId::new("one").unwrap(),
                            ScalarValue::Int64(1),
                        )),
                        op: BinaryOp::Divide,
                        right: Box::new(Expression::literal(
                            ExprId::new("zero").unwrap(),
                            ScalarValue::Int64(0),
                        )),
                    },
                ),
            )],
        }))
    } else {
        project(input_scan(1), "n")
    };
    let mut declaration = query(input_scan(0), relation, Kind::Scalar);
    if !empty_outer {
        declaration.evaluation_guard = Some(Expression::literal(
            ExprId::new("skip").unwrap(),
            ScalarValue::Boolean(false),
        ));
    }
    VortexRelationalPlan::Subquery(Box::new(declaration))
}

#[test]
fn streaming_subqueries_skipped_inner_drains_raw_input_without_expression_or_cardinality_evaluation()
 {
    for empty_outer in [false, true] {
        for divide in [false, true] {
            let plan = skipped_scalar(empty_outer, divide);
            let prepared = prepare_sources(&plan, 2, 8 << 20).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let left = if empty_outer { &[][..] } else { &LEFT[..] };
            let mut inputs = Inputs::new(left, &RIGHT, 2);
            let result = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            let expected = if empty_outer {
                vec![]
            } else {
                expected(&vec![Value::Null; 4])
            };
            assert_eq!(json_rows(&result), expected);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.subquery_validation_sources, 1);
            assert_eq!(input.subquery_validation_batches, 3);
            assert_eq!(input.subquery_validation_rows, 3);
            assert_eq!(input.subquery_build_rows_detached, 0);
            assert_eq!(input.replay_sources, 0);
            assert_eq!(inputs.calls, [if empty_outer { 2 } else { 4 }, 4]);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn streaming_subqueries_skipped_inner_failure_prevents_completion_and_releases_owners() {
    let plan = skipped_scalar(false, true);
    let prepared = prepare_sources(&plan, 2, 8 << 20).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for fault in ["late", "schema", "session", "cancel"] {
        let cancellation = CancellationToken::default();
        let foreign = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                if uri.as_str() == URIS[1] && ordinal == 2 {
                    match fault {
                        "late" => {
                            return Err(batch_input::failed("injected skipped producer failure"));
                        }
                        "schema" => {
                            return ResidentMemorySource::from_batch_columns(
                                session,
                                &[MemoryColumn {
                                    name: "wrong",
                                    values: MemoryColumnValues::Int64(&[Some(9)]),
                                }],
                            )
                            .map(Some);
                        }
                        "session" => {
                            return source(&foreign, &[Some(9)], &[Some("foreign")]).map(Some);
                        }
                        _ => cancellation.cancel(),
                    }
                }
                inputs.next(uri, ordinal, session)
            })
            .unwrap()
            .collect_jsonl(&cancellation);
        let error = result
            .err()
            .expect("unused input must still be valid")
            .to_string();
        let diagnostic = match fault {
            "late" => "injected skipped producer failure",
            "schema" | "session" => "declared schema, session or finite batch bounds",
            _ => "cancel",
        };
        assert!(error.contains(diagnostic), "{fault}: {error}");
        assert_eq!(inputs.calls, [4, if fault == "cancel" { 3 } else { 2 }]);
        assert_eq!(foreign.snapshot().memory.reserved_bytes, 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(
            inputs
                .prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
    }
    let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
    let result = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(json_rows(&result), expected(&vec![Value::Null; 4]));
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[test]
fn streaming_subqueries_shared_scalar_reenters_completed_replay_without_reopening_producer() {
    let plan = VortexRelationalPlan::Subquery(Box::new(query(
        input_scan(0),
        limit(project(input_scan(0), "n"), 1),
        Kind::Scalar,
    )));
    let prepared = prepare_sources(&plan, 1, 8 << 20).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let mut inputs = Inputs::new(&LEFT, &[], 2);
        let result = prepared
            .with_batch_input(&mut |session| {
                inputs.next(&DatasetUri::new(URIS[0]).unwrap(), inputs.calls[0], session)
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected(&vec![json!(1); 4]));
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.replay_prefilled_sources, 1);
        assert_eq!(input.replay_sources, 1);
        assert_eq!(input.replay_passes, 2);
        assert_eq!(input.replay_rows, 8);
        assert_eq!(input.subquery_outer_batches_detached, 0);
        assert_eq!(input.rows, 4);
        assert_eq!(input.subquery_build_rows_detached, 1);
        assert_eq!(inputs.calls, [4, 0]);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_subqueries_nested_static_inners_keep_roles_through_execution_time_binding() {
    let mut nested = query(
        input_scan(1),
        limit(project(input_scan(1), "n"), 1),
        Kind::Scalar,
    );
    nested.output_column = "inner_answer".into();
    let inner = limit(
        project(
            VortexRelationalPlan::Subquery(Box::new(nested)),
            "inner_answer",
        ),
        1,
    );
    let plan = VortexRelationalPlan::Subquery(Box::new(query(input_scan(0), inner, Kind::Scalar)));
    let uris = URIS
        .iter()
        .map(|uri| DatasetUri::new(*uri).unwrap())
        .collect::<Vec<_>>();
    let prepared = prepare_relational_with_dynamic_inputs(
        &uris,
        policy(),
        65_536,
        |schemas| {
            for uri in &uris {
                schemas.register_batch_source(uri.clone(), |session| source(session, &[], &[]))?;
            }
            Ok(())
        },
        move |schemas| schemas.resolve_output(&plan).map(|(resolved, _)| resolved),
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let mut inputs = Inputs::new(&LEFT, &RIGHT, 2);
        let result = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(json_rows(&result), expected(&vec![json!(2); 4]));
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.replay_prefilled_sources, 1);
        assert_eq!(input.replay_passes, 2);
        assert_eq!(input.replay_rows, 6);
        assert_eq!(input.subquery_outer_rows_detached, 4);
        assert_eq!(input.subquery_build_rows_detached, 2);
        assert_eq!(inputs.calls, [4, 4]);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_subqueries_opaque_dynamic_inner_is_refused_before_producer_or_closure_demand() {
    let uri = DatasetUri::new(URIS[0]).unwrap();
    let prepared = prepare_relational_with_dynamic_inputs(
        std::slice::from_ref(&uri),
        policy(),
        65_536,
        |schemas| schemas.register_batch_source(uri.clone(), |session| source(session, &[], &[])),
        |schemas| {
            let relation =
                schemas.defer_subquery(4096, |_| panic!("opaque inner must remain unsupported"))?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                input_scan(0),
                relation,
                Kind::Exists,
            ))))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .with_batch_input(&mut |_| panic!("unsupported shape must precede producer demand"))
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("SL-NATIVE-BATCH") && error.contains("dynamic schema"),
        "{error}"
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
