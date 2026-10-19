//! Declared dynamic inners preserve parameter state and finite producer owners.

use super::*;
use crate::relational_query::{
    VortexRelationalQuantifier as Quantifier, VortexRelationalSpillPolicy,
    VortexRelationalSubquery, VortexRelationalSubqueryKind as Kind,
};
use serde_json::{Value, json};
use shardloom_core::{ComparisonOp, ExpressionKind, ScalarValue};

#[path = "local_primitive_relational_batch_deferred_composition_tests.rs"]
mod composition;
#[path = "local_primitive_relational_batch_deferred_fault_tests.rs"]
mod faults;
#[path = "local_primitive_relational_batch_deferred_lifecycle_tests.rs"]
mod lifecycle;
#[path = "local_primitive_relational_batch_deferred_pressure_tests.rs"]
mod pressure;

fn key() -> VortexRelationalJoinKey {
    VortexRelationalJoinKey {
        left: ColumnRef::new("entity").unwrap(),
        right: ColumnRef::new("entity").unwrap(),
    }
}

fn inner(index: usize) -> VortexRelationalPlan {
    pivot(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: scan(index),
        right: VortexRelationalPlan::Outer,
        kind: JoinKind::Inner,
        keys: vec![key()],
        condition: None,
        columns: ["entity", "category", "amount"]
            .into_iter()
            .map(|name| VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new(name).unwrap(),
                output_column: name.into(),
            })
            .collect(),
    })))
}

fn query(relation: VortexRelationalPlan, kind: Kind) -> VortexRelationalSubquery {
    VortexRelationalSubquery {
        input: scan(0),
        relation,
        kind,
        correlation: vec![],
        output_column: "answer".into(),
        evaluation_guard: None,
        negated: false,
    }
}

fn expected(answer: bool) -> Vec<Value> {
    vec![
        json!({"entity":1,"category":"a","amount":3,"answer":answer}),
        json!({"entity":1,"category":"b","amount":5,"answer":answer}),
        json!({"entity":2,"category":"a","amount":7,"answer":answer}),
        json!({"entity":2,"category":"a","amount":11,"answer":answer}),
    ]
}

fn collect(prepared: &PreparedVortexRelational) -> (CollectedVortexRelational, [u64; 2]) {
    let mut calls = [0; 2];
    let mut prior: Option<Weak<MemoryLease>> = None;
    let result = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            let index = URIS.iter().position(|name| *name == uri.as_str()).unwrap();
            assert_eq!(ordinal, calls[index]);
            calls[index] += 1;
            let batch = next(session, ordinal, 1)?;
            prior = batch
                .as_ref()
                .map(ResidentMemorySource::batch_release_witness)
                .transpose()?;
            Ok(batch)
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    (result, calls)
}

#[test]
#[allow(clippy::too_many_lines)] // Keep both pressure strategies beside the same complete replay oracle.
fn streaming_dynamic_subqueries_preserve_predicates_parameter_schemas_replay_and_owner_release() {
    let mut cases = vec![
        (
            Kind::In {
                columns: vec![key()],
            },
            false,
            true,
        ),
        (
            Kind::In {
                columns: vec![key()],
            },
            true,
            false,
        ),
        (Kind::Exists, false, true),
        (Kind::Exists, true, false),
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
                Kind::Quantified {
                    columns: key(),
                    comparison,
                    quantifier,
                },
                false,
                matches!(
                    comparison,
                    ComparisonOp::Eq | ComparisonOp::LtEq | ComparisonOp::GtEq
                ),
            ));
        }
    }
    for shared in [false, true] {
        for (kind, negated, answer) in &cases {
            for spilling in [false, true] {
                let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
                let spill_directory = fixture.0.join("dynamic-subquery-matrix");
                fs::create_dir(&spill_directory).unwrap();
                let domains = Arc::new(Mutex::new(Vec::new()));
                let observed = domains.clone();
                let kind = kind.clone();
                let negated = *negated;
                let count = if shared { 1 } else { 2 };
                let prepared = prepare_streams(count, move |schemas| {
                    let index = usize::from(!shared);
                    let observed = observed.clone();
                    let uri = DatasetUri::new(URIS[index])?;
                    let relation = schemas.defer_subquery_with_sources(
                        &[uri.clone(), uri],
                        65_536,
                        move |schemas| {
                            let (relation, columns) = schemas.resolve_output(&inner(index))?;
                            observed.lock().unwrap().push(columns);
                            Ok(project(relation, &["entity".into()]))
                        },
                    )?;
                    let mut query = query(relation, kind.clone());
                    query.negated = negated;
                    Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
                });
                let prepared = if spilling {
                    prepared
                        .with_spill(
                            VortexRelationalSpillPolicy::new(&spill_directory, 64 << 20, 1 << 20)
                                .unwrap(),
                        )
                        .unwrap()
                } else {
                    prepared
                };
                let baseline = prepared.snapshot().memory.reserved_bytes;
                for execution in 1..=2 {
                    let (result, calls) = collect(&prepared);
                    assert_eq!(json_rows(&result), expected(*answer));
                    assert_eq!(calls, [4, if shared { 0 } else { 4 }]);
                    let input = result.execution.input.as_ref().unwrap();
                    assert_eq!(input.sources, count);
                    assert_eq!(input.replay_sources, 1);
                    assert_eq!(input.replay_prefilled_sources, u64::from(shared));
                    assert_eq!(input.replay_passes, if shared { 5 } else { 3 });
                    assert_eq!(input.replay_rows, if shared { 20 } else { 12 });
                    assert_eq!(result.execution.runtime.completed_executions, execution);
                    assert_eq!(
                        result.execution.spillable_subquery_stages,
                        if spilling { 4 } else { 0 }
                    );
                    if spilling {
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
                    assert_eq!(fs::read_dir(&spill_directory).unwrap().count(), 0);
                }
                assert_eq!(
                    *domains.lock().unwrap(),
                    [
                        vec!["entity", "pivot_a", "pivot_b"],
                        vec!["entity", "pivot_a", "pivot_b"],
                        vec!["entity", "pivot_a"],
                        vec!["entity", "pivot_a"],
                    ]
                    .into_iter()
                    .cycle()
                    .take(8)
                    .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn streaming_dynamic_subqueries_skip_lowering_but_validate_unused_sources_without_replay() {
    for mode in ["empty", "false", "null", "unchosen"] {
        let prepared = prepare_streams(2, move |schemas| {
            let relation = schemas.defer_subquery_with_sources(
                &[DatasetUri::new(URIS[1])?],
                65_536,
                move |_| {
                    assert_eq!(
                        mode, "unchosen",
                        "unselected dynamic inner must not be lowered"
                    );
                    Ok(VortexRelationalPlan::Outer)
                },
            )?;
            let mut query = query(relation, Kind::Exists);
            if mode == "empty" {
                query.input = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input: scan(0),
                    count: 0,
                    offset: 0,
                }));
            } else if mode != "unchosen" {
                let value = Expression::literal(
                    ExprId::new("guard")?,
                    if mode == "false" {
                        ScalarValue::Boolean(false)
                    } else {
                        ScalarValue::Null
                    },
                );
                query.evaluation_guard = Some(Expression::new(
                    ExprId::new("boolean-guard")?,
                    ExpressionKind::Cast {
                        expr: Box::new(value),
                        target_dtype: shardloom_core::LogicalDType::Boolean,
                    },
                ));
            }
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let (result, calls) = collect(&prepared);
        assert_eq!(
            json_rows(&result),
            if mode == "empty" {
                vec![]
            } else {
                expected(mode == "unchosen")
            }
        );
        assert_eq!(calls, [4, 4]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.subquery_validation_sources, 1);
        assert_eq!(input.subquery_validation_rows, 4);
        assert_eq!(input.replay_sources, 0);
        assert_eq!(input.replay_passes, 0);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_reject_source_scope_escape_before_inner_demand() {
    for mode in ["scan", "discovery", "nested", "reuse", "later-child"] {
        let prepared = prepare_streams(2, move |schemas| {
            let declared = usize::from(mode == "later-child");
            let relation = schemas.defer_subquery_with_sources(
                &[DatasetUri::new(URIS[declared])?],
                65_536,
                move |schemas| match mode {
                    "scan" => Ok(scan(1)),
                    "discovery" => schemas
                        .resolve_output(&pivot(scan(1)))
                        .map(|(plan, _)| plan),
                    "nested" => schemas.defer_subquery_with_sources(
                        &[DatasetUri::new(URIS[1])?],
                        65_536,
                        |_| Ok(scan(1)),
                    ),
                    "reuse" => {
                        schemas.declare_source_reuse(&DatasetUri::new(URIS[1])?)?;
                        Ok(scan(0))
                    }
                    _ => Ok(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
                        left: pivot(scan(1)),
                        right: scan(0),
                        kind: JoinKind::Inner,
                        keys: vec![key()],
                        condition: None,
                        columns: vec![VortexRelationalJoinColumn {
                            side: Side::Left,
                            column: ColumnRef::new("entity")?,
                            output_column: "entity".into(),
                        }],
                    }))),
                },
            )?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                relation,
                Kind::Exists,
            ))))
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = [0; 2];
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| {
                let index = URIS.iter().position(|name| *name == uri.as_str()).unwrap();
                calls[index] += 1;
                assert_eq!(index, 0, "source validation must precede any inner demand");
                next(session, ordinal, 1)
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("outside its declaration"), "{mode}: {error}");
        assert!(calls[0] > 0);
        assert_eq!(calls[1], 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_source_free_inner_uses_only_each_outer_parameter() {
    let prepared = prepare_streams(1, |schemas| {
        let relation = schemas.defer_subquery_with_sources(&[], 65_536, |schemas| {
            schemas
                .resolve_output(&pivot(VortexRelationalPlan::Outer))
                .map(|(plan, _)| plan)
        })?;
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
            relation,
            Kind::Exists,
        ))))
    });
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let (result, calls) = collect(&prepared);
        assert_eq!(json_rows(&result), expected(true));
        assert_eq!(calls, [4, 0]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.replay_sources, 0);
        assert_eq!(input.replay_passes, 0);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_validate_declarations_before_producer_demand() {
    for mode in ["unknown", "too-many", "zero-credit", "denied-credit"] {
        let prepared = prepare_streams(1, move |schemas| {
            let sources = match mode {
                "unknown" => vec![DatasetUri::new("memory://undeclared")?],
                "too-many" => vec![DatasetUri::new(URIS[0])?; 129],
                _ => vec![DatasetUri::new(URIS[0])?],
            };
            let bytes = match mode {
                "zero-credit" => 0,
                "denied-credit" => 32 << 20,
                _ => 65_536,
            };
            let relation = schemas.defer_subquery_with_sources(&sources, bytes, |_| {
                panic!("invalid declarations must not invoke the lowerer")
            })?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                relation,
                Kind::Exists,
            ))))
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let error = prepared
            .with_batch_input(&mut |_| panic!("invalid declaration must precede source demand"))
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap()
            .to_string();
        let message = match mode {
            "unknown" => "local Vortex input",
            "too-many" => "128 declared sources",
            "zero-credit" => "metadata credits",
            _ => "reservation",
        };
        assert!(error.contains(message), "{mode}: {error}");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_opaque_resident_child_cannot_escape_parent_source_scope() {
    let uris = URIS
        .iter()
        .map(|name| DatasetUri::new(*name).unwrap())
        .collect::<Vec<_>>();
    let prepared = prepare_relational_with_dynamic_inputs(
        &uris,
        policy(),
        65_536,
        |schemas| {
            for uri in &uris {
                schemas.register_memory_source(uri.clone(), |session| {
                    ResidentMemorySource::from_columns(
                        session,
                        &[
                            MemoryColumn {
                                name: "entity",
                                values: MemoryColumnValues::Int64(&[Some(1)]),
                            },
                            MemoryColumn {
                                name: "category",
                                values: MemoryColumnValues::Utf8(&[Some("a")]),
                            },
                            MemoryColumn {
                                name: "amount",
                                values: MemoryColumnValues::Int64(&[Some(3)]),
                            },
                        ],
                        crate::resident_memory_source::MemorySourceBounds::default(),
                    )
                })?;
            }
            Ok(())
        },
        |schemas| {
            let relation = schemas.defer_subquery_with_sources(
                &[DatasetUri::new(URIS[0])?],
                65_536,
                |schemas| {
                    let relation = schemas.defer_subquery(65_536, |_| Ok(scan(1)))?;
                    let mut query = query(relation, Kind::Exists);
                    query.input = VortexRelationalPlan::Outer;
                    Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
                },
            )?;
            Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                relation,
                Kind::Exists,
            ))))
        },
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("outside its declaration"), "{error}");
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
