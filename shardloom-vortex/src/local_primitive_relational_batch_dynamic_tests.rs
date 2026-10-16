//! Discovery owns completed native pivot state, never a second producer pass.

use super::*;
use crate::{
    relational_query::VortexRelationalSpillPolicy,
    resident_memory_source::{MemoryColumn, MemoryColumnValues},
};
use std::{cell::Cell, sync::Weak};

const URIS: [&str; 2] = ["memory://pivot", "memory://other-pivot"];

fn scan(index: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(URIS[index]).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn source(
    session: &ResidentVortexSession,
    entities: &[Option<i64>],
    categories: &[Option<&str>],
    amounts: &[Option<i64>],
) -> Result<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "entity",
                values: MemoryColumnValues::Int64(entities),
            },
            MemoryColumn {
                name: "category",
                values: MemoryColumnValues::Utf8(categories),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Int64(amounts),
            },
        ],
    )
}

fn next(
    session: &ResidentVortexSession,
    ordinal: u64,
    factor: i64,
) -> Result<Option<ResidentMemorySource>> {
    match ordinal {
        0 => source(session, &[], &[], &[]).map(Some),
        1 => source(
            session,
            &[Some(1), Some(1)],
            &[Some("a"), Some("b")],
            &[Some(3 * factor), Some(5 * factor)],
        )
        .map(Some),
        2 => source(
            session,
            &[Some(2), Some(2)],
            &[Some("a"), Some("a")],
            &[Some(7 * factor), Some(11 * factor)],
        )
        .map(Some),
        3 => Ok(None),
        _ => panic!("dynamic discovery replayed its producer"),
    }
}

fn prepare_streams(
    count: usize,
    lower: impl Fn(&mut VortexRelationalPreparation<'_>) -> Result<VortexRelationalPlan>
    + Send
    + 'static,
) -> PreparedVortexRelational {
    let uris = URIS[..count]
        .iter()
        .map(|uri| DatasetUri::new(*uri).unwrap())
        .collect::<Vec<_>>();
    prepare_relational_with_dynamic_inputs(
        &uris,
        policy(),
        65_536,
        |schemas| {
            for uri in &uris {
                schemas
                    .register_batch_source(uri.clone(), |session| source(session, &[], &[], &[]))?;
            }
            Ok(())
        },
        lower,
    )
    .unwrap()
}

fn values(
    array: &ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<serde_json::Value>> {
    let mut sink =
        crate::local_primitives::collect::JsonRows::new(context.memory(), 1 << 20, false)?;
    sink.append_native(array, context)?;
    Ok(serde_json::from_str(sink.finish()?.value()).unwrap())
}

#[test]
fn streaming_subqueries_static_inner_can_reuse_completed_dynamic_outer_pivot_source() {
    use crate::relational_query::{
        VortexRelationalLimit, VortexRelationalSubquery, VortexRelationalSubqueryKind,
    };
    let prepared = prepare_streams(1, |schemas| {
        schemas.declare_source_reuse(&DatasetUri::new(URIS[0]).unwrap())?;
        let (outer, _) = schemas.resolve_output(&pivot(scan(0)))?;
        let relation = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: project(scan(0), &["amount".into()]),
            count: 1,
            offset: 0,
        }));
        Ok(VortexRelationalPlan::Subquery(Box::new(
            VortexRelationalSubquery {
                input: outer,
                relation,
                kind: VortexRelationalSubqueryKind::Scalar,
                evaluation_guard: None,
                correlation: vec![],
                output_column: "answer".into(),
                negated: false,
            },
        )))
    });
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let mut ordinal = 0;
        let mut prior: Option<Weak<MemoryLease>> = None;
        let result = prepared
            .with_batch_input(&mut |session| {
                assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                let batch = next(session, ordinal, 1)?;
                ordinal += 1;
                prior = batch
                    .as_ref()
                    .map(ResidentMemorySource::batch_release_witness)
                    .transpose()?;
                Ok(batch)
            })
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            vec![
                serde_json::json!({"entity":1,"pivot_a":3.0,"pivot_b":5.0,"answer":3}),
                serde_json::json!({"entity":2,"pivot_a":18.0,"pivot_b":null,"answer":3}),
            ]
        );
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(ordinal, 4);
        assert_eq!(input.replay_sources, 1);
        assert_eq!(input.replay_passes, 1);
        assert_eq!(input.replay_prefilled_sources, 0);
        assert_eq!(result.execution.runtime.completed_executions, execution);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_dynamic_pivot_is_inert_until_execution_and_releases_input_before_demand() {
    let lower_calls = Arc::new(AtomicUsize::new(0));
    let calls = lower_calls.clone();
    let prepared = prepare_streams(1, move |schemas| {
        calls.fetch_add(1, Ordering::Relaxed);
        let (resolved, columns) = schemas.resolve_output(&pivot(scan(0)))?;
        assert_eq!(columns, ["entity", "pivot_a", "pivot_b"]);
        Ok(project(resolved, &columns))
    });
    assert_eq!(lower_calls.load(Ordering::Relaxed), 0);
    assert_eq!(prepared.snapshot().prepared_source_opens, 0);
    assert!(prepared.output_dtype().is_none());
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let ordinal = Cell::new(0_u64);
        let ended = Cell::new(false);
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut producer = |session: &ResidentVortexSession| {
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            let source = next(session, ordinal.get(), 1)?;
            ordinal.set(ordinal.get() + 1);
            ended.set(source.is_none());
            prior = source
                .as_ref()
                .map(ResidentMemorySource::batch_release_witness)
                .transpose()?;
            Ok(source)
        };
        let mut retained = Vec::new();
        let mut actual = Vec::new();
        let report = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                assert!(
                    ended.get(),
                    "downstream output must follow complete discovery"
                );
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected());
        assert_eq!(ordinal.get(), 4);
        assert_eq!(lower_calls.load(Ordering::Relaxed), execution);
        assert_eq!(report.dynamic_schema_stages, 1);
        assert_eq!(report.scan_rows_delivered, 4);
        assert_eq!(report.runtime.completed_executions, execution as u64);
        let input = report.input.as_ref().unwrap();
        assert_eq!(
            (input.sources, input.payload_batches, input.rows),
            (1, 3, 4)
        );
        assert_eq!(input.max_retained_input_batches, 1);
        assert!(input.end_of_input_observed && input.output_ownership_detached);
        assert!(report.native_io_certificate.is_certified());
        assert!(!report.native_io_certificate.side_effects.fallback_attempted);
        drop(report);
        assert!(prepared.snapshot().memory.reserved_bytes > baseline);
        drop(retained);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
    let memory = prepared.session.memory().clone();
    let mut ordinal = 0;
    let mut producer = |session: &ResidentVortexSession| {
        let source = next(session, ordinal, 1)?;
        ordinal += 1;
        Ok(source)
    };
    let owned = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .execute_native(&CancellationToken::default())
        .unwrap();
    drop(prepared);
    assert_eq!(owned.result.row_count(), 2);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(owned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn streamed_dynamic_empty_domains_keep_actual_schema_and_drain_zero_limits() {
    let fixture = fixture();
    let workspace = fixture.0.join("stream-pivot-runs");
    fs::create_dir(&workspace).unwrap();
    for spill in [false, true] {
        for empty in [false, true] {
            let mut prepared = prepare_streams(1, move |schemas| {
                let input = if empty {
                    scan(0)
                } else {
                    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                        input: scan(0),
                        offset: 0,
                        count: 0,
                    }))
                };
                let (resolved, columns) = schemas.resolve_output(&pivot(input))?;
                assert_eq!(columns, ["entity"]);
                Ok(project(resolved, &columns))
            });
            if spill {
                prepared = prepared
                    .with_spill(
                        VortexRelationalSpillPolicy::new(workspace.clone(), 128 << 20, 1 << 20)
                            .unwrap(),
                    )
                    .unwrap();
            }
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut ordinal = 0;
            let mut producer = |session: &ResidentVortexSession| {
                let result = if empty {
                    Ok(None)
                } else {
                    next(session, ordinal, 1)
                };
                ordinal += 1;
                result
            };
            let mut outputs = 0;
            let report = prepared
                .with_batch_input(&mut producer)
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |array, _| {
                    outputs += 1;
                    assert!(array.is_empty());
                    assert_eq!(
                        array.dtype(),
                        &DType::struct_(
                            [(
                                "entity",
                                DType::Primitive(PType::I64, Nullability::Nullable)
                            )],
                            Nullability::NonNullable
                        )
                    );
                    Ok(())
                })
                .unwrap();
            assert_eq!(outputs, 1);
            assert_eq!(ordinal, if empty { 1 } else { 4 });
            assert_eq!(
                report.input.as_ref().unwrap().rows,
                if empty { 0 } else { 4 }
            );
            assert!(report.input.as_ref().unwrap().end_of_input_observed);
            drop(report);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
    }
}

#[test]
fn streamed_dynamic_two_pivots_join_completed_states_with_fresh_values() {
    let prepared = prepare_streams(2, |schemas| {
        let (left, _) = schemas.resolve_output(&pivot(scan(0)))?;
        let (right, _) = schemas.resolve_output(&pivot(scan(1)))?;
        Ok(VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left,
            right,
            kind: JoinKind::Inner,
            condition: None,
            keys: vec![VortexRelationalJoinKey {
                left: ColumnRef::new("entity")?,
                right: ColumnRef::new("entity")?,
            }],
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("entity")?,
                    output_column: "entity".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("pivot_a")?,
                    output_column: "left_a".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: ColumnRef::new("pivot_a")?,
                    output_column: "right_a".into(),
                },
            ],
        })))
    });
    let baseline = prepared.snapshot().memory.reserved_bytes;
    for execution in 1..=2 {
        let mut calls = [0; 2];
        let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
            let index = URIS
                .iter()
                .position(|candidate| *candidate == uri.as_str())
                .unwrap();
            assert_eq!(ordinal, calls[index]);
            calls[index] += 1;
            next(
                session,
                ordinal,
                i64::from(execution) * (i64::try_from(index).unwrap() + 1),
            )
        };
        let result = prepared
            .with_batch_inputs(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let factor = f64::from(execution);
        assert_eq!(
            json_rows(&result),
            serde_json::json!([
                {"entity":1,"left_a":3.0*factor,"right_a":6.0*factor},
                {"entity":2,"left_a":18.0*factor,"right_a":36.0*factor},
            ])
            .as_array()
            .unwrap()
            .clone()
        );
        assert_eq!(calls, [4, 4]);
        assert_eq!(result.execution.dynamic_schema_stages, 2);
        assert_eq!(result.execution.input.as_ref().unwrap().sources, 2);
        assert_eq!(result.execution.input.as_ref().unwrap().rows, 8);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_dynamic_failures_abort_discovery_and_preserve_existing_destination() {
    use crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex;
    let fixture = fixture();
    let workspace = fixture.0.join("failed-pivot-runs");
    fs::create_dir(&workspace).unwrap();
    fs::write(workspace.join("foreign"), b"keep").unwrap();
    let path = fixture.0.join("prior.vortex");
    for failure in ["producer", "schema", "cancel", "binding"] {
        let prepared = prepare_streams(1, move |schemas| {
            let (resolved, columns) = schemas.resolve_output(&pivot(scan(0)))?;
            Ok(if failure == "binding" {
                project(resolved, &["not_observed".into()])
            } else {
                project(resolved, &columns)
            })
        })
        .with_spill(
            VortexRelationalSpillPolicy::new(workspace.clone(), 128 << 20, 1 << 20).unwrap(),
        )
        .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        fs::write(&path, b"previous destination").unwrap();
        let cancellation = CancellationToken::default();
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            let ordinal = calls;
            calls += 1;
            if ordinal == 3 {
                match failure {
                    "producer" => return Err(batch_input::failed("dynamic producer sentinel")),
                    "schema" => {
                        return ResidentMemorySource::from_batch_columns(
                            session,
                            &[MemoryColumn {
                                name: "changed",
                                values: MemoryColumnValues::Int64(&[Some(1)]),
                            }],
                        )
                        .map(Some);
                    }
                    "cancel" => cancellation.cancel(),
                    _ => {}
                }
            }
            next(session, ordinal, 1)
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_controlled(&path, Vortex, true, &cancellation)
            .err()
            .unwrap();
        assert_eq!(calls, 4, "{failure}: {error}");
        assert_eq!(fs::read(&path).unwrap(), b"previous destination");
        assert_eq!(fs::read(workspace.join("foreign")).unwrap(), b"keep");
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 3);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_dynamic_consumer_failure_and_unused_source_never_certify_completion() {
    for unused in [false, true] {
        let prepared = prepare_streams(if unused { 2 } else { 1 }, |schemas| {
            schemas
                .resolve_output(&pivot(scan(0)))
                .map(|(resolved, _)| resolved)
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let calls = Cell::new(0);
        let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
            assert_eq!(uri.as_str(), URIS[0]);
            calls.set(calls.get() + 1);
            next(session, ordinal, 1)
        };
        let error = prepared
            .with_batch_inputs(&mut producer)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| {
                assert_eq!(calls.get(), 4);
                if unused {
                    Ok(())
                } else {
                    Err(batch_input::failed("dynamic consumer sentinel"))
                }
            })
            .err()
            .unwrap();
        assert!(
            error.to_string().contains(if unused {
                "explicit end-of-input"
            } else {
                "consumer sentinel"
            }),
            "{error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streamed_dynamic_unsupported_shapes_and_initial_cancellation_precede_demand() {
    for shape in ["set-left-outer", "set-right-outer", "unary", "cancel"] {
        let prepared = prepare_streams(1, move |schemas| {
            let plan = match shape {
                "set-left-outer" | "set-right-outer" => {
                    VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                        left: if shape == "set-left-outer" {
                            VortexRelationalPlan::Outer
                        } else {
                            scan(0)
                        },
                        right: if shape == "set-right-outer" {
                            VortexRelationalPlan::Outer
                        } else {
                            scan(0)
                        },
                        kind: SetKind::UnionAll,
                    }))
                }
                "unary" => VortexRelationalPlan::Unary(Box::new(VortexRelationalUnary {
                    input: scan(0),
                    request: VortexQueryPrimitiveRequest::for_relational_input(
                        VortexQueryPrimitiveKind::Unsupported,
                        shardloom_plan::ProjectionRequest::All,
                    ),
                })),
                _ => panic!("cancelled execution must not invoke lowering"),
            };
            schemas.resolve_output(&plan).map(|(resolved, _)| resolved)
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let cancellation = CancellationToken::default();
        if shape == "cancel" {
            cancellation.cancel();
        }
        let mut producer = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
            panic!("unsupported dynamic subtree must precede producer demand")
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&cancellation)
            .err()
            .unwrap();
        let expected = match shape {
            "set-left-outer" | "set-right-outer" => "correlated source",
            "unary" => "unary stateful",
            _ => "cancel",
        };
        assert!(
            error.to_string().to_lowercase().contains(expected),
            "{error}"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

fn same_source_pivots(
    left: VortexRelationalPlan,
    right: VortexRelationalPlan,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left,
        right,
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("entity").unwrap(),
            right: ColumnRef::new("entity").unwrap(),
        }],
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("entity").unwrap(),
                output_column: "entity".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("pivot_a").unwrap(),
                output_column: "a".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("pivot_b").unwrap(),
                output_column: "b".into(),
            },
        ],
    }))
}

#[test]
fn streaming_replay_dynamic_pivots_infer_complete_subtree_or_use_early_declaration() {
    for separate in [false, true] {
        for spill in [false, true] {
            let fixture = fixture();
            let prepared = prepare_streams(1, move |schemas| {
                if separate {
                    schemas.declare_source_reuse(&DatasetUri::new(URIS[0])?)?;
                    let (left, _) = schemas.resolve_output(&pivot(scan(0)))?;
                    let (right, _) = schemas.resolve_output(&pivot(scan(0)))?;
                    Ok(same_source_pivots(left, right))
                } else {
                    let plan = same_source_pivots(pivot(scan(0)), pivot(scan(0)));
                    schemas.resolve_output(&plan).map(|(plan, _)| plan)
                }
            });
            let prepared = if spill {
                let runs = fixture.0.join("replay-pivot-runs");
                fs::create_dir(&runs).unwrap();
                prepared
                    .with_spill(VortexRelationalSpillPolicy::new(runs, 128 << 20, 1 << 20).unwrap())
                    .unwrap()
            } else {
                prepared
            };
            let baseline = prepared.snapshot().memory.reserved_bytes;
            for factor in 1u32..=2 {
                let mut calls = 0;
                let mut prior: Option<Weak<MemoryLease>> = None;
                let mut producer = |session: &ResidentVortexSession| {
                    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                    let batch = next(session, calls, i64::from(factor))?;
                    calls += 1;
                    prior = batch
                        .as_ref()
                        .map(ResidentMemorySource::batch_release_witness)
                        .transpose()?;
                    Ok(batch)
                };
                let result = prepared
                    .with_batch_input(&mut producer)
                    .unwrap()
                    .collect_jsonl(&CancellationToken::default())
                    .unwrap();
                assert_eq!(
                    json_rows(&result),
                    [
                        serde_json::json!({"entity":1,"a":3.0*f64::from(factor),"b":5.0*f64::from(factor)}),
                        serde_json::json!({"entity":2,"a":18.0*f64::from(factor),"b":null}),
                    ]
                );
                assert_eq!(calls, 4);
                assert_eq!(result.execution.dynamic_schema_stages, 2);
                let input = result.execution.input.as_ref().unwrap();
                assert_eq!(input.rows, 4);
                assert_eq!(input.replay_passes, 1);
                assert_eq!(input.replay_rows, 4);
                assert!(result.execution.native_io_certificate.is_certified());
                drop(result);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if spill {
                    assert_eq!(
                        fs::read_dir(fixture.0.join("replay-pivot-runs"))
                            .unwrap()
                            .count(),
                        0
                    );
                }
            }
        }
    }
}

#[test]
fn streaming_replay_dynamic_late_or_undeclared_reuse_fails_without_second_producer() {
    for late_declaration in [false, true] {
        let prepared = prepare_streams(1, move |schemas| {
            let (left, _) = schemas.resolve_output(&pivot(scan(0)))?;
            if late_declaration {
                schemas.declare_source_reuse(&DatasetUri::new(URIS[0])?)?;
            }
            let (right, _) = schemas.resolve_output(&pivot(scan(0)))?;
            Ok(same_source_pivots(left, right))
        });
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            let batch = next(session, calls, 1)?;
            calls += 1;
            Ok(batch)
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("source reuse must be declared before its first input demand"),
            "{error}"
        );
        assert_eq!(calls, 4);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
