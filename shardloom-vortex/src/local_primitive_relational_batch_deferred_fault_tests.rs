//! Deferred discovery and completion failures discard only owned provisional state.

use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat::Vortex;
use std::sync::atomic::AtomicBool;

#[test]
#[allow(clippy::too_many_lines)] // Keep failed publication and fresh-execution recovery in one fault matrix.
fn streaming_dynamic_subqueries_late_discovery_failures_preserve_destinations_and_recover() {
    for shared in [false, true] {
        for failure in ["producer", "schema", "cancel", "binding"] {
            let fixture = fixture();
            let workspace = fixture.0.join("deferred-runs");
            fs::create_dir(&workspace).unwrap();
            fs::write(workspace.join("foreign"), b"keep").unwrap();
            let prior_path = fixture.0.join("prior.vortex");
            fs::write(&prior_path, b"previous destination").unwrap();
            let path = fixture.0.join("new.vortex");
            let failing = Arc::new(AtomicBool::new(true));
            let fail_binding = failing.clone();
            let prepared = prepare_streams(if shared { 1 } else { 2 }, move |schemas| {
                let index = usize::from(!shared);
                let fail_binding = fail_binding.clone();
                let relation = schemas.defer_subquery_with_sources(
                    &[DatasetUri::new(URIS[index])?],
                    65_536,
                    move |schemas| {
                        let (relation, _) = schemas.resolve_output(&inner(index))?;
                        Ok(project(
                            relation,
                            &[
                                if failure == "binding" && fail_binding.load(Ordering::Relaxed) {
                                    "not_observed".into()
                                } else {
                                    "entity".into()
                                },
                            ],
                        ))
                    },
                )?;
                Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
                    relation,
                    Kind::Exists,
                ))))
            })
            .with_spill(VortexRelationalSpillPolicy::new(&workspace, 128 << 20, 1 << 20).unwrap())
            .unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let existing = prepared
                .with_batch_inputs(&mut |_, _, _| {
                    panic!("existing destination must reject before demand")
                })
                .unwrap()
                .write_controlled(&prior_path, Vortex, true, &CancellationToken::default())
                .err()
                .unwrap()
                .to_string();
            assert!(existing.contains("existing destination"), "{existing}");
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            let cancellation = CancellationToken::default();
            let mut calls = [0; 2];
            let mut prior: Option<Weak<MemoryLease>> = None;
            let error = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| {
                    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                    let index = usize::from(uri.as_str() == URIS[1]);
                    assert_eq!(ordinal, calls[index]);
                    calls[index] += 1;
                    if index == usize::from(!shared) && ordinal == 3 {
                        match failure {
                            "producer" => {
                                return Err(batch_input::failed("deferred producer sentinel"));
                            }
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
                    let batch = next(session, ordinal, 1)?;
                    prior = batch
                        .as_ref()
                        .map(ResidentMemorySource::batch_release_witness)
                        .transpose()?;
                    Ok(batch)
                })
                .unwrap()
                .write_controlled(&path, Vortex, true, &cancellation)
                .err()
                .unwrap()
                .to_string();
            let message = match failure {
                "producer" => "deferred producer sentinel",
                "schema" => "schema",
                "cancel" => "cancel",
                _ => "not_observed",
            };
            assert!(error.contains(message), "{shared}/{failure}: {error}");
            assert_eq!(calls[usize::from(!shared)], 4);
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            assert!(!path.exists());
            assert_eq!(fs::read(&prior_path).unwrap(), b"previous destination");
            assert_eq!(fs::read(workspace.join("foreign")).unwrap(), b"keep");
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 3);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            failing.store(false, Ordering::Relaxed);
            let (result, calls) = collect(&prepared);
            assert_eq!(json_rows(&result), expected(true));
            assert_eq!(calls, [4, if shared { 0 } else { 4 }]);
            assert_eq!(result.execution.runtime.completed_executions, 1);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
        }
    }
}

#[test]
fn streaming_dynamic_subqueries_unused_late_failures_block_publication_without_lowering() {
    for empty in [false, true] {
        for failure in ["producer", "schema", "cancel"] {
            let fixture = fixture();
            let prior_path = fixture.0.join("prior.vortex");
            fs::write(&prior_path, b"previous destination").unwrap();
            let path = fixture.0.join("new.vortex");
            let prepared = prepare_streams(2, move |schemas| {
                let relation = schemas.defer_subquery_with_sources(
                    &[DatasetUri::new(URIS[1])?],
                    65_536,
                    |_| panic!("unused failing source must never invoke the inner lowerer"),
                )?;
                let mut query = query(relation, Kind::Exists);
                if empty {
                    query.input = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                        input: scan(0),
                        count: 0,
                        offset: 0,
                    }));
                } else {
                    query.evaluation_guard = Some(Expression::literal(
                        ExprId::new("false-guard")?,
                        ScalarValue::Boolean(false),
                    ));
                }
                Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query)))
            });
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let cancellation = CancellationToken::default();
            let mut calls = [0; 2];
            let mut prior: Option<Weak<MemoryLease>> = None;
            let error = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| {
                    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                    let index = usize::from(uri.as_str() == URIS[1]);
                    assert_eq!(ordinal, calls[index]);
                    calls[index] += 1;
                    if index == 1 && ordinal == 3 {
                        match failure {
                            "producer" => {
                                return Err(batch_input::failed("unused deferred sentinel"));
                            }
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
                            _ => cancellation.cancel(),
                        }
                    }
                    let batch = next(session, ordinal, 1)?;
                    prior = batch
                        .as_ref()
                        .map(ResidentMemorySource::batch_release_witness)
                        .transpose()?;
                    Ok(batch)
                })
                .unwrap()
                .write_controlled(&path, Vortex, true, &cancellation)
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains(match failure {
                    "producer" => "unused deferred sentinel",
                    "schema" => "schema",
                    _ => "cancel",
                }),
                "{failure}: {error}"
            );
            assert_eq!(calls, [4, 4]);
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            assert!(!path.exists());
            assert_eq!(fs::read(&prior_path).unwrap(), b"previous destination");
            assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            let (result, _) = collect(&prepared);
            assert_eq!(
                json_rows(&result),
                if empty { vec![] } else { expected(false) }
            );
            assert_eq!(result.execution.input.as_ref().unwrap().replay_sources, 0);
            assert_eq!(result.execution.runtime.completed_executions, 1);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

fn ordinary(shared: bool) -> PreparedVortexRelational {
    prepare_streams(if shared { 1 } else { 2 }, move |schemas| {
        let index = usize::from(!shared);
        let relation = schemas.defer_subquery_with_sources(
            &[DatasetUri::new(URIS[index])?],
            65_536,
            move |schemas| schemas.resolve_output(&inner(index)).map(|(plan, _)| plan),
        )?;
        Ok(VortexRelationalPlan::CorrelatedSubquery(Box::new(query(
            relation,
            Kind::Exists,
        ))))
    })
}

#[test]
fn streaming_dynamic_subqueries_consumer_failure_cancellation_and_retained_output_recover() {
    for shared in [false, true] {
        let prepared = ordinary(shared);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for cancel in [false, true] {
            let cancellation = CancellationToken::default();
            let mut prior: Option<Weak<MemoryLease>> = None;
            let mut emitted = 0;
            let error = prepared
                .with_batch_inputs(&mut |_, ordinal, session| {
                    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
                    let batch = next(session, ordinal, 1)?;
                    prior = batch
                        .as_ref()
                        .map(ResidentMemorySource::batch_release_witness)
                        .transpose()?;
                    Ok(batch)
                })
                .unwrap()
                .for_each_batch(&cancellation, |array, _| {
                    if array.is_empty() {
                        return Ok(());
                    }
                    emitted += array.len();
                    if cancel {
                        cancellation.cancel();
                        Ok(())
                    } else {
                        Err(batch_input::failed("deferred consumer sentinel"))
                    }
                })
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains(if cancel {
                    "cancel"
                } else {
                    "deferred consumer sentinel"
                }),
                "{error}"
            );
            assert!(emitted > 0);
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
        let mut retained = Vec::new();
        let mut actual = Vec::new();
        let report = prepared
            .with_batch_inputs(&mut |_, ordinal, session| next(session, ordinal, 1))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |array, context| {
                actual.extend(values(&array, context)?);
                retained.push(array);
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected(true));
        assert_eq!(report.runtime.completed_executions, 1);
        drop(report);
        assert!(prepared.snapshot().memory.reserved_bytes > baseline);
        let held = prepared.snapshot().memory.reserved_bytes;
        let (result, _) = collect(&prepared);
        assert_eq!(json_rows(&result), expected(true));
        assert_eq!(result.execution.runtime.completed_executions, 2);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, held);
        drop(retained);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

#[test]
fn streaming_dynamic_subqueries_outer_copy_denial_precedes_inner_demand_and_recovers() {
    let prepared = ordinary(false);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let grant = policy().resource_envelope.memory_budget_bytes;
    let mut held_credit = None;
    let mut prior = None;
    let mut calls = 0;
    let error = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert_eq!(
                uri.as_str(),
                URIS[0],
                "copy denial must precede inner demand"
            );
            assert_eq!(ordinal, 0);
            calls += 1;
            let batch = source(
                session,
                &[Some(1)],
                &[Some(&"wide".repeat(1024))],
                &[Some(3)],
            )?;
            prior = Some(batch.batch_release_witness()?);
            let available = grant - session.memory().snapshot().reserved_bytes;
            held_credit = Some(session.memory().reserve(available - 128)?);
            Ok(Some(batch))
        })
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("reservation denied"), "{error}");
    assert_eq!(calls, 1);
    assert_eq!(prior.unwrap().strong_count(), 0);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(held_credit);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let (result, _) = collect(&prepared);
    assert_eq!(json_rows(&result), expected(true));
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
