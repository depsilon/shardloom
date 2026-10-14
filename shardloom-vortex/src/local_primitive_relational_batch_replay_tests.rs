//! Source replay never reopens a producer or retains its original buffer owners.

use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;

#[path = "local_primitive_relational_batch_replay_pressure_tests.rs"]
mod pressure_tests;

fn next(
    session: &ResidentVortexSession,
    calls: &mut usize,
    prior: &mut Option<Weak<MemoryLease>>,
    empty: bool,
    suffix: &str,
) -> Result<Option<ResidentMemorySource>> {
    assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
    let ordinal = *calls;
    *calls += 1;
    if empty {
        assert_eq!(ordinal, 0, "empty producer replayed");
        return Ok(None);
    }
    let batch = match ordinal {
        0 | 2 => {
            let start = ordinal;
            let strings = STREAM[start..start + 2]
                .iter()
                .map(|row| format!("{}{suffix}", row.1))
                .collect::<Vec<_>>();
            source(
                session,
                &STREAM[start..start + 2]
                    .iter()
                    .map(|row| row.0)
                    .collect::<Vec<_>>(),
                &strings
                    .iter()
                    .map(|value| Some(value.as_str()))
                    .collect::<Vec<_>>(),
            )?
        }
        1 => source(session, &[], &[])?,
        3 => return Ok(None),
        _ => panic!("completed producer replayed"),
    };
    *prior = Some(batch.batch_release_witness()?);
    Ok(Some(batch))
}

#[test]
fn streaming_replay_self_join_all_kinds_empty_null_ties_and_final_output_ownership() {
    for spill in [false, true] {
        for empty in [false, true] {
            for kind in [
                JoinKind::Inner,
                JoinKind::Left,
                JoinKind::Right,
                JoinKind::Full,
                JoinKind::LeftSemi,
                JoinKind::LeftAnti,
                JoinKind::Cross,
            ] {
                let fixture = ordinary_file();
                let prepared = prepared(&joined(scan(), false, kind), &fixture, None, spill);
                let memory = prepared.session.memory().clone();
                let mut calls = 0;
                let mut prior = None;
                let mut producer = |session: &ResidentVortexSession| {
                    next(session, &mut calls, &mut prior, empty, "")
                };
                let mut actual = Vec::new();
                let mut held = Vec::new();
                let report = prepared
                    .with_batch_input(&mut producer)
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |array, context| {
                        actual.extend(values(&array, context)?);
                        held.push(array);
                        Ok(())
                    })
                    .unwrap();
                let expected = if empty {
                    vec![]
                } else {
                    reference_rows(&STREAM, &STREAM, kind)
                };
                assert_eq!(
                    actual, expected,
                    "spill={spill} empty={empty} kind={kind:?}"
                );
                assert_eq!(calls, if empty { 1 } else { 4 });
                let input = report.input.as_ref().unwrap();
                assert_eq!(input.sources, 1);
                assert_eq!(input.rows, if empty { 0 } else { 4 });
                assert_eq!(input.payload_batches, if empty { 0 } else { 3 });
                assert_eq!(input.replay_sources, 1);
                assert_eq!(input.replay_passes, 1);
                assert_eq!(input.replay_rows, input.rows);
                assert_eq!(input.replay_rows_detached, input.rows);
                assert_eq!(input.replay_spilled_sources, 0);
                assert!(input.end_of_input_observed && input.output_ownership_detached);
                assert!(report.native_io_certificate.is_certified());
                assert!(!report.native_io_certificate.side_effects.fallback_attempted);
                if let Some(spill) = &report.spill {
                    assert!(spill.owned_cleanup_completed);
                    assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
                }
                drop(report);
                drop(prepared);
                assert!(
                    memory.snapshot().reserved_bytes > 0,
                    "held output lost its credits"
                );
                drop(held);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

fn narrow_scan(text: bool, op: shardloom_core::ComparisonOp) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new("memory://stream").unwrap(),
        projection: shardloom_plan::ProjectionRequest::Columns(
            if text { vec!["n", "s"] } else { vec!["n"] }
                .into_iter()
                .map(|name| ColumnRef::new(name).unwrap())
                .collect(),
        ),
        predicate: Some(shardloom_core::PredicateExpr::Compare {
            column: ColumnRef::new("n").unwrap(),
            op,
            value: shardloom_core::StatValue::Int64(1),
        }),
    })
}

#[test]
fn streaming_replay_preserves_different_scan_projections_filters_and_unused_columns() {
    for spill in [false, true] {
        let fixture = ordinary_file();
        let mut plan = joined(scan(), false, JoinKind::Inner);
        if let VortexRelationalPlan::Join(join) = &mut plan {
            join.left = narrow_scan(true, shardloom_core::ComparisonOp::GtEq);
            join.right = narrow_scan(false, shardloom_core::ComparisonOp::LtEq);
            join.columns.truncate(1);
        }
        let batch = |session: &ResidentVortexSession, empty: bool| {
            let numbers: Vec<_> = STREAM.iter().map(|row| row.0).collect();
            let text: Vec<_> = STREAM.iter().map(|row| Some(row.1)).collect();
            ResidentMemorySource::from_batch_columns(
                session,
                &[
                    MemoryColumn {
                        name: "n",
                        values: MemoryColumnValues::Int64(if empty { &[] } else { &numbers }),
                    },
                    MemoryColumn {
                        name: "s",
                        values: MemoryColumnValues::Utf8(if empty { &[] } else { &text }),
                    },
                    MemoryColumn {
                        name: "unused",
                        values: MemoryColumnValues::Float64(if empty {
                            &[]
                        } else {
                            &[Some(f64::MAX), Some(f64::MIN), Some(-0.0), None]
                        }),
                    },
                ],
            )
        };
        let prepared = prepare_relational_with_schema(policy(), |schemas| {
            schemas.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
                batch(session, true)
            })?;
            Ok(plan)
        })
        .unwrap();
        let prepared = if spill {
            let path = fixture.0.join("replay");
            fs::create_dir(&path).unwrap();
            prepared
                .with_spill(VortexRelationalSpillPolicy::new(path, 128 << 20, 1 << 20).unwrap())
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = 0;
        let mut producer = |session: &ResidentVortexSession| {
            calls += 1;
            match calls {
                1 => batch(session, false).map(Some),
                2 => Ok(None),
                _ => panic!("producer replayed"),
            }
        };
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            [
                json!({"left":"a"}),
                json!({"left":"a"}),
                json!({"left":"c"}),
                json!({"left":"c"})
            ]
        );
        assert_eq!(calls, 2);
        assert_eq!(result.execution.input.as_ref().unwrap().replay_rows, 4);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}

fn triple() -> VortexRelationalPlan {
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: joined(scan(), false, JoinKind::Inner),
        right: scan(),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("left").unwrap(),
            right: ColumnRef::new("s").unwrap(),
        }],
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("left").unwrap(),
                output_column: "left".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("s").unwrap(),
                output_column: "third".into(),
            },
        ],
    }))
}

#[test]
fn streaming_replay_three_scans_fresh_executions_and_native_write_reopen() {
    for spill in [false, true] {
        let fixture = ordinary_file();
        let prepared = prepared(&triple(), &fixture, None, spill);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for write in [false, true] {
            let suffix = if write { "-fresh" } else { "" };
            let mut calls = 0;
            let mut prior = None;
            let mut producer = |session: &ResidentVortexSession| {
                next(session, &mut calls, &mut prior, false, suffix)
            };
            let expected: Vec<_> = ["a", "a", "b", "c", "c"].into_iter()
                .map(|text| json!({"left":format!("{text}{suffix}"), "third":format!("{text}{suffix}")})).collect();
            let input = prepared.with_batch_input(&mut producer).unwrap();
            let execution = if write {
                let path = fixture.0.join("replayed.vortex");
                let written = input
                    .write_controlled(&path, Format::Vortex, false, &CancellationToken::default())
                    .unwrap();
                let reopened = prepare_relational(
                    &other_scan(DatasetUri::new(path.to_string_lossy()).unwrap()),
                    policy(),
                )
                .unwrap();
                assert_eq!(reopened.output_dtype(), prepared.output_dtype());
                assert_eq!(
                    json_rows(
                        &reopened
                            .collect_jsonl(&CancellationToken::default())
                            .unwrap()
                    ),
                    expected
                );
                written.execution
            } else {
                let result = input.collect_jsonl(&CancellationToken::default()).unwrap();
                assert_eq!(json_rows(&result), expected);
                result.execution
            };
            assert_eq!(calls, 4);
            assert_eq!(execution.input.as_ref().unwrap().replay_passes, 2);
            assert_eq!(execution.input.as_ref().unwrap().replay_rows, 8);
            assert_eq!(execution.input.as_ref().unwrap().rows, 4);
            drop(execution);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        }
    }
}

#[test]
fn streaming_replay_zero_limit_requires_source_end_and_rejects_late_failure() {
    for spill in [false, true] {
        for fail in [false, true] {
            let fixture = ordinary_file();
            let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: triple(),
                offset: 0,
                count: 0,
            }));
            let prepared = prepared(&plan, &fixture, None, spill);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut calls = 0;
            let mut prior = None;
            let mut producer = |session: &ResidentVortexSession| {
                if fail && calls == 3 {
                    calls += 1;
                    return Err(batch_input::failed("replay late producer sentinel"));
                }
                next(session, &mut calls, &mut prior, false, "")
            };
            let result = prepared
                .with_batch_input(&mut producer)
                .unwrap()
                .collect_jsonl(&CancellationToken::default());
            if fail {
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("replay late producer sentinel")
                );
                assert_eq!(prepared.snapshot().completed_executions, 0);
            } else {
                let result = result.unwrap();
                assert_eq!(json_rows(&result), Vec::<Value>::new());
                assert_eq!(result.execution.input.as_ref().unwrap().replay_passes, 2);
                assert!(
                    result
                        .execution
                        .input
                        .as_ref()
                        .unwrap()
                        .end_of_input_observed
                );
            }
            assert_eq!(calls, 4);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            if spill {
                assert_eq!(
                    fs::read_dir(fixture.0.join("join-runs")).unwrap().count(),
                    0
                );
            }
        }
    }
}

#[test]
fn streaming_replay_two_repeated_sources_keep_independent_ordinals_and_completion() {
    for spill in [false, true] {
        let fixture = ordinary_file();
        let other = other_scan(DatasetUri::new("memory://second").unwrap());
        let mut second = joined(other.clone(), false, JoinKind::Inner);
        if let VortexRelationalPlan::Join(join) = &mut second {
            join.left = other;
        }
        let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
            left: joined(scan(), false, JoinKind::Inner),
            right: second,
            kind: JoinKind::Cross,
            keys: vec![],
            condition: None,
            columns: vec![
                VortexRelationalJoinColumn {
                    side: Side::Left,
                    column: ColumnRef::new("left").unwrap(),
                    output_column: "a".into(),
                },
                VortexRelationalJoinColumn {
                    side: Side::Right,
                    column: ColumnRef::new("right").unwrap(),
                    output_column: "b".into(),
                },
            ],
        }));
        let prepared = prepare_relational_with_schema(policy(), |schemas| {
            for uri in ["memory://stream", "memory://second"] {
                schemas.register_batch_source(DatasetUri::new(uri)?, |session| {
                    source(session, &[], &[])
                })?;
            }
            Ok(plan)
        })
        .unwrap();
        let prepared = if spill {
            let path = fixture.0.join("two-replays");
            fs::create_dir(&path).unwrap();
            prepared
                .with_spill(VortexRelationalSpillPolicy::new(path, 128 << 20, 1 << 20).unwrap())
                .unwrap()
        } else {
            prepared
        };
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut calls = [0usize; 2];
        let mut prior = [None, None];
        let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
            let index = match uri.as_str() {
                "memory://stream" => 0,
                "memory://second" => 1,
                _ => panic!("unknown source"),
            };
            assert_eq!(ordinal, calls[index] as u64);
            next(
                session,
                &mut calls[index],
                &mut prior[index],
                false,
                if index == 1 { "-second" } else { "" },
            )
        };
        let result = prepared
            .with_batch_inputs(&mut producer)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let mut expected = Vec::new();
        for left in ["a", "a", "b", "c", "c"] {
            for right in ["a", "c", "b", "a", "c"] {
                expected.push(json!({"a":left, "b":format!("{right}-second")}));
            }
        }
        assert_eq!(json_rows(&result), expected);
        assert_eq!(calls, [4, 4]);
        let input = result.execution.input.as_ref().unwrap();
        assert_eq!(input.sources, 2);
        assert_eq!(input.rows, 8);
        assert_eq!(input.replay_sources, 2);
        assert_eq!(input.replay_passes, 2);
        assert_eq!(input.replay_rows, 8);
        drop(result);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
