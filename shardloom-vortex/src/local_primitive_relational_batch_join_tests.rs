//! One finite batch producer may occur once on either side of native joins.

use super::*;
use crate::relational_query::{VortexRelationalLimit, VortexRelationalSpillPolicy};
use serde_json::{Value, json};
use vortex::array::arrays::VarBinViewArray;

#[path = "local_primitive_relational_batch_join_pressure_tests.rs"]
mod pressure_tests;

#[path = "local_primitive_relational_batch_join_composition_tests.rs"]
mod composition_tests;

#[path = "local_primitive_relational_batch_join_fault_tests.rs"]
mod fault_tests;

const ORDINARY: [(Option<i64>, &str); 3] = [(Some(1), "r1"), (Some(3), "r3"), (None, "rn")];
const STREAM: [(Option<i64>, &str); 4] =
    [(Some(1), "a"), (Some(2), "b"), (None, "n"), (Some(1), "c")];

fn other_scan(uri: DatasetUri) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: uri,
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn joined(other: VortexRelationalPlan, stream_right: bool, kind: JoinKind) -> VortexRelationalPlan {
    let (left, right) = if stream_right {
        (other, scan())
    } else {
        (scan(), other)
    };
    let mut columns = vec![VortexRelationalJoinColumn {
        side: Side::Left,
        column: ColumnRef::new("s").unwrap(),
        output_column: "left".into(),
    }];
    if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
        columns.push(VortexRelationalJoinColumn {
            side: Side::Right,
            column: ColumnRef::new("s").unwrap(),
            output_column: "right".into(),
        });
    }
    VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left,
        right,
        kind,
        columns,
        condition: None,
        keys: if kind == JoinKind::Cross {
            vec![]
        } else {
            vec![VortexRelationalJoinKey {
                left: ColumnRef::new("n").unwrap(),
                right: ColumnRef::new("n").unwrap(),
            }]
        },
    }))
}

fn ordinary_file() -> Fixture {
    Fixture::new(
        StructArray::new(
            FieldNames::from(["n", "s"]),
            vec![
                PrimitiveArray::from_option_iter(ORDINARY.map(|row| row.0)).into_array(),
                VarBinViewArray::from_iter_nullable_str(ORDINARY.map(|row| Some(row.1)))
                    .into_array(),
            ],
            ORDINARY.len(),
            Validity::NonNullable,
        )
        .into_array(),
        2,
    )
}

fn prepared(
    plan: &VortexRelationalPlan,
    fixture: &Fixture,
    resident: Option<bool>,
    spill: bool,
) -> PreparedVortexRelational {
    let prepared = prepare_relational_with_schema(policy(), |schema| {
        let register = |schema: &mut VortexRelationalPreparation<'_>| {
            schema.register_memory_source(DatasetUri::new("memory://ordinary")?, |session| {
                ResidentMemorySource::from_columns(
                    session,
                    &[
                        MemoryColumn {
                            name: "n",
                            values: MemoryColumnValues::Int64(&ORDINARY.map(|row| row.0)),
                        },
                        MemoryColumn {
                            name: "s",
                            values: MemoryColumnValues::Utf8(&ORDINARY.map(|row| Some(row.1))),
                        },
                    ],
                    MemorySourceBounds::default(),
                )
            })
        };
        if resident == Some(true) {
            register(schema)?;
        }
        schema.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
            source(session, &[], &[])
        })?;
        if resident == Some(false) {
            register(schema)?;
        }
        Ok(plan.clone())
    })
    .unwrap();
    if spill {
        let runs = fixture.0.join("join-runs");
        fs::create_dir_all(&runs).unwrap();
        prepared
            .with_spill(VortexRelationalSpillPolicy::new(runs, 128 << 20, 1 << 20).unwrap())
            .unwrap()
    } else {
        prepared
    }
}

fn reference(stream_right: bool, kind: JoinKind) -> Vec<Value> {
    let (left, right) = if stream_right {
        (&ORDINARY[..], &STREAM[..])
    } else {
        (&STREAM[..], &ORDINARY[..])
    };
    let mut matched = vec![false; right.len()];
    let mut rows = Vec::new();
    for &(key, value) in left {
        let mut found = false;
        for (position, &(other, other_value)) in right.iter().enumerate() {
            if kind == JoinKind::Cross || (key.is_some() && key == other) {
                found = true;
                matched[position] = true;
                if !matches!(kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
                    rows.push(json!({"left":value,"right":other_value}));
                }
            }
        }
        match kind {
            JoinKind::Left | JoinKind::Full if !found => {
                rows.push(json!({"left":value,"right":null}));
            }
            JoinKind::LeftSemi if found => rows.push(json!({"left":value})),
            JoinKind::LeftAnti if !found => rows.push(json!({"left":value})),
            _ => {}
        }
    }
    if matches!(kind, JoinKind::Right | JoinKind::Full) {
        for (position, &(_, value)) in right.iter().enumerate() {
            if !matched[position] {
                rows.push(json!({"left":null,"right":value}));
            }
        }
    }
    rows
}

#[test]
#[allow(clippy::too_many_lines)] // Shared matrix covers both source orders and all seven join kinds.
fn streaming_join_either_side_all_kinds_and_mixed_sources_release_each_producer_batch() {
    for resident in [None, Some(true), Some(false)] {
        for stream_right in [false, true] {
            for spill in [false, true] {
                let fixture = ordinary_file();
                let other = if resident.is_some() {
                    other_scan(DatasetUri::new("memory://ordinary").unwrap())
                } else {
                    fixture.scan()
                };
                for kind in [
                    JoinKind::Inner,
                    JoinKind::Left,
                    JoinKind::Right,
                    JoinKind::Full,
                    JoinKind::LeftSemi,
                    JoinKind::LeftAnti,
                    JoinKind::Cross,
                ] {
                    let prepared = prepared(
                        &joined(other.clone(), stream_right, kind),
                        &fixture,
                        resident,
                        spill,
                    );
                    let baseline = prepared.snapshot().memory.reserved_bytes;
                    let memory = prepared.session.memory().clone();
                    let mut calls = 0;
                    let mut prior: Option<Weak<MemoryLease>> = None;
                    let mut producer = |session: &ResidentVortexSession| {
                        assert!(
                            prior
                                .as_ref()
                                .is_none_or(|witness| witness.strong_count() == 0)
                        );
                        let batch = match calls {
                            0 => source(
                                session,
                                &STREAM[..2].iter().map(|row| row.0).collect::<Vec<_>>(),
                                &STREAM[..2]
                                    .iter()
                                    .map(|row| Some(row.1))
                                    .collect::<Vec<_>>(),
                            )?,
                            1 => source(session, &[], &[])?,
                            2 => source(
                                session,
                                &STREAM[2..].iter().map(|row| row.0).collect::<Vec<_>>(),
                                &STREAM[2..]
                                    .iter()
                                    .map(|row| Some(row.1))
                                    .collect::<Vec<_>>(),
                            )?,
                            3 => {
                                calls += 1;
                                return Ok(None);
                            }
                            _ => panic!("single-use source replayed"),
                        };
                        calls += 1;
                        prior = Some(batch.batch_release_witness()?);
                        Ok(Some(batch))
                    };
                    let mut held = Vec::new();
                    let mut actual = Vec::new();
                    let report = prepared
                        .with_batch_input(&mut producer)
                        .unwrap()
                        .for_each_batch(&CancellationToken::default(), |array, context| {
                            actual.extend(values(&array, context)?);
                            held.push(array);
                            Ok(())
                        })
                        .unwrap();
                    assert_eq!(
                        actual,
                        reference(stream_right, kind),
                        "resident={resident:?}, stream_right={stream_right}, spill={spill}, kind={kind:?}"
                    );
                    assert_eq!(calls, 4);
                    assert_eq!(report.prepared_sources, 2);
                    assert!(report.native_io_certificate.is_certified());
                    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
                    let input = report.input.as_ref().unwrap();
                    assert!(input.end_of_input_observed && input.output_ownership_detached);
                    assert_eq!(input.rows, 4);
                    assert_eq!(input.payload_batches, 3);
                    assert_eq!(input.max_retained_input_batches, 1);
                    assert_eq!(
                        input.join_build_rows_detached,
                        if stream_right { 4 } else { 3 }
                    );
                    assert!(input.join_build_batches_detached > 0);
                    assert_eq!(input.ordering_rows_detached, 0);
                    if let Some(spill) = &report.spill {
                        assert_eq!(fs::read_dir(&spill.workspace).unwrap().count(), 0);
                    }
                    drop(report);
                    assert!(prepared.snapshot().memory.reserved_bytes > baseline);
                    drop(prepared);
                    assert!(memory.snapshot().reserved_bytes > 0);
                    drop(held);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}

#[test]
fn streaming_join_zero_limit_still_drains_and_rejects_a_late_producer_error() {
    for stream_right in [false, true] {
        for spill in [false, true] {
            for fail in [false, true] {
                let fixture = ordinary_file();
                let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                    input: joined(fixture.scan(), stream_right, JoinKind::Full),
                    offset: 0,
                    count: 0,
                }));
                let prepared = prepared(&plan, &fixture, None, spill);
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let mut calls = 0;
                let mut producer = |session: &ResidentVortexSession| {
                    calls += 1;
                    if calls == 1 {
                        return source(session, &[Some(1)], &[Some("a")]).map(Some);
                    }
                    if fail {
                        Err(super::super::super::failed("late join producer error"))
                    } else {
                        Ok(None)
                    }
                };
                let result = prepared
                    .with_batch_input(&mut producer)
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |array, _| {
                        assert!(array.is_empty());
                        Ok(())
                    });
                assert_eq!(calls, 2);
                if fail {
                    assert!(
                        result
                            .err()
                            .unwrap()
                            .to_string()
                            .contains("late join producer error")
                    );
                    assert_eq!(prepared.snapshot().completed_executions, 0);
                } else {
                    let report = result.unwrap();
                    assert_eq!(report.output_rows, 0);
                    assert!(report.input.as_ref().unwrap().end_of_input_observed);
                }
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
}

#[test]
fn streaming_join_registration_rejects_uri_collisions_missing_use_and_multiple_producers() {
    for batch_first in [false, true] {
        let result = prepare_relational_with_schema(policy(), |schema| {
            let batch = |schema: &mut VortexRelationalPreparation<'_>| {
                schema.register_batch_source(DatasetUri::new("memory://stream")?, |session| {
                    source(session, &[], &[])
                })
            };
            let ordinary = |schema: &mut VortexRelationalPreparation<'_>| {
                schema.register_memory_source(DatasetUri::new("memory://stream")?, |session| {
                    ResidentMemorySource::from_int64_range(session, "n", 0, 1, 0)
                })
            };
            if batch_first {
                batch(schema)?;
                ordinary(schema)?;
            } else {
                ordinary(schema)?;
                batch(schema)?;
            }
            Ok(scan())
        });
        assert!(result.is_err());
    }
    let result = prepare_relational_with_schema(policy(), |schema| {
        for uri in ["memory://stream", "memory://second"] {
            schema.register_batch_source(DatasetUri::new(uri)?, |session| {
                source(session, &[], &[])
            })?;
        }
        Ok(scan())
    });
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("one unique declared batch source")
    );
    let fixture = ordinary_file();
    let result = prepare(&fixture.scan(), 4 << 20);
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("does not use its declared batch source")
    );
}
