//! Nested joins and aggregate/order/limit composition share one source lifecycle.

use super::*;
use crate::{
    local_primitives::VortexLocalPrimitiveRowExportFormat as Format,
    query_primitive::VortexSimpleAggregateMeasure,
    relational_query::{
        VortexRelationalAggregate, VortexRelationalNullOrder, VortexRelationalOrderKey,
        VortexRelationalSort,
    },
};

fn composed(fixture: &Fixture, stream_right: bool) -> VortexRelationalPlan {
    let inner = joined(fixture.scan(), stream_right, JoinKind::Full);
    let nested = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: inner,
        right: fixture.scan(),
        kind: JoinKind::Left,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new(if stream_right { "left" } else { "right" }).unwrap(),
            right: ColumnRef::new("s").unwrap(),
        }],
        columns: vec![VortexRelationalJoinColumn {
            side: Side::Left,
            column: ColumnRef::new("left").unwrap(),
            output_column: "group".into(),
        }],
    }));
    let grouped = VortexRelationalPlan::Aggregate(Box::new(VortexRelationalAggregate {
        input: nested,
        group_by: vec![ColumnRef::new("group").unwrap()],
        measures: vec![VortexSimpleAggregateMeasure::new(
            "count",
            None,
            "rows".into(),
        )],
    }));
    let ordered = VortexRelationalPlan::Sort(Box::new(VortexRelationalSort {
        input: grouped,
        keys: vec![
            VortexRelationalOrderKey {
                column: ColumnRef::new("rows").unwrap(),
                descending: true,
                nulls: Some(VortexRelationalNullOrder::Last),
            },
            VortexRelationalOrderKey {
                column: ColumnRef::new("group").unwrap(),
                descending: false,
                nulls: Some(VortexRelationalNullOrder::Last),
            },
        ],
    }));
    VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
        input: ordered,
        offset: 1,
        count: 3,
    }))
}

#[test]
#[allow(clippy::too_many_lines)] // One composition checks both stream positions, strategies and terminals.
fn streaming_join_nested_aggregate_order_limit_and_native_write_reopen_preserve_complete_values() {
    for stream_right in [false, true] {
        for spill in [false, true] {
            let fixture = ordinary_file();
            let prepared = prepared(&composed(&fixture, stream_right), &fixture, None, spill);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let expected = if stream_right {
                vec![
                    json!({"group":null,"rows":2}),
                    json!({"group":"r3","rows":1}),
                    json!({"group":"rn","rows":1}),
                ]
            } else {
                vec![
                    json!({"group":"a","rows":1}),
                    json!({"group":"b","rows":1}),
                    json!({"group":"c","rows":1}),
                ]
            };
            for write in [false, true] {
                let mut calls = 0;
                let mut prior: Option<Weak<MemoryLease>> = None;
                let mut producer = |session: &ResidentVortexSession| {
                    assert!(
                        prior
                            .as_ref()
                            .is_none_or(|witness| witness.strong_count() == 0)
                    );
                    calls += 1;
                    if calls == 3 {
                        return Ok(None);
                    }
                    assert!(calls <= 2);
                    let start = (calls - 1) * 2;
                    let batch = source(
                        session,
                        &STREAM[start..start + 2]
                            .iter()
                            .map(|row| row.0)
                            .collect::<Vec<_>>(),
                        &STREAM[start..start + 2]
                            .iter()
                            .map(|row| Some(row.1))
                            .collect::<Vec<_>>(),
                    )?;
                    prior = Some(batch.batch_release_witness()?);
                    Ok(Some(batch))
                };
                let mut actual = Vec::new();
                let input = prepared.with_batch_input(&mut producer).unwrap();
                let report = if write {
                    let target = fixture.0.join("composed.vortex");
                    let result = input
                        .write_controlled(
                            &target,
                            Format::Vortex,
                            false,
                            &CancellationToken::default(),
                        )
                        .unwrap();
                    assert_eq!(result.output.rows_written, expected.len() as u64);
                    let reopened = prepare_relational(
                        &other_scan(DatasetUri::new(target.display().to_string()).unwrap()),
                        policy(),
                    )
                    .unwrap();
                    assert_eq!(reopened.output_dtype(), prepared.output_dtype());
                    actual = json_rows(
                        &reopened
                            .collect_jsonl(&CancellationToken::default())
                            .unwrap(),
                    );
                    result.execution
                } else {
                    input
                        .for_each_json_batch(&CancellationToken::default(), 2, 1 << 20, |batch| {
                            actual.extend(
                                serde_json::from_str::<Vec<Value>>(batch.values_json.value())
                                    .unwrap(),
                            );
                            Ok(())
                        })
                        .unwrap()
                };
                assert_eq!(
                    actual, expected,
                    "stream_right={stream_right}, spill={spill}, write={write}"
                );
                assert_eq!(calls, 3);
                assert_eq!(
                    report.prepared_sources, 2,
                    "ordinary repeated file shares one held generation"
                );
                assert_eq!(report.ordered_join_stages, if spill { 2 } else { 0 });
                assert_eq!(report.ordered_aggregate_stages, u64::from(spill));
                let input = report.input.as_ref().unwrap();
                assert_eq!(input.rows, 4);
                assert_eq!(
                    input.join_build_rows_detached,
                    if stream_right { 7 } else { 6 }
                );
                assert!(input.ordering_rows_detached > 0);
                assert!(input.end_of_input_observed);
                assert!(report.native_io_certificate.is_certified());
                drop(report);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if spill {
                    assert_eq!(
                        fs::read_dir(fixture.0.join("join-runs")).unwrap().count(),
                        0
                    );
                }
            }
            assert_eq!(prepared.snapshot().completed_executions, 2);
        }
    }
}

#[test]
fn streaming_join_mixed_file_mutation_and_final_consumer_failure_never_complete() {
    for stream_right in [false, true] {
        for spill in [false, true] {
            for failure in ["source", "consumer", "cancel"] {
                let fixture = ordinary_file();
                let prepared = prepared(
                    &joined(fixture.scan(), stream_right, JoinKind::Full),
                    &fixture,
                    None,
                    spill,
                );
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let mut calls = 0;
                let mut producer = |session: &ResidentVortexSession| {
                    calls += 1;
                    if calls == 1 {
                        source(session, &[Some(1)], &[Some("a")]).map(Some)
                    } else {
                        Ok(None)
                    }
                };
                let token = CancellationToken::default();
                let mut changed = false;
                let result = prepared
                    .with_batch_input(&mut producer)
                    .unwrap()
                    .for_each_batch(&token, |_, _| {
                        if !changed {
                            changed = true;
                            match failure {
                                "source" => fixture.replace(),
                                "consumer" => {
                                    return Err(super::super::super::super::failed(
                                        "join final consumer failure",
                                    ));
                                }
                                "cancel" => token.cancel(),
                                _ => unreachable!(),
                            }
                        }
                        Ok(())
                    });
                let error = result
                    .err()
                    .expect("provisional delivery must not become completion");
                let expected = match failure {
                    "source" => "changed",
                    "consumer" => "join final consumer failure",
                    _ => "cancel",
                };
                assert!(error.to_string().contains(expected), "{failure}: {error}");
                assert!(changed);
                assert_eq!(prepared.snapshot().completed_executions, 0);
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
