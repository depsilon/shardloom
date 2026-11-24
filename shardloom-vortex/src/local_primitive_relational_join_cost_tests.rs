//! Attribution runs through the same prepared native owners and complete consumers.

use super::*;
use crate::local_primitives::native_relational_join_cost::{self as cost, Phase};
use serde_json::json;

fn fixtures() -> (Fixture, Fixture) {
    (
        Fixture::new(keyed(&[Some(2), None, Some(1)], &[10, 11, 12]), 1),
        Fixture::new(keyed(&[Some(2), Some(2), Some(3)], &[20, 21, 22]), 1),
    )
}

fn assert_complete_clock(report: &cost::Report, result: &CollectedVortexRelational) {
    let classified: u128 = report.nodes[..report.node_count]
        .iter()
        .map(|node| node.nanos[..cost::ENGINE_PHASES].iter().sum::<u128>())
        .sum();
    assert!(classified <= result.timing.execution.as_nanos());
    assert!(report.excluded_calls[Phase::SinkExcluded as usize] > 0);
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(
        !result
            .execution
            .native_io_certificate
            .side_effects
            .fallback_attempted
    );
}

#[test]
fn join_cost_prepared_full_join_keeps_rows_sink_boundary_and_final_refunds() {
    let (left, right) = fixtures();
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Full), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    assert_eq!(
        json!(json_rows(&result)),
        json!([
            {"debit":10,"credit":20},{"debit":10,"credit":21},
            {"debit":11,"credit":null},{"debit":12,"credit":null},{"debit":null,"credit":22},
        ])
    );
    assert_complete_clock(&report, &result);
    assert_eq!(report.node_count, 1);
    let node = &report.nodes[0];
    assert_eq!(node.kind, "full");
    assert_eq!(node.parent, None);
    assert_eq!(
        (node.build_rows, node.probe_rows, node.output_rows),
        (3, 3, 5)
    );
    assert_eq!(node.output_cells, 10);
    assert!(!node.membership_only && !node.conditional);
    for phase in [
        Phase::BuildKeys,
        Phase::ProbeKeys,
        Phase::Payload,
        Phase::Downstream,
        Phase::OuterFinish,
    ] {
        assert!(node.calls[phase as usize] > 0, "missing {phase:?}");
    }
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn join_cost_nested_producer_and_consumer_keep_distinct_owners() {
    let (left, right) = fixtures();
    let third = Fixture::new(keyed(&[Some(50), Some(51)], &[10, 10]), 1);
    let plan = VortexRelationalPlan::Join(Box::new(VortexRelationalJoin {
        left: join(&left, &right, JoinKind::Inner),
        right: third.scan(),
        kind: JoinKind::Inner,
        condition: None,
        keys: vec![VortexRelationalJoinKey {
            left: ColumnRef::new("debit").unwrap(),
            right: ColumnRef::new("amount").unwrap(),
        }],
        columns: vec![
            VortexRelationalJoinColumn {
                side: Side::Left,
                column: ColumnRef::new("debit").unwrap(),
                output_column: "debit".into(),
            },
            VortexRelationalJoinColumn {
                side: Side::Right,
                column: ColumnRef::new("entity").unwrap(),
                output_column: "credit".into(),
            },
        ],
    }));
    let prepared = prepare_relational(&plan, policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    assert_eq!(
        json!(json_rows(&result)),
        json!([
            {"debit":10,"credit":50},{"debit":10,"credit":51},
            {"debit":10,"credit":50},{"debit":10,"credit":51},
        ])
    );
    assert_complete_clock(&report, &result);
    assert_eq!(report.node_count, 2);
    let parent = &report.nodes[0];
    let child = &report.nodes[1];
    assert_eq!((parent.parent, child.parent), (None, Some(0)));
    assert_eq!(
        (parent.build_rows, parent.probe_rows, parent.output_rows),
        (2, 2, 4)
    );
    assert_eq!(
        (child.build_rows, child.probe_rows, child.output_rows),
        (3, 1, 2)
    );
    assert!(parent.calls[Phase::Downstream as usize] > 0);
    assert!(child.calls[Phase::Downstream as usize] > 0);
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn join_cost_membership_build_reports_original_input_rows() {
    let (left, right) = fixtures();
    let VortexRelationalPlan::Join(mut plan) = join(&left, &right, JoinKind::LeftSemi) else {
        unreachable!();
    };
    plan.columns.retain(|column| column.side == Side::Left);
    let prepared = prepare_relational(&VortexRelationalPlan::Join(plan), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    assert_eq!(json_rows(&result), [json!({"debit":10})]);
    assert_complete_clock(&report, &result);
    assert_eq!(report.node_count, 1);
    let node = &report.nodes[0];
    assert!(node.membership_only);
    assert_eq!(
        (node.build_rows, node.probe_rows, node.output_rows),
        (3, 1, 1)
    );
    assert!(node.calls[Phase::BuildKeys as usize] > 0);
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn join_cost_consumer_failure_and_cancellation_close_scopes_before_fresh_retry() {
    let (left, right) = fixtures();
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Inner), policy()).unwrap();
    let memory = prepared.session.memory().clone();
    for cancel in [false, true] {
        let cancellation = CancellationToken::default();
        let observation = cost::Session::start();
        let error = prepared
            .for_each_batch(&cancellation, |_, _| {
                if cancel {
                    cancellation.cancel();
                    Ok(())
                } else {
                    Err(shardloom_core::ShardLoomError::InvalidOperation(
                        "join cost consumer failure".into(),
                    ))
                }
            })
            .err()
            .expect("consumer failure must stop the query");
        assert!(error.to_string().contains(if cancel {
            "cancelled"
        } else {
            "join cost consumer failure"
        }));
        let report = observation.finish();
        assert_eq!(report.node_count, 1);
        assert!(report.nodes[0].calls[Phase::Downstream as usize] > 0);
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    assert_eq!(
        json_rows(&result),
        [
            json!({"debit":10,"credit":20}),
            json!({"debit":10,"credit":21})
        ]
    );
    assert_complete_clock(&report, &result);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[cfg(feature = "vortex-write")]
#[test]
fn join_cost_ordered_strategy_is_excluded_from_resident_attribution() {
    let (left, right) = fixtures();
    let directory = left.0.join("join-cost-spill");
    fs::create_dir(&directory).unwrap();
    let prepared = prepare_relational(&join(&left, &right, JoinKind::Full), policy())
        .unwrap()
        .with_spill(
            crate::relational_query::VortexRelationalSpillPolicy::new(
                directory.clone(),
                128 << 20,
                1 << 20,
            )
            .unwrap(),
        )
        .unwrap();
    let memory = prepared.session.memory().clone();
    let observation = cost::Session::start();
    let result = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    let report = observation.finish();
    assert_eq!(
        json!(json_rows(&result)),
        json!([
            {"debit":10,"credit":20},{"debit":10,"credit":21},
            {"debit":11,"credit":null},{"debit":12,"credit":null},{"debit":null,"credit":22},
        ])
    );
    assert_eq!(report.node_count, 0);
    assert_eq!(report.excluded_calls[Phase::OrderedExcluded as usize], 1);
    assert!(report.excluded_calls[Phase::SinkExcluded as usize] > 0);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
    drop(result);
    drop(prepared);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
