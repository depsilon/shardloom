//! Finite set branches retain native membership without retaining their producers.

use super::*;
use crate::relational_query::{VortexRelationalLimit, VortexRelationalSetKind as SetKind};
use serde_json::{Value, json};

#[path = "local_primitive_relational_batch_set_pressure_tests.rs"]
mod pressure_tests;

const URIS: [&str; 2] = ["memory://set-left", "memory://set-right"];
const KINDS: [SetKind; 4] = [
    SetKind::UnionAll,
    SetKind::UnionDistinct,
    SetKind::Intersect,
    SetKind::Except,
];
type Row = (Option<i64>, Option<&'static str>);
const LEFT: [Row; 5] = [
    (Some(3), Some("港")),
    (None, None),
    (Some(3), Some("港")),
    (Some(2), Some("β")),
    (Some(1), Some("")),
];
const RIGHT: [Row; 4] = [
    (None, None),
    (Some(2), Some("β")),
    (Some(9), Some(" ")),
    (Some(9), Some(" ")),
];

fn input_scan(index: usize) -> VortexRelationalPlan {
    VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: DatasetUri::new(URIS[index]).unwrap(),
        projection: shardloom_plan::ProjectionRequest::All,
        predicate: None,
    })
}

fn set(
    left: VortexRelationalPlan,
    right: VortexRelationalPlan,
    kind: SetKind,
) -> VortexRelationalPlan {
    VortexRelationalPlan::Set(Box::new(VortexRelationalSet { left, right, kind }))
}

fn prepared_set(
    plan: &VortexRelationalPlan,
    sources: usize,
    grant: u64,
) -> PreparedVortexRelational {
    let mut resources = policy();
    resources.resource_envelope.memory_budget_bytes = grant;
    prepare_relational_with_schema(resources, |schemas| {
        for uri in &URIS[..sources] {
            schemas.register_batch_source(DatasetUri::new(*uri)?, |session| {
                source(session, &[], &[])
            })?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

fn reference(left: &[Row], right: &[Row], kind: SetKind) -> Vec<Value> {
    let selected = if matches!(kind, SetKind::UnionAll | SetKind::UnionDistinct) {
        left.iter().chain(right).copied().collect::<Vec<_>>()
    } else {
        left.iter()
            .copied()
            .filter(|row| right.contains(row) == (kind == SetKind::Intersect))
            .collect()
    };
    let mut unique = Vec::new();
    selected
        .into_iter()
        .filter(|row| {
            if kind == SetKind::UnionAll {
                true
            } else if unique.contains(row) {
                false
            } else {
                unique.push(*row);
                true
            }
        })
        .map(|(n, s)| json!({"n": n, "s": s}))
        .collect()
}

fn optional_spill(
    prepared: PreparedVortexRelational,
    enabled: bool,
) -> (PreparedVortexRelational, Fixture) {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("set-runs");
    fs::create_dir(&directory).unwrap();
    let prepared = if enabled {
        prepared
            .with_spill(
                crate::relational_query::VortexRelationalSpillPolicy::new(
                    directory,
                    128 << 20,
                    1 << 20,
                )
                .unwrap(),
            )
            .unwrap()
    } else {
        prepared
    };
    (prepared, fixture)
}

struct Inputs<'a> {
    rows: [&'a [Row]; 2],
    calls: [u64; 2],
    positions: [usize; 2],
    ended: [bool; 2],
    prior: Option<Weak<MemoryLease>>,
}

impl<'a> Inputs<'a> {
    fn new(left: &'a [Row], right: &'a [Row]) -> Self {
        Self {
            rows: [left, right],
            calls: [0; 2],
            positions: [0; 2],
            ended: [false; 2],
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
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        let side = URIS
            .iter()
            .position(|expected| *expected == uri.as_str())
            .unwrap();
        assert!(!self.ended[side], "an ended producer was reopened");
        assert_eq!(ordinal, self.calls[side]);
        self.calls[side] += 1;
        let rows = if ordinal == 0 {
            &[][..]
        } else if self.positions[side] == self.rows[side].len() {
            self.ended[side] = true;
            return Ok(None);
        } else {
            let start = self.positions[side];
            let end = (start + 2).min(self.rows[side].len());
            self.positions[side] = end;
            &self.rows[side][start..end]
        };
        let batch = source(
            session,
            &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            &rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )?;
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }
}

#[test]
fn streaming_sets_complete_nulls_duplicates_empty_branches_and_retained_outputs() {
    let all_null = [(None, None); 3];
    for spill in [false, true] {
        for (left, right) in [
            (&LEFT[..], &RIGHT[..]),
            (&[][..], &RIGHT[..]),
            (&LEFT[..], &[][..]),
            (&[][..], &[][..]),
            (&all_null[..], &all_null[..]),
        ] {
            for kind in KINDS {
                let plan = set(input_scan(0), input_scan(1), kind);
                let (prepared, fixture) = optional_spill(prepared_set(&plan, 2, 4 << 20), spill);
                let directory = fixture.0.join("set-runs");
                let memory = prepared.session.memory().clone();
                let mut inputs = Inputs::new(left, right);
                let mut held = Vec::new();
                let mut actual = Vec::new();
                let report = prepared
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
                let expected = reference(left, right, kind);
                assert_eq!(actual, expected, "{kind:?}");
                assert_eq!(inputs.ended, [true; 2]);
                assert_eq!(inputs.positions, [left.len(), right.len()]);
                assert_eq!(
                    inputs.calls,
                    [
                        left.len().div_ceil(2) as u64 + 2,
                        right.len().div_ceil(2) as u64 + 2
                    ]
                );
                let input = report.input.as_ref().unwrap();
                assert_eq!(input.sources, 2);
                assert_eq!(input.rows, (left.len() + right.len()) as u64);
                assert_eq!(input.payload_batches, inputs.calls.iter().sum::<u64>() - 2);
                assert_eq!(input.replay_sources, 0);
                assert_eq!(input.replay_passes, 0);
                assert!(input.end_of_input_observed && input.output_ownership_detached);
                assert!(report.native_io_certificate.is_certified());
                assert!(!report.native_io_certificate.side_effects.fallback_attempted);
                assert_eq!(
                    report.ordered_set_stages,
                    u64::from(spill && kind != SetKind::UnionAll)
                );
                assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
                drop(report);
                drop(prepared);
                if !expected.is_empty() {
                    assert!(
                        memory.snapshot().reserved_bytes > 0,
                        "retained output lost its credits"
                    );
                }
                let mut context = VortexSession::default().create_execution_ctx();
                let mut after_teardown = Vec::new();
                for array in &held {
                    let numbers =
                        crate::local_primitives::logical_field_from_native_array(array, "n")
                            .unwrap();
                    let strings =
                        crate::local_primitives::logical_field_from_native_array(array, "s")
                            .unwrap();
                    for row in 0..array.len() {
                        let n = result_batch::scalar_value(&numbers, row, &mut context)
                            .unwrap()
                            .into_json()
                            .unwrap();
                        let s = result_batch::scalar_value(&strings, row, &mut context)
                            .unwrap()
                            .into_json()
                            .unwrap();
                        after_teardown.push(json!({"n":n,"s":s}));
                    }
                }
                assert_eq!(after_teardown, expected);
                drop(context);
                drop(held);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
            }
        }
    }
}

#[test]
fn streaming_sets_inspect_both_branches_and_preserve_static_depth_refusals() {
    for kind in KINDS {
        for left_outer in [false, true] {
            let (left, right) = if left_outer {
                (VortexRelationalPlan::Outer, input_scan(0))
            } else {
                (input_scan(0), VortexRelationalPlan::Outer)
            };
            let plan = set(left, right, kind);
            let error = batch_input::classify(&plan, &DatasetUri::new(URIS[0]).unwrap())
                .unwrap_err()
                .to_string();
            assert!(error.contains("correlated source"), "{error}");
        }
    }
    let mut plan = input_scan(0);
    for _ in 0..26 {
        plan = set(plan, input_scan(1), SetKind::UnionAll);
    }
    let error = batch_input::classify(&plan, &DatasetUri::new(URIS[0]).unwrap())
        .unwrap_err()
        .to_string();
    assert!(error.contains("exceeds 24 levels"), "{error}");
}

#[test]
fn streaming_sets_reuse_one_producer_through_native_replay_for_every_kind() {
    for (spill, kind) in [false, true]
        .into_iter()
        .flat_map(|spill| KINDS.map(|kind| (spill, kind)))
    {
        let plan = set(input_scan(0), input_scan(0), kind);
        let (prepared, fixture) = optional_spill(prepared_set(&plan, 1, 8 << 20), spill);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for execution in 1..=2 {
            let mut inputs = Inputs::new(&LEFT, &[]);
            let result = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), reference(&LEFT, &LEFT, kind));
            assert_eq!(inputs.ended, [true, false]);
            assert_eq!(inputs.calls, [5, 0]);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.sources, 1);
            assert_eq!(input.rows, LEFT.len() as u64);
            assert_eq!(input.replay_sources, 1);
            assert_eq!(input.replay_passes, 1);
            assert_eq!(input.replay_rows, LEFT.len() as u64);
            assert_eq!(input.replay_rows_detached, LEFT.len() as u64);
            assert!(input.end_of_input_observed && input.output_ownership_detached);
            assert_eq!(result.execution.runtime.completed_executions, execution);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(fixture.0.join("set-runs")).unwrap().count(), 0);
        }
    }
}

#[test]
fn streaming_sets_nested_replay_and_zero_limits_drain_all_producers() {
    let nested = set(
        set(input_scan(0), input_scan(1), SetKind::UnionDistinct),
        input_scan(0),
        SetKind::Except,
    );
    let mut plans = KINDS
        .into_iter()
        .map(|kind| {
            (
                set(input_scan(0), input_scan(1), kind),
                reference(&LEFT, &RIGHT, kind),
                0,
            )
        })
        .collect::<Vec<_>>();
    plans.push((nested, vec![json!({"n":9,"s":" "})], 1));
    for (input_plan, expected, replay_sources) in plans {
        for (spill, limit) in [(false, 0), (false, 2), (true, 0), (true, 2)] {
            let plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: input_plan.clone(),
                offset: 0,
                count: limit,
            }));
            let (prepared, fixture) = optional_spill(prepared_set(&plan, 2, 8 << 20), spill);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut inputs = Inputs::new(&LEFT, &RIGHT);
            let result = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
                .unwrap()
                .collect_jsonl(&CancellationToken::default())
                .unwrap();
            assert_eq!(json_rows(&result), expected[..expected.len().min(limit)]);
            assert_eq!(inputs.ended, [true; 2]);
            assert_eq!(inputs.calls, [5, 4]);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(input.rows, (LEFT.len() + RIGHT.len()) as u64);
            assert_eq!(input.sources, 2);
            assert_eq!(input.replay_sources, replay_sources);
            assert_eq!(input.replay_passes, replay_sources);
            assert!(input.end_of_input_observed);
            drop(result);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(fixture.0.join("set-runs")).unwrap().count(), 0);
        }
    }
}

#[test]
fn streaming_sets_failure_cancellation_and_consumer_errors_release_all_state() {
    for kind in KINDS {
        for (spill, fault) in [false, true].into_iter().flat_map(|spill| {
            ["left", "right", "consumer", "cancel", "initial-cancel"].map(|fault| (spill, fault))
        }) {
            let (prepared, fixture) = optional_spill(
                prepared_set(&set(input_scan(0), input_scan(1), kind), 2, 8 << 20),
                spill,
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            if fault == "initial-cancel" {
                token.cancel();
            }
            let mut inputs = Inputs::new(&LEFT, &RIGHT);
            let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                if ordinal == 2
                    && ((fault == "left" && uri.as_str() == URIS[0])
                        || (fault == "right" && uri.as_str() == URIS[1]))
                {
                    return Err(shardloom_core::ShardLoomError::InvalidOperation(
                        "set producer failed late".into(),
                    ));
                }
                inputs.next(uri, ordinal, session)
            };
            let result = prepared
                .with_batch_inputs(&mut producer)
                .unwrap()
                .for_each_batch(&token, |array, _| {
                    if !array.is_empty() {
                        if fault == "consumer" {
                            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                                "set consumer failed".into(),
                            ));
                        }
                        if fault == "cancel" {
                            token.cancel();
                        }
                    }
                    Ok(())
                });
            let error = result
                .err()
                .expect("failed set execution must not certify completion")
                .to_string();
            let expected = match fault {
                "left" | "right" => "set producer failed late",
                "consumer" => "set consumer failed",
                _ => "cancel",
            };
            assert!(error.contains(expected), "{fault}/{kind:?}: {error}");
            if fault == "initial-cancel" {
                assert_eq!(inputs.calls, [0, 0]);
            }
            assert!(
                inputs
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(fixture.0.join("set-runs")).unwrap().count(), 0);
        }
    }
}
