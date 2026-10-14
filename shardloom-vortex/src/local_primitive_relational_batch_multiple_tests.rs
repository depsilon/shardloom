//! Distinct producers keep independent demand, ownership and completion state.

use super::*;

const URIS: [&str; 2] = ["memory://stream", "memory://second"];

fn plan(kind: JoinKind, stream_right: bool) -> VortexRelationalPlan {
    joined(
        other_scan(DatasetUri::new(URIS[1]).unwrap()),
        stream_right,
        kind,
    )
}

fn prepare_multiple(plan: &VortexRelationalPlan) -> PreparedVortexRelational {
    prepare_relational_with_schema(policy(), |schemas| {
        for uri in URIS {
            schemas.register_batch_source(DatasetUri::new(uri)?, |session| {
                source(session, &[], &[])
            })?;
        }
        Ok(plan.clone())
    })
    .unwrap()
}

fn next(
    index: usize,
    ordinal: u64,
    empty: bool,
    session: &ResidentVortexSession,
) -> Result<Option<ResidentMemorySource>> {
    if empty {
        assert_eq!(ordinal, 0);
        return Ok(None);
    }
    let rows = match (index, ordinal) {
        (0, 0) => &[][..],
        (0, 1) => &STREAM[..2],
        (0, 2) => &STREAM[2..],
        (0, 3) | (1, 1) => return Ok(None),
        (1, 0) => &ORDINARY[..],
        _ => panic!("source replayed or received another source's ordinal"),
    };
    source(
        session,
        &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        &rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
    )
    .map(Some)
}

#[test]
#[allow(clippy::too_many_lines)] // Both source orders, all joins, empty sides and fresh execution state.
fn multiple_streams_all_joins_empty_sources_unequal_batches_and_fresh_execution() {
    let fixture = ordinary_file();
    let workspace = fixture.0.join("multiple-runs");
    fs::create_dir(&workspace).unwrap();
    for spill in [false, true] {
        for stream_right in [false, true] {
            for empty in [[false, false], [true, false], [false, true], [true, true]] {
                for kind in [
                    JoinKind::Inner,
                    JoinKind::Left,
                    JoinKind::Right,
                    JoinKind::Full,
                    JoinKind::LeftSemi,
                    JoinKind::LeftAnti,
                    JoinKind::Cross,
                ] {
                    let mut prepared = prepare_multiple(&plan(kind, stream_right));
                    if spill {
                        prepared = prepared
                            .with_spill(
                                VortexRelationalSpillPolicy::new(
                                    workspace.clone(),
                                    128 << 20,
                                    1 << 20,
                                )
                                .unwrap(),
                            )
                            .unwrap();
                    }
                    let baseline = prepared.snapshot().memory.reserved_bytes;
                    for execution in 1..=2 {
                        let mut calls = [0_u64; 2];
                        let mut prior: Option<Weak<MemoryLease>> = None;
                        let mut provider =
                            |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                                assert!(
                                    prior.as_ref().is_none_or(|owner| owner.strong_count() == 0)
                                );
                                let index = URIS
                                    .iter()
                                    .position(|expected| *expected == uri.as_str())
                                    .unwrap();
                                assert_eq!(ordinal, calls[index]);
                                calls[index] += 1;
                                let source = next(index, ordinal, empty[index], session)?;
                                prior = source
                                    .as_ref()
                                    .map(ResidentMemorySource::batch_release_witness)
                                    .transpose()?;
                                Ok(source)
                            };
                        let mut held = Vec::new();
                        let mut actual = Vec::new();
                        let result = prepared
                            .with_batch_inputs(&mut provider)
                            .unwrap()
                            .for_each_batch(&CancellationToken::default(), |array, context| {
                                actual.extend(values(&array, context)?);
                                held.push(array);
                                Ok(())
                            })
                            .unwrap();
                        let stream = if empty[0] { &[][..] } else { &STREAM[..] };
                        let second = if empty[1] { &[][..] } else { &ORDINARY[..] };
                        let (left, right) = if stream_right {
                            (second, stream)
                        } else {
                            (stream, second)
                        };
                        assert_eq!(
                            actual,
                            reference_rows(left, right, kind),
                            "spill={spill} stream_right={stream_right} empty={empty:?} kind={kind:?}"
                        );
                        assert_eq!(
                            calls,
                            [if empty[0] { 1 } else { 4 }, if empty[1] { 1 } else { 2 }]
                        );
                        let input = result.input.as_ref().unwrap();
                        assert_eq!(input.sources, 2);
                        assert_eq!(input.payload_batches, calls.iter().sum::<u64>() - 2);
                        assert_eq!(
                            usize::try_from(input.rows).unwrap(),
                            stream.len() + second.len()
                        );
                        assert_eq!(
                            input.max_retained_input_batches,
                            usize::from(!empty.iter().all(|value| *value))
                        );
                        assert!(input.end_of_input_observed && input.output_ownership_detached);
                        assert_eq!(result.prepared_sources, 2);
                        assert_eq!(result.runtime.completed_executions, execution);
                        assert!(result.native_io_certificate.is_certified());
                        assert!(
                            result
                                .native_io_certificate
                                .source_capability_report
                                .statistics_availability
                                .contains("completion_aware_native_batch_sources=2")
                        );
                        assert!(!result.native_io_certificate.side_effects.fallback_attempted);
                        drop(result);
                        drop(held);
                        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn multiple_streams_keep_single_source_adapters_unambiguous_and_charge_metadata_before_demand() {
    let prepared = prepare_multiple(&plan(JoinKind::Inner, false));
    let mut single = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("ambiguous provider must not be called")
    };
    assert!(
        prepared
            .with_batch_input(&mut single)
            .err()
            .unwrap()
            .to_string()
            .contains("exactly one")
    );
    #[cfg(feature = "universal-format-io")]
    {
        let mut arrow = || -> Result<Option<arrow_array::RecordBatch>> {
            panic!("ambiguous Arrow provider must not be called")
        };
        assert!(
            prepared
                .with_arrow_batch_input(&mut arrow)
                .err()
                .unwrap()
                .to_string()
                .contains("exactly one")
        );
    }
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let memory = prepared.session.memory();
    let held = memory
        .reserve(memory.snapshot().limit_bytes - baseline)
        .unwrap();
    let mut provider = |_: &DatasetUri,
                        _: u64,
                        _: &ResidentVortexSession|
     -> Result<Option<ResidentMemorySource>> {
        panic!("denied execution metadata must precede demand")
    };
    assert!(
        prepared
            .with_batch_inputs(&mut provider)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .is_err()
    );
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(held);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
}

#[test]
fn multiple_streams_zero_output_still_observes_each_end_or_failure() {
    for target in 0..2 {
        for failure in ["producer", "cancellation", "schema"] {
            let fixture = ordinary_file();
            let workspace = fixture.0.join("multiple-fault-runs");
            fs::create_dir(&workspace).unwrap();
            fs::write(workspace.join("foreign"), b"keep").unwrap();
            let query = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
                input: plan(JoinKind::Inner, false),
                count: 0,
                offset: 0,
            }));
            let prepared = prepare_multiple(&query)
                .with_spill(
                    VortexRelationalSpillPolicy::new(workspace.clone(), 128 << 20, 1 << 20)
                        .unwrap(),
                )
                .unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let cancellation = CancellationToken::default();
            let mut fault_reached = false;
            let mut provider = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                let index = URIS
                    .iter()
                    .position(|expected| *expected == uri.as_str())
                    .unwrap();
                if index == target && ordinal == if index == 0 { 3 } else { 1 } {
                    fault_reached = true;
                    return match failure {
                        "producer" => Err(batch_input::failed("second-source fault sentinel")),
                        "cancellation" => {
                            cancellation.cancel();
                            Ok(None)
                        }
                        "schema" => ResidentMemorySource::from_batch_columns(
                            session,
                            &[MemoryColumn {
                                name: "changed",
                                values: MemoryColumnValues::Int64(&[Some(1)]),
                            }],
                        )
                        .map(Some),
                        _ => unreachable!(),
                    };
                }
                next(index, ordinal, false, session)
            };
            let error = prepared
                .with_batch_inputs(&mut provider)
                .unwrap()
                .collect_jsonl(&cancellation)
                .err()
                .unwrap();
            assert!(fault_reached, "{failure}: {error}");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read(workspace.join("foreign")).unwrap(), b"keep");
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 1);
        }
    }
}
