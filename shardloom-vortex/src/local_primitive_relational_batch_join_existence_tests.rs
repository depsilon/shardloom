//! Existence joins exhaust producers and retain only execution-local key state.

use super::*;

const URIS: [&str; 2] = ["memory://stream", "memory://right"];

fn prepared_existence(kind: JoinKind, zero: bool) -> PreparedVortexRelational {
    let mut plan = joined(other_scan(DatasetUri::new(URIS[1]).unwrap()), false, kind);
    let VortexRelationalPlan::Join(join) = &mut plan else {
        unreachable!()
    };
    // Repeated key names need two comparison positions, but one retained field.
    join.keys.push(join.keys[0].clone());
    if zero {
        plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: plan,
            offset: 0,
            count: 0,
        }));
    }
    prepare_relational_with_schema(policy(), |schema| {
        for uri in URIS {
            schema.register_batch_source(DatasetUri::new(uri)?, |session| {
                source(session, &[], &[])
            })?;
        }
        Ok(plan)
    })
    .unwrap()
}

#[test]
fn existence_membership_pipeline_releases_producers_and_never_reuses_prior_execution() {
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        let prepared = prepared_existence(kind, false);
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let mut held = Vec::new();
        for execution in 1..=2 {
            let right = [
                (Some(execution), "unused-one"),
                (Some(execution), "unused-two"),
                (None, "unused-null"),
            ];
            let mut calls = [0u64; 2];
            let mut prior: Option<Weak<MemoryLease>> = None;
            let mut provider = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                assert!(
                    prior
                        .as_ref()
                        .is_none_or(|witness| witness.strong_count() == 0)
                );
                let index = URIS
                    .iter()
                    .position(|expected| *expected == uri.as_str())
                    .unwrap();
                assert_eq!(calls[index], ordinal);
                calls[index] += 1;
                let rows = match (index, ordinal) {
                    (0, 0) => &STREAM[..2],
                    (0, 1) => &STREAM[2..],
                    (1, 0) => &right[..1],
                    (1, 1) => &[][..],
                    (1, 2) => &right[1..],
                    (0, 2) | (1, 3) => return Ok(None),
                    _ => panic!("unexpected producer replay"),
                };
                let batch = source(
                    session,
                    &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    &rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
                )?;
                prior = Some(batch.batch_release_witness()?);
                Ok(Some(batch))
            };
            #[cfg(debug_assertions)]
            let observation =
                crate::local_primitives::native_relational_observation::Session::start();
            let mut actual = Vec::new();
            let result = prepared
                .with_batch_inputs(&mut provider)
                .unwrap()
                .for_each_batch(&CancellationToken::default(), |array, context| {
                    assert_eq!(array.dtype(), &prepared.output_dtype().unwrap());
                    actual.extend(values(&array, context)?);
                    held.push(array);
                    Ok(())
                })
                .unwrap();
            #[cfg(debug_assertions)]
            assert_eq!(observation.finish().join_retained_right_rows, 1);
            assert_eq!(actual, reference_rows(&STREAM, &right, kind));
            assert_eq!(calls, [3, 4]);
            assert_eq!(
                result.runtime.completed_executions,
                u64::try_from(execution).unwrap()
            );
            let input = result.input.as_ref().unwrap();
            assert_eq!(input.rows, 7);
            assert_eq!(input.join_build_rows_detached, 3);
            assert_eq!(input.max_retained_input_batches, 1);
            assert!(input.end_of_input_observed && input.output_ownership_detached);
            assert!(result.native_io_certificate.is_certified());
            assert!(!result.native_io_certificate.side_effects.fallback_attempted);
            drop(result);
            if execution == 1 {
                held.clear();
                assert_eq!(memory.snapshot().reserved_bytes, baseline);
            }
        }
        drop(prepared);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn existence_membership_pipeline_late_faults_and_denials_prevent_completion() {
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        for fault in ["producer", "cancel", "consumer", "denied"] {
            let prepared = prepared_existence(kind, matches!(fault, "producer" | "cancel"));
            let memory = prepared.session.memory().clone();
            let baseline = memory.snapshot().reserved_bytes;
            let token = CancellationToken::default();
            let mut right_calls = 0;
            let mut provider = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                if uri.as_str() == URIS[1] {
                    right_calls += 1;
                    if ordinal == 2 {
                        if fault == "producer" {
                            return Err(failed("late existence producer failure"));
                        }
                        if fault == "cancel" {
                            token.cancel();
                        }
                        return Ok(None);
                    }
                    source(session, &[Some(1), Some(1)], &[Some("unused"), None]).map(Some)
                } else if ordinal == 0 {
                    source(session, &[Some(1), Some(2)], &[Some("a"), Some("b")]).map(Some)
                } else {
                    Ok(None)
                }
            };
            let occupied = (fault == "denied").then(|| {
                memory
                    .reserve(memory.snapshot().limit_bytes - baseline)
                    .unwrap()
            });
            let mut emitted = 0;
            let error = prepared
                .with_batch_inputs(&mut provider)
                .unwrap()
                .for_each_batch(&token, |_, _| {
                    emitted += 1;
                    Err(failed("existence consumer failure"))
                })
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains(match fault {
                    "producer" => "late existence producer failure",
                    "cancel" => "cancel",
                    "consumer" => "existence consumer failure",
                    _ => "reservation denied",
                }),
                "{fault}: {error}"
            );
            assert_eq!(emitted, usize::from(fault == "consumer"));
            if fault != "denied" {
                assert_eq!(right_calls, 3);
            }
            assert_eq!(prepared.snapshot().completed_executions, 0);
            drop(occupied);
            assert_eq!(memory.snapshot().reserved_bytes, baseline);
            drop(prepared);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn existence_membership_still_validates_unused_right_payload_before_retention() {
    let fixture = Fixture::new(
        StructArray::new(
            FieldNames::from(["n", "unused"]),
            vec![
                PrimitiveArray::from_option_iter([Some(1i64), Some(1)]).into_array(),
                PrimitiveArray::from_iter([0.0f64, f64::NAN]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .into_array(),
        2,
    );
    for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
        let prepared = prepared(&joined(fixture.scan(), false, kind), &fixture, None, false);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let mut input = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
            panic!("right validation must precede left producer demand")
        };
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| {
                panic!("invalid build emitted output")
            })
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "native result batch: numeric value differs from its finite declared dtype; no fallback execution was attempted"
        );
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    }
}
