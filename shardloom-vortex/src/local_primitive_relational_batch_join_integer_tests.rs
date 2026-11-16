//! Directory admission cannot change the prepared producer or completion contract.

use super::*;
use crate::local_primitives::native_relational_join_cost as cost;
use shardloom_core::ShardLoomError;

#[test]
#[allow(clippy::too_many_lines)] // One exact-reference matrix covers both producer positions and completion failures.
fn dense_integer_join_prepared_streams_preserve_completion_spill_and_escaped_owners() {
    let file_rows = (0..128)
        .map(Some)
        .chain([Some(7), None])
        .enumerate()
        .map(|(row, key)| (key, format!("file-{row}")))
        .collect::<Vec<_>>();
    let stream_rows = (0..128)
        .map(Some)
        .chain([Some(7), None, Some(128)])
        .enumerate()
        .map(|(row, key)| (key, format!("stream-{row}")))
        .collect::<Vec<_>>();
    let file_refs = file_rows
        .iter()
        .map(|(key, text)| (*key, text.as_str()))
        .collect::<Vec<_>>();
    let stream_refs = stream_rows
        .iter()
        .map(|(key, text)| (*key, text.as_str()))
        .collect::<Vec<_>>();
    for stream_right in [false, true] {
        for kind in [JoinKind::Inner, JoinKind::Full] {
            for spill in [false, true] {
                for late_error in [false, true] {
                    let fixture = Fixture::new(
                        StructArray::new(
                            FieldNames::from(["n", "s"]),
                            vec![
                                PrimitiveArray::from_option_iter(file_rows.iter().map(|row| row.0))
                                    .into_array(),
                                VarBinViewArray::from_iter_nullable_str(
                                    file_rows.iter().map(|row| Some(row.1.as_str())),
                                )
                                .into_array(),
                            ],
                            file_rows.len(),
                            Validity::NonNullable,
                        )
                        .into_array(),
                        17,
                    );
                    let prepared = prepared(
                        &joined(fixture.scan(), stream_right, kind),
                        &fixture,
                        None,
                        spill,
                    );
                    let memory = prepared.session.memory().clone();
                    let observation = cost::Session::start();
                    let mut calls = 0;
                    let mut prior: Option<Weak<MemoryLease>> = None;
                    let mut producer = |session: &ResidentVortexSession| {
                        assert!(
                            prior
                                .as_ref()
                                .is_none_or(|witness| witness.strong_count() == 0)
                        );
                        let call = calls;
                        calls += 1;
                        let rows = match call {
                            0 => &stream_refs[..64],
                            1 => &stream_refs[..0],
                            2 => &stream_refs[64..128],
                            3 => &stream_refs[128..],
                            4 if late_error => {
                                return Err(ShardLoomError::InvalidOperation(
                                    "intentional late integer-join producer failure".into(),
                                ));
                            }
                            4 => return Ok(None),
                            _ => panic!("single-use integer-join producer replayed"),
                        };
                        let batch = source(
                            session,
                            &rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                            &rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>(),
                        )?;
                        prior = Some(batch.batch_release_witness()?);
                        Ok(Some(batch))
                    };
                    let mut actual = Vec::new();
                    let mut held = Vec::new();
                    let outcome = prepared
                        .with_batch_input(&mut producer)
                        .unwrap()
                        .for_each_batch(&CancellationToken::default(), |array, context| {
                            actual.extend(values(&array, context)?);
                            held.push(array);
                            Ok(())
                        });
                    let report = observation.finish();
                    assert_eq!(calls, 5);
                    assert_eq!(outcome.is_err(), late_error);
                    if let Ok(executed) = &outcome {
                        let (left, right) = if stream_right {
                            (&file_refs, &stream_refs)
                        } else {
                            (&stream_refs, &file_refs)
                        };
                        assert_eq!(actual, reference_rows(left, right, kind));
                        assert_eq!(executed.ordered_join_stages, u64::from(spill));
                        assert!(executed.native_io_certificate.is_certified());
                        assert!(
                            !executed
                                .native_io_certificate
                                .side_effects
                                .fallback_attempted
                        );
                        let input = executed.input.as_ref().unwrap();
                        assert!(input.end_of_input_observed && input.output_ownership_detached);
                        assert_eq!(input.rows, stream_rows.len() as u64);
                        assert_eq!(input.max_retained_input_batches, 1);
                    } else {
                        assert!(
                            outcome
                                .as_ref()
                                .err()
                                .expect("declared producer failure")
                                .to_string()
                                .contains("intentional late integer-join producer failure")
                        );
                    }
                    if spill {
                        assert_eq!(report.node_count, 0);
                        assert_eq!(
                            fs::read_dir(fixture.0.join("join-runs")).unwrap().count(),
                            0
                        );
                    } else {
                        assert_eq!(report.node_count, 1);
                        let expected = u64::from(!(stream_right && late_error));
                        assert_eq!(report.integer_lookup_work[0].admitted, expected);
                        if expected != 0 {
                            let probe_rows = if stream_right {
                                file_rows.len()
                            } else {
                                stream_rows.len()
                            };
                            assert_eq!(report.integer_lookup_work[0].probe_rows, probe_rows as u64);
                        }
                    }
                    drop(outcome);
                    drop(prepared);
                    drop(held);
                    assert_eq!(memory.snapshot().reserved_bytes, 0);
                }
            }
        }
    }
}
