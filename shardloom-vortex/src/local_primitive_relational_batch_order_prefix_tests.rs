//! Bounded ordering validates and drains sources before releasing owned results.

use super::*;

#[test]
fn streamed_ordering_bounded_prefix_releases_each_source_and_amortizes_tiny_batches() {
    let rows = fixture_rows()
        .into_iter()
        .cycle()
        .take(1025)
        .enumerate()
        .map(|(id, mut row)| {
            row.id = i64::try_from(id).unwrap();
            row
        })
        .collect::<Vec<_>>();
    let keys = vec![
        key("n", true, NullOrder::Last),
        key("s", false, NullOrder::First),
    ];
    let mut expected = rows.clone();
    reference_sort(&mut expected, &keys);
    for batch in [1, 17, 257] {
        let prepared = prepare_rows(&limited(sorted(scan(), keys.clone()), 3, 7));
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let mut chunks = std::iter::once(&rows[..0]).chain(rows.chunks(batch));
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut input = |session: &ResidentVortexSession| {
            assert!(
                prior
                    .as_ref()
                    .is_none_or(|witness| witness.strong_count() == 0)
            );
            chunks
                .next()
                .map(|rows| {
                    let source = rows_source(session, rows)?;
                    prior = Some(source.batch_release_witness()?);
                    Ok(source)
                })
                .transpose()
        };
        #[cfg(debug_assertions)]
        let observation = crate::local_primitives::native_relational_observation::Session::start();
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected[3..10].iter().map(Row::json).collect::<Vec<_>>()
        );
        let report = result.execution.input.as_ref().unwrap();
        assert_eq!(report.rows, rows.len() as u64);
        assert!(report.end_of_input_observed && report.output_ownership_detached);
        assert_eq!(report.max_retained_input_batches, 1);
        #[cfg(debug_assertions)]
        {
            let counts = observation.finish();
            assert_eq!(
                counts.sort_streaming_prefix_rows_validated,
                rows.len() as u64
            );
            assert!(counts.sort_streaming_prefix_peak_rows <= 40);
            assert!(counts.sort_streaming_prefix_rows_copied <= rows.len() as u64 * 2);
            assert_eq!(
                report.ordering_rows_detached,
                counts.sort_streaming_prefix_rows_copied
            );
            if batch == 257 {
                assert!(counts.sort_streaming_prefix_rows_copied < rows.len() as u64 / 8);
            }
        }
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        drop(prepared);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn floating_source(
    session: &ResidentVortexSession,
    payload: &[Option<f64>],
    keys: &[Option<i64>],
) -> Result<ResidentMemorySource> {
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "payload",
                values: MemoryColumnValues::Float64(payload),
            },
            MemoryColumn {
                name: "key",
                values: MemoryColumnValues::Int64(keys),
            },
        ],
    )
}

#[test]
fn streamed_ordering_discarded_payload_precedes_key_errors_and_next_source_demand() {
    for count in [0, 1] {
        for second_rows in [1, 16] {
            for fault in ["payload", "key", "both"] {
                // Native files can carry unobserved nonfinite values; object
                // intake rejects them before the ordering boundary tested here.
                let mut values = vec![Some(1.0); second_rows];
                let mut keys = vec![Some(100_i64); second_rows];
                if fault != "key" {
                    values[second_rows - 1] = Some(f64::NAN);
                }
                if fault != "payload" {
                    keys[0] = None;
                }
                let fixture = Fixture::new(
                    StructArray::new(
                        FieldNames::from(["payload", "key"]),
                        vec![
                            PrimitiveArray::from_option_iter(values).into_array(),
                            PrimitiveArray::from_option_iter(keys).into_array(),
                        ],
                        second_rows,
                        Validity::NonNullable,
                    )
                    .into_array(),
                    second_rows,
                );
                let order = VortexRelationalOrderKey {
                    column: ColumnRef::new("key").unwrap(),
                    descending: false,
                    nulls: None,
                };
                let prepared = prepare_relational_with_schema(policy(), |preparation| {
                    preparation
                        .register_batch_source(DatasetUri::new("memory://stream")?, |session| {
                            floating_source(session, &[], &[])
                        })?;
                    Ok(VortexRelationalPlan::Set(Box::new(VortexRelationalSet {
                        left: limited(sorted(fixture.scan(), vec![order]), 0, count),
                        right: scan(),
                        kind: crate::relational_query::VortexRelationalSetKind::UnionAll,
                    })))
                })
                .unwrap();
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let mut calls = 0;
                let mut input =
                    |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
                        calls += 1;
                        panic!("invalid discarded payload must stop the next source demand");
                    };
                let error = prepared
                    .with_batch_input(&mut input)
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |_, _| {
                        panic!("invalid payload or key must prevent output")
                    })
                    .err()
                    .unwrap()
                    .to_string();
                assert_eq!(calls, 0);
                assert!(
                    error.contains(if fault == "key" {
                        "NULLS FIRST or NULLS LAST"
                    } else {
                        "numeric value differs from its finite declared dtype"
                    }),
                    "{fault}: {error}"
                );
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            }
        }
    }
}

#[test]
fn streamed_ordering_bounded_prefix_failure_cleanup_allows_fresh_retry() {
    let rows = fixture_rows();
    let keys = vec![key("n", false, NullOrder::Last)];
    let mut expected = rows.clone();
    reference_sort(&mut expected, &keys);
    for fault in ["denied", "cancel", "consumer"] {
        let prepared = prepare_rows(&limited(sorted(scan(), keys.clone()), 0, 7));
        let memory = prepared.session.memory().clone();
        let baseline = memory.snapshot().reserved_bytes;
        let token = CancellationToken::default();
        let mut held = None;
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            if calls > 3 {
                return Ok(None);
            }
            let source = rows_source(session, &rows)?;
            if calls == 2 {
                if fault == "denied" {
                    let snapshot = memory.snapshot();
                    held = Some(memory.reserve(snapshot.limit_bytes - snapshot.reserved_bytes)?);
                } else if fault == "cancel" {
                    token.cancel();
                }
            }
            Ok(Some(source))
        };
        let mut emitted = 0;
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .for_each_batch(&token, |_, _| {
                emitted += 1;
                Err(batch_input::failed("bounded prefix consumer failure"))
            })
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains(match fault {
                "denied" => "reservation denied",
                "cancel" => "cancel",
                _ => "bounded prefix consumer failure",
            }),
            "{fault}: {error}"
        );
        assert_eq!(emitted, usize::from(fault == "consumer"));
        assert_eq!(calls, if fault == "consumer" { 4 } else { 2 });
        assert_eq!(prepared.snapshot().completed_executions, 0);
        drop(held);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
        let mut once = true;
        let mut input = |session: &ResidentVortexSession| {
            if std::mem::take(&mut once) {
                rows_source(session, &rows).map(Some)
            } else {
                Ok(None)
            }
        };
        let result = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        assert_eq!(
            json_rows(&result),
            expected[..7].iter().map(Row::json).collect::<Vec<_>>()
        );
        assert_eq!(prepared.snapshot().completed_executions, 1);
        drop(result);
        assert_eq!(memory.snapshot().reserved_bytes, baseline);
    }
}

#[test]
fn streamed_ordering_bounded_prefix_handles_a_population_larger_than_its_grant() {
    let total = 65 * 512 + 7;
    let text = "λ".repeat(256);
    let prepared = prepare(
        &limited(sorted(scan(), vec![key("n", false, NullOrder::Last)]), 3, 7),
        4 << 20,
    )
    .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut delivered = 0;
    let mut prior: Option<Weak<MemoryLease>> = None;
    #[cfg(debug_assertions)]
    let observation = crate::local_primitives::native_relational_observation::Session::start();
    let mut input = |session: &ResidentVortexSession| {
        assert!(
            prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        if delivered == total {
            return Ok(None);
        }
        let count = (total - delivered).min(512);
        let values = (delivered..delivered + count)
            .map(|row| Some(i64::try_from(total - row - 1).unwrap()))
            .collect::<Vec<_>>();
        let source = source(session, &values, &vec![Some(text.as_str()); count])?;
        prior = Some(source.batch_release_witness()?);
        delivered += count;
        Ok(Some(source))
    };
    let result = prepared
        .with_batch_input(&mut input)
        .unwrap()
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        json_rows(&result),
        (3..10)
            .map(|n| serde_json::json!({"n":n,"s":text}))
            .collect::<Vec<_>>()
    );
    let report = result.execution.input.as_ref().unwrap();
    assert_eq!(delivered, total);
    assert!(report.end_of_input_observed);
    assert!(report.input_logical_bytes > 4 * (4 << 20));
    assert!(report.ordering_rows_detached < total as u64 / 8);
    assert!(result.execution.runtime.memory.peak_reserved_bytes <= 4 << 20);
    assert!(result.execution.spill.is_none());
    #[cfg(debug_assertions)]
    {
        let counts = observation.finish();
        assert!(counts.sort_numeric_prefixes > 0);
        assert!(counts.sort_numeric_prefix_comparisons > 0);
        assert_eq!(counts.sort_streaming_prefix_rows_validated, total as u64);
    }
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}
