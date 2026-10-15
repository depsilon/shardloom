use super::super::writer_tests::{FORMATS, read_rows};
use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;

#[path = "local_primitive_relational_batch_destination_pressure_tests.rs"]
mod pressure_tests;

#[test]
#[allow(clippy::too_many_lines)] // All adapters share the same completion and exact-value matrix.
fn completion_input_all_writers_preserve_values_and_require_source_end() {
    for format in FORMATS {
        for case in ["mixed", "empty", "nulls", "late_failure"] {
            let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
            let prepared = prepare(&scan(), 16 << 20).unwrap();
            let initial = prepared.snapshot().memory.reserved_bytes;
            let path = fixture.0.join(format!("{case}.{}", format.as_str()));
            let mut calls = 0;
            let mut input = |session: &ResidentVortexSession| {
                calls += 1;
                assert!(!path.exists(), "publication must wait for explicit end");
                if calls == 4 {
                    return if case == "late_failure" {
                        Err(batch_input::failed(
                            "destination input failed at completion",
                        ))
                    } else {
                        Ok(None)
                    };
                }
                let (numbers, text): (&[Option<i64>], &[Option<&str>]) = match (case, calls) {
                    ("empty", _) | (_, 1 | 3) => (&[], &[]),
                    ("nulls", _) => (&[None, None], &[None, None]),
                    _ => (
                        &[Some(i64::MIN), None, Some(i64::MAX), Some(0)],
                        &[Some("λ\"\n"), None, Some(""), Some("東京")],
                    ),
                };
                Ok(Some(source(session, numbers, text)?))
            };
            let result = prepared
                .with_batch_input(&mut input)
                .unwrap()
                .write_controlled(&path, format, false, &CancellationToken::default());
            if case == "late_failure" {
                assert!(
                    result
                        .err()
                        .unwrap()
                        .to_string()
                        .contains("destination input failed at completion"),
                    "{format:?}"
                );
                assert!(!path.exists());
                assert_eq!(prepared.snapshot().completed_executions, 0);
            } else {
                let result = result.unwrap_or_else(|error| panic!("{format:?} {case}: {error}"));
                let expected = match case {
                    "empty" => serde_json::json!([]),
                    "nulls" => serde_json::json!([{"n":null,"s":null},{"n":null,"s":null}]),
                    _ => serde_json::json!([
                        {"n":i64::MIN,"s":"λ\"\n"}, {"n":null,"s":null},
                        {"n":i64::MAX,"s":""}, {"n":0,"s":"東京"},
                    ]),
                };
                let expected = expected.as_array().unwrap();
                assert_eq!(result.execution.output_rows, expected.len() as u64);
                assert_eq!(result.output.rows_written, expected.len() as u64);
                assert_eq!(result.execution.runtime.completed_executions, 1);
                assert!(
                    result
                        .execution
                        .input
                        .as_ref()
                        .unwrap()
                        .end_of_input_observed
                );
                assert!(result.execution.native_io_certificate.is_certified());
                assert!(
                    !result
                        .execution
                        .native_io_certificate
                        .side_effects
                        .fallback_attempted
                );
                assert!(!result.output.evidence.side_effects.fallback_attempted);
                if format == Format::Csv {
                    let csv = match case {
                        "empty" => "n,s\n",
                        "nulls" => "n,s\n,\n,\n",
                        _ => {
                            "n,s\n-9223372036854775808,\"λ\"\"\n\"\n,\n9223372036854775807,\"\"\n0,東京\n"
                        }
                    };
                    assert_eq!(fs::read_to_string(&path).unwrap(), csv);
                } else {
                    assert_eq!(
                        &read_rows(&path, format, &prepared.output_dtype().unwrap()),
                        expected,
                        "{format:?} {case}"
                    );
                }
                drop(result);
            }
            assert_eq!(calls, 4, "{format:?} {case}");
            assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
            assert!(
                fs::read_dir(&fixture.0).unwrap().all(|entry| {
                    let path = entry.unwrap().path();
                    path == fixture.0.join("input.vortex")
                        || path == fixture.0.join(format!("{case}.{}", format.as_str()))
                }),
                "no owned staging remains"
            );
        }
    }
}

#[test]
fn completion_input_fanout_executes_once_for_all_writers_and_typed_empty_output() {
    for empty in [false, true] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let prepared = prepare(&scan(), 32 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let targets = FORMATS.map(|format| {
            (
                fixture.0.join(format!("output.{}", format.as_str())),
                format,
            )
        });
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            assert!(targets.iter().all(|(path, _)| !path.exists()));
            if calls == 4 {
                return Ok(None);
            }
            Ok(Some(if empty {
                source(session, &[], &[])?
            } else {
                source(session, &[Some(calls), None], &[Some("東京"), None])?
            }))
        };
        let written = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .write_many_controlled(&targets, false, &CancellationToken::default())
            .unwrap();
        assert_eq!(calls, 4);
        assert_eq!(written.execution.runtime.completed_executions, 1);
        assert_eq!(written.execution.output_rows, if empty { 0 } else { 6 });
        assert!(
            written
                .execution
                .input
                .as_ref()
                .unwrap()
                .end_of_input_observed
        );
        assert_eq!(written.execution.input.as_ref().unwrap().replay_sources, 0);
        assert!(written.execution.native_io_certificate.is_certified());
        assert!(
            !written
                .execution
                .native_io_certificate
                .side_effects
                .fallback_attempted
        );
        let replay = written.replay.as_ref().unwrap();
        assert!(!replay.spilled);
        assert_eq!(replay.rows_detached, written.execution.output_rows);
        assert_eq!(replay.batches_detached, if empty { 0 } else { 3 });
        assert_eq!(replay.replays, 8);
        assert_eq!(written.outputs.len(), 8);
        let expected = if empty {
            serde_json::json!([])
        } else {
            serde_json::json!([
                {"n":1,"s":"東京"},{"n":null,"s":null},
                {"n":2,"s":"東京"},{"n":null,"s":null},
                {"n":3,"s":"東京"},{"n":null,"s":null},
            ])
        };
        for ((path, format), output) in targets.iter().zip(&written.outputs) {
            assert_eq!(output.output_path, path.display().to_string());
            assert_eq!(output.rows_written, written.execution.output_rows);
            assert_eq!(output.rows_scanned, written.execution.scan_rows_delivered);
            assert!(!output.evidence.side_effects.fallback_attempted);
            if *format == Format::Csv {
                assert_eq!(
                    fs::read_to_string(path).unwrap(),
                    if empty {
                        "n,s\n"
                    } else {
                        "n,s\n1,東京\n,\n2,東京\n,\n3,東京\n,\n"
                    }
                );
            } else {
                assert_eq!(
                    &read_rows(path, *format, &prepared.output_dtype().unwrap()),
                    expected.as_array().unwrap()
                );
            }
        }
        drop(written);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 9);
    }
}

#[test]
fn completion_input_fanout_failures_leave_no_published_prefix_or_owners() {
    for failure in ["producer", "schema", "cancel", "later_adapter"] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let prepared = prepare(&scan(), 32 << 20).unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("first.vortex"), Format::Vortex),
            (fixture.0.join("second.parquet"), Format::Parquet),
        ];
        let token = CancellationToken::default();
        let text = if failure == "later_adapter" {
            "x".repeat(65_537)
        } else {
            "ordinary".into()
        };
        let mut calls = 0;
        let mut input = |session: &ResidentVortexSession| {
            calls += 1;
            assert!(targets.iter().all(|(path, _)| !path.exists()));
            if calls == 2 {
                return match failure {
                    "producer" => Err(batch_input::failed("late destination producer error")),
                    "schema" => Ok(Some(ResidentMemorySource::from_batch_columns(
                        session,
                        &[MemoryColumn {
                            name: "wrong",
                            values: MemoryColumnValues::Int64(&[Some(2)]),
                        }],
                    )?)),
                    "cancel" => {
                        token.cancel();
                        Ok(None)
                    }
                    _ => Ok(None),
                };
            }
            assert_eq!(calls, 1);
            Ok(Some(source(session, &[Some(1)], &[Some(text.as_str())])?))
        };
        let error = prepared
            .with_batch_input(&mut input)
            .unwrap()
            .write_many_controlled(&targets, false, &token)
            .err()
            .expect("whole fanout must fail");
        if failure == "producer" {
            assert!(
                error
                    .to_string()
                    .contains("late destination producer error")
            );
        }
        if failure == "later_adapter" {
            assert!(error.to_string().contains("string"), "{error}");
        }
        assert_eq!(calls, 2, "{failure}");
        assert!(targets.iter().all(|(path, _)| !path.exists()));
        assert_eq!(prepared.snapshot().completed_executions, 0, "{failure}");
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            initial,
            "{failure}"
        );
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1, "{failure}");
    }
}

#[test]
fn completion_input_fanout_target_admission_and_cancellation_precede_demand() {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let prepared = prepare(&scan(), 8 << 20).unwrap();
    let target = (fixture.0.join("missing/output.vortex"), Format::Vortex);
    for targets in [vec![], vec![target.clone(); 2], vec![target.clone(); 33]] {
        let mut input = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
            panic!("invalid targets must not demand input")
        };
        assert!(
            prepared
                .with_batch_input(&mut input)
                .unwrap()
                .write_many_controlled(&targets, false, &CancellationToken::default())
                .is_err()
        );
        assert!(!fixture.0.join("missing").exists());
    }
    let token = CancellationToken::default();
    token.cancel();
    let mut input = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("cancelled fanout must not demand input")
    };
    assert!(
        prepared
            .with_batch_input(&mut input)
            .unwrap()
            .write_many_controlled(&[target], false, &token)
            .is_err()
    );
    assert!(!fixture.0.join("missing").exists());
}
