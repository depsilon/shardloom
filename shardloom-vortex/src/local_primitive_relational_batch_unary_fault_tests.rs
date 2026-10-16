//! Unary failure, cancellation, publication and clean-reuse acceptance.

use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use crate::relational_query::VortexRelationalLimit;
use std::cell::Cell;

fn no_live_input(input: &Input<'_>) {
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
}

fn clean_reuse(prepared: &PreparedVortexRelational, expected: &[Value], baseline: u64) {
    let mut input = Input::new(&ROWS, 2);
    let mut actual = Vec::new();
    let report = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| {
            assert_eq!(uri.as_str(), "memory://stream");
            input.next(ordinal, session)
        })
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            actual.extend(values(&array, context)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert!(input.ended);
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert_eq!(report.runtime.completed_executions, 1);
    assert!(report.native_io_certificate.is_certified());
    drop(report);
    no_live_input(&input);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
}

#[allow(clippy::too_many_lines)] // One failure matrix shares its publication and reuse assertions.
fn failed_case(
    name: &str,
    request: VortexQueryPrimitiveRequest,
    expected: &[Value],
    mode: &str,
    failure: &str,
) {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let foreign = fixture.0.join("foreign");
    let existing = fixture.0.join("existing.vortex");
    let target = fixture.0.join("new.vortex");
    fs::write(&foreign, b"preserve foreign contents").unwrap();
    fs::write(&existing, b"preserve existing destination").unwrap();
    let mut plan = unary(scan(), request);
    if mode == "zero-limit" {
        plan = VortexRelationalPlan::Limit(Box::new(VortexRelationalLimit {
            input: plan,
            count: 0,
            offset: 0,
        }));
    }
    let prepared = prepare_unary(&plan, 8 << 20).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let token = CancellationToken::default();
    if failure == "initial-cancel" {
        token.cancel();
    }
    let mut input = Input::new(&ROWS, 2);
    let reached = Cell::new(false);
    let delivered = Cell::new(0);
    let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        assert_eq!(uri.as_str(), "memory://stream");
        no_live_input(&input);
        if ordinal == 5 {
            assert_eq!(
                input.position, 4,
                "fault follows two actual payload batches"
            );
            reached.set(true);
            match failure {
                "producer" => return Err(batch_input::failed("unary producer sentinel")),
                "schema" => {
                    return ResidentMemorySource::from_batch_columns(
                        session,
                        &[MemoryColumn {
                            name: "wrong",
                            values: MemoryColumnValues::Int64(&[Some(1)]),
                        }],
                    )
                    .map(Some);
                }
                "input-cancel" => token.cancel(),
                _ => panic!("initial cancellation demanded input"),
            }
        }
        input.next(ordinal, session)
    };
    let batch_input = prepared.with_batch_inputs(&mut producer).unwrap();
    let result = if mode == "write" {
        batch_input
            .write_controlled(&target, Format::Vortex, false, &token)
            .map(|result| result.execution)
    } else {
        batch_input.for_each_batch(&token, |array, _| {
            delivered.set(delivered.get() + array.len());
            Ok(())
        })
    };
    let error = result
        .err()
        .expect("incomplete unary input cannot report success");
    let diagnostic = match failure {
        "producer" => "unary producer sentinel",
        "schema" => "declared schema",
        _ => "cancel",
    };
    assert!(
        error.to_string().contains(diagnostic),
        "{name}/{mode}/{failure}: {error}"
    );
    assert_eq!(reached.get(), failure != "initial-cancel");
    if failure == "initial-cancel" {
        assert_eq!(input.calls, 0);
    }
    if mode == "zero-limit" {
        assert_eq!(delivered.get(), 0);
    }
    assert!(!input.ended);
    no_live_input(&input);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert!(!target.exists());
    assert_eq!(fs::read(&foreign).unwrap(), b"preserve foreign contents");
    assert_eq!(
        fs::read(&existing).unwrap(),
        b"preserve existing destination"
    );
    assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp")
    }));
    clean_reuse(
        &prepared,
        if mode == "zero-limit" { &[] } else { expected },
        baseline,
    );
}

#[test]
fn streaming_unary_late_failures_and_cancellation_deny_publication_even_after_zero_limit() {
    for (name, request, expected) in cases(&ROWS) {
        for mode in ["consume", "zero-limit", "write"] {
            for failure in ["producer", "schema", "initial-cancel", "input-cancel"] {
                failed_case(&name, request.clone(), &expected, mode, failure);
            }
        }
    }
}

#[test]
fn streaming_unary_consumer_failure_and_output_cancellation_release_state_for_reuse() {
    for (name, request, expected) in cases(&ROWS) {
        for cancel in [false, true] {
            let prepared = prepare_unary(&unary(scan(), request.clone()), 8 << 20).unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let mut input = Input::new(&ROWS, 2);
            let mut callbacks = 0;
            let error = prepared
                .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                .unwrap()
                .for_each_batch(&token, |_, _| {
                    callbacks += 1;
                    if cancel {
                        token.cancel();
                        Ok(())
                    } else {
                        Err(batch_input::failed("unary consumer sentinel"))
                    }
                })
                .err()
                .expect("refused delivery cannot certify completion");
            assert_eq!(callbacks, 1);
            assert!(
                error.to_string().contains(if cancel {
                    "cancel"
                } else {
                    "unary consumer sentinel"
                }),
                "{name}: {error}"
            );
            // Future-dependent unary families may complete input before their first output.
            // The required invariant is absent successful completion and released ownership.
            no_live_input(&input);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean_reuse(&prepared, &expected, baseline);
        }
    }
}
