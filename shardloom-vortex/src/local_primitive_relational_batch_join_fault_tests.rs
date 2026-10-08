//! Failures after real join spill must release credits and prevent completion.

use super::*;
use crate::local_primitives::{
    VortexLocalPrimitiveRowExportFormat as Format,
    native_relational_spill::{AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN},
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

#[cfg(unix)]
#[path = "local_primitive_relational_batch_join_recovery_tests.rs"]
mod recovery_tests;

const TOTAL: usize = 8 * 1024 + 7;
const GRANT: u64 = 16 << 20;

fn number(row: usize) -> i64 {
    [1, 3, 2][row % 3]
}

fn text(row: usize) -> String {
    format!("{row:09}{}", "x".repeat(503))
}

struct Input {
    delivered: usize,
    calls: usize,
    prior: Option<Weak<MemoryLease>>,
}

impl Input {
    fn new() -> Self {
        Self {
            delivered: 0,
            calls: 0,
            prior: None,
        }
    }

    fn next(&mut self, session: &ResidentVortexSession) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        self.calls += 1;
        if self.delivered == TOTAL {
            return Ok(None);
        }
        let end = (self.delivered + 1024).min(TOTAL);
        let numbers = (self.delivered..end)
            .map(|row| Some(number(row)))
            .collect::<Vec<_>>();
        let strings = (self.delivered..end).map(text).collect::<Vec<_>>();
        let refs = strings
            .iter()
            .map(|value| Some(value.as_str()))
            .collect::<Vec<_>>();
        let batch = source(session, &numbers, &refs)?;
        self.prior = Some(batch.batch_release_witness()?);
        self.delivered = end;
        Ok(Some(batch))
    }
}

fn spill(workspace: &std::path::Path, quota: u64) -> VortexRelationalSpillPolicy {
    VortexRelationalSpillPolicy::new(workspace, quota, 1 << 20).unwrap()
}

fn runs(workspace: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(workspace)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| {
                        path.extension()
                            .is_some_and(|extension| extension == "vortex")
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect()
}

fn prepare_join(
    fixture: &Fixture,
    directory: &std::path::Path,
    quota: u64,
) -> PreparedVortexRelational {
    prepare(&joined(fixture.scan(), true, JoinKind::Full), GRANT)
        .unwrap()
        .with_spill(spill(directory, quota))
        .unwrap()
}

fn hooks_consumed() {
    assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
}

#[test]
#[allow(clippy::too_many_lines)] // One failure matrix shares the real streamed build and probe.
fn streaming_join_spilled_faults_refund_state_and_never_report_completion() {
    for failure in [
        "producer",
        "schema",
        "input-cancel",
        "merge-cancel",
        "consumer-cancel",
        "consumer",
        "quota",
        "corrupt-open",
        "corrupt-probe",
        "replace-probe",
        "allocation-probe",
    ] {
        let fixture = ordinary_file();
        let directory = fixture.0.join("join-runs");
        fs::create_dir(&directory).unwrap();
        let prepared = prepare_join(
            &fixture,
            &directory,
            if failure == "quota" {
                32 << 10
            } else {
                128 << 20
            },
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let triggered = Rc::new(Cell::new(0));
        if failure == "corrupt-open" {
            let triggered = Rc::clone(&triggered);
            BEFORE_RUN_OPEN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    assert!(fs::metadata(path).unwrap().len() > 1);
                    fs::OpenOptions::new()
                        .write(true)
                        .open(path)
                        .unwrap()
                        .set_len(1)
                        .unwrap();
                    triggered.set(1);
                }));
            });
        }
        if failure == "merge-cancel" {
            let triggered = Rc::clone(&triggered);
            let token = token.clone();
            AFTER_MERGE_BLOCK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |rows| {
                    assert!(rows > 0);
                    triggered.set(rows);
                    token.cancel();
                }));
            });
        }
        let mut input = Input::new();
        let mut producer = |session: &ResidentVortexSession| {
            if input.calls == 5 && matches!(failure, "producer" | "schema" | "input-cancel") {
                assert!(
                    !runs(&directory).is_empty(),
                    "must follow actual join spill"
                );
                triggered.set(1);
                match failure {
                    "producer" => return Err(batch_input::failed("join producer after spill")),
                    "schema" => {
                        return Ok(Some(ResidentMemorySource::from_batch_columns(
                            session,
                            &[MemoryColumn {
                                name: "wrong",
                                values: MemoryColumnValues::Int64(&[Some(1)]),
                            }],
                        )?));
                    }
                    "input-cancel" => token.cancel(),
                    _ => unreachable!(),
                }
            }
            input.next(session)
        };
        let mut emitted = 0;
        let mut emitted_rows = 0;
        let mut held_credit = None;
        let mut replacement = None;
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&token, |array, context| {
                assert_ne!(array.len(), 0);
                emitted += 1;
                emitted_rows += array.len();
                if emitted != 1 {
                    return Ok(());
                }
                let paths = runs(&directory);
                assert_eq!(
                    paths.len(),
                    1,
                    "the completed build owns one real sorted run"
                );
                match failure {
                    "consumer-cancel" => token.cancel(),
                    "consumer" => return Err(batch_input::failed("join consumer refused")),
                    "corrupt-probe" => {
                        fs::OpenOptions::new()
                            .write(true)
                            .open(&paths[0])
                            .unwrap()
                            .set_len(1)
                            .unwrap();
                        triggered.set(1);
                    }
                    "replace-probe" => {
                        let original = fs::read(&paths[0]).unwrap();
                        let backup = fixture.0.join("original-run.vortex");
                        fs::rename(&paths[0], &backup).unwrap();
                        fs::write(&paths[0], &original).unwrap();
                        replacement = Some((paths[0].clone(), backup, original));
                        triggered.set(1);
                    }
                    "allocation-probe" => {
                        let available = GRANT - context.memory().snapshot().reserved_bytes;
                        held_credit = Some(context.memory().reserve(available - 1024)?);
                        triggered.set(1);
                    }
                    _ => {}
                }
                Ok(())
            });
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{failure} completed"))
            .to_string();
        assert!(
            error.contains(match failure {
                "producer" => "join producer after spill",
                "schema" => "declared",
                "quota" => "quota",
                "consumer" => "join consumer refused",
                "corrupt-open" | "corrupt-probe" | "replace-probe" => "changed",
                "allocation-probe" => "reservation denied",
                _ => "cancel",
            }),
            "{failure}: {error}"
        );
        if failure == "allocation-probe" {
            // Releasing the first candidate/output can admit a smaller next
            // batch. The contract is denial during the remaining probe, not
            // a specific allocation count or an exact provisional batch count.
            assert!(emitted > 0 && emitted_rows < TOTAL + 1);
        } else {
            assert_eq!(
                emitted,
                usize::from(matches!(
                    failure,
                    "consumer" | "consumer-cancel" | "corrupt-probe" | "replace-probe"
                )),
                "{failure}"
            );
        }
        if !matches!(failure, "consumer" | "consumer-cancel" | "quota") {
            assert!(triggered.get() > 0);
        }
        drop(held_credit);
        hooks_consumed();
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            baseline,
            "{failure}"
        );
        assert!(
            input
                .prior
                .as_ref()
                .is_none_or(|witness| witness.strong_count() == 0)
        );
        if let Some((path, backup, bytes)) = replacement {
            // A same-byte replacement has a foreign identity. Failed cleanup
            // must preserve it; only this fixture restores the original inode.
            assert_eq!(fs::read(&path).unwrap(), bytes);
            assert!(
                spill(&directory, 128 << 20)
                    .cleanup_abandoned(path.parent().unwrap())
                    .is_err()
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
            fs::remove_file(&path).unwrap();
            fs::rename(backup, &path).unwrap();
            spill(&directory, 128 << 20)
                .cleanup_abandoned(path.parent().unwrap())
                .unwrap();
        }
        assert_eq!(fs::read_dir(directory).unwrap().count(), 0, "{failure}");
    }
}

fn expected() -> Vec<Value> {
    (0..TOTAL)
        .step_by(3)
        .map(|row| json!({"left":"r1","right":text(row)}))
        .chain(
            (1..TOTAL)
                .step_by(3)
                .map(|row| json!({"left":"r3","right":text(row)})),
        )
        .chain(std::iter::once(json!({"left":"rn","right":null})))
        .chain(
            (2..TOTAL)
                .step_by(3)
                .map(|row| json!({"left":null,"right":text(row)})),
        )
        .collect()
}

fn complete(prepared: &PreparedVortexRelational) -> ExecutedVortexRelational {
    let mut input = Input::new();
    let mut producer = |session: &ResidentVortexSession| input.next(session);
    let expected = expected();
    let mut seen = 0;
    let report = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            for row in values(&array, context)? {
                assert_eq!(row, expected[seen]);
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, TOTAL + 1);
    assert_eq!(input.calls, TOTAL.div_ceil(1024) + 1);
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert!(report.spill.as_ref().unwrap().runs_written > 3);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    report
}

#[test]
#[allow(clippy::too_many_lines)] // Publication assertions share one accepted destination and plan.
fn streaming_join_spilled_write_reopens_and_refuses_partial_publication() {
    let fixture = ordinary_file();
    let directory = fixture.0.join("join-runs");
    fs::create_dir(&directory).unwrap();
    let prepared = prepare_join(&fixture, &directory, 128 << 20);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let output = fixture.0.join("complete.vortex");
    let mut input = Input::new();
    let mut producer = |session: &ResidentVortexSession| input.next(session);
    let report = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .write_controlled(
            &output,
            Format::Vortex,
            false,
            &CancellationToken::default(),
        )
        .unwrap();
    assert_eq!(report.output.rows_written, (TOTAL + 1) as u64);
    assert!(report.execution.spill.as_ref().unwrap().runs_written > 3);
    assert!(
        report
            .execution
            .input
            .as_ref()
            .unwrap()
            .end_of_input_observed
    );
    let dtype = prepared.output_dtype();
    drop(report);
    let scan = other_scan(DatasetUri::new(output.display().to_string()).unwrap());
    let reopened = prepare_relational(&scan, policy()).unwrap();
    assert_eq!(reopened.output_dtype(), dtype);
    let expected = expected();
    let mut seen = 0;
    reopened
        .for_each_batch(&CancellationToken::default(), |array, context| {
            for row in values(&array, context)? {
                assert_eq!(row, expected[seen]);
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(seen, expected.len());
    let original = fs::read(&output).unwrap();
    let mut unopened = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("replacement must be refused before producer demand")
    };
    let error = prepared
        .with_batch_input(&mut unopened)
        .unwrap()
        .write_controlled(&output, Format::Vortex, true, &CancellationToken::default())
        .err()
        .unwrap()
        .to_string();
    assert!(
        error.contains("atomic generation-conditional replacement is unavailable"),
        "{error}"
    );
    assert_eq!(fs::read(&output).unwrap(), original);
    for failure in ["producer", "cleanup"] {
        let target = fixture.0.join(format!("failed-{failure}.vortex"));
        let foreign = Rc::new(RefCell::new(None));
        if failure == "cleanup" {
            let foreign = Rc::clone(&foreign);
            BEFORE_RUN_OPEN.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    let path = path.parent().unwrap().join("foreign");
                    fs::write(&path, b"keep unknown join bytes").unwrap();
                    *foreign.borrow_mut() = Some(path);
                }));
            });
        }
        let mut input = Input::new();
        let mut producer = |session: &ResidentVortexSession| {
            if failure == "producer" && input.calls == 5 {
                assert_ne!(runs(&directory).len(), 0);
                return Err(batch_input::failed("join writer after spill"));
            }
            input.next(session)
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_controlled(
                &target,
                Format::Vortex,
                false,
                &CancellationToken::default(),
            )
            .err()
            .expect("failed join must not publish")
            .to_string();
        if failure == "producer" {
            assert!(error.contains("join writer after spill"), "{error}");
        }
        if failure == "cleanup" {
            let path = foreign
                .borrow()
                .clone()
                .expect("actual join run hook must fire");
            assert_eq!(fs::read(&path).unwrap(), b"keep unknown join bytes");
            fs::remove_file(&path).unwrap();
            fs::remove_dir(path.parent().unwrap()).unwrap();
        }
        assert!(!target.exists());
        assert_eq!(fs::read(&output).unwrap(), original);
        assert_eq!(prepared.snapshot().completed_executions, 1);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        hooks_consumed();
    }
}
