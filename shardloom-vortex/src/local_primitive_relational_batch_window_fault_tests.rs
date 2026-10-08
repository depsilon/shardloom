//! Faults in real window runs, summaries and cached reads never certify completion.

use super::*;
use crate::local_primitives::{
    native_relational_spill::{AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN},
    native_relational_window::spill::{BEFORE_CACHE_HIT, Progress, WINDOW_PROGRESS},
};
use std::{cell::RefCell, rc::Rc};

#[cfg(unix)]
#[path = "local_primitive_relational_batch_window_recovery_tests.rs"]
mod recovery_tests;

const TOTAL: usize = 8 * 1024 + 7;

fn runs(directory: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
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
                vec![]
            }
        })
        .collect()
}

fn hooks_consumed() {
    assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
    assert!(BEFORE_CACHE_HIT.with(|hook| hook.borrow().is_none()));
    assert!(WINDOW_PROGRESS.with(|hook| hook.borrow().is_none()));
}

fn phase(failure: &str) -> Option<Progress> {
    match failure {
        "bounds-cancel" => Some(Progress::Bounds),
        "interval-cancel" => Some(Progress::DistinctIntervals),
        "events-cancel" | "events-allocation" => Some(Progress::DistinctEvents),
        "summaries-cancel" => Some(Progress::ExtremaSummaries),
        "summary-read-cancel" => Some(Progress::ExtremaResults),
        _ => None,
    }
}

#[test]
#[allow(clippy::too_many_lines)] // Shared matrix proves each failure against the same actual native stores.
fn streaming_window_spilled_failures_cancel_generation_and_credit_denials_release_owned_state() {
    for failure in [
        "pre-cancel",
        "producer",
        "schema",
        "input-cancel",
        "merge-cancel",
        "quota",
        "corrupt-open",
        "bounds-cancel",
        "interval-cancel",
        "events-cancel",
        "summaries-cancel",
        "summary-read-cancel",
        "events-allocation",
        "cached-corrupt",
        "cached-replace",
        "consumer",
        "consumer-cancel",
        "output-allocation",
    ] {
        let fixture = fixture();
        let directory = fixture.0.join("window-runs");
        let prepared = prepare_window(
            &directory,
            GRANT,
            Some(if failure == "quota" {
                32 << 10
            } else {
                128 << 20
            }),
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let token = CancellationToken::default();
        let triggered = Rc::new(Cell::new(0));
        let held_credit = Rc::new(RefCell::new(None));
        let replacement = Rc::new(RefCell::new(None));
        if failure == "pre-cancel" {
            token.cancel();
        }
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
        if let Some(target) = phase(failure) {
            let triggered = Rc::clone(&triggered);
            let token = token.clone();
            let directory = directory.clone();
            let memory = prepared.session.memory().clone();
            let held_credit = Rc::clone(&held_credit);
            WINDOW_PROGRESS.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |phase, rows| {
                    if phase != target || rows < 1024 {
                        return false;
                    }
                    assert!(
                        !runs(&directory).is_empty(),
                        "window phase must follow actual spill"
                    );
                    triggered.set(rows);
                    if failure == "events-allocation" {
                        let available = GRANT - memory.snapshot().reserved_bytes;
                        *held_credit.borrow_mut() = Some(memory.reserve(available - 1024).unwrap());
                    } else {
                        token.cancel();
                    }
                    true
                }));
            });
        }
        if matches!(failure, "cached-corrupt" | "cached-replace") {
            let triggered = Rc::clone(&triggered);
            let replacement = Rc::clone(&replacement);
            let backup = fixture.0.join("original-run.vortex");
            BEFORE_CACHE_HIT.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    assert!(fs::metadata(path).unwrap().len() > 1);
                    if failure == "cached-corrupt" {
                        fs::OpenOptions::new()
                            .write(true)
                            .open(path)
                            .unwrap()
                            .set_len(1)
                            .unwrap();
                    } else {
                        let bytes = fs::read(path).unwrap();
                        fs::rename(path, &backup).unwrap();
                        fs::write(path, &bytes).unwrap();
                        *replacement.borrow_mut() = Some((path.to_path_buf(), backup, bytes));
                    }
                    triggered.set(1);
                }));
            });
        }
        let mut input = LargeInput::new(TOTAL);
        let mut producer = |session: &ResidentVortexSession| {
            if input.calls == 5 && matches!(failure, "producer" | "schema" | "input-cancel") {
                assert_ne!(runs(&directory), [] as [std::path::PathBuf; 0]);
                triggered.set(1);
                match failure {
                    "producer" => return Err(batch_input::failed("window producer after spill")),
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
                    _ => unreachable!(),
                }
            }
            input.next(session)
        };
        let mut emitted = 0;
        let mut emitted_rows = 0;
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .for_each_batch(&token, |array, context| {
                assert!(!array.is_empty());
                emitted += 1;
                emitted_rows += array.len();
                if emitted == 1 {
                    match failure {
                        "consumer" => {
                            return Err(batch_input::failed("window consumer after spill"));
                        }
                        "consumer-cancel" => token.cancel(),
                        "output-allocation" => {
                            let available = GRANT - context.memory().snapshot().reserved_bytes;
                            *held_credit.borrow_mut() =
                                Some(context.memory().reserve(available - 1024)?);
                            triggered.set(1);
                        }
                        _ => {}
                    }
                }
                Ok(())
            });
        let error = result
            .err()
            .unwrap_or_else(|| panic!("{failure} completed"))
            .to_string();
        let expected = match failure {
            "producer" => "window producer after spill",
            "schema" => "declared",
            "quota" => "quota",
            "corrupt-open" | "cached-corrupt" | "cached-replace" => "changed",
            "events-allocation" | "output-allocation" => "reservation denied",
            "consumer" => "window consumer after spill",
            _ => "cancel",
        };
        assert!(error.contains(expected), "{failure}: {error}");
        if failure == "output-allocation" {
            assert!(emitted > 0 && emitted_rows < TOTAL);
        } else {
            assert_eq!(
                emitted,
                usize::from(matches!(failure, "consumer" | "consumer-cancel")),
                "{failure}"
            );
        }
        if failure == "pre-cancel" {
            assert_eq!(input.calls, 0);
        }
        if !matches!(
            failure,
            "pre-cancel" | "quota" | "consumer" | "consumer-cancel"
        ) {
            assert!(triggered.get() > 0, "{failure}");
        }
        held_credit.borrow_mut().take();
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
                .is_none_or(|prior| prior.strong_count() == 0)
        );
        if let Some((path, backup, bytes)) = replacement.borrow_mut().take() {
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
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 0, "{failure}");
    }
}

#[test]
fn streaming_window_failed_native_writes_preserve_destinations_and_foreign_files() {
    let fixture = fixture();
    let directory = fixture.0.join("window-runs");
    let target = fixture.0.join("protected.vortex");
    fs::write(&target, b"existing user output").unwrap();
    let prepared = prepare_window(&directory, GRANT, Some(128 << 20));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut producer = |_: &ResidentVortexSession| -> Result<Option<ResidentMemorySource>> {
        panic!("existing target must reject before input demand")
    };
    assert!(
        prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_controlled(&target, Format::Vortex, true, &CancellationToken::default())
            .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"existing user output");
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    let foreign = directory.join("user.txt");
    fs::write(&foreign, b"unowned spill workspace file").unwrap();
    for failure in ["producer", "summary-cancel", "quota"] {
        let prepared = prepare_window(
            &directory,
            GRANT,
            Some(if failure == "quota" {
                32 << 10
            } else {
                128 << 20
            }),
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let output = fixture.0.join(format!("failed-{failure}.vortex"));
        let token = CancellationToken::default();
        if failure == "summary-cancel" {
            let token = token.clone();
            WINDOW_PROGRESS.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |phase, rows| {
                    if phase != Progress::ExtremaSummaries || rows < 1024 {
                        return false;
                    }
                    token.cancel();
                    true
                }));
            });
        }
        let mut input = LargeInput::new(TOTAL);
        let mut producer = |session: &ResidentVortexSession| {
            if failure == "producer" && input.delivered == TOTAL {
                assert_ne!(runs(&directory), [] as [std::path::PathBuf; 0]);
                return Err(batch_input::failed("window final producer failure"));
            }
            input.next(session)
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_controlled(&output, Format::Vortex, false, &token)
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains(match failure {
                "producer" => "window final producer failure",
                "quota" => "quota",
                _ => "cancel",
            }),
            "{failure}: {error}"
        );
        assert!(!output.exists());
        assert_eq!(fs::read(&target).unwrap(), b"existing user output");
        assert_eq!(fs::read(&foreign).unwrap(), b"unowned spill workspace file");
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert!(
            input
                .prior
                .as_ref()
                .is_none_or(|prior| prior.strong_count() == 0)
        );
        hooks_consumed();
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
        let mut names = fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["input.vortex", "protected.vortex", "window-runs"]);
    }
}
