//! Sampling run validation, failure cleanup and complete fresh executions.

use super::*;
use crate::local_primitives::{
    native_relational_spill::{AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN},
    prepared_unary::BEFORE_POPULATION_CACHE_HIT,
};
use std::{
    cell::{Cell, RefCell},
    path::Path,
    rc::Rc,
};

const FAILURE_ROWS: usize = 65;
const FAILURE_WIDTH: usize = 32768;
const FAILURE_GRANT: u64 = 32 << 20;

#[path = "local_primitive_relational_sampling_population_recovery_tests.rs"]
mod recovery_tests;

fn cases() -> Vec<Value> {
    let reference: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/native_sampling_population_reference.json"
    ))
    .unwrap();
    let cases = reference["failure_cases"].as_array().unwrap().clone();
    assert_eq!(cases.len(), 6);
    for case in &cases {
        assert_eq!(case["rows"], FAILURE_ROWS);
        assert_eq!(case["width"], FAILURE_WIDTH);
        assert_eq!(case["grant_bytes"], FAILURE_GRANT);
    }
    cases
}

fn prepared(case: &Value, directory: &Path) -> PreparedVortexRelational {
    with_spill(
        prepare_unary(&unary(scan(), sample_request(case)), FAILURE_GRANT).unwrap(),
        directory,
    )
}

fn released(input: &WideInput) {
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
}

fn complete(prepared: &PreparedVortexRelational, case: &Value) -> ExecutedVortexRelational {
    let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
    let expected = case["expected"].as_array().unwrap();
    let mut delivered = 0;
    let report = prepared
        .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            for row in values(&array, context)? {
                let position = usize::try_from(expected[delivered].as_u64().unwrap()).unwrap();
                assert_eq!(row, expected_row(position, FAILURE_WIDTH, true));
                delivered += 1;
            }
            Ok(())
        })
        .unwrap();
    assert!(input.ended);
    released(&input);
    assert_eq!(delivered, expected.len());
    assert_eq!(report.spillable_sampling_stages, 1);
    assert_eq!(
        report.spillable_sampling_population_rows,
        case["population"].as_u64().unwrap()
    );
    assert!(report.spill.as_ref().unwrap().runs_written > 0);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    assert!(report.runtime.memory.peak_reserved_bytes <= FAILURE_GRANT);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.fallback_attempted);
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
    report
}

fn run_files(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.is_dir().then_some(path)
        })
        .flat_map(|path| {
            fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap().path())
        })
        .filter(|path| path.extension().is_some_and(|suffix| suffix == "vortex"))
        .collect()
}

fn damage(path: &Path, kind: &str) {
    match kind {
        "corrupt" => {
            use std::io::{Read as _, Seek as _, Write as _};
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let mut byte = [0];
            file.read_exact(&mut byte).unwrap();
            file.rewind().unwrap();
            file.write_all(&[byte[0] ^ 1]).unwrap();
        }
        "truncate" => fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(1)
            .unwrap(),
        "replace" => {
            let replacement = path.with_extension("replacement");
            fs::copy(path, &replacement).unwrap();
            fs::rename(replacement, path).unwrap();
        }
        _ => unreachable!(),
    }
}

fn no_hooks() {
    assert!(BEFORE_RUN_OPEN.with(|slot| slot.borrow().is_none()));
    assert!(BEFORE_POPULATION_CACHE_HIT.with(|slot| slot.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|slot| slot.borrow().is_none()));
}

#[test]
#[allow(clippy::too_many_lines)] // Each generation failure retains its complete independent restart control.
fn sampling_population_validates_open_cached_and_both_merge_generations() {
    for case in cases() {
        for phase in ["open", "cached", "merge-left", "merge-right"] {
            if phase == "cached" && !case["replace"].as_bool().unwrap() {
                continue;
            }
            for kind in ["corrupt", "truncate", "replace"] {
                let (_fixture, directory) = workspace();
                let prepared = prepared(&case, &directory);
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let captured = Rc::new(RefCell::new(None::<PathBuf>));
                let observed = Rc::clone(&captured);
                let fired = Rc::new(Cell::new(false));
                let seen = Rc::clone(&fired);
                let hook = Box::new(move |path: &Path| {
                    *observed.borrow_mut() = Some(path.to_path_buf());
                    seen.set(true);
                    damage(path, kind);
                });
                match phase {
                    "open" => BEFORE_RUN_OPEN.with(|slot| *slot.borrow_mut() = Some(hook)),
                    "cached" => {
                        BEFORE_POPULATION_CACHE_HIT.with(|slot| *slot.borrow_mut() = Some(hook));
                    }
                    "merge-left" | "merge-right" => {
                        let arm = Box::new(move |path: &Path| {
                            let path = path.to_path_buf();
                            AFTER_MERGE_BLOCK.with(|slot| {
                                *slot.borrow_mut() = Some(Box::new(move |_| hook(&path)));
                            });
                        });
                        if phase == "merge-left" {
                            BEFORE_RUN_OPEN.with(|slot| *slot.borrow_mut() = Some(arm));
                        } else {
                            BEFORE_RUN_OPEN.with(|slot| {
                                *slot.borrow_mut() = Some(Box::new(move |_| {
                                    BEFORE_RUN_OPEN.with(|slot| *slot.borrow_mut() = Some(arm));
                                }));
                            });
                        }
                    }
                    _ => unreachable!(),
                }
                let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
                let error = prepared
                    .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
                    .err()
                    .unwrap_or_else(|| panic!("{}/{phase}/{kind} completed", case["name"]))
                    .to_string();
                assert!(fired.get());
                assert!(
                    error.contains("changed"),
                    "{}/{phase}/{kind}: {error}",
                    case["name"]
                );
                released(&input);
                no_hooks();
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(prepared.snapshot().completed_executions, 0);
                let path = captured.borrow().clone().unwrap();
                let preserved = if kind == "replace" {
                    assert!(path.exists());
                    assert!(
                        VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)
                            .unwrap()
                            .cleanup_abandoned(path.parent().unwrap())
                            .is_err()
                    );
                    Some(fs::read(&path).unwrap())
                } else {
                    clean(&directory);
                    None
                };
                drop(complete(&prepared, &case));
                assert_eq!(prepared.snapshot().completed_executions, 1);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if let Some(bytes) = preserved {
                    assert_eq!(fs::read(path).unwrap(), bytes);
                } else {
                    clean(&directory);
                }
            }
        }
    }
}

#[test]
fn sampling_population_quota_grant_and_cancelled_merge_release_owned_state() {
    for case in cases() {
        let (_fixture, directory) = workspace();
        let initial = prepared(&case, &directory);
        let report = complete(&initial, &case);
        let quota = report.spill.as_ref().unwrap().peak_disk_bytes - 1;
        drop((report, initial));
        clean(&directory);
        let limited = prepare_unary(&unary(scan(), sample_request(&case)), FAILURE_GRANT)
            .unwrap()
            .with_spill(VortexRelationalSpillPolicy::new(&directory, quota, 1 << 20).unwrap())
            .unwrap();
        let baseline = limited.snapshot().memory.reserved_bytes;
        let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
        let error = limited
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("quota"), "{error}");
        released(&input);
        assert_eq!(limited.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(limited.snapshot().completed_executions, 0);
        clean(&directory);

        let prepared = prepared(&case, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let held = prepared
            .session
            .memory()
            .reserve(FAILURE_GRANT - baseline - (1 << 20))
            .unwrap();
        let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
        let error = prepared
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("reservation denied"), "{error}");
        released(&input);
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            baseline + held.bytes()
        );
        drop(held);
        clean(&directory);

        let cancellation = CancellationToken::default();
        let cancel = cancellation.clone();
        let fired = Rc::new(Cell::new(false));
        let seen = Rc::clone(&fired);
        AFTER_MERGE_BLOCK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |rows| {
                assert!(rows > 0);
                seen.set(true);
                cancel.cancel();
            }));
        });
        let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
        let error = prepared
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&cancellation, |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("cancel"), "{error}");
        assert!(fired.get());
        released(&input);
        no_hooks();
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        clean(&directory);
        drop(complete(&prepared, &case));
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}

#[test]
fn sampling_population_late_producer_schema_consumer_and_cancellation_never_complete() {
    for case in cases() {
        for mode in ["producer", "schema", "consumer", "cancel"] {
            let (_fixture, directory) = workspace();
            let prepared = prepared(&case, &directory);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
            let cancellation = CancellationToken::default();
            let fired = Cell::new(false);
            let spilled = Cell::new(false);
            let error = prepared
                .with_batch_inputs(&mut |_, ordinal, session| {
                    if input.position == FAILURE_ROWS {
                        assert_ne!(run_files(&directory), [] as [PathBuf; 0]);
                        spilled.set(true);
                        if matches!(mode, "producer" | "schema") {
                            fired.set(true);
                            if mode == "producer" {
                                return Err(failed("sample population producer sentinel"));
                            }
                            return source(session, &[Some(1)], &[Some("changed")]).map(Some);
                        }
                    }
                    input.next(ordinal, session)
                })
                .unwrap()
                .for_each_batch(&cancellation, |_, _| {
                    assert!(spilled.get());
                    if matches!(mode, "consumer" | "cancel") {
                        assert!(!fired.replace(true));
                        if mode == "consumer" {
                            return Err(failed("sample population consumer sentinel"));
                        }
                        cancellation.cancel();
                    }
                    Ok(())
                })
                .err()
                .unwrap()
                .to_string();
            assert!(fired.get(), "{}/{mode}", case["name"]);
            let expected = match mode {
                "producer" => "producer sentinel",
                "schema" => "schema",
                "consumer" => "consumer sentinel",
                _ => "cancel",
            };
            assert!(error.contains(expected), "{}/{mode}: {error}", case["name"]);
            released(&input);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            clean(&directory);
            drop(complete(&prepared, &case));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&directory);
        }
    }
}
