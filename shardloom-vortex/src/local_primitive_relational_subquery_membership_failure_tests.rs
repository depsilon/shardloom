//! Predicate run failures refund owners and require a complete fresh execution.

use super::*;
use crate::local_primitives::native_relational_spill::{AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN};
use std::{
    cell::{Cell, RefCell},
    path::Path,
    rc::Rc,
};

#[path = "local_primitive_relational_subquery_membership_recovery_tests.rs"]
mod recovery_tests;

pub(super) fn workspace() -> (Fixture, PathBuf) {
    let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
    let directory = fixture.0.join("predicate-failure");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("foreign"), b"keep").unwrap();
    (fixture, directory)
}

pub(super) fn clean(directory: &Path) {
    assert_eq!(fs::read_dir(directory).unwrap().count(), 1);
    assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
}

fn released(input: &WideInput) {
    assert!(
        input
            .prior
            .as_ref()
            .is_none_or(|owner| owner.strong_count() == 0)
    );
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
    assert!(BEFORE_CACHE_HIT.with(|slot| slot.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|slot| slot.borrow().is_none()));
}

#[test]
#[allow(clippy::too_many_lines)] // Every generation/family pair has a complete fresh restart control.
fn subquery_membership_validates_open_cached_and_both_merge_generations() {
    for (index, query) in cases().iter().enumerate() {
        for phase in ["open", "cached", "merge-left", "merge-right"] {
            for kind in ["corrupt", "truncate", "replace"] {
                let (_fixture, directory) = workspace();
                let prepared = prepared(query, &directory);
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
                    "cached" => BEFORE_CACHE_HIT.with(|slot| *slot.borrow_mut() = Some(hook)),
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
                let mut input = WideInput::new(ROWS, WIDTH, index == 5);
                let error = prepared
                    .with_batch_inputs(&mut |uri, ordinal, session| {
                        input.next(uri, ordinal, session)
                    })
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
                    .err()
                    .unwrap_or_else(|| panic!("{index}/{phase}/{kind} completed"))
                    .to_string();
                assert!(fired.get());
                assert!(error.contains("changed"), "{index}/{phase}/{kind}: {error}");
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
                let report = complete(&prepared, query, ROWS, WIDTH, index == 5);
                assert!(report.spill.as_ref().unwrap().runs_written > 2);
                drop(report);
                assert_eq!(prepared.snapshot().completed_executions, 1);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                if let Some(bytes) = preserved {
                    assert_eq!(fs::read(path).unwrap(), bytes);
                    assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"keep");
                } else {
                    clean(&directory);
                }
            }
        }
    }
}

#[test]
fn subquery_membership_quota_grant_and_cancelled_merge_release_owned_state() {
    for (index, query) in cases().iter().enumerate() {
        let (_fixture, directory) = workspace();
        let initial = prepared(query, &directory);
        let report = complete(&initial, query, ROWS, WIDTH, index == 5);
        let quota = report.spill.as_ref().unwrap().peak_disk_bytes - 1;
        drop((report, initial));
        clean(&directory);
        let limited = prepare_sources(
            &VortexRelationalPlan::Subquery(Box::new(query.clone())),
            2,
            GRANT,
        )
        .unwrap()
        .with_spill(VortexRelationalSpillPolicy::new(&directory, quota, 1 << 20).unwrap())
        .unwrap();
        let baseline = limited.snapshot().memory.reserved_bytes;
        let mut input = WideInput::new(ROWS, WIDTH, index == 5);
        let error = limited
            .with_batch_inputs(&mut |uri, ordinal, session| input.next(uri, ordinal, session))
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

        let prepared = prepared(query, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let held = prepared
            .session
            .memory()
            .reserve(GRANT - baseline - (1 << 20))
            .unwrap();
        let mut input = WideInput::new(ROWS, WIDTH, index == 5);
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| input.next(uri, ordinal, session))
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
        assert_eq!(prepared.snapshot().completed_executions, 0);
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
        let mut input = WideInput::new(ROWS, WIDTH, index == 5);
        let error = prepared
            .with_batch_inputs(&mut |uri, ordinal, session| input.next(uri, ordinal, session))
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
        drop(complete(&prepared, query, ROWS, WIDTH, index == 5));
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}

#[test]
fn subquery_membership_late_producer_schema_consumer_and_cancellation_never_complete() {
    for (index, query) in cases().iter().enumerate() {
        for mode in ["producer", "schema", "consumer", "cancel"] {
            let (_fixture, directory) = workspace();
            let prepared = prepared(query, &directory);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = WideInput::new(ROWS, WIDTH, index == 5);
            let cancellation = CancellationToken::default();
            let fired = Cell::new(false);
            let spilled = Cell::new(false);
            let error = prepared
                .with_batch_inputs(&mut |uri, ordinal, session| {
                    if uri.as_str() == URIS[1] && ordinal == ROWS.div_ceil(127) as u64 {
                        assert_ne!(run_files(&directory), [] as [PathBuf; 0]);
                        spilled.set(true);
                        if matches!(mode, "producer" | "schema") {
                            fired.set(true);
                            if mode == "producer" {
                                return Err(batch_input::failed("predicate producer sentinel"));
                            }
                            return ResidentMemorySource::from_batch_columns(
                                session,
                                &[MemoryColumn {
                                    name: "wrong",
                                    values: MemoryColumnValues::Int64(&[Some(1)]),
                                }],
                            )
                            .map(Some);
                        }
                    }
                    input.next(uri, ordinal, session)
                })
                .unwrap()
                .for_each_batch(&cancellation, |_, _| {
                    assert!(spilled.get());
                    if matches!(mode, "consumer" | "cancel") {
                        assert!(!fired.replace(true));
                        if mode == "consumer" {
                            return Err(batch_input::failed("predicate consumer sentinel"));
                        }
                        cancellation.cancel();
                    }
                    Ok(())
                })
                .err()
                .unwrap()
                .to_string();
            assert!(fired.get(), "{index}/{mode}");
            let expected = match mode {
                "producer" => "producer sentinel",
                "schema" => "schema",
                "consumer" => "consumer sentinel",
                _ => "cancel",
            };
            assert!(error.contains(expected), "{index}/{mode}: {error}");
            released(&input);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            clean(&directory);
            drop(complete(&prepared, query, ROWS, WIDTH, index == 5));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&directory);
        }
    }
}
