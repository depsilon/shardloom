//! Actual membership-run failure ownership and complete fresh-execution controls.

use super::*;
use crate::local_primitives::native_relational_spill::{
    AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN,
    replacement::{AFTER_COUNT_PASS, BEFORE_CACHE_HIT, BEFORE_GAP_HIT},
};
use crate::relational_query::VortexRelationalSpillPolicy;
use std::{
    cell::{Cell, RefCell},
    path::Path,
    rc::Rc,
};

#[path = "local_primitive_relational_unary_membership_recovery_tests.rs"]
mod recovery_tests;

const GRANT: u64 = 32 << 20;
const WIDTH: usize = 8192;

fn keys(gaps: bool) -> Vec<usize> {
    if gaps {
        (0..257)
            .map(|key| key * 4)
            .chain((0..257).flat_map(|key| [key * 4 + 1, key * 4 + 2]))
            .collect()
    } else {
        (0..257)
            .chain((0..257).rev().filter(|key| key % 2 == 0))
            .chain(257..321)
            .collect()
    }
}

struct Input<'a> {
    keys: &'a [usize],
    position: Rc<Cell<usize>>,
    calls: u64,
    ended: bool,
    prior: Option<Weak<MemoryLease>>,
}

impl<'a> Input<'a> {
    fn new(keys: &'a [usize]) -> Self {
        Self {
            keys,
            position: Rc::new(Cell::new(0)),
            calls: 0,
            ended: false,
            prior: None,
        }
    }

    fn next(
        &mut self,
        ordinal: u64,
        session: &ResidentVortexSession,
    ) -> Result<Option<ResidentMemorySource>> {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
        assert!(!self.ended, "membership producer reopened");
        assert_eq!(ordinal, self.calls);
        self.calls += 1;
        let start = self.position.get();
        if start == self.keys.len() {
            self.ended = true;
            return Ok(None);
        }
        let end = (start + 41).min(self.keys.len());
        let batch = ordinal_source(session, start, &self.keys[start..end], WIDTH)?;
        self.position.set(end);
        self.prior = Some(batch.batch_release_witness()?);
        Ok(Some(batch))
    }

    fn released(&self) {
        assert!(
            self.prior
                .as_ref()
                .is_none_or(|owner| owner.strong_count() == 0)
        );
    }
}

fn prepared(req: &VortexQueryPrimitiveRequest, directory: &Path) -> PreparedVortexRelational {
    with_spill(
        prepare_unary(&unary(scan(), req.clone()), GRANT).unwrap(),
        directory,
    )
}

fn complete(
    prepared: &PreparedVortexRelational,
    req: &VortexQueryPrimitiveRequest,
    keys: &[usize],
) -> ExecutedVortexRelational {
    let expected = expected_positions(keys, req, 0);
    let mut input = Input::new(keys);
    let mut delivered = 0;
    let report = prepared.with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
        .unwrap().for_each_batch(&CancellationToken::default(), |array, context| {
            for actual in values(&array, context)? {
                let (ordinal, duplicate) = expected[delivered];
                let wanted = match req.kind {
                    Kind::DistinctRows => json!({"s":key_text(keys[ordinal], WIDTH)}),
                    Kind::DuplicateMaskRows => json!({"duplicated":duplicate}),
                    _ => json!({"n":ordinal,"s":key_text(keys[ordinal], WIDTH),"items":[ordinal,null]}),
                };
                assert_eq!(actual, wanted);
                delivered += 1;
            }
            Ok(())
        }).unwrap();
    assert!(input.ended);
    input.released();
    assert_eq!(delivered, expected.len());
    assert_eq!(report.spillable_unary_membership_stages, 1);
    assert_eq!(report.spillable_unary_input_rows, keys.len() as u64);
    assert_eq!(report.spillable_unary_output_rows, delivered as u64);
    assert!(report.spill.as_ref().unwrap().runs_written > 0);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    assert!(report.runtime.memory.peak_reserved_bytes <= GRANT);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    assert!(report.input.as_ref().unwrap().end_of_input_observed);
    assert_eq!(report.input.as_ref().unwrap().replay_sources, 0);
    report
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
    assert!(BEFORE_GAP_HIT.with(|slot| slot.borrow().is_none()));
    assert!(AFTER_COUNT_PASS.with(|slot| slot.borrow().is_none()));
    assert!(AFTER_MERGE_BLOCK.with(|slot| slot.borrow().is_none()));
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

#[test]
#[allow(clippy::too_many_lines)] // The injected generation and preserved/restarted ownership form one control.
fn unary_membership_validates_open_cache_gap_and_both_merge_generations() {
    for (name, req) in membership_cases() {
        for phase in ["open", "cached", "gap", "counted", "merging"] {
            for kind in ["corrupt", "truncate", "replace"] {
                let (_fixture, directory) = workspace();
                let keys = keys(phase == "gap");
                let prepared = prepared(&req, &directory);
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let captured = Rc::new(RefCell::new(None::<PathBuf>));
                let observed = Rc::clone(&captured);
                let hook = Box::new(move |path: &Path| {
                    *observed.borrow_mut() = Some(path.to_path_buf());
                    damage(path, kind);
                });
                match phase {
                    "open" => BEFORE_RUN_OPEN.with(|slot| *slot.borrow_mut() = Some(hook)),
                    "cached" => BEFORE_CACHE_HIT.with(|slot| *slot.borrow_mut() = Some(hook)),
                    "gap" => BEFORE_GAP_HIT.with(|slot| *slot.borrow_mut() = Some(hook)),
                    "counted" => AFTER_COUNT_PASS.with(|slot| *slot.borrow_mut() = Some(hook)),
                    "merging" => {
                        // Arm after the replacement count pass so mask identity
                        // ordering cannot consume this generic merge hook first.
                        let observed = Rc::clone(&captured);
                        AFTER_COUNT_PASS.with(|slot| {
                            *slot.borrow_mut() = Some(Box::new(move |path| {
                                *observed.borrow_mut() = Some(path.to_path_buf());
                                let path = path.to_path_buf();
                                AFTER_MERGE_BLOCK.with(|slot| {
                                    *slot.borrow_mut() =
                                        Some(Box::new(move |_| damage(&path, kind)));
                                });
                            }));
                        });
                    }
                    _ => unreachable!(),
                }
                let mut input = Input::new(&keys);
                let error = prepared
                    .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
                    .unwrap()
                    .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
                    .err()
                    .unwrap_or_else(|| panic!("{name}/{phase}/{kind} unexpectedly completed"))
                    .to_string();
                assert!(error.contains("changed"), "{name}/{phase}/{kind}: {error}");
                input.released();
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert_eq!(prepared.snapshot().completed_executions, 0);
                no_hooks();
                let path = captured.borrow().clone().unwrap();
                let preserved = if kind == "replace" {
                    assert!(path.exists(), "foreign generation must be preserved");
                    let bytes = fs::read(&path).unwrap();
                    assert!(
                        VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20)
                            .unwrap()
                            .cleanup_abandoned(path.parent().unwrap())
                            .is_err()
                    );
                    Some(bytes)
                } else {
                    clean(&directory);
                    None
                };
                // Re-execution has a new store and consumes the full source once.
                let report = complete(&prepared, &req, &keys);
                assert_eq!(report.runtime.completed_executions, 1);
                drop(report);
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
fn unary_membership_quota_grant_and_cancelled_merge_release_owned_state() {
    for (name, req) in membership_cases() {
        let (_fixture, directory) = workspace();
        let keys = keys(false);
        let initial = prepared(&req, &directory);
        let report = complete(&initial, &req, &keys);
        let quota = report.spill.as_ref().unwrap().peak_disk_bytes - 1;
        drop((report, initial));
        clean(&directory);
        let limited = prepare_unary(&unary(scan(), req.clone()), GRANT)
            .unwrap()
            .with_spill(VortexRelationalSpillPolicy::new(&directory, quota, 1 << 20).unwrap())
            .unwrap();
        let baseline = limited.snapshot().memory.reserved_bytes;
        let mut input = Input::new(&keys);
        let error = limited
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("quota"), "{name}: {error}");
        input.released();
        assert_eq!(limited.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(limited.snapshot().completed_executions, 0);
        clean(&directory);

        let prepared = prepared(&req, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let held = prepared
            .session
            .memory()
            .reserve(GRANT - baseline - (1 << 20))
            .unwrap();
        let mut input = Input::new(&keys);
        let error = prepared
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&CancellationToken::default(), |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("reservation denied"), "{name}: {error}");
        input.released();
        assert_eq!(
            prepared.snapshot().memory.reserved_bytes,
            baseline + held.bytes()
        );
        drop(held);
        clean(&directory);

        let cancellation = CancellationToken::default();
        let cancel = cancellation.clone();
        let merged = Rc::new(Cell::new(0));
        let observed = Rc::clone(&merged);
        AFTER_COUNT_PASS.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |_| {
                AFTER_MERGE_BLOCK.with(|slot| {
                    *slot.borrow_mut() = Some(Box::new(move |rows| {
                        observed.set(rows);
                        cancel.cancel();
                    }));
                });
            }));
        });
        let mut input = Input::new(&keys);
        let error = prepared
            .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
            .unwrap()
            .for_each_batch(&cancellation, |_, _| Ok(()))
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("cancel"), "{name}: {error}");
        assert!(merged.get() > 0);
        input.released();
        no_hooks();
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(prepared.snapshot().completed_executions, 0);
        clean(&directory);
        drop(complete(&prepared, &req, &keys));
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}

#[test]
fn unary_membership_late_producer_schema_consumer_and_cancellation_never_complete() {
    for (name, req) in membership_cases() {
        for mode in ["producer", "schema", "consumer", "cancel"] {
            let (_fixture, directory) = workspace();
            let keys = keys(false);
            let prepared = prepared(&req, &directory);
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let mut input = Input::new(&keys);
            let position = Rc::clone(&input.position);
            let cancellation = CancellationToken::default();
            let fired = Cell::new(false);
            let error = prepared
                .with_batch_inputs(&mut |_, ordinal, session| {
                    if input.position.get() >= 300 && matches!(mode, "producer" | "schema") {
                        assert_ne!(run_files(&directory), Vec::<PathBuf>::new());
                        fired.set(true);
                        if mode == "producer" {
                            return Err(failed("unary membership producer sentinel"));
                        }
                        return source(session, &[Some(1)], &[Some("changed")]).map(Some);
                    }
                    input.next(ordinal, session)
                })
                .unwrap()
                .for_each_batch(&cancellation, |_, _| {
                    if position.get() >= 300 && matches!(mode, "consumer" | "cancel") {
                        assert_ne!(run_files(&directory), Vec::<PathBuf>::new());
                        assert!(!fired.replace(true));
                        if mode == "consumer" {
                            return Err(failed("unary membership consumer sentinel"));
                        }
                        cancellation.cancel();
                    }
                    Ok(())
                })
                .err()
                .unwrap()
                .to_string();
            assert!(fired.get(), "{name}/{mode}");
            let message = match mode {
                "producer" => "producer sentinel",
                "schema" => "schema",
                "consumer" => "consumer sentinel",
                _ => "cancel",
            };
            assert!(error.contains(message), "{name}/{mode}: {error}");
            input.released();
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(prepared.snapshot().completed_executions, 0);
            clean(&directory);
            drop(complete(&prepared, &req, &keys));
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            clean(&directory);
        }
    }
}
