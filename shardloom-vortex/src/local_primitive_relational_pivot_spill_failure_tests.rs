use super::*;
use crate::local_primitives::native_relational_spill::{
    AFTER_MERGE_BLOCK, BEFORE_RUN_OPEN,
    pivot::{AFTER_COUNT_PASS, BEFORE_CACHE_HIT, BEFORE_GAP_HIT},
};
use std::{cell::RefCell, path::Path, rc::Rc};

fn assert_clean(fixture: &Fixture, prepared: &PreparedVortexRelational, baseline: u64) {
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(prepared.snapshot().completed_executions, 0);
    assert_eq!(
        fs::read_dir(fixture.0.join("pivot-runs")).unwrap().count(),
        0
    );
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
        _ => panic!("unknown damage"),
    }
}

#[test]
fn native_pivot_spill_validates_open_cache_and_both_merge_pass_generations() {
    for phase in ["open", "cached", "gap", "counted", "merging"] {
        for kind in ["corrupt", "truncate", "replace"] {
            let groups = if phase == "gap" { 1025 } else { 257 };
            let values = [Some(3.0), Some(7.0), Some(0.0)];
            let passes = if phase == "gap" { 3 } else { 2 };
            let fixture = fixture(&values[..passes], groups, 37);
            let mut policy = policy();
            if phase == "gap" {
                // This generation test needs multiple sparse runs. Its sliced
                // fixture retains the complete source backing per source block;
                // pressure acceptance uses independent blocks and a 16-MiB grant.
                policy.resource_envelope.memory_budget_bytes = 256 << 20;
            }
            let prepared = prepare_with_policy(&fixture, plan(&fixture, "sum"), true, policy);
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
                    let observed = Rc::clone(&captured);
                    BEFORE_RUN_OPEN.with(|slot| {
                        *slot.borrow_mut() = Some(Box::new(move |path| {
                            *observed.borrow_mut() = Some(path.to_path_buf());
                        }));
                    });
                    let observed = Rc::clone(&captured);
                    AFTER_MERGE_BLOCK.with(|slot| {
                        *slot.borrow_mut() = Some(Box::new(move |_| {
                            damage(observed.borrow().as_ref().unwrap(), kind);
                        }));
                    });
                }
                _ => unreachable!(),
            }
            let error = prepared
                .for_each_batch(&CancellationToken::default(), |_, _| {
                    panic!("damaged state reached output")
                })
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("changed"), "{phase}/{kind}: {error}");
            assert_eq!(
                prepared.snapshot().memory.reserved_bytes,
                baseline,
                "{phase}/{kind}: {error}"
            );
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert!(BEFORE_RUN_OPEN.with(|slot| slot.borrow().is_none()));
            assert!(BEFORE_CACHE_HIT.with(|slot| slot.borrow().is_none()));
            assert!(BEFORE_GAP_HIT.with(|slot| slot.borrow().is_none()));
            assert!(AFTER_COUNT_PASS.with(|slot| slot.borrow().is_none()));
            assert!(AFTER_MERGE_BLOCK.with(|slot| slot.borrow().is_none()));
            let path = captured.borrow().clone().unwrap();
            if kind == "replace" {
                assert!(path.exists(), "foreign replacement must be preserved");
                assert!(
                    VortexRelationalSpillPolicy::new(
                        fixture.0.join("pivot-runs"),
                        128 << 20,
                        1 << 20
                    )
                    .unwrap()
                    .cleanup_abandoned(path.parent().unwrap())
                    .is_err()
                );
                assert!(path.exists());
            } else {
                assert_clean(&fixture, &prepared, baseline);
                // A new call starts from source and completes; no failed state is reused.
                let (rows, report) = complete(&prepared);
                assert_eq!(rows.len(), groups);
                for row in rows {
                    assert_eq!(row["pivot_a"], json!(10.0));
                }
                assert_eq!(report.runtime.completed_executions, 1);
            }
        }
    }
}

#[test]
fn native_pivot_spill_quota_grant_and_partial_merge_denials_release_owned_state() {
    let fixture = fixture(&[Some(3.0), Some(7.0)], 257, 41);
    let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
    let (_, report) = complete(&prepared);
    let quota = report.spill.as_ref().unwrap().peak_disk_bytes - 1;
    drop((report, prepared));
    let prepared = prepare(&fixture, plan(&fixture, "sum"), false)
        .with_spill(
            VortexRelationalSpillPolicy::new(fixture.0.join("pivot-runs"), quota, 1 << 20).unwrap(),
        )
        .unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let error = prepared
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("quota failure reached output")
        })
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("quota"), "{error}");
    assert_clean(&fixture, &prepared, baseline);
    drop(prepared);

    let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let held = prepared
        .session
        .memory()
        .reserve((32 << 20) - baseline - (1 << 20))
        .unwrap();
    let error = prepared.execute_owned().err().unwrap().to_string();
    assert!(error.contains("reservation denied"), "{error}");
    assert_eq!(
        prepared.snapshot().memory.reserved_bytes,
        baseline + held.bytes()
    );
    drop(held);
    assert_clean(&fixture, &prepared, baseline);

    let cancellation = CancellationToken::default();
    let cancel = cancellation.clone();
    let merged = Rc::new(RefCell::new(0));
    let observed = Rc::clone(&merged);
    AFTER_MERGE_BLOCK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |rows| {
            *observed.borrow_mut() = rows;
            cancel.cancel();
        }));
    });
    let error = prepared
        .for_each_batch(&cancellation, |_, _| panic!("partial merge reached output"))
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("cancel"), "{error}");
    assert!(*merged.borrow() > 0);
    assert_clean(&fixture, &prepared, baseline);
    assert_eq!(complete(&prepared).0.len(), 257);
}

#[test]
fn native_pivot_spill_late_consumers_cancellation_and_source_changes_never_complete() {
    for mode in ["consumer", "cancel", "source"] {
        let fixture = fixture(&[Some(3.0), Some(7.0)], 257, 47);
        let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let cancellation = CancellationToken::default();
        let mut calls = 0;
        let error = prepared
            .for_each_batch(&cancellation, |_, _| {
                calls += 1;
                match mode {
                    "consumer" => Err(failed("pivot consumer rejected batch")),
                    "cancel" => {
                        cancellation.cancel();
                        Ok(())
                    }
                    "source" => {
                        fixture.replace();
                        Ok(())
                    }
                    _ => unreachable!(),
                }
            })
            .err()
            .unwrap()
            .to_string();
        assert_eq!(calls, 1);
        assert!(
            error.contains(match mode {
                "consumer" => "consumer rejected",
                "cancel" => "cancel",
                _ => "changed",
            }),
            "{error}"
        );
        assert_clean(&fixture, &prepared, baseline);
        if mode != "source" {
            assert_eq!(complete(&prepared).0.len(), 257);
        }
    }
}

#[test]
fn native_pivot_spill_empty_input_is_inert_and_zero_limit_keeps_existing_rejection() {
    let fixture = fixture(&[], 0, 1);
    let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
    let (rows, report) = complete(&prepared);
    assert_eq!(rows, [] as [serde_json::Value; 0]);
    assert_eq!(report.spilled_pivot_input_rows, 0);
    assert_eq!(report.spilled_pivot_cells, 0);
    assert_eq!(report.spilled_pivot_reader_opens, 0);
    assert_eq!(report.spill.as_ref().unwrap().runs_written, 0);
    for spill in [false, true] {
        let mut plan = plan(&fixture, "sum");
        let VortexRelationalPlan::Unary(unary) = &mut plan else {
            unreachable!()
        };
        unary.request.source_order_limit = Some(0);
        let prepared = prepare(&fixture, plan, spill);
        let error = prepared.execute_owned().err().unwrap().to_string();
        assert!(error.contains("positive optional limit"), "{error}");
    }
}
