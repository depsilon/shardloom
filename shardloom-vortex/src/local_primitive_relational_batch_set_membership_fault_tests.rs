//! Distinct membership faults after real spill preserve ownership and publication.

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
#[path = "local_primitive_relational_batch_set_membership_recovery_tests.rs"]
mod recovery_tests;

fn complete(prepared: &PreparedVortexRelational, kind: SetKind) -> ExecutedVortexRelational {
    let mut inputs = WideInputs::new(true);
    inputs.right_offset = ROWS / 2;
    let mut seen = 0;
    let report = prepared
        .with_batch_inputs(&mut |uri, ordinal, session| inputs.next(uri, ordinal, session))
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |array, context| {
            assert!(array.len() <= 256);
            for row in values(&array, context)? {
                let expected = if kind == SetKind::Intersect {
                    ROWS / 2 + seen
                } else {
                    seen
                };
                check_row(&row, expected);
                seen += 1;
            }
            Ok(())
        })
        .unwrap();
    let expected = match kind {
        SetKind::UnionDistinct => ROWS + ROWS / 2,
        SetKind::Intersect => ROWS - ROWS / 2,
        SetKind::Except => ROWS / 2,
        SetKind::UnionAll => unreachable!(),
    };
    assert_eq!(seen, expected);
    assert_eq!(report.output_rows, expected as u64);
    assert_eq!(inputs.ended, [true; 2]);
    assert_eq!(report.ordered_set_groups, (ROWS + ROWS / 2) as u64);
    assert_eq!(report.ordered_set_input_rows, (2 * ROWS) as u64);
    assert_eq!(report.ordered_set_stages, 1);
    assert_eq!(report.input.as_ref().unwrap().replay_spilled_sources, 0);
    assert!(report.spill.as_ref().unwrap().runs_written > 3);
    assert!(report.spill.as_ref().unwrap().run_block_rows <= 256);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    report
}

#[test]
fn streaming_set_membership_writes_reject_existing_targets_before_input() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        let (fixture, directory) = workspace();
        let target = fixture.0.join("existing.vortex");
        fs::write(&target, b"preserve existing destination").unwrap();
        let prepared = with_spill(
            prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
            &directory,
            128 << 20,
        );
        let baseline = prepared.snapshot().memory.reserved_bytes;
        for overwrite in [false, true] {
            let mut unopened = |_: &DatasetUri, _: u64, _: &ResidentVortexSession| {
                panic!("existing destination must reject before input demand")
            };
            let error = prepared
                .with_batch_inputs(&mut unopened)
                .unwrap()
                .write_controlled(
                    &target,
                    Format::Vortex,
                    overwrite,
                    &CancellationToken::default(),
                )
                .err()
                .unwrap()
                .to_string();
            assert!(
                error.contains("atomic generation-conditional replacement is unavailable"),
                "{kind:?}/{overwrite}: {error}"
            );
            assert_eq!(fs::read(&target).unwrap(), b"preserve existing destination");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
        }
    }
}

#[test]
#[allow(clippy::too_many_lines)] // One matrix proves the same ownership/publication contract at every fault point.
fn streaming_set_membership_faults_cannot_complete_or_publish_partial_output() {
    for kind in [SetKind::UnionDistinct, SetKind::Intersect, SetKind::Except] {
        for write in [false, true] {
            for (failure, side) in [
                ("producer", 0),
                ("producer", 1),
                ("schema", 0),
                ("schema", 1),
                ("input-cancel", 0),
                ("input-cancel", 1),
                ("corrupt-open", 0),
                ("truncate-open", 0),
                ("replace-open", 0),
                ("merge-cancel", 0),
                ("quota", 0),
                ("consumer", 0),
                ("consumer-cancel", 0),
                ("consumer-allocation", 0),
            ] {
                if write && failure.starts_with("consumer") {
                    continue;
                }
                let (fixture, directory) = workspace();
                let foreign = directory.join("foreign");
                fs::write(&foreign, b"preserve unrelated file").unwrap();
                let target = fixture.0.join("failed.vortex");
                let existing = fixture.0.join("existing.vortex");
                fs::write(&existing, b"preserve existing destination").unwrap();
                let prepared = with_spill(
                    prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
                    &directory,
                    if failure == "quota" {
                        2 << 20
                    } else {
                        128 << 20
                    },
                );
                let baseline = prepared.snapshot().memory.reserved_bytes;
                let token = CancellationToken::default();
                let triggered = Rc::new(Cell::new(false));
                let replacement = Rc::new(RefCell::new(None));
                if failure.ends_with("open") {
                    let triggered = Rc::clone(&triggered);
                    let replacement = Rc::clone(&replacement);
                    let backup = fixture.0.join("original-run.vortex");
                    BEFORE_RUN_OPEN.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move |path| {
                            assert!(fs::metadata(path).unwrap().len() > 1);
                            match failure {
                                "corrupt-open" => {
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
                                "truncate-open" => fs::OpenOptions::new()
                                    .write(true)
                                    .open(path)
                                    .unwrap()
                                    .set_len(1)
                                    .unwrap(),
                                "replace-open" => {
                                    let bytes = fs::read(path).unwrap();
                                    fs::rename(path, &backup).unwrap();
                                    fs::write(path, &bytes).unwrap();
                                    *replacement.borrow_mut() =
                                        Some((path.to_path_buf(), backup, bytes));
                                }
                                _ => unreachable!(),
                            }
                            triggered.set(true);
                        }));
                    });
                }
                if failure == "merge-cancel" {
                    let token = token.clone();
                    let triggered = Rc::clone(&triggered);
                    AFTER_MERGE_BLOCK.with(|hook| {
                        *hook.borrow_mut() = Some(Box::new(move |rows| {
                            assert!(rows > 0);
                            triggered.set(true);
                            token.cancel();
                        }));
                    });
                }
                let mut inputs = WideInputs::new(true);
                inputs.right_offset = ROWS / 2;
                let mut producer =
                    |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                        if uri.as_str() == URIS[side]
                            && ordinal == 6
                            && matches!(failure, "producer" | "schema" | "input-cancel")
                        {
                            assert_ne!(super::fault_tests::runs(&directory), Vec::<PathBuf>::new());
                            triggered.set(true);
                            match failure {
                                "producer" => {
                                    return Err(batch_input::failed(
                                        "set membership producer sentinel",
                                    ));
                                }
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
                        inputs.next(uri, ordinal, session)
                    };
                let input = prepared.with_batch_inputs(&mut producer).unwrap();
                let mut emitted = 0;
                let result = if write {
                    input
                        .write_controlled(&target, Format::Vortex, false, &token)
                        .map(|result| result.execution)
                } else {
                    input.for_each_batch(&token, |array, context| {
                        assert!(!array.is_empty());
                        emitted += array.len();
                        assert_ne!(super::fault_tests::runs(&directory), Vec::<PathBuf>::new());
                        triggered.set(true);
                        match failure {
                            "consumer" => {
                                Err(batch_input::failed("set membership consumer sentinel"))
                            }
                            "consumer-cancel" => {
                                token.cancel();
                                Ok(())
                            }
                            "consumer-allocation" => context.memory().reserve(GRANT).map(|_| ()),
                            _ => panic!("input failure reached completed set output"),
                        }
                    })
                };
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{kind:?}/{failure}/{side}/{write} completed"))
                    .to_string();
                let expected = match failure {
                    "producer" => "set membership producer sentinel",
                    "schema" => "declared schema",
                    "corrupt-open" | "truncate-open" | "replace-open" => "changed",
                    "quota" => "quota",
                    "consumer" => "set membership consumer sentinel",
                    "consumer-allocation" => "reservation denied",
                    _ => "cancel",
                };
                assert!(
                    error.contains(expected),
                    "{kind:?}/{failure}/{side}/{write}: {error}"
                );
                if failure != "quota" {
                    assert!(triggered.get());
                }
                assert_eq!(emitted > 0, failure.starts_with("consumer"));
                if failure.starts_with("consumer") {
                    assert_eq!(inputs.ended, [true; 2]);
                }
                assert!(BEFORE_RUN_OPEN.with(|hook| hook.borrow().is_none()));
                assert!(AFTER_MERGE_BLOCK.with(|hook| hook.borrow().is_none()));
                assert_eq!(prepared.snapshot().completed_executions, 0);
                assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                assert!(
                    inputs
                        .prior
                        .as_ref()
                        .is_none_or(|owner| owner.strong_count() == 0)
                );
                if let Some((path, backup, bytes)) = replacement.borrow_mut().take() {
                    assert_eq!(fs::read(&path).unwrap(), bytes);
                    let policy =
                        VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20).unwrap();
                    assert!(policy.cleanup_abandoned(path.parent().unwrap()).is_err());
                    assert_eq!(fs::read(&path).unwrap(), bytes);
                    fs::remove_file(&path).unwrap();
                    fs::rename(backup, &path).unwrap();
                    policy.cleanup_abandoned(path.parent().unwrap()).unwrap();
                }
                assert!(!target.exists());
                assert_eq!(
                    fs::read(&existing).unwrap(),
                    b"preserve existing destination"
                );
                assert_eq!(fs::read(&foreign).unwrap(), b"preserve unrelated file");
                assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
                assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .contains(".tmp")
                }));
                // A new execution on the same prepared owner must not retain any failed state.
                if !write && failure == "consumer" {
                    let report = complete(&prepared, kind);
                    assert_eq!(report.runtime.completed_executions, 1);
                    drop(report);
                    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
                    assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
                }
            }
        }
    }
}
