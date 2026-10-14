//! Real replay runs fail closed and retain no completed report or destination.

use super::*;

#[cfg(unix)]
#[path = "local_primitive_relational_batch_replay_recovery_tests.rs"]
mod recovery_tests;

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
                Vec::new()
            }
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // One fault matrix checks identical ownership/publication invariants.
fn streaming_replay_faults_release_native_runs_and_prevent_completion_or_publication() {
    for write in [false, true] {
        for failure in [
            "producer",
            "schema",
            "input-cancel",
            "corrupt",
            "quota",
            "consumer",
            "replay-cancel",
            "replay-allocation",
        ] {
            if write && matches!(failure, "consumer" | "replay-cancel" | "replay-allocation") {
                continue;
            }
            let fixture = ordinary_file();
            let directory = fixture.0.join("replay-fault-runs");
            fs::create_dir(&directory).unwrap();
            let foreign = directory.join("foreign");
            fs::write(&foreign, b"preserve unrelated file").unwrap();
            let existing = fixture.0.join("existing.vortex");
            fs::write(&existing, b"preserve existing destination").unwrap();
            let target = fixture.0.join("new.vortex");
            let prepared = prepare(&plan(), GRANT)
                .unwrap()
                .with_spill(
                    VortexRelationalSpillPolicy::new(
                        &directory,
                        if failure == "quota" {
                            2 << 20
                        } else {
                            128 << 20
                        },
                        1 << 20,
                    )
                    .unwrap(),
                )
                .unwrap();
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let ended = Cell::new(false);
            let triggered = Cell::new(false);
            let delivered = Cell::new(0);
            let mut state = Producer::new();
            let mut producer = |session: &ResidentVortexSession| {
                if state.calls == 6
                    && matches!(failure, "producer" | "schema" | "input-cancel" | "corrupt")
                {
                    let live = runs(&directory);
                    assert!(
                        !live.is_empty(),
                        "fault must follow real source replay spill"
                    );
                    triggered.set(true);
                    match failure {
                        "producer" => return Err(batch_input::failed("replay producer sentinel")),
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
                        "corrupt" => {
                            fs::OpenOptions::new()
                                .write(true)
                                .open(&live[0])
                                .unwrap()
                                .set_len(1)
                                .unwrap();
                        }
                        _ => {}
                    }
                }
                let batch = state.next(session)?;
                ended.set(batch.is_none());
                if failure == "input-cancel" && triggered.get() {
                    token.cancel();
                }
                Ok(batch)
            };
            let input = prepared.with_batch_input(&mut producer).unwrap();
            let result = if write {
                input
                    .write_controlled(&target, Format::Vortex, false, &token)
                    .map(|result| result.execution)
            } else {
                input.for_each_batch(&token, |array, context| {
                    assert!(ended.get(), "source end must precede replay output");
                    assert_ne!(runs(&directory), Vec::<std::path::PathBuf>::new());
                    delivered.set(delivered.get() + array.len());
                    triggered.set(true);
                    match failure {
                        "consumer" => Err(batch_input::failed("replay consumer sentinel")),
                        "replay-cancel" => {
                            token.cancel();
                            Ok(())
                        }
                        "replay-allocation" => context.memory().reserve(GRANT).map(|_| ()),
                        _ => panic!("input failure reached a consumer"),
                    }
                })
            };
            let error = result
                .err()
                .expect("failed replay must not certify completion");
            let expected = match failure {
                "producer" => "replay producer sentinel",
                "schema" => "declared schema",
                "input-cancel" | "replay-cancel" => "cancel",
                "corrupt" => "byte length changed",
                "quota" => "quota",
                "consumer" => "replay consumer sentinel",
                _ => "reservation denied",
            };
            assert!(
                error.to_string().contains(expected),
                "failure={failure} write={write}: {error}"
            );
            if failure != "quota" {
                assert!(triggered.get());
            }
            if matches!(failure, "consumer" | "replay-cancel" | "replay-allocation") {
                assert!(delivered.get() > 0 && delivered.get() < ROWS);
            } else {
                assert_eq!(delivered.get(), 0);
            }
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
            assert_eq!(
                fs::read(&existing).unwrap(),
                b"preserve existing destination"
            );
            assert!(!target.exists());
            assert_eq!(fs::read(&foreign).unwrap(), b"preserve unrelated file");
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
            assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp")
            }));
        }
    }
}
