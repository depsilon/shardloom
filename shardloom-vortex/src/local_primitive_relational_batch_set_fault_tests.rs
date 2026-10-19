//! Source replay faults cannot publish an apparently complete UNION ALL result.

use super::*;
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as Format;
use std::cell::Cell;

pub(super) fn runs(directory: &std::path::Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "vortex"))
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)] // Every injected fault shares the same publication/ownership contract.
fn streaming_union_source_replay_faults_preserve_foreign_files_and_deny_completion() {
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
            let (fixture, directory) = workspace();
            let foreign = directory.join("foreign");
            fs::write(&foreign, b"preserve unrelated file").unwrap();
            let existing = fixture.0.join("existing.vortex");
            fs::write(&existing, b"preserve existing destination").unwrap();
            let target = fixture.0.join("new.vortex");
            let prepared = with_spill(
                prepared_set(&repeated_union(), 1, GRANT),
                &directory,
                if failure == "quota" {
                    2 << 20
                } else {
                    128 << 20
                },
            );
            let baseline = prepared.snapshot().memory.reserved_bytes;
            let token = CancellationToken::default();
            let triggered = Cell::new(false);
            let ended = Cell::new(false);
            let delivered = Cell::new(0);
            let mut inputs = WideInputs::new(true);
            let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
                if ordinal == 6
                    && matches!(failure, "producer" | "schema" | "input-cancel" | "corrupt")
                {
                    let live = runs(&directory);
                    assert!(
                        !live.is_empty(),
                        "fault must follow real source replay spill"
                    );
                    triggered.set(true);
                    match failure {
                        "producer" => {
                            return Err(batch_input::failed("set replay producer sentinel"));
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
                        "corrupt" => fs::OpenOptions::new()
                            .write(true)
                            .open(&live[0])
                            .unwrap()
                            .set_len(1)
                            .unwrap(),
                        _ => {}
                    }
                }
                let batch = inputs.next(uri, ordinal, session)?;
                ended.set(batch.is_none());
                if failure == "input-cancel" && triggered.get() {
                    token.cancel();
                }
                Ok(batch)
            };
            let input = prepared.with_batch_inputs(&mut producer).unwrap();
            let result = if write {
                input
                    .write_controlled(&target, Format::Vortex, false, &token)
                    .map(|result| result.execution)
            } else {
                input.for_each_batch(&token, |array, context| {
                    let replay = delivered.get() >= ROWS;
                    delivered.set(delivered.get() + array.len());
                    if !replay {
                        return Ok(());
                    }
                    assert!(ended.get());
                    assert_ne!(runs(&directory), Vec::<PathBuf>::new());
                    triggered.set(true);
                    match failure {
                        "consumer" => Err(batch_input::failed("set replay consumer sentinel")),
                        "replay-cancel" => {
                            token.cancel();
                            Ok(())
                        }
                        "replay-allocation" => context.memory().reserve(GRANT).map(|_| ()),
                        _ => panic!("input failure reached replay output"),
                    }
                })
            };
            let error = result
                .err()
                .expect("failed set replay cannot certify completion");
            let expected = match failure {
                "producer" => "set replay producer sentinel",
                "schema" => "declared schema",
                "input-cancel" | "replay-cancel" => "cancel",
                "corrupt" => "byte length changed",
                "quota" => "quota",
                "consumer" => "set replay consumer sentinel",
                _ => "reservation denied",
            };
            assert!(
                error.to_string().contains(expected),
                "{failure}/{write}: {error}"
            );
            if failure != "quota" {
                assert!(triggered.get());
            }
            if !write {
                assert!(
                    delivered.get() < ROWS * 2,
                    "provisional rows must never masquerade as complete output"
                );
                if matches!(failure, "consumer" | "replay-cancel" | "replay-allocation") {
                    assert!(delivered.get() > ROWS);
                }
            }
            assert!(
                inputs
                    .prior
                    .as_ref()
                    .is_none_or(|owner| owner.strong_count() == 0)
            );
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
