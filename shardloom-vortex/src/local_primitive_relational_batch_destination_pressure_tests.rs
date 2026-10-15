use super::*;
use crate::relational_query::VortexRelationalSpillPolicy;

const GRANT: u64 = 16 << 20;
const ROWS: usize = 8193;
const BATCH: usize = 251;

fn text(row: usize) -> String {
    format!("{row:08}{}", "p".repeat(2040))
}

fn next(
    session: &ResidentVortexSession,
    ordinal: &mut usize,
) -> Result<Option<ResidentMemorySource>> {
    let start = *ordinal * BATCH;
    *ordinal += 1;
    if start >= ROWS {
        assert_eq!(
            start / BATCH,
            ROWS.div_ceil(BATCH),
            "producer cannot be replayed"
        );
        return Ok(None);
    }
    let end = (start + BATCH).min(ROWS);
    let numbers = (start..end)
        .map(|row| Some(i64::try_from(row).unwrap()))
        .collect::<Vec<_>>();
    let strings = (start..end).map(text).collect::<Vec<_>>();
    let refs = strings
        .iter()
        .map(|value| Some(value.as_str()))
        .collect::<Vec<_>>();
    source(session, &numbers, &refs).map(Some)
}

fn runs(directory: &std::path::Path) -> Vec<PathBuf> {
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
fn completion_input_fanout_pressure_denies_resident_and_replays_native_runs_under_same_grant() {
    for spill in [false, true] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let workspace = fixture.0.join("runs");
        fs::create_dir(&workspace).unwrap();
        let prepared = prepare(&scan(), GRANT).unwrap();
        let prepared = if spill {
            prepared
                .with_spill(
                    VortexRelationalSpillPolicy::new(&workspace, 128 << 20, 1 << 20).unwrap(),
                )
                .unwrap()
        } else {
            prepared
        };
        let initial = prepared.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("output.vortex"), Format::Vortex),
            (fixture.0.join("output.parquet"), Format::Parquet),
        ];
        let mut ordinal = 0;
        let mut prior: Option<Weak<MemoryLease>> = None;
        let mut producer = |session: &ResidentVortexSession| {
            assert!(prior.as_ref().is_none_or(|owner| owner.strong_count() == 0));
            assert!(targets.iter().all(|(path, _)| !path.exists()));
            let source = next(session, &mut ordinal)?;
            prior = source
                .as_ref()
                .map(ResidentMemorySource::batch_release_witness)
                .transpose()?;
            Ok(source)
        };
        let result = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_many_controlled(&targets, false, &CancellationToken::default());
        if spill {
            let result = result.unwrap();
            assert_eq!(ordinal, ROWS.div_ceil(BATCH) + 1);
            assert_eq!(result.execution.output_rows, ROWS as u64);
            assert_eq!(result.execution.runtime.completed_executions, 1);
            assert!(result.execution.runtime.memory.peak_reserved_bytes <= GRANT);
            let replay = result.replay.as_ref().unwrap();
            assert!(replay.spilled);
            assert_eq!(replay.rows_detached, ROWS as u64);
            assert!(replay.buffer_bytes_copied > GRANT);
            assert_eq!(replay.replays, 2);
            let input = result.execution.input.as_ref().unwrap();
            assert_eq!(
                input.replay_sources, 0,
                "result retention is not source replay"
            );
            assert!(input.end_of_input_observed);
            let spill = result.execution.spill.as_ref().unwrap();
            assert!(spill.runs_written > 3 && spill.merge_passes > 1);
            assert!(spill.owned_cleanup_completed && spill.peak_disk_bytes <= spill.quota_bytes);
            assert!(result.execution.native_io_certificate.is_certified());
            for (path, format) in &targets {
                let rows = read_rows(path, *format, &prepared.output_dtype().unwrap());
                assert_eq!(rows.len(), ROWS);
                for (index, row) in rows.into_iter().enumerate() {
                    assert_eq!(row, serde_json::json!({"n":index,"s":text(index)}));
                }
            }
            drop(result);
        } else {
            let error = result.err().expect("complete result cannot fit this grant");
            assert!(error.to_string().contains("reservation denied"), "{error}");
            assert_eq!(prepared.snapshot().completed_executions, 0);
            assert!(targets.iter().all(|(path, _)| !path.exists()));
        }
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
    }
}

#[test]
fn completion_input_fanout_spill_faults_do_not_publish_and_preserve_unknown_files() {
    for fault in ["quota", "corrupt", "cancel", "cleanup"] {
        let fixture = Fixture::new(keyed(&[Some(0)], &[0]), 1);
        let workspace = fixture.0.join("runs");
        fs::create_dir(&workspace).unwrap();
        let prepared = prepare(&scan(), GRANT)
            .unwrap()
            .with_spill(
                VortexRelationalSpillPolicy::new(
                    &workspace,
                    if fault == "quota" { 2 << 20 } else { 128 << 20 },
                    1 << 20,
                )
                .unwrap(),
            )
            .unwrap();
        let initial = prepared.snapshot().memory.reserved_bytes;
        let targets = [
            (fixture.0.join("one.vortex"), Format::Vortex),
            (fixture.0.join("two.parquet"), Format::Parquet),
        ];
        let cancellation = CancellationToken::default();
        let mut ordinal = 0;
        let mut unknown = None;
        let mut triggered = false;
        let mut producer = |session: &ResidentVortexSession| {
            if ordinal == 6 && fault != "quota" {
                let live = runs(&workspace);
                assert!(!live.is_empty(), "fault must follow native result spill");
                triggered = true;
                match fault {
                    "corrupt" => fs::OpenOptions::new()
                        .write(true)
                        .open(&live[0])
                        .unwrap()
                        .set_len(1)
                        .unwrap(),
                    "cancel" => cancellation.cancel(),
                    "cleanup" => {
                        let path = live[0].parent().unwrap().join("foreign");
                        fs::write(&path, b"preserve foreign bytes").unwrap();
                        unknown = Some(path);
                    }
                    _ => unreachable!(),
                }
            }
            next(session, &mut ordinal)
        };
        let error = prepared
            .with_batch_input(&mut producer)
            .unwrap()
            .write_many_controlled(&targets, false, &cancellation)
            .err()
            .expect("fault must prevent success");
        let expected = match fault {
            "quota" => "quota",
            "corrupt" => "byte length changed",
            "cancel" => "cancel",
            _ => "native sort spill I/O",
        };
        assert!(error.to_string().contains(expected), "{fault}: {error}");
        assert!(fault == "quota" || triggered);
        assert!(targets.iter().all(|(path, _)| !path.exists()));
        assert_eq!(prepared.snapshot().completed_executions, 0);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, initial);
        if let Some(path) = unknown {
            assert_eq!(fs::read(path).unwrap(), b"preserve foreign bytes");
        } else {
            assert_eq!(fs::read_dir(&workspace).unwrap().count(), 0);
        }
        assert!(!fs::read_dir(&fixture.0).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".shardloom-fanout-")
        }));
    }
}
