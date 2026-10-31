//! Dead-owner cleanup restarts the full pivot; it never resumes partial state.

use super::*;
use crate::local_primitives::native_relational_spill::replacement::AFTER_COUNT_PASS;
use std::{
    cell::Cell,
    io::Read as _,
    mem::ManuallyDrop,
    process::{Child, Command, Stdio},
    rc::Rc,
    time::{Duration, Instant},
};

const ROOT: &str = "SHARDLOOM_PIVOT_RECOVERY_ROOT";
const STREAMED: &str = "SHARDLOOM_PIVOT_RECOVERY_STREAMED";
const GROUPS: usize = 257;
const ROWS: usize = GROUPS * 2;

fn stream_batch(
    session: &ResidentVortexSession,
    start: usize,
    end: usize,
) -> Result<ResidentMemorySource> {
    use crate::resident_memory_source::{MemoryColumn, MemoryColumnValues};
    let keys = (start..end)
        .map(|row| key(row % GROUPS))
        .collect::<Vec<_>>();
    ResidentMemorySource::from_batch_columns(
        session,
        &[
            MemoryColumn {
                name: "entity",
                values: MemoryColumnValues::Utf8(
                    &keys
                        .iter()
                        .map(|value| Some(value.as_str()))
                        .collect::<Vec<_>>(),
                ),
            },
            MemoryColumn {
                name: "category",
                values: MemoryColumnValues::Utf8(&vec![Some("a"); end - start]),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Float64(
                    &(start..end)
                        .map(|row| Some(if row < GROUPS { 3.0 } else { 7.0 }))
                        .collect::<Vec<_>>(),
                ),
            },
        ],
    )
}

fn prepare_recovery(fixture: &Fixture, streamed: bool) -> PreparedVortexRelational {
    if !streamed {
        return prepare(fixture, plan(fixture, "sum"), true);
    }
    let uri = DatasetUri::new("memory://pivot-recovery").unwrap();
    let mut plan = plan(fixture, "sum");
    if let VortexRelationalPlan::Unary(unary) = &mut plan
        && let VortexRelationalPlan::Scan(scan) = &mut unary.input
    {
        scan.source_uri = uri.clone();
    }
    let prepared = prepare_relational_with_dynamic_inputs(
        std::slice::from_ref(&uri),
        policy(),
        65_536,
        |schemas| schemas.register_batch_source(uri.clone(), |session| stream_batch(session, 0, 0)),
        move |schemas| schemas.resolve_output(&plan).map(|(plan, _)| plan),
    )
    .unwrap();
    fs::create_dir_all(fixture.0.join("pivot-runs")).unwrap();
    prepared
        .with_spill(
            VortexRelationalSpillPolicy::new(fixture.0.join("pivot-runs"), 128 << 20, 1 << 20)
                .unwrap(),
        )
        .unwrap()
}

fn execute_recovery(
    prepared: &PreparedVortexRelational,
    streamed: bool,
    ended: &Cell<bool>,
    delivered: &Cell<usize>,
    consume: impl FnMut(crate::local_primitives::collect::SerializedVortexResultBatch) -> Result<()>,
) -> ExecutedVortexRelational {
    if !streamed {
        return prepared
            .for_each_json_batch(&CancellationToken::default(), 17, 1 << 20, consume)
            .unwrap();
    }
    let mut start = 0;
    let mut producer = |session: &ResidentVortexSession| {
        assert!(
            !ended.get(),
            "a restarted execution still consumes its producer once"
        );
        if start == ROWS {
            ended.set(true);
            return Ok(None);
        }
        let end = (start + 41).min(ROWS);
        let batch = stream_batch(session, start, end)?;
        start = end;
        delivered.set(end);
        Ok(Some(batch))
    };
    let report = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .for_each_json_batch(&CancellationToken::default(), 17, 1 << 20, consume)
        .unwrap();
    assert!(ended.get());
    assert_eq!(delivered.get(), ROWS);
    let input = report.input.as_ref().unwrap();
    assert_eq!(input.rows, ROWS as u64);
    assert_eq!(input.payload_batches, ROWS.div_ceil(41) as u64);
    assert_eq!(input.sources, 1);
    assert!(input.end_of_input_observed && input.output_ownership_detached);
    report
}

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        // Child::wait closes its stdin; retain the pipe until termination.
        let _stdin = self.0.stdin.take();
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn native_pivot_spill_recovery_process_child() {
    let Some(directory) = std::env::var_os(ROOT) else {
        return;
    };
    // The parent owns this input and directory even if the child fails early.
    let fixture = ManuallyDrop::new(Fixture(PathBuf::from(directory)));
    let streamed = std::env::var_os(STREAMED).is_some();
    let prepared = prepare_recovery(&fixture, streamed);
    let ended = Rc::new(Cell::new(false));
    let input_ended = ended.clone();
    let delivered = Rc::new(Cell::new(0));
    let input_rows = delivered.clone();
    let memory = prepared.session.memory().clone();
    let root = fixture.0.clone();
    AFTER_COUNT_PASS.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |path| {
            let ready = json!({"directory":path.parent().unwrap(),"phase":"counted_before_merge",
                              "streamed":streamed,"input_ended":input_ended.get(),
                              "input_rows":input_rows.get(),
                              "peak_reserved_bytes":memory.snapshot().peak_reserved_bytes});
            let temporary = root.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, root.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill the pivot while its counted runs are live");
        }));
    });
    let _ = execute_recovery(&prepared, streamed, &ended, &delivered, |_| {
        panic!("pivot completed before process death")
    });
    panic!("child must stop with retained native pivot state");
}

#[test]
fn native_pivot_spill_recovery_preserves_live_and_unknown_owners_then_restarts() {
    recovery(false);
}

#[test]
fn streamed_pivot_spill_recovery_preserves_live_and_unknown_owners_then_restarts() {
    recovery(true);
}

#[allow(clippy::too_many_lines)] // Keep the process lifecycle and ownership assertions together.
fn recovery(streamed: bool) {
    let fixture = fixture(&[Some(3.0), Some(7.0)], GROUPS, 41);
    let workspace = fixture.0.join("pivot-runs");
    let mut command = Command::new(std::env::current_exe().unwrap());
    if streamed {
        command.env(STREAMED, "1");
    } else {
        command.env_remove(STREAMED);
    }
    let mut child = ChildOwner(
        command
            .args([
                "native_pivot_spill_recovery_process_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROOT, &fixture.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let ready = fixture.0.join("ready.json");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "pivot child ended before handshake"
        );
        assert!(
            Instant::now() < deadline,
            "pivot child did not retain counted runs"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: Value = serde_json::from_slice(&fs::read(ready).unwrap()).unwrap();
    assert_eq!(ready["phase"], "counted_before_merge");
    assert_eq!(ready["streamed"], streamed);
    if streamed {
        assert!(ready["input_rows"].as_u64().unwrap() > 0);
        assert!(ready["input_rows"].as_u64().unwrap() <= ROWS as u64);
        if ready["input_ended"] == true {
            assert_eq!(ready["input_rows"], ROWS);
        }
    }
    assert!(ready["peak_reserved_bytes"].as_u64().unwrap() <= 32 << 20);
    let owned = PathBuf::from(ready["directory"].as_str().unwrap());
    let canonical_workspace = fs::canonicalize(&workspace).unwrap();
    assert_eq!(owned.parent(), Some(canonical_workspace.as_path()));
    let members = fs::read_dir(&owned)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    assert!(members.len() >= 3);
    let policy = VortexRelationalSpillPolicy::new(&workspace, 128 << 20, 1 << 20).unwrap();
    let error = policy.cleanup_abandoned(&owned).unwrap_err().to_string();
    assert!(error.contains("workspace is active"), "{error}");
    for (path, bytes) in &members {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    // Prevent EOF from unwinding the child and deleting the abandoned runs.
    let _stdin = child.0.stdin.take();
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let unknown = owned.join("user.txt");
    fs::write(&unknown, b"unowned pivot recovery witness").unwrap();
    assert!(policy.cleanup_abandoned(&owned).is_err());
    assert_eq!(
        fs::read(&unknown).unwrap(),
        b"unowned pivot recovery witness"
    );
    for (path, bytes) in &members {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    fs::remove_file(unknown).unwrap();
    policy.cleanup_abandoned(&owned).unwrap();
    assert!(!owned.exists());

    let prepared = prepare_recovery(&fixture, streamed);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let mut actual = Vec::new();
    let ended = Cell::new(false);
    let delivered = Cell::new(0);
    let report = execute_recovery(&prepared, streamed, &ended, &delivered, |batch| {
        assert!(!streamed || ended.get());
        let rows = serde_json::from_str::<Vec<Value>>(batch.values_json.value()).unwrap();
        assert!(rows.len() <= 17);
        actual.extend(rows);
        Ok(())
    });
    assert_eq!(
        actual,
        (0..GROUPS)
            .map(|index| json!({"entity":key(index),"pivot_a":10.0}))
            .collect::<Vec<_>>()
    );
    assert_eq!(report.runtime.completed_executions, 1);
    assert!(report.native_io_certificate.is_certified());
    assert!(!report.native_io_certificate.side_effects.fallback_attempted);
    assert_eq!(report.spilled_pivot_stages, 1);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(workspace).unwrap().count(), 0);
}
