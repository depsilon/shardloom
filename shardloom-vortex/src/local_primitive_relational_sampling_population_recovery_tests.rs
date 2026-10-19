//! Recover abandoned sample-population runs, then restart from the source; never resume state.

use super::*;
use std::{
    cell::Cell,
    io::Read as _,
    process::{Child, Command, Stdio},
    rc::Rc,
    time::{Duration, Instant},
};

const ROOT: &str = "SHARDLOOM_SAMPLING_RECOVERY_ROOT";
const CASE: &str = "SHARDLOOM_SAMPLING_RECOVERY_CASE";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sampling_population_recovery_process_child() {
    let Some(root) = std::env::var_os(ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let case_name = std::env::var(CASE).unwrap();
    let case = cases()
        .into_iter()
        .find(|case| case["name"].as_str() == Some(case_name.as_str()))
        .unwrap_or_else(|| panic!("unknown sampling recovery case: {case_name}"));
    let directory = root.join("unary-runs");
    let prepared = prepared(&case, &directory);
    let mut input = WideInput::new(FAILURE_ROWS, FAILURE_WIDTH, true);
    let producer_ended = Rc::new(Cell::new(false));
    let ended_in_hook = Rc::clone(&producer_ended);
    let memory = prepared.session.memory().clone();
    AFTER_MERGE_BLOCK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |rows| {
            assert!(rows > 0);
            let runs = run_files(&directory);
            assert_ne!(
                runs,
                [] as [PathBuf; 0],
                "sampling recovery must retain native runs"
            );
            let owned = fs::canonicalize(runs[0].parent().unwrap()).unwrap();
            let ready = json!({"directory":owned, "phase":"merging_population",
                "variant":case_name, "merged_rows":rows,
                "peak_reserved_bytes":memory.snapshot().peak_reserved_bytes,
                "producer_ended":ended_in_hook.get()});
            let temporary = root.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, root.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill the child during sample population merging");
        }));
    });
    let ended_in_input = Rc::clone(&producer_ended);
    let result = prepared
        .with_batch_inputs(&mut |_, ordinal, session| {
            let batch = input.next(ordinal, session)?;
            if batch.is_none() {
                ended_in_input.set(true);
            }
            Ok(batch)
        })
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()));
    panic!(
        "child must stop with retained sampling population state: {:?}",
        result.err()
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep process death, ownership checks, cleanup, and source restart together.
fn sampling_population_recovery_preserves_live_unknown_and_foreign_owners_then_restarts() {
    for case in cases() {
        let case_name = case["name"].as_str().unwrap();
        let (fixture, directory) = workspace();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "sampling_population_recovery_process_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ROOT, &fixture.0)
                .env(CASE, case_name)
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
                "{case_name}: child ended before handshake"
            );
            assert!(
                Instant::now() < deadline,
                "{case_name}: child did not retain merged sampling runs"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let ready: Value = serde_json::from_slice(&fs::read(ready).unwrap()).unwrap();
        assert_eq!(ready["variant"], case_name);
        assert_eq!(ready["phase"], "merging_population");
        assert!(ready["merged_rows"].as_u64().unwrap() > 0);
        assert!(ready["peak_reserved_bytes"].as_u64().unwrap() <= FAILURE_GRANT);
        assert_eq!(ready["producer_ended"], false);
        let owned = PathBuf::from(ready["directory"].as_str().unwrap());
        let canonical = fs::canonicalize(&directory).unwrap();
        assert_eq!(owned.parent(), Some(canonical.as_path()));
        let members = fs::read_dir(&owned)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<Vec<_>>();
        assert!(members.len() >= 3);
        let policy = VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20).unwrap();
        let error = policy.cleanup_abandoned(&owned).unwrap_err().to_string();
        assert!(
            error.contains("workspace is active"),
            "{case_name}: {error}"
        );
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"preserve");
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());

        let unknown = owned.join("user.txt");
        fs::write(&unknown, b"unowned sampling recovery witness").unwrap();
        assert!(policy.cleanup_abandoned(&owned).is_err());
        assert_eq!(
            fs::read(&unknown).unwrap(),
            b"unowned sampling recovery witness"
        );
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        fs::remove_file(unknown).unwrap();
        policy.cleanup_abandoned(&owned).unwrap();
        assert!(!owned.exists());
        clean(&directory);

        let prepared = prepared(&case, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let report = complete(&prepared, &case);
        assert_eq!(report.runtime.completed_executions, 1);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}
