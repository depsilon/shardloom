//! Abandoned predicate populations permit owned cleanup and complete restart.

use super::*;
use std::{
    io::Read as _,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_SUBQUERY_MEMBERSHIP_RECOVERY_WORKSPACE";
const CASE: &str = "SHARDLOOM_SUBQUERY_MEMBERSHIP_RECOVERY_CASE";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn subquery_membership_recovery_process_child() {
    let Some(directory) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let index = std::env::var(CASE).unwrap().parse::<usize>().unwrap();
    let query = cases().remove(index);
    let prepared = prepared(&query, &directory);
    let mut input = WideInput::new(ROWS, WIDTH, index == 5);
    let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        if uri.as_str() == URIS[1] && ordinal == 6 {
            released(&input);
            let runs = run_files(&directory);
            assert_ne!(runs, [] as [std::path::PathBuf; 0]);
            let ready = json!({"directory":runs[0].parent().unwrap(), "calls":input.calls,
                "peak_reserved_bytes":prepared.snapshot().memory.peak_reserved_bytes});
            let temporary = directory.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, directory.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill the live predicate owner");
        }
        input.next(uri, ordinal, session)
    };
    let _ = prepared
        .with_batch_inputs(&mut producer)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("predicate completed before process death")
        });
    panic!("child must stop with incomplete predicate state");
}

#[test]
fn subquery_membership_recovery_preserves_live_and_unknown_owners_then_restarts_exactly() {
    for (index, query) in cases().iter().enumerate() {
        let (_fixture, directory) = workspace();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "subquery_membership_recovery_process_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(WORKSPACE, &directory)
                .env(CASE, index.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let ready = directory.join("ready.json");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "predicate child ended before handshake"
            );
            assert!(
                Instant::now() < deadline,
                "predicate child never reached real spill"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let ready: Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
        assert_eq!(ready["calls"], json!([0, 6]));
        assert!(ready["peak_reserved_bytes"].as_u64().unwrap() <= GRANT);
        let owned = PathBuf::from(ready["directory"].as_str().unwrap());
        assert_eq!(owned.parent(), Some(directory.as_path()));
        let members = fs::read_dir(&owned)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect::<Vec<_>>();
        assert!(members.len() >= 2);
        let policy = VortexRelationalSpillPolicy::new(&directory, 128 << 20, 1 << 20).unwrap();
        let error = policy.cleanup_abandoned(&owned).unwrap_err().to_string();
        assert!(error.contains("workspace is active"), "{error}");
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        let unknown = owned.join("user.txt");
        fs::write(&unknown, b"unowned recovery witness").unwrap();
        assert!(policy.cleanup_abandoned(&owned).is_err());
        assert_eq!(fs::read(&unknown).unwrap(), b"unowned recovery witness");
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        fs::remove_file(unknown).unwrap();
        policy.cleanup_abandoned(&owned).unwrap();
        assert!(!owned.exists());
        fs::remove_file(directory.join("ready.json")).unwrap();
        let prepared = prepared(query, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let report = complete(&prepared, query, ROWS, WIDTH, index == 5);
        assert!(report.spill.as_ref().unwrap().runs_written > 2);
        assert_eq!(report.runtime.completed_executions, 1);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}
