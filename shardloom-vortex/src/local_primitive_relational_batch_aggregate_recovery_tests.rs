//! Dead-owner cleanup is exercised through a real general aggregation.

use super::*;
use std::{
    io::{Read as _, Write as _},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_AGGREGATE_RECOVERY_WORKSPACE";

struct ChildOwner(Child);

impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn ordered_aggregate_recovery_process_child() {
    let Some(directory) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let prepared = prepare(&plan(), GRANT)
        .unwrap()
        .with_spill(spill(&directory, 128 << 20))
        .unwrap();
    let mut input = Input::new(TOTAL);
    let mut provider = |session: &ResidentVortexSession| {
        if input.calls == 5 {
            assert_eq!(input.prior.as_ref().unwrap().strong_count(), 0);
            let retained = runs(&directory);
            assert_ne!(retained.len(), 0);
            let ready = json!({"directory":retained[0].parent().unwrap(),"input_rows":input.delivered,
                "peak_reserved_bytes":prepared.snapshot().memory.peak_reserved_bytes});
            let temporary = directory.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, directory.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            assert_eq!(signal, [b'x']);
            // Leave the actual aggregate's active native runs without destructors.
            std::process::exit(73);
        }
        input.next(session)
    };
    let _ = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("aggregate completed before child exit")
        });
    panic!("child must exit while aggregation owns spill state");
}

#[test]
fn ordered_aggregate_recovery_refuses_live_owner_cleans_dead_runs_and_restarts_exactly() {
    let fixture = workspace();
    let directory = fixture.0.join("runs");
    fs::create_dir(&directory).unwrap();
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "ordered_aggregate_recovery_process_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(WORKSPACE, &directory)
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
            "aggregate child ended before handshake"
        );
        assert!(
            Instant::now() < deadline,
            "aggregate child did not reach spill state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
    assert_eq!(ready["input_rows"], 5 * 1024);
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
    let policy = spill(&directory, 128 << 20);
    let error = policy.cleanup_abandoned(&owned).unwrap_err().to_string();
    assert!(error.contains("workspace is active"), "{error}");
    for (path, bytes) in members {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    child.0.stdin.as_mut().unwrap().write_all(b"x").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert_eq!(status.code(), Some(73));
            break;
        }
        assert!(Instant::now() < deadline, "aggregate child did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    policy.cleanup_abandoned(&owned).unwrap();
    assert!(!owned.exists());
    fs::remove_file(directory.join("ready.json")).unwrap();
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 0);
    let prepared = prepare(&plan(), GRANT).unwrap().with_spill(policy).unwrap();
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let result = complete(&prepared, TOTAL);
    drop(result);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
}
