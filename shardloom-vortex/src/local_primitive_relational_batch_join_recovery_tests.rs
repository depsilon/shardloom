//! Process death permits owned cleanup/restart, never execution resumption.

use super::*;
use std::{
    io::Read as _,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_JOIN_RECOVERY_WORKSPACE";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn streaming_join_recovery_process_child() {
    let Some(directory) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let scan = other_scan(
        DatasetUri::new(
            directory
                .parent()
                .unwrap()
                .join("input.vortex")
                .display()
                .to_string(),
        )
        .unwrap(),
    );
    let prepared = prepare(&joined(scan, true, JoinKind::Full), GRANT)
        .unwrap()
        .with_spill(spill(&directory, 128 << 20))
        .unwrap();
    let mut input = Input::new();
    let mut producer = |session: &ResidentVortexSession| {
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
            panic!("the parent must kill this process while native join runs are live");
        }
        input.next(session)
    };
    let _ = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("join completed before process death")
        });
    panic!("child must stop while its build owns spill state");
}

#[test]
fn streaming_join_recovery_refuses_live_and_unknown_owners_then_restarts_exactly() {
    let fixture = ordinary_file();
    let directory = fixture.0.join("join-runs");
    fs::create_dir(&directory).unwrap();
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "streaming_join_recovery_process_child",
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
            "join child ended before handshake"
        );
        assert!(
            Instant::now() < deadline,
            "join child did not reach spill state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
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
    let prepared = prepare_join(&fixture, &directory, 128 << 20);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    drop(complete(&prepared));
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
}
