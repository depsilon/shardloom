//! A killed summary builder leaves owned runs for explicit cleanup and restart.

use super::*;
use std::{
    io::Read as _,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_WINDOW_RECOVERY_WORKSPACE";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn streaming_window_recovery_process_child() {
    let Some(directory) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let prepared = prepare_window(&directory, GRANT, Some(128 << 20));
    let memory = prepared.session.memory().clone();
    let marker_directory = directory.clone();
    WINDOW_PROGRESS.with(|hook| *hook.borrow_mut() = Some(Box::new(move |phase, rows| {
        if phase != Progress::ExtremaSummaries || rows < 1024 { return false; }
        let retained = runs(&marker_directory);
        assert_ne!(retained, [] as [std::path::PathBuf; 0]);
        let ready = json!({"directory":retained[0].parent().unwrap(),"input_rows":TOTAL,"summary_rows":rows,"peak_reserved_bytes":memory.snapshot().peak_reserved_bytes});
        let temporary = marker_directory.join("ready.tmp");
        fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
        fs::rename(temporary, marker_directory.join("ready.json")).unwrap();
        let mut signal = [0];
        std::io::stdin().read_exact(&mut signal).unwrap();
        panic!("the parent must kill this process while native window summaries are live");
    })));
    let mut input = LargeInput::new(TOTAL);
    let mut producer = |session: &ResidentVortexSession| input.next(session);
    let _ = prepared
        .with_batch_input(&mut producer)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("window completed before process death")
        });
    panic!("child must stop while its window owns native runs");
}

#[test]
fn streaming_window_recovery_preserves_live_and_unknown_owners_then_restarts_complete_values() {
    let fixture = fixture();
    let directory = fixture.0.join("window-runs");
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "streaming_window_recovery_process_child",
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
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "window child ended before handshake"
        );
        assert!(
            Instant::now() < deadline,
            "window child did not reach retained summary state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
    assert_eq!(ready["input_rows"], TOTAL);
    assert!(ready["summary_rows"].as_u64().unwrap() >= 1024);
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
    fs::write(&unknown, b"unowned window recovery witness").unwrap();
    assert!(policy.cleanup_abandoned(&owned).is_err());
    assert_eq!(
        fs::read(&unknown).unwrap(),
        b"unowned window recovery witness"
    );
    for (path, bytes) in &members {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    fs::remove_file(unknown).unwrap();
    policy.cleanup_abandoned(&owned).unwrap();
    assert!(!owned.exists());
    fs::remove_file(directory.join("ready.json")).unwrap();
    let prepared = prepare_window(&directory, GRANT, Some(128 << 20));
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let report = complete(&prepared, TOTAL).unwrap();
    assert_eq!(report.output_rows, TOTAL as u64);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    drop(report);
    assert_eq!(prepared.snapshot().completed_executions, 1);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(directory).unwrap().count(), 0);
}
