//! Real streamed operators leave recoverable runs after a process exits.

use super::*;
use std::{
    io::{Read as _, Write as _},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_STREAM_ORDER_RECOVERY_WORKSPACE";

struct ChildOwner(Child);

impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn streamed_ordering_recovery_process_child() {
    let Some(workspace) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let workspace = PathBuf::from(workspace);
    let spill = VortexRelationalSpillPolicy::new(&workspace, 64 << 20, 1 << 20).unwrap();
    let prepared = prepare(&order_plan(), 8 << 20)
        .unwrap()
        .with_spill(spill)
        .unwrap();
    let mut input = Input::new(16 * 1024);
    let text = "λ".repeat(256);
    let mut provider = |session: &ResidentVortexSession| {
        if input.calls == 8 {
            assert_eq!(input.prior.as_ref().unwrap().strong_count(), 0);
            let directories = fs::read_dir(&workspace)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.is_dir()
                        && path
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with("shardloom-query-")
                })
                .collect::<Vec<_>>();
            assert_eq!(directories.len(), 1);
            let ready = serde_json::json!({"directory":directories[0], "input_rows":input.delivered,
                "peak_reserved_bytes":prepared.snapshot().memory.peak_reserved_bytes});
            // Publish a complete test handshake separately from the native run directory.
            let temporary = workspace.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(&temporary, workspace.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            assert_eq!(signal, [b'x']);
            // Bypass destructors while actual native sort state and runs are alive.
            std::process::exit(73);
        }
        input.next(session, &text)
    };
    let _ = prepared
        .with_batch_input(&mut provider)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("child delivered before it exited")
        });
    panic!("child must exit while the complete source is still pending");
}

#[test]
fn streamed_ordering_recovery_refuses_live_owner_then_cleans_abandoned_runs_for_restart() {
    let fixture = Fixture::new(keyed(&[], &[]), 1);
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "streamed_ordering_recovery_process_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(WORKSPACE, &fixture.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let ready = fixture.0.join("ready.json");
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child ended before the ready handshake"
        );
        assert!(
            Instant::now() < deadline,
            "streamed child did not reach retained state"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
    assert_eq!(ready["input_rows"], 8 * 1024);
    assert!(ready["peak_reserved_bytes"].as_u64().unwrap() <= 8 << 20);
    let directory = PathBuf::from(ready["directory"].as_str().unwrap());
    assert_eq!(directory.parent(), Some(fixture.0.as_path()));
    let members = fs::read_dir(&directory)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect::<Vec<_>>();
    assert!(
        members.len() >= 2,
        "a marker plus real native runs must remain"
    );
    let policy = spill(&fixture, 64 << 20);
    let error = policy
        .cleanup_abandoned(&directory)
        .unwrap_err()
        .to_string();
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
        assert!(Instant::now() < deadline, "streamed child did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    policy.cleanup_abandoned(&directory).unwrap();
    assert!(!directory.exists());
    fs::remove_file(fixture.0.join("ready.json")).unwrap();
    assert_names(&fixture, &["input.vortex"]);
    let prepared = prepare(&order_plan(), 8 << 20)
        .unwrap()
        .with_spill(policy)
        .unwrap();
    let before = prepared.snapshot().memory.reserved_bytes;
    let execution = complete(&prepared, 8 * 1024 + 7, &"λ".repeat(256), false);
    assert!(execution.spill.as_ref().unwrap().owned_cleanup_completed);
    drop(execution);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, before);
    assert_names(&fixture, &["input.vortex"]);
}
