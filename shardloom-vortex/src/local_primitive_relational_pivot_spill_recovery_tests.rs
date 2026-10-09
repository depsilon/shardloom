//! Dead-owner cleanup restarts the full pivot; it never resumes partial state.

use super::*;
use crate::local_primitives::native_relational_spill::pivot::AFTER_COUNT_PASS;
use std::{
    io::Read as _,
    mem::ManuallyDrop,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const ROOT: &str = "SHARDLOOM_PIVOT_RECOVERY_ROOT";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
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
    let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
    let memory = prepared.session.memory().clone();
    let root = fixture.0.clone();
    AFTER_COUNT_PASS.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |path| {
            let ready = json!({"directory":path.parent().unwrap(),"phase":"counted_before_merge",
                              "peak_reserved_bytes":memory.snapshot().peak_reserved_bytes});
            let temporary = root.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, root.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill the pivot while its counted runs are live");
        }));
    });
    let _ = prepared.for_each_batch(&CancellationToken::default(), |_, _| {
        panic!("pivot completed before process death")
    });
    panic!("child must stop with retained native pivot state");
}

#[test]
fn native_pivot_spill_recovery_preserves_live_and_unknown_owners_then_restarts() {
    let fixture = fixture(&[Some(3.0), Some(7.0)], 257, 41);
    let workspace = fixture.0.join("pivot-runs");
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
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

    let prepared = prepare(&fixture, plan(&fixture, "sum"), true);
    let baseline = prepared.snapshot().memory.reserved_bytes;
    let (actual, report) = complete(&prepared);
    assert_eq!(
        actual,
        (0..257)
            .map(|index| json!({"entity":key(index),"pivot_a":10.0}))
            .collect::<Vec<_>>()
    );
    assert_eq!(report.runtime.completed_executions, 1);
    assert!(report.spill.as_ref().unwrap().owned_cleanup_completed);
    drop(report);
    assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
    assert_eq!(fs::read_dir(workspace).unwrap().count(), 0);
}
