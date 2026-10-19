//! Recover abandoned owned runs, then restart from the source; never resume state.

use super::*;
use std::{
    io::Read as _,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const ROOT: &str = "SHARDLOOM_UNARY_MEMBERSHIP_RECOVERY_ROOT";
const VARIANT: &str = "SHARDLOOM_UNARY_MEMBERSHIP_RECOVERY_VARIANT";

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn unary_membership_recovery_process_child() {
    let Some(root) = std::env::var_os(ROOT) else {
        return;
    };
    let root = PathBuf::from(root);
    let index: usize = std::env::var(VARIANT).unwrap().parse().unwrap();
    let (name, req) = membership_cases().into_iter().nth(index).unwrap();
    let prepared = prepared(&req, &root.join("unary-runs"));
    let keys = keys(false);
    let mut input = Input::new(&keys);
    let position = Rc::clone(&input.position);
    let memory = prepared.session.memory().clone();
    AFTER_COUNT_PASS.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |path| {
            let ready = json!({"directory":path.parent().unwrap(), "phase":"counted_before_merge",
            "variant":name, "input_rows":position.get(),
            "peak_reserved_bytes":memory.snapshot().peak_reserved_bytes});
            let temporary = root.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, root.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill the child with live counted membership runs");
        }));
    });
    let result = prepared
        .with_batch_inputs(&mut |_, ordinal, session| input.next(ordinal, session))
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| Ok(()));
    panic!(
        "child must stop with retained membership state: {:?}",
        result.err()
    );
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the exact process, file-owner, and fresh restart lifecycle together.
fn unary_membership_recovery_preserves_live_unknown_and_foreign_owners_then_restarts() {
    for (index, (name, req)) in membership_cases().into_iter().enumerate() {
        let (fixture, directory) = workspace();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "unary_membership_recovery_process_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(ROOT, &fixture.0)
                .env(VARIANT, index.to_string())
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
                "{name}: child ended before handshake"
            );
            assert!(
                Instant::now() < deadline,
                "{name}: child did not retain counted runs"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let ready: Value = serde_json::from_slice(&fs::read(ready).unwrap()).unwrap();
        assert_eq!(ready["variant"], name);
        assert_eq!(ready["phase"], "counted_before_merge");
        assert!(ready["input_rows"].as_u64().unwrap() > 0);
        assert!(ready["input_rows"].as_u64().unwrap() < keys(false).len() as u64);
        assert!(ready["peak_reserved_bytes"].as_u64().unwrap() <= GRANT);
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
        assert!(error.contains("workspace is active"), "{name}: {error}");
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        assert_eq!(fs::read(directory.join("foreign")).unwrap(), b"preserve");
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());

        let unknown = owned.join("user.txt");
        fs::write(&unknown, b"unowned unary recovery witness").unwrap();
        assert!(policy.cleanup_abandoned(&owned).is_err());
        assert_eq!(
            fs::read(&unknown).unwrap(),
            b"unowned unary recovery witness"
        );
        for (path, bytes) in &members {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        fs::remove_file(unknown).unwrap();
        policy.cleanup_abandoned(&owned).unwrap();
        assert!(!owned.exists());
        clean(&directory);

        let prepared = prepared(&req, &directory);
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let report = complete(&prepared, &req, &keys(false));
        assert_eq!(report.runtime.completed_executions, 1);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        clean(&directory);
    }
}
