//! Process death permits exact owned cleanup followed by a complete fresh query.

use super::*;
use std::{
    io::Read as _,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_SET_MEMBERSHIP_RECOVERY_WORKSPACE";
const KIND: &str = "SHARDLOOM_SET_MEMBERSHIP_RECOVERY_KIND";

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
fn streaming_set_membership_recovery_process_child() {
    let Some(directory) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let directory = PathBuf::from(directory);
    let kind = match std::env::var(KIND).unwrap().as_str() {
        "union" => SetKind::UnionDistinct,
        "intersect" => SetKind::Intersect,
        "except" => SetKind::Except,
        _ => panic!("unexpected recovery kind"),
    };
    let prepared = with_spill(
        prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT),
        &directory,
        128 << 20,
    );
    let mut inputs = WideInputs::new(true);
    inputs.right_offset = ROWS / 2;
    let mut producer = |uri: &DatasetUri, ordinal: u64, session: &ResidentVortexSession| {
        if ordinal == 6 {
            assert_eq!(inputs.prior.as_ref().unwrap().strong_count(), 0);
            let runs = super::super::fault_tests::runs(&directory);
            assert_ne!(runs, Vec::<PathBuf>::new());
            let ready = json!({"directory":runs[0].parent().unwrap(), "positions":inputs.positions,
                "peak_reserved_bytes":prepared.snapshot().memory.peak_reserved_bytes});
            let temporary = directory.join("ready.tmp");
            fs::write(&temporary, serde_json::to_vec(&ready).unwrap()).unwrap();
            fs::rename(temporary, directory.join("ready.json")).unwrap();
            let mut signal = [0];
            std::io::stdin().read_exact(&mut signal).unwrap();
            panic!("parent must kill a live membership owner");
        }
        inputs.next(uri, ordinal, session)
    };
    let _ = prepared
        .with_batch_inputs(&mut producer)
        .unwrap()
        .for_each_batch(&CancellationToken::default(), |_, _| {
            panic!("membership completed before process death")
        });
    panic!("child must stop with incomplete set membership");
}

#[test]
fn streaming_set_membership_recovery_preserves_live_and_unknown_owners_then_restarts_exactly() {
    for (name, kind) in [
        ("union", SetKind::UnionDistinct),
        ("intersect", SetKind::Intersect),
        ("except", SetKind::Except),
    ] {
        let (_fixture, directory) = workspace();
        let foreign = directory.join("foreign");
        fs::write(&foreign, b"preserve outer workspace file").unwrap();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "streaming_set_membership_recovery_process_child",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(WORKSPACE, &directory)
                .env(KIND, name)
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
                "membership child ended before handshake"
            );
            assert!(
                Instant::now() < deadline,
                "membership child never reached real spill"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let ready: Value = serde_json::from_slice(&fs::read(&ready).unwrap()).unwrap();
        let positions = ready["positions"].as_array().unwrap();
        assert_eq!(
            positions
                .iter()
                .map(|value| value.as_u64().unwrap())
                .sum::<u64>(),
            (6 * INPUT_ROWS) as u64
        );
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
        // Prevent EOF from unwinding the child and deleting the abandoned runs.
        let _stdin = child.0.stdin.take();
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
        let prepared = prepared_set(&set(input_scan(0), input_scan(1), kind), 2, GRANT)
            .with_spill(policy)
            .unwrap();
        let baseline = prepared.snapshot().memory.reserved_bytes;
        let report = complete(&prepared, kind);
        assert_eq!(report.runtime.completed_executions, 1);
        drop(report);
        assert_eq!(prepared.snapshot().memory.reserved_bytes, baseline);
        assert_eq!(
            fs::read(&foreign).unwrap(),
            b"preserve outer workspace file"
        );
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 1);
    }
}
