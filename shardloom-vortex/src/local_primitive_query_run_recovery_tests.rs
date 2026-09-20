use super::*;
use std::io::BufRead as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_WORKSPACE: &str = "SHARDLOOM_RUN_RECOVERY_CHILD_WORKSPACE";
const CHILD_NAMESPACE: &str = "SHARDLOOM_RUN_RECOVERY_CHILD_NAMESPACE";
const READY: &str = "SHARDLOOM_RECOVERY_DIRECTORY=";

fn policy(workspace: &Path, namespace: &str) -> QueryRunStorePolicy {
    let cancel = Arc::new(AtomicBool::new(false));
    match namespace {
        "count" => QueryRunStorePolicy::weighted_utf8_count(workspace.into(), 32 << 20, cancel),
        "distinct" => {
            QueryRunStorePolicy::exact_integer_distinct(workspace.into(), 32 << 20, cancel)
        }
        "sort" => QueryRunStorePolicy::numeric_sort(
            &crate::VortexSortSpillPolicy::new(workspace, 32 << 20, 4 << 20).unwrap(),
        ),
        _ => panic!("unknown recovery test namespace"),
    }
}

// A real process exit bypasses Rust destructors, unlike dropping a test fixture.
#[test]
fn recovery_process_child() {
    let Some(workspace) = std::env::var_os(CHILD_WORKSPACE) else {
        return;
    };
    let namespace = std::env::var(CHILD_NAMESPACE).unwrap();
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let runtime = local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let session = VortexSession::default().with_handle(runtime.handle());
    let work = Arc::new(memory.reserve(1 << 20).unwrap());
    let mut store = QueryRunStore::new(
        policy(Path::new(&workspace), &namespace),
        memory.clone(),
        memory.reserve(128 << 10).unwrap(),
    )
    .unwrap();
    for values in [[i32::MIN, 0], [17, i32::MAX]] {
        store
            .write_arrays(&spec(2), arrays(&values), &runtime, &session, &work)
            .unwrap();
    }
    println!("{READY}{}", store.directory().display());
    std::io::stdout().flush().unwrap();
    let mut command = [0];
    std::io::Read::read_exact(&mut std::io::stdin(), &mut command).unwrap();
    assert_eq!(command, [b'x']);
    std::process::exit(71);
}

struct ChildOwner(Child);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn all_namespaces_reject_live_recovery_and_clean_after_process_exit() {
    for namespace in ["sort", "distinct", "count"] {
        let workspace = Workspace::new();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args(["recovery_process_child", "--nocapture", "--test-threads=1"])
                .env(CHILD_WORKSPACE, &workspace.0)
                .env(CHILD_NAMESPACE, namespace)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                let line = line.unwrap();
                if let Some((_, path)) = line.split_once(READY) {
                    let _ = send.send(PathBuf::from(path));
                    break;
                }
            }
        });
        let directory = receive.recv_timeout(Duration::from_secs(20)).unwrap();
        reader.join().unwrap();
        let policy = policy(&workspace.0, namespace);
        let marker_before = fs::read(directory.join(OWNERSHIP_MARKER)).unwrap();
        assert!(
            recover(&policy, &directory)
                .unwrap_err()
                .to_string()
                .contains("workspace is active")
        );
        assert_eq!(
            fs::read(directory.join(OWNERSHIP_MARKER)).unwrap(),
            marker_before
        );
        assert_eq!(fs::read_dir(&directory).unwrap().count(), 3);
        child.0.stdin.as_mut().unwrap().write_all(b"x").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert_eq!(status.code(), Some(71));
                break;
            }
            assert!(Instant::now() < deadline, "recovery child did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
        recover(&policy, &directory).unwrap();
        workspace.assert_empty();
    }
}

#[test]
fn cancelled_recovery_preserves_marker_and_can_be_retried() {
    let workspace = Workspace::new();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let mut store = store(&workspace, &memory);
    let directory = store.directory().to_path_buf();
    store.abandon_for_recovery_test();
    let policy = workspace.policy();
    policy.cancellation.store(true, Ordering::Release);
    assert!(
        recover(&policy, &directory)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(directory.join(OWNERSHIP_MARKER).exists());
    policy.cancellation.store(false, Ordering::Release);
    recover(&policy, &directory).unwrap();
    drop(store);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    workspace.assert_empty();
}
