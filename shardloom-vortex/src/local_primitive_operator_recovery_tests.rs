//! Test-only process handoff for real operators stopped with validated native runs.

use super::{OWNERSHIP_MARKER, QueryRunStore};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read as _, Write as _},
    os::unix::process::ExitStatusExt as _,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const WORKSPACE: &str = "SHARDLOOM_OPERATOR_RECOVERY_WORKSPACE";
const SOURCE: &str = "SHARDLOOM_OPERATOR_RECOVERY_SOURCE";
const CASE: &str = "SHARDLOOM_OPERATOR_RECOVERY_CASE";
const READY: &str = "operator-ready.json";

pub(crate) struct ChildInput {
    pub(crate) workspace: PathBuf,
    pub(crate) source: PathBuf,
    pub(crate) case: String,
}

pub(crate) fn child_input() -> Option<ChildInput> {
    let workspace = PathBuf::from(std::env::var_os(WORKSPACE)?);
    let source = PathBuf::from(std::env::var_os(SOURCE).unwrap());
    assert!(workspace.is_dir() && source.is_file());
    Some(ChildInput {
        workspace,
        source,
        case: std::env::var(CASE).unwrap(),
    })
}

fn namespace(case: &str) -> &'static str {
    match case {
        "sort" => "shardloom-query-sort-",
        "count_single" | "count_compound" => "shardloom-query-weighted-count-",
        "distinct" => "shardloom-query-integer-distinct-",
        _ => panic!("unknown operator recovery case"),
    }
}

/// Environment values are set only on the spawned test process. Unlike a
/// thread-local callback, this gate follows the run store onto runtime threads.
pub(super) fn after_validated_run(store: &QueryRunStore) {
    let Some(workspace) = std::env::var_os(WORKSPACE) else {
        return;
    };
    let case = std::env::var(CASE).unwrap();
    if store.policy.workspace != Path::new(&workspace)
        || store.policy.namespace.prefix() != namespace(&case)
        || store.snapshot.runs_written != 2
    {
        return;
    }
    assert_eq!(store.snapshot.runs_validated, 2);
    let ready = serde_json::json!({
        "case": case,
        "directory": store.directory,
        "runs_written": store.snapshot.runs_written,
        "runs_validated": store.snapshot.runs_validated,
        "live_disk_bytes": store.snapshot.live_disk_bytes,
    });
    let workspace = Path::new(&workspace);
    let temporary = workspace.join("operator-ready.tmp");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .unwrap();
    file.write_all(&serde_json::to_vec(&ready).unwrap())
        .unwrap();
    file.flush().unwrap();
    drop(file);
    fs::rename(temporary, workspace.join(READY)).unwrap();
    // A killed process cannot run QueryRunStore::drop or fixture destructors.
    let mut command = [0];
    let _ = std::io::stdin().read_exact(&mut command);
    panic!("parent must kill the operator process while native runs are live");
}

struct ChildOwner(Child);

impl Drop for ChildOwner {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn files(directory: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            assert!(fs::symlink_metadata(&path).unwrap().is_file());
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

/// The parent owns the source fixture throughout. The child borrows its path;
/// it never constructs a fixture whose destructor could remove that source.
#[allow(clippy::too_many_lines)] // Keep the kill, preservation and cleanup contract in one scope.
pub(crate) fn recover_operator(
    child_test: &str,
    workspace: &Path,
    source: &Path,
    case: &str,
    cleanup: impl Fn(&Path) -> shardloom_core::Result<()>,
) {
    assert_eq!(fs::read_dir(workspace).unwrap().count(), 0);
    let source_before = Sha256::digest(fs::read(source).unwrap());
    let (_, child_test) = child_test
        .split_once("::")
        .expect("module_path-qualified child test");
    let mut child = ChildOwner(
        Command::new(std::env::current_exe().unwrap())
            .args([child_test, "--exact", "--nocapture", "--test-threads=1"])
            .env(WORKSPACE, workspace)
            .env(SOURCE, source)
            .env(CASE, case)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let ready_path = workspace.join(READY);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready_path.exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "operator exited before native runs were ready"
        );
        assert!(
            Instant::now() < deadline,
            "operator did not produce two validated native runs"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(&ready_path).unwrap()).unwrap();
    assert_eq!(ready["case"], case);
    assert_eq!(ready["runs_written"], 2);
    assert_eq!(ready["runs_validated"], 2);
    assert!(ready["live_disk_bytes"].as_u64().unwrap() > 0);
    let directory = PathBuf::from(ready["directory"].as_str().unwrap());
    assert_eq!(
        directory.parent(),
        Some(fs::canonicalize(workspace).unwrap().as_path())
    );
    assert!(
        directory
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with(namespace(case))
    );
    let before = files(&directory);
    assert_eq!(before.len(), 3);
    assert!(before.contains_key(&directory.join(OWNERSHIP_MARKER)));
    assert!(
        cleanup(&directory)
            .unwrap_err()
            .to_string()
            .contains("workspace is active")
    );
    assert_eq!(files(&directory), before);
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
    assert_eq!(files(&directory), before);

    let unknown = directory.join("unowned.keep");
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&unknown)
        .unwrap()
        .write_all(b"unowned recovery control")
        .unwrap();
    let with_unknown = files(&directory);
    assert!(
        cleanup(&directory)
            .unwrap_err()
            .to_string()
            .contains("unknown file")
    );
    assert_eq!(files(&directory), with_unknown);
    fs::remove_file(unknown).unwrap();
    cleanup(&directory).unwrap();
    assert!(!directory.exists());
    fs::remove_file(ready_path).unwrap();
    assert_eq!(fs::read_dir(workspace).unwrap().count(), 0);
    assert_eq!(Sha256::digest(fs::read(source).unwrap()), source_before);
}
