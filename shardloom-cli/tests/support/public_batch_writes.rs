//! Exercise the real batch protocol: every writer uses create-if-absent
//! publication, including resident single writes with overwrite permission.

use super::{complete_result, run_facade, unique_vortex_binding_dir};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead as _, BufReader, Write as _},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

const FORMATS: [&str; 8] = [
    "vortex",
    "parquet",
    "arrow_ipc",
    "avro",
    "orc",
    "json",
    "jsonl",
    "csv",
];
const ORIGINAL: &[u8] = b"existing destination must survive rejected writes";

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = unique_vortex_binding_dir("batch-writes");
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Worker(Child);

impl Drop for Worker {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn write(
    path: &Path,
    format: &str,
    streaming: bool,
    overwrite: bool,
    fanout: Option<&Path>,
) -> (bool, Value, usize) {
    let binding = json!({"memory://batches": {"input_format": "memory", "memory_input": {
        "kind": "batches", "schema": [["id", "int64"], ["label", "utf8"]],
        "streaming": streaming,
    }}})
    .to_string();
    let mut args = vec![
        "run".to_string(),
        "python".into(),
        "--source-bindings".into(),
        binding,
        "--sql".into(),
        "SELECT id,label FROM 'memory://batches'".into(),
        "--request".into(),
        format!("write_{format}"),
        "--output".into(),
        path.to_str().unwrap().into(),
        "--memory-bytes".into(),
        (32 << 20).to_string(),
        "--max-parallelism".into(),
        "1".into(),
    ];
    if overwrite {
        args.push("--allow-overwrite".into());
    }
    if let Some(fanout) = fanout {
        args.extend([
            "--fanout-output".into(),
            format!("jsonl={}", fanout.display()),
        ]);
    }
    let mut worker = Worker(
        Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["python-worker", "--batch-stream"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut input = worker.0.stdin.take().unwrap();
    let mut output = BufReader::new(worker.0.stdout.take().unwrap());
    writeln!(
        input,
        "{}",
        json!({"args": args, "stream_results": false, "batch_rows": 2048})
    )
    .unwrap();
    input.flush().unwrap();
    let mut demands = 0;
    loop {
        let mut line = String::new();
        assert!(
            output.read_line(&mut line).unwrap() > 0,
            "worker closed before a final envelope"
        );
        let frame: Value = serde_json::from_str(&line).unwrap();
        if frame.get("kind").is_none() {
            // Keep input open until the transaction exits: EOF is cancellation.
            let status = worker.0.wait().unwrap();
            assert_eq!(frame["fallback"]["attempted"], false, "{frame}");
            return (status.success(), frame, demands);
        }
        assert_eq!(
            frame,
            json!({"kind": "input", "uri": "memory://batches",
            "index": demands, "max_rows": 2048, "max_bytes": 8 << 20})
        );
        let reply = match demands {
            0 => json!({"kind": "rows", "uri": "memory://batches", "index": 0,
                        "rows": [["1", "港"], [null, null]]}),
            1 => json!({"kind": "rows", "uri": "memory://batches", "index": 1,
                        "rows": [["-3", "next"]]}),
            2 => json!({"kind": "end", "uri": "memory://batches", "index": 2}),
            _ => panic!("input demanded after declared completion"),
        };
        writeln!(input, "{reply}").unwrap();
        input.flush().unwrap();
        demands += 1;
    }
}

#[test]
fn resident_batch_writes_create_new_destinations_with_either_overwrite_setting() {
    let fixture = Fixture::new();
    for overwrite in [false, true] {
        for format in FORMATS {
            let path = fixture.0.join(format!("create-{overwrite}.{format}"));
            let (ok, envelope, demands) = write(&path, format, false, overwrite, None);
            assert!(ok, "{format}: {envelope}");
            assert_eq!(demands, 3);
            assert_eq!(envelope["status"], "success");
            assert_eq!(
                complete_result::field_value(&envelope, "output_row_count"),
                "3"
            );
            assert_eq!(
                complete_result::field_value(&envelope, "native_batch_protocol"),
                "shardloom.native_batches.v1"
            );
            assert_eq!(
                complete_result::field_value(&envelope, "public_workflow_external_engine_invoked"),
                "false"
            );
            assert!(path.is_file());
        }
        let expected = vec![
            json!({"id": 1, "label": "港"}),
            json!({"id": null, "label": null}),
            json!({"id": -3, "label": "next"}),
        ];
        let jsonl =
            fs::read_to_string(fixture.0.join(format!("create-{overwrite}.jsonl"))).unwrap();
        assert_eq!(
            jsonl
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        let source = fixture.0.join(format!("create-{overwrite}.vortex"));
        let sql = format!("SELECT * FROM '{}'", source.display());
        let (ok, stdout) = run_facade(&[
            "run",
            "python",
            "--sql",
            &sql,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--memory-bytes",
            "33554432",
            "--max-parallelism",
            "1",
            "--format",
            "json",
        ]);
        assert!(ok, "{stdout}");
        assert_eq!(
            complete_result::rows(&serde_json::from_str(&stdout).unwrap()),
            expected
        );
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2 * FORMATS.len());
}

#[test]
fn single_batch_write_rejections_preserve_existing_files_before_input_demand() {
    let fixture = Fixture::new();
    for (streaming, overwrite) in [(false, false), (false, true), (true, false), (true, true)] {
        for format in FORMATS {
            let path = fixture.0.join(format!("existing.{format}"));
            fs::write(&path, ORIGINAL).unwrap();
            let (ok, envelope, demands) = write(&path, format, streaming, overwrite, None);
            assert!(!ok, "{streaming}/{overwrite}/{format}: {envelope}");
            assert_eq!(demands, 0, "{envelope}");
            assert_eq!(fs::read(&path).unwrap(), ORIGINAL);
        }
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), FORMATS.len());
}

#[test]
fn batch_fanout_keeps_existing_target_rejection_for_resident_and_streaming_input() {
    let fixture = Fixture::new();
    let primary = fixture.0.join("new.vortex");
    let existing = fixture.0.join("existing.jsonl");
    fs::write(&existing, ORIGINAL).unwrap();
    for streaming in [false, true] {
        let (ok, envelope, demands) = write(&primary, "vortex", streaming, true, Some(&existing));
        assert!(!ok, "{envelope}");
        assert!(
            envelope.to_string().contains("existing destination"),
            "{envelope}"
        );
        assert_eq!(demands, 0);
        assert!(!primary.exists());
        assert_eq!(fs::read(&existing).unwrap(), ORIGINAL);
    }
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 1);
}
