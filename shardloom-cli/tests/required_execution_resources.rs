//! Public admission must precede input inspection, fixture execution and output.

use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shardloom-required-resources-{}-{nonce}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn run(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .args(["--format", "json"])
        // Ambient invalid settings must neither supply nor replace a grant.
        .env("SHARDLOOM_MEMORY_GB", "invalid")
        .env("SHARDLOOM_MAX_PARALLELISM", "eight")
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
    )
}

#[test]
fn durable_checkpoint_workspace_is_admitted_before_directory_creation() {
    let directory = TestDirectory::new();
    let target = directory.0.join("checkpoint");
    let (success, report) = run(&[
        "live-hybrid-durable-checkpoint-smoke",
        target.to_str().unwrap(),
        "--memory-bytes",
        "1",
        "--max-parallelism",
        "1",
    ]);
    assert!(!success);
    assert!(report.contains("memory reservation denied"), "{report}");
    assert!(!target.exists());
    let grant = shardloom_core::live_hybrid_durable_checkpoint_workspace_bytes().to_string();
    let (success, report) = run(&[
        "live-hybrid-durable-checkpoint-smoke",
        target.to_str().unwrap(),
        "--memory-bytes",
        &grant,
        "--max-parallelism",
        "1",
    ]);
    assert!(success, "{report}");
    assert!(report.contains("conservative_fixed_ten_record_checkpoint_workspace_estimate"));
    assert!(target.join("cg22-live-hybrid-checkpoint.json").is_file());
    assert!(
        !target
            .join("cg22-live-hybrid-checkpoint.partial.json")
            .exists()
    );
}

#[test]
fn data_commands_reject_invalid_resources_before_input_or_output() {
    let directory = TestDirectory::new();
    let absent_path = directory.0.join("must-not-open-or-create");
    let absent = absent_path.to_str().unwrap();
    let commands: &[&[&str]] = &[
        &["vortex-file-metadata-open", absent],
        &["vortex-count", absent],
        &["vortex-run", absent, "count"],
        &["vortex-prepare", absent, absent],
        &["object-store-read-smoke", absent],
        &["object-store-write-smoke", absent, absent],
        &["object-store-write-recovery-smoke", absent],
        &["object-store-partition-discovery-smoke", absent],
        &["local-table-metadata-read-smoke"],
        &["iceberg-metadata-read-smoke", absent],
        &["delta-log-metadata-read-smoke", absent],
        &["hudi-timeline-metadata-read-smoke", absent],
        &["local-delete-tombstone-read-smoke"],
        &["local-append-only-cdc-overlay-smoke"],
        &["local-table-append-commit-rehearsal-smoke", absent],
        &["local-table-commit-recovery-smoke", absent],
        &[
            "sqlite-local-import-export-smoke",
            absent,
            "--table",
            "rows",
            "--export-jsonl",
            absent,
            "--roundtrip-db",
            absent,
        ],
        &["live-fixture-run"],
        &["hybrid-overlay-run"],
        &["live-hybrid-state-transition-smoke"],
        &["live-hybrid-durable-checkpoint-smoke", absent],
        &["distributed-local-fixture-run"],
        &["udf-local-scalar-fixture-smoke", "not-an-integer"],
        &["embedding-vector-local-fixture-smoke", "--memory-gb"],
        &["spill-payload-roundtrip", absent, "payload", "--memory-gb"],
    ];
    let invalid: &[&[&str]] = &[
        &[],
        &["--memory-gb", "1"],
        &["--max-parallelism", "1"],
        &["--memory-gb", "1", "--max-parallelism", "eight"],
        &["--memory-bytes", "0", "--max-parallelism", "1"],
        &[
            "--memory-gb",
            "18446744073709551615",
            "--max-parallelism",
            "1",
        ],
        &[
            "--memory-bytes",
            "100",
            "--max-parallelism",
            "2",
            "--parallelism-limit",
            "1",
        ],
    ];
    for command in commands {
        for invalid in invalid {
            let args = [*command, *invalid].concat();
            let (success, text) = run(&args);
            assert!(!success, "{args:?}: {text}");
            assert!(text.contains("SL_CONFIGURATION_ERROR"), "{args:?}: {text}");
            assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0, "{args:?}");
        }
    }
}

#[test]
fn fixture_data_report_distinguishes_declaration_admission_and_observation() {
    let directory = TestDirectory::new();
    let input = directory.0.join("object.bin");
    fs::write(&input, b"abc").unwrap();
    let (success, text) = run(&[
        "object-store-read-smoke",
        input.to_str().unwrap(),
        "--memory-bytes",
        "1500000001",
        "--max-parallelism",
        "3",
        "--memory-origin",
        "platform",
        "--parallelism-origin",
        "context",
    ]);
    assert!(success, "{text}");
    let report: serde_json::Value = serde_json::from_str(&text).unwrap();
    for (key, expected) in [
        ("execution_resource_declared_memory_bytes", "1500000001"),
        ("execution_resource_declared_max_parallelism", "3"),
        ("execution_resource_memory_origin", "platform"),
        ("execution_resource_parallelism_origin", "context"),
        ("execution_resource_admitted_memory_bytes", "1500000001"),
        ("execution_resource_admitted_max_parallelism", "1"),
        ("execution_resource_observed_native_reserved_bytes", "0"),
        (
            "execution_resource_observed_native_peak_reserved_bytes",
            "3",
        ),
        (
            "execution_resource_memory_observation_scope",
            "fixture_owned_buffers_and_workspace_writer;excludes_uninstrumented_metadata_provider_decode_transients_reports_and_process_rss",
        ),
        ("fixture_io_denied_reservations", "0"),
        (
            "execution_resource_observed_peak_active_lanes",
            "unavailable",
        ),
        (
            "execution_resource_whole_process_memory_limit_enforced",
            "false",
        ),
        ("object_store_bytes_read", "3"),
        ("fallback_attempted", "false"),
    ] {
        let fields = report["fields"].as_array().unwrap();
        let found: Vec<_> = fields.iter().filter(|entry| entry["key"] == key).collect();
        assert_eq!(found.len(), 1, "{key}: {text}");
        assert_eq!(found[0]["value"], expected, "{key}: {text}");
    }
}

#[test]
fn inert_discovery_remains_available_without_resources() {
    for args in [
        &["status"][..],
        &["capabilities"],
        &["vortex-metadata-probe", "never-open.vortex"],
    ] {
        let (_, text) = run(args);
        assert!(!text.contains("SL_CONFIGURATION_ERROR"), "{args:?}: {text}");
        assert!(
            serde_json::from_str::<serde_json::Value>(&text).is_ok(),
            "{text}"
        );
    }
}
