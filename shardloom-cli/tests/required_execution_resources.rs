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

fn field<'a>(report: &'a serde_json::Value, key: &str) -> &'a str {
    let matches = report["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["key"] == key)
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "{key}: {report}");
    matches[0]["value"].as_str().unwrap()
}

#[test]
fn executing_text_and_fixed_fixtures_refuse_tiny_grants_and_report_owned_workspaces() {
    let commands: &[&[&str]] = &[
        &["live-fixture-run", "group-count", "metric"],
        &["hybrid-overlay-run", "group-count", "metric"],
        &["live-hybrid-state-transition-smoke"],
        &["distributed-local-fixture-run", "2", "fault-injection"],
        &["local-table-metadata-read-smoke"],
        &["local-delete-tombstone-read-smoke"],
        &["local-append-only-cdc-overlay-smoke"],
        &["udf-local-scalar-fixture-smoke", "3,null,-4"],
        &[
            "embedding-vector-local-fixture-smoke",
            "alpha;beta;gamma",
            "--query",
            "beta",
        ],
    ];
    for command in commands {
        let (success, text) =
            run(&[*command, &["--memory-bytes", "1", "--max-parallelism", "1"]].concat());
        assert!(!success, "{command:?}: {text}");
        assert!(
            text.contains("memory reservation denied"),
            "{command:?}: {text}"
        );
        let denied: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            field(&denied, "execution_resource_observed_native_reserved_bytes"),
            "0"
        );
        assert_eq!(field(&denied, "fixture_io_denied_reservations"), "1");

        let (success, text) = run(&[
            *command,
            &["--memory-bytes", "8388608", "--max-parallelism", "3"],
        ]
        .concat());
        assert!(success, "{command:?}: {text}");
        let report: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            field(&report, "execution_resource_declared_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_max_parallelism"),
            "1"
        );
        let live = field(&report, "execution_resource_observed_native_reserved_bytes")
            .parse::<u64>()
            .unwrap();
        let peak = field(
            &report,
            "execution_resource_observed_native_peak_reserved_bytes",
        )
        .parse::<u64>()
        .unwrap();
        assert!(
            live > 1 && live <= peak && peak <= 8_388_608,
            "{command:?}: {text}"
        );
        assert!(
            field(&report, "execution_resource_memory_observation_scope")
                .contains("conservative_reservation_not_measured_heap")
        );
        assert_eq!(
            field(
                &report,
                "execution_resource_whole_process_memory_limit_enforced"
            ),
            "false"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_spill_io_performed"),
            "false"
        );
        assert_eq!(field(&report, "fallback_attempted"), "false");
    }
}

#[test]
fn admitted_fixture_parse_and_runtime_errors_retain_live_workspace_evidence() {
    let commands: &[&[&str]] = &[
        &["live-fixture-run", "unknown"],
        &["live-fixture-run", "count", "unexpected"],
        &["live-fixture-run", "count", "unexpected", "extra"],
        &["live-fixture-run", "filter", "not-a-predicate"],
        &["hybrid-overlay-run", "unknown"],
        &["hybrid-overlay-run", "count", "unexpected"],
        &["hybrid-overlay-run", "count", "unexpected", "extra"],
        &["hybrid-overlay-run", "filter", "not-a-predicate"],
        &["live-hybrid-state-transition-smoke", "extra"],
        &["distributed-local-fixture-run", "many"],
        &["distributed-local-fixture-run", "0"],
        &["distributed-local-fixture-run", "2", "unknown"],
        &["distributed-local-fixture-run", "2", "none", "extra"],
        &["local-table-metadata-read-smoke", "extra"],
        &["local-delete-tombstone-read-smoke", "extra"],
        &["local-append-only-cdc-overlay-smoke", "extra"],
    ];
    for command in commands {
        let (success, text) = run(&[
            *command,
            &["--memory-bytes", "8388608", "--max-parallelism", "3"],
        ]
        .concat());
        assert!(!success, "{command:?}: {text}");
        // One complete envelope, with the lease still alive during emission.
        let report: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(report["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(
            field(&report, "execution_resource_declared_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_declared_max_parallelism"),
            "3"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_max_parallelism"),
            "1"
        );
        let live = field(&report, "execution_resource_observed_native_reserved_bytes")
            .parse::<u64>()
            .unwrap();
        let peak = field(
            &report,
            "execution_resource_observed_native_peak_reserved_bytes",
        )
        .parse::<u64>()
        .unwrap();
        assert!(
            live > 0 && live <= peak && peak <= 8_388_608,
            "{command:?}: {text}"
        );
        assert_eq!(field(&report, "fixture_io_denied_reservations"), "0");
        assert!(
            field(&report, "execution_resource_memory_observation_scope")
                .contains("conservative_reservation_not_measured_heap")
        );
        assert_eq!(
            field(
                &report,
                "execution_resource_whole_process_memory_limit_enforced"
            ),
            "false"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_spill_io_performed"),
            "false"
        );
        assert_eq!(report["fallback"]["attempted"], false);
    }
}

#[test]
fn admitted_spill_constructor_errors_report_zero_io_without_creating_a_workspace() {
    let directory = TestDirectory::new();
    let target = directory.0.join("not-created");
    let commands: &[&[&str]] = &[
        &["spill-payload-roundtrip", "", "payload-1", "hello"],
        &[
            "spill-payload-roundtrip",
            target.to_str().unwrap(),
            "../bad",
            "hello",
        ],
        &[
            "spill-payload-roundtrip",
            target.to_str().unwrap(),
            "payload-1",
            "",
        ],
    ];
    for command in commands {
        let (success, text) = run(&[
            *command,
            &["--memory-bytes", "8388608", "--max-parallelism", "2"],
        ]
        .concat());
        assert!(!success, "{command:?}: {text}");
        let report: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(report["diagnostics"].as_array().unwrap().len(), 1);
        assert_eq!(
            field(&report, "execution_resource_declared_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_memory_bytes"),
            "8388608"
        );
        assert_eq!(
            field(&report, "execution_resource_admitted_max_parallelism"),
            "1"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_native_reserved_bytes"),
            "0"
        );
        assert_eq!(
            field(
                &report,
                "execution_resource_observed_native_peak_reserved_bytes"
            ),
            "0"
        );
        assert_eq!(field(&report, "fixture_io_denied_reservations"), "0");
        assert_eq!(
            field(&report, "execution_resource_observed_spill_io_performed"),
            "false"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_spill_bytes"),
            "0"
        );
        assert_eq!(
            field(&report, "execution_resource_spill_observation_scope"),
            "synthetic_payload_only;query_spill_not_implied"
        );
        assert_eq!(report["fallback"]["attempted"], false);
        assert!(!target.exists());
    }
}

#[test]
fn synthetic_spill_admission_precedes_creation_and_reports_its_actual_scope() {
    let directory = TestDirectory::new();
    let target = directory.0.join("spill-workspace");
    let command = [
        "spill-payload-roundtrip",
        target.to_str().unwrap(),
        "payload-1",
        "hello",
    ];
    let (success, text) = run(&[
        &command[..],
        &["--memory-bytes", "1", "--max-parallelism", "1"],
    ]
    .concat());
    assert!(!success, "{text}");
    assert!(text.contains("memory reservation denied"), "{text}");
    assert!(!target.exists());
    let denied: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        field(&denied, "execution_resource_observed_native_reserved_bytes"),
        "0"
    );

    // The CLI requires an existing caller-owned workspace. The core roundtrip
    // tests separately exercise its explicit CreateWorkspace option.
    if shardloom_exec::spill_payload_fs_feature_enabled() {
        fs::create_dir(&target).unwrap();
    }
    let (success, text) = run(&[
        &command[..],
        &["--memory-bytes", "1048576", "--max-parallelism", "2"],
    ]
    .concat());
    let report: serde_json::Value = serde_json::from_str(&text).unwrap();
    let peak = field(
        &report,
        "execution_resource_observed_native_peak_reserved_bytes",
    )
    .parse::<u64>()
    .unwrap();
    assert!((10..=1_048_576).contains(&peak), "{text}");
    assert!(
        field(&report, "execution_resource_memory_observation_scope")
            .contains("retained_capacity_readback_and_staging_writer")
    );
    assert_eq!(
        field(&report, "execution_resource_spill_observation_scope"),
        "synthetic_payload_only;query_spill_not_implied"
    );
    if shardloom_exec::spill_payload_fs_feature_enabled() {
        assert!(success, "{text}");
        assert!(target.is_dir());
        assert_eq!(field(&report, "bytes_read"), "5");
        assert_eq!(field(&report, "verification_passed"), "true");
        assert_eq!(
            field(&report, "execution_resource_observed_spill_io_performed"),
            "true"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_spill_bytes"),
            "5"
        );
    } else {
        assert!(success, "{text}");
        assert!(!target.exists());
        assert_eq!(
            field(&report, "execution_resource_observed_spill_io_performed"),
            "false"
        );
        assert_eq!(
            field(&report, "execution_resource_observed_spill_bytes"),
            "0"
        );
    }
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
