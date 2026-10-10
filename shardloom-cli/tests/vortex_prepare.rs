use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(all(unix, feature = "vortex-write", feature = "vortex-local-primitives"))]
#[path = "support/complete_result.rs"]
mod complete_result;

#[cfg(any(
    all(unix, feature = "vortex-write", feature = "vortex-local-primitives"),
    all(feature = "vortex-write", feature = "universal-format-io")
))]
use std::path::Path;

#[cfg(feature = "universal-format-io")]
use std::{fs::File, sync::Arc};

static UNIQUE_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    let counter = UNIQUE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{counter}-{nanos}.{extension}",
        std::process::id(),
    ))
}

#[cfg(feature = "vortex-write")]
fn unique_extensionless_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    let counter = UNIQUE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{counter}-{nanos}",
        std::process::id(),
    ))
}

#[cfg(feature = "vortex-write")]
fn unique_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    let counter = UNIQUE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{counter}-{nanos}",
        std::process::id(),
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create unique dir");
    path
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[cfg(feature = "vortex-write")]
fn assert_ingest_array_build(stdout: &str, streaming: bool) {
    let expected = if streaming {
        [
            ("vortex_array_build_provider_kind", "vortex_array_kernel"),
            (
                "vortex_array_build_provider_surface",
                "ArrayRef::from_arrow(RecordBatch);ordered_morsel_vortex_array_prefetch;streaming ArrayIterator",
            ),
            (
                "vortex_array_build_strategy",
                "ordered_morsel_vortex_array_prefetch_threadlocal_conversion_merge",
            ),
            (
                "vortex_array_build_input_layout",
                "streaming_arrow_record_batch_columnar_source_state",
            ),
            ("vortex_array_build_record_batch_count", "1"),
            ("vortex_array_build_manual_scalar_copy_avoided", "true"),
        ]
    } else {
        [
            ("vortex_array_build_provider_kind", "shardloom_kernel"),
            (
                "vortex_array_build_provider_surface",
                "shardloom_scalar_rows_to_vortex_struct",
            ),
            (
                "vortex_array_build_strategy",
                "scalar_rows_to_vortex_struct",
            ),
            ("vortex_array_build_input_layout", "materialized_rows"),
            ("vortex_array_build_record_batch_count", "0"),
            ("vortex_array_build_manual_scalar_copy_avoided", "false"),
        ]
    };
    for (key, value) in expected {
        assert!(
            stdout.contains(&field(key, value)),
            "{key} must equal {value}"
        );
    }
    if streaming {
        // The default two-lane grant shares its caller and one background
        // driver across source, conversion and native provider work.
        for (key, value) in [
            ("vortex_writer_runtime_requested_parallelism", "2"),
            ("vortex_writer_runtime_applied_parallelism", "2"),
            ("vortex_writer_runtime_background_workers", "1"),
            (
                "vortex_writer_physical_design_source_executor_applied_parallelism",
                "1",
            ),
            (
                "vortex_writer_physical_design_array_build_worker_count",
                "2",
            ),
            ("vortex_array_build_prefetch_window", "2"),
        ] {
            assert!(
                stdout.contains(&field(key, value)),
                "{key} must equal {value}"
            );
        }
    }
}

#[cfg(all(unix, feature = "vortex-write", feature = "vortex-local-primitives"))]
fn assert_prepared_collect_values(path: &Path, sql: &str, expected: &serde_json::Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "run",
            "sql",
            "--input",
            &path.display().to_string(),
            "--input-format",
            "vortex",
            "--sql",
            sql,
            "--request",
            "collect",
            "--bounded",
            "true",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("collect prepared values");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let envelope: serde_json::Value = serde_json::from_slice(&output.stdout).expect("envelope");
    assert_eq!(envelope["status"], "success");
    let fields = envelope["fields"].as_array().expect("fields");
    for key in [
        "public_workflow_fallback_attempted",
        "public_workflow_external_engine_invoked",
    ] {
        assert!(
            fields
                .iter()
                .any(|entry| entry["key"] == key && entry["value"] == "false")
        );
    }
    assert_eq!(
        complete_result::rows(&envelope),
        *expected.as_array().unwrap()
    );
    assert_eq!(
        complete_result::field_value(&envelope, "output_row_count")
            .parse::<usize>()
            .expect("output row count"),
        expected.as_array().expect("expected rows").len()
    );
}

#[cfg(feature = "vortex-write")]
#[derive(Clone, Copy)]
struct ExpectedAdapterEvidence<'a> {
    source_format: &'a str,
    extension: &'a str,
    adapter_id: &'a str,
    registry_entry_id: &'a str,
    admitted_extensions: &'a str,
    feature_gate: &'a str,
    boundary: &'a str,
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
type StructuredVortexIngestCase = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    fn(&Path),
);

#[cfg(feature = "vortex-write")]
fn assert_inferred_adapter_evidence(stdout: &str, expected: ExpectedAdapterEvidence<'_>) {
    assert!(stdout.contains(&field("source_format", expected.source_format)));
    assert!(stdout.contains(&field("source_format_inferred", "true")));
    assert!(stdout.contains(&field("source_format_inference_kind", "path_extension")));
    assert!(stdout.contains(&field(
        "source_format_inference_extension",
        expected.extension
    )));
    assert!(stdout.contains(&field(
        "source_format_inference_registry_route",
        "local_path_extension_adapter_registry"
    )));
    assert!(stdout.contains(&field("source_adapter_id", expected.adapter_id)));
    assert!(stdout.contains(&field(
        "source_adapter_registry_entry_id",
        expected.registry_entry_id
    )));
    assert!(stdout.contains(&field(
        "source_adapter_admitted_extensions",
        expected.admitted_extensions
    )));
    assert!(stdout.contains(&field("source_adapter_feature_gate", expected.feature_gate)));
    assert!(stdout.contains(&field("source_adapter_boundary", expected.boundary)));
    assert!(stdout.contains(&field(
        "source_adapter_selection_reason",
        "inferred_at_read_ingest_boundary"
    )));
}

#[cfg(not(feature = "vortex-write"))]
#[test]
fn vortex_prepare_blocks_without_vortex_write_feature() {
    let source_path = unique_path("vortex-ingest-source", "csv");
    let target_path = unique_path("vortex-ingest-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("schema_version", "shardloom.vortex_prepare.v1")));
    assert!(stdout.contains(&field("command_family", "prepared_source_backed_execution")));
    assert!(stdout.contains(&field("execution_mode", "prepared_vortex")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("ingress_route", "vortex_ingest")));
    assert!(stdout.contains(&field("vortex_ingest_performed", "false")));
    assert!(stdout.contains(&field("vortex_ingest_status", "blocked_feature_gate")));
    assert!(stdout.contains(&field(
        "vortex_ingest_blocker_id",
        "vortex_ingest.requires_vortex_write_feature"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_schema_version",
        "shardloom.vortex_scout_ingress.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_status",
        "blocked_feature_gate"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_quarantine_required", "false")));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_unsupported_diagnostic_code",
        "vortex_ingest.requires_vortex_write_feature"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_fallback_attempted", "false")));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_status",
        "blocked_feature_gate"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_strategy_admitted",
        "false"
    )));
    assert!(stdout.contains(&field("vortex_copy_budget_status", "blocked_feature_gate")));
    assert!(stdout.contains(&field("vortex_copy_budget_fallback_attempted", "false")));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_status",
        "blocked_feature_gate"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_vortex_first_decision",
        "blocked_until_vortex_or_shardloom_evidence"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field("prepared_state_created", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        !target_path.exists(),
        "feature-gated blocker must not write {}",
        target_path.display()
    );

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(not(feature = "vortex-write"))]
#[test]
fn vortex_prepare_native_source_blocks_with_structured_feature_gate() {
    let source_path = unique_path("vortex-ingest-native-source", "vortex");
    let target_path = unique_path("vortex-ingest-native-target", "vortex");
    fs::write(&source_path, b"not-a-real-vortex-file").expect("write native source placeholder");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--input-format",
            "vortex",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("schema_version", "shardloom.vortex_prepare.v1")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("vortex_ingest_status", "blocked_feature_gate")));
    assert!(stdout.contains(&field(
        "vortex_ingest_blocker_id",
        "vortex_ingest.requires_vortex_write_feature"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(!target_path.exists());

    fs::remove_file(source_path).expect("remove native source placeholder");
}

#[test]
fn vortex_prepare_missing_args_emits_json_error_without_stderr() {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["vortex-prepare", "--format", "json"])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("missing local source path"));
    assert!(stdout.contains("no fallback execution was attempted"));
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_prepare_writes_reopens_vortex_prepared_state() {
    let streaming = cfg!(feature = "universal-format-io");
    let source_path = unique_path("vortex-ingest-source", "csv");
    let target_path = unique_path("vortex-ingest-target", "vortex");
    fs::write(
        &source_path,
        "id,label,amount,active\n1,alpha,8,true\n2,beta,15,false\n",
    )
    .expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("schema_version", "shardloom.vortex_prepare.v1")));
    assert!(stdout.contains(&field("command_family", "prepared_source_backed_execution")));
    assert!(stdout.contains(&field("execution_mode", "prepared_vortex")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("source_io_performed", "true")));
    assert_inferred_adapter_evidence(
        &stdout,
        ExpectedAdapterEvidence {
            source_format: "csv",
            extension: ".csv",
            adapter_id: "local_csv_input_adapter",
            registry_entry_id: "shardloom.local_input_adapter.csv.v1",
            admitted_extensions: ".csv",
            feature_gate: "default",
            boundary: "local_text_source_state_adapter",
        },
    );
    assert!(stdout.contains(&field("source_adapter_id", "local_csv_input_adapter")));
    assert!(stdout.contains(&field("ingress_route", "vortex_ingest")));
    assert!(stdout.contains(&field("vortex_ingest_status", "prepared_state_created")));
    assert!(stdout.contains(&field("prepared_state_created", "true")));
    assert!(stdout.contains(&field("prepared_state_reuse_hit", "false")));
    assert!(stdout.contains(&field("timing_scope", "vortex_ingest_prepare_once")));
    assert!(stdout.contains(&field("certification_level", "ingest_certified")));
    assert!(stdout.contains(&field(
        "certification_status",
        "production_admitted_local_workflow_certified"
    )));
    assert!(stdout.contains(&field("preparation_included_in_timing", "true")));
    assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
    assert!(stdout.contains("\"key\":\"vortex_digest_millis\""));
    assert_ingest_array_build(&stdout, streaming);
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_schema_version",
        "shardloom.vortex_preparation_spine.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_schema_version",
        "shardloom.vortex_scout_ingress.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_status",
        "admitted_scout_ingress_clean"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_anomaly_count", "0")));
    assert!(stdout.contains(&field("vortex_scout_ingress_anomaly_families", "none")));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_schema_drift_status",
        "not_detected_no_prior_schema_baseline"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_unsupported_shape_status",
        "not_detected"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_quarantine_required", "false")));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_fallback_attempted", "false")));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_schema_version",
        "shardloom.vortex_layout_write_advisor.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_status",
        "admitted_local_layout_write_strategy"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_strategy_admitted",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_runtime_decision_applied",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_selected_strategy",
        "single_vortex_artifact_embedded_olap_layout_statistics"
    )));
    assert!(stdout.contains("\"key\":\"vortex_layout_write_advisor_strategy_decision_digest\""));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_provider_admitted",
        "true"
    )));
    assert!(stdout.contains(&field("vortex_layout_write_advisor_blocker", "none")));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_status",
        "admitted_local_preparation_spine"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_vortex_first_decision",
        if streaming {
            "use_vortex_native_provider"
        } else {
            "implement_shardloom_kernel"
        }
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_provider_kind",
        if streaming {
            "vortex_array_kernel"
        } else {
            "shardloom_kernel"
        }
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_source_surface",
        if streaming {
            "streaming_local_columnar_source_state_arrow_record_batches"
        } else {
            "local_text_source_state_scalar_rows"
        }
    )));
    assert!(stdout.contains(&field("vortex_preparation_spine_split_count", "1")));
    assert!(stdout.contains(&field("vortex_preparation_spine_source_split_count", "1")));
    assert!(
        stdout.contains(
            "\"key\":\"vortex_preparation_spine_source_split_refs\",\"value\":\"local-csv-"
        )
    );
    assert!(stdout.contains(":split=1:bytes=0.."));
    assert!(stdout.contains(":rows=0..2"));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_native_io_certificate_status",
        "certified_local_vortex_preparation_spine"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_prepared_artifact_segment_evidence_status",
        "writer_and_reopen_metadata_row_count_verified"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_schema_version",
        "shardloom.vortex_capillary_preparation.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_status",
        "not_requested_below_threshold"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_activation_result",
        "skipped"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_activation_reason",
        "below_threshold_small_local_fixture"
    )));
    assert!(stdout.contains(&field("vortex_capillary_preparation_task_count", "0")));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_native_io_certificate_status",
        "certified"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_pulseweave_status",
        "not_requested"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_pulseweave_runtime_decision_applied",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_no_standalone_lane_status",
        "not_requested_below_threshold_no_standalone_lane"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_schema_version",
        "shardloom.vortex_copy_budget.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_status",
        "admitted_scoped_buffer_reuse_with_unmeasured_segments"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_buffer_reuse_status",
        if streaming {
            "admitted_columnar_source_state_reuse_with_digest_and_row_count_proof"
        } else {
            "admitted_read_once_source_buffer_carry_with_digest_and_row_count_proof"
        }
    )));
    assert!(stdout.contains(&field("vortex_copy_budget_buffer_reuse_count", "1")));
    assert!(!stdout.contains("artifact_adjacent_manifest_created_after_miss"));
    assert!(stdout.contains(&field(
        "vortex_write_timing_split_schema_version",
        "shardloom.vortex_write_timing_split.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_context_reuse_status",
        if streaming {
            "artifact_source_conversion_writer_share_native_runtime"
        } else {
            "thread_local_write_context_opened_for_first_artifact"
        }
    )));
    assert!(stdout.contains(&field(
        "vortex_reopen_hot_path_status",
        "performed_metadata_row_count_for_ingest_certification"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_unsafe_lifetime_shortcut_status",
        "blocked_no_unsafe_lifetime_shortcuts"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field("input_row_count", "2")));
    assert!(stdout.contains(&field("source_columns", "id,label,amount,active")));
    assert!(stdout.contains(&field(
        "column_family_summary",
        "id:int64,label:utf8,amount:int64,active:boolean"
    )));
    assert!(stdout.contains(&field("writer_row_count", "2")));
    assert!(stdout.contains(&field("reopen_row_count", "2")));
    assert!(stdout.contains(&field(
        "reopen_verification_status",
        "reopen_metadata_row_count_verified"
    )));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "false")));
    assert!(stdout.contains(&field(
        "certification_status",
        "production_admitted_local_workflow_certified"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "local_workflow_runtime_supported"
    )));
    assert!(stdout.contains(&field(
        "local_workflow_input_row_cap",
        "none_synthetic_row_cap_disabled"
    )));
    assert!(stdout.contains(&field(
        "local_workflow_synthetic_input_row_cap_enabled",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_workflow_synthetic_output_row_cap_enabled",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_workflow_synthetic_source_byte_cap_enabled",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_workflow_synthetic_join_candidate_cap_enabled",
        "false"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(target_path.exists());
    assert!(fs::metadata(&target_path).expect("metadata").len() > 0);

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(target_path).expect("remove target vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_normalizes_nested_jsonl_as_utf8_payload() {
    let source_path = unique_path("vortex-ingest-nested-source", "jsonl");
    let target_path = unique_path("vortex-ingest-nested-target", "vortex");
    fs::write(&source_path, "{\"id\":1,\"payload\":{\"nested\":true}}\n")
        .expect("write nested source jsonl");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("vortex_ingest_status", "prepared_state_created")));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_status",
        "admitted_scout_ingress_clean"
    )));
    assert!(stdout.contains(&field("column_family_summary", "id:int64,payload:utf8")));
    assert!(stdout.contains(&field("vortex_scout_ingress_quarantine_required", "false")));
    assert!(stdout.contains(&field("vortex_scout_ingress_fallback_attempted", "false")));
    assert!(stdout.contains(&field("writer_row_count", "1")));
    assert!(stdout.contains(&field("reopen_row_count", "1")));
    assert!(target_path.exists());

    #[cfg(all(unix, feature = "vortex-local-primitives"))]
    assert_prepared_collect_values(
        &target_path,
        "SELECT id, payload FROM hits LIMIT 10",
        &serde_json::json!([{"id": 1, "payload": "{\"nested\":true}"}]),
    );

    fs::remove_file(source_path).expect("remove nested source jsonl");
    fs::remove_file(target_path).expect("remove normalized Vortex artifact");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_applies_append_only_differential_overlay() {
    let source_path = unique_path("vortex-ingest-delta-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n")
        .expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n3,gamma,21\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_schema_version",
        "shardloom.vortex_differential_preparation.v1"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_status",
        "admitted_append_only_delta_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_update_mode",
        "append_only"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_delta_row_count",
        "1"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_schema_compatibility_status",
        "compatible_source_schema_and_column_families"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_prepared_state_reuse_status",
        "base_prepared_state_reused_for_delta_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_base_reprepare_performed",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_delta_artifact_written",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_overlay_applied",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_native_io_certificate_status",
        "certified_local_vortex_differential_preparation_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_no_standalone_lane_status",
        "funnelled_through_vortex_ingest_source_state_to_prepared_state_delta_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_external_engine_invoked",
        "false"
    )));
    assert!(target_path.exists());
    assert!(delta_target_path.exists());

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source csv");
    fs::remove_file(target_path).expect("remove base vortex");
    fs::remove_file(delta_target_path).expect("remove delta vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_preserves_declared_input_format_for_extensionless_delta() {
    let source_path = unique_extensionless_path("vortex-ingest-delta-extensionless-base");
    let delta_source_path = unique_extensionless_path("vortex-ingest-delta-extensionless-change");
    let target_path = unique_path("vortex-ingest-delta-extensionless-base-target", "vortex");
    let delta_target_path =
        unique_path("vortex-ingest-delta-extensionless-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n")
        .expect("write extensionless base source csv");
    fs::write(&delta_source_path, "id,label,amount\n3,gamma,21\n")
        .expect("write extensionless delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--input-format",
            "csv",
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "csv")));
    assert!(stdout.contains(&field("source_format_inferred", "false")));
    assert!(stdout.contains(&field(
        "source_format_inference_kind",
        "declared_input_format"
    )));
    assert!(stdout.contains(&field(
        "source_format_inference_extension",
        "not_applicable"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_status",
        "admitted_append_only_delta_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_delta_row_count",
        "1"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_external_engine_invoked",
        "false"
    )));
    assert!(target_path.exists());
    assert!(delta_target_path.exists());

    fs::remove_file(source_path).expect("remove extensionless base source csv");
    fs::remove_file(delta_source_path).expect("remove extensionless delta source csv");
    fs::remove_file(target_path).expect("remove base vortex");
    fs::remove_file(delta_target_path).expect("remove delta vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)] // Keep the complete source-drift/overwrite lifecycle together.
fn vortex_prepare_source_drift_requires_explicit_overwrite_without_sidecars() {
    let root = unique_dir("vortex-ingest-auto-refinement");
    let source_path = root.join("input.csv");
    let target_path = root.join("prepared.vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n")
        .expect("write base source csv");

    let first = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("first vortex-prepare command runs");
    assert!(
        first.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let base_artifact = fs::read(&target_path).expect("read base artifact after first prepare");

    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("append source csv");
    let second = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("second vortex-prepare command runs");

    assert!(
        !second.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert!(
        second.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        fs::read(&target_path).expect("read base artifact after refused overwrite"),
        base_artifact,
        "source drift must not implicitly rewrite the prepared artifact"
    );

    let stdout = String::from_utf8(second.stdout).expect("stdout is utf8");
    assert!(stdout.contains("overwrite is disabled"), "{stdout}");

    let replacement = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("explicit replacement prepare runs");
    let stdout = String::from_utf8(replacement.stdout).expect("replacement stdout");
    assert!(replacement.status.success(), "{stdout}");
    assert!(stdout.contains(&field("writer_row_count", "3")));
    assert!(stdout.contains(&field("reopen_row_count", "3")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    #[cfg(all(unix, feature = "vortex-local-primitives"))]
    assert_prepared_collect_values(
        &target_path,
        "SELECT id, label, amount FROM hits LIMIT 10",
        &serde_json::json!([
            {"id": 1, "label": "alpha", "amount": 8},
            {"id": 2, "label": "beta", "amount": 15},
            {"id": 3, "label": "gamma", "amount": 21},
        ]),
    );
    let mut files = fs::read_dir(&root)
        .expect("artifact directory")
        .map(|entry| entry.expect("entry").file_name())
        .collect::<Vec<_>>();
    files.sort();
    assert_eq!(files, vec!["input.csv", "prepared.vortex"]);

    fs::remove_dir_all(root).expect("remove auto refinement root");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_blocks_update_mode_differential_overlay() {
    let source_path = unique_path("vortex-ingest-delta-update-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-update-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-update-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-update-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n1,alpha-prime,9\n")
        .expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--delta-update-mode",
            "update",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_status",
        "blocked_update_mode_policy"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_update_mode",
        "update"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_overlay_applied",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_external_engine_invoked",
        "false"
    )));

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source csv");
    if target_path.exists() {
        fs::remove_file(target_path).expect("remove base vortex");
    }
    if delta_target_path.exists() {
        fs::remove_file(delta_target_path).expect("remove delta vortex");
    }
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_rejects_differential_minimal_certification_before_writes() {
    let source_path = unique_path("vortex-ingest-delta-minimal-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-minimal-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-minimal-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-minimal-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n2,beta,15\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--certification-level",
            "ingest_minimal",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "vortex_ingest differential preparation requires ingest_certified replay evidence before any base or delta write"
    ));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(
        !target_path.exists(),
        "minimal-cert blocker must not write base {}",
        target_path.display()
    );
    assert!(
        !delta_target_path.exists(),
        "minimal-cert blocker must not write delta {}",
        delta_target_path.display()
    );

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source csv");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_rejects_shared_differential_target_before_writes() {
    let source_path = unique_path("vortex-ingest-delta-shared-target-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-shared-target-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-shared-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n2,beta,15\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "vortex_ingest differential preparation requires distinct base and delta targets"
    ));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(
        !target_path.exists(),
        "shared-target blocker must not write {}",
        target_path.display()
    );

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source csv");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_rejects_differential_overlay_with_mismatched_normalized_schema() {
    let source_path = unique_path("vortex-ingest-delta-scout-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-scout-change", "jsonl");
    let target_path = unique_path("vortex-ingest-delta-scout-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-scout-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(
        &delta_source_path,
        "{\"id\":2,\"payload\":{\"nested\":true}}\n",
    )
    .expect("write nested delta source jsonl");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_status",
        "blocked_schema_mismatch"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_schema_compatibility_status",
        "blocked_source_schema_or_column_family_mismatch"
    )));
    assert!(!stdout.contains("blocked_scout_ingress"));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        target_path.exists(),
        "base preparation remains available after overlay rejection {}",
        target_path.display()
    );
    assert!(
        delta_target_path.exists(),
        "normalized delta is prepared but its schema must not be admitted as an overlay {}",
        delta_target_path.display()
    );

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source jsonl");
    fs::remove_file(target_path).expect("remove base vortex");
    fs::remove_file(delta_target_path).expect("remove normalized delta vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_prepare_prepares_json_jsonl_and_ndjson_through_text_adapter_registry() {
    let cases = [
        (
            "json",
            "json",
            "local_json_input_adapter",
            "shardloom.local_input_adapter.json.v1",
            ".json",
            "[{\"id\":1,\"label\":\"alpha\",\"amount\":8,\"active\":true},{\"id\":2,\"label\":\"beta\",\"amount\":15,\"active\":false}]\n",
        ),
        (
            "jsonl",
            "jsonl",
            "local_jsonl_input_adapter",
            "shardloom.local_input_adapter.jsonl.v1",
            ".jsonl,.ndjson",
            "{\"id\":1,\"label\":\"alpha\",\"amount\":8,\"active\":true}\n{\"id\":2,\"label\":\"beta\",\"amount\":15,\"active\":false}\n",
        ),
        (
            "ndjson",
            "jsonl",
            "local_jsonl_input_adapter",
            "shardloom.local_input_adapter.jsonl.v1",
            ".jsonl,.ndjson",
            "{\"id\":1,\"label\":\"alpha\",\"amount\":8,\"active\":true}\n{\"id\":2,\"label\":\"beta\",\"amount\":15,\"active\":false}\n",
        ),
    ];

    for (extension, source_format, adapter_id, registry_entry_id, admitted_extensions, content) in
        cases
    {
        let source_path = unique_path("vortex-ingest-text-source", extension);
        let target_path = unique_path("vortex-ingest-text-target", "vortex");
        fs::write(&source_path, content).expect("write source");

        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "vortex-prepare",
                &source_path.display().to_string(),
                &target_path.display().to_string(),
                "--allow-overwrite",
                "--memory-gb",
                "4",
                "--max-parallelism",
                "2",
                "--format",
                "json",
            ])
            .output()
            .expect("vortex-prepare command runs");

        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains("\"command\":\"vortex-prepare\""));
        assert!(stdout.contains("\"status\":\"success\""));
        assert_inferred_adapter_evidence(
            &stdout,
            ExpectedAdapterEvidence {
                source_format,
                extension: &format!(".{extension}"),
                adapter_id,
                registry_entry_id,
                admitted_extensions,
                feature_gate: "default",
                boundary: "local_text_source_state_adapter",
            },
        );
        assert!(stdout.contains(&field("ingress_route", "vortex_ingest")));
        assert!(stdout.contains(&field("vortex_ingest_status", "prepared_state_created")));
        let streaming = cfg!(feature = "universal-format-io");
        if source_format == "json" {
            assert!(stdout.contains(&field(
                "source_read_buffer_carry_status",
                "read_once_buffer_carried_to_text_parser"
            )));
            assert!(stdout.contains(&field(
                "source_read_mmap_eligibility_status",
                "not_used_owned_text_buffer_default"
            )));
        }
        assert!(stdout.contains(&field(
            "source_state_materialization_layout",
            if !streaming {
                "scalar_row_map"
            } else if source_format == "json" {
                "whole_json_typed_columns_with_batched_writer"
            } else {
                "inferred_text_to_streaming_arrow_record_batch_source_state"
            }
        )));
        assert!(stdout.contains(&field(
            "source_state_parse_normalization",
            if !streaming {
                "local_text_to_scalar_rows"
            } else if source_format == "json" {
                "json_adapter_to_whole_typed_columns"
            } else {
                "inferred_text_to_record_batch_stream"
            }
        )));
        assert!(stdout.contains(&field(
            "source_state_columnar_preserved",
            if streaming { "true" } else { "false" }
        )));
        assert!(stdout.contains(&field(
            "source_state_record_batch_count",
            if streaming { "1" } else { "0" }
        )));
        assert!(stdout.contains(&field("source_columns", "id,label,amount,active")));
        assert!(stdout.contains(&field("input_row_count", "2")));
        assert!(stdout.contains(&field(
            "column_family_summary",
            "id:int64,label:utf8,amount:int64,active:boolean"
        )));
        assert!(stdout.contains(&field("writer_row_count", "2")));
        assert!(stdout.contains(&field("reopen_row_count", "2")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(target_path.exists());

        #[cfg(all(unix, feature = "vortex-local-primitives"))]
        assert_prepared_collect_values(
            &target_path,
            "SELECT id, label, amount, active FROM hits LIMIT 10",
            &serde_json::json!([
                {"id": 1, "label": "alpha", "amount": 8, "active": true},
                {"id": 2, "label": "beta", "amount": 15, "active": false},
            ]),
        );

        fs::remove_file(source_path).expect("remove source");
        fs::remove_file(target_path).expect("remove target vortex");
    }
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_minimal_certification_skips_reopen_scan() {
    let source_path = unique_path("vortex-ingest-minimal-source", "csv");
    let target_path = unique_path("vortex-ingest-minimal-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--certification-level",
            "ingest_minimal",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("certification_level", "ingest_minimal")));
    assert!(stdout.contains(&field(
        "certification_status",
        "minimal_ingest_evidence_reported"
    )));
    assert!(stdout.contains(&field("writer_row_count", "2")));
    assert!(stdout.contains(&field("reopen_row_count", "2")));
    assert!(stdout.contains(&field(
        "reopen_verification_status",
        "not_performed_ingest_minimal"
    )));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "false")));
    assert!(stdout.contains(&field(
        "native_io_certificate_status",
        "minimal_local_vortex_ingest_digest_only"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_status",
        "not_requested_below_threshold"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_activation_result",
        "skipped"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_pulseweave_status",
        "not_requested"
    )));
    assert!(stdout.contains(&field(
        "vortex_capillary_preparation_pulseweave_runtime_decision_applied",
        "false"
    )));
    assert!(stdout.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "vortex_ingest_output_workspace_path_safety_status",
        "enforced"
    )));
    assert!(stdout.contains(&field("vortex_ingest_output_within_workspace", "true")));
    assert!(stdout.contains(&field("vortex_ingest_output_commit_status", "committed")));
    assert!(stdout.contains(&field(
        "vortex_ingest_output_cleanup_status",
        "no_staging_artifacts_remaining"
    )));
    assert!(stdout.contains(&field("vortex_ingest_output_fallback_attempted", "false")));
    assert!(target_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(target_path).expect("remove target vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_prepare_full_replay_requires_output_replay_evidence() {
    let source_path = unique_path("vortex-ingest-full-replay-source", "csv");
    let target_path = unique_path("vortex-ingest-full-replay-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--certification-level",
            "ingest_full_replay",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(
        stdout.contains("ingest_full_replay requires downstream result replay/output evidence")
    );
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(
        !target_path.exists(),
        "full replay blocker must not write {}",
        target_path.display()
    );

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_prepare_preserves_columnar_source_state_for_parquet() {
    let source_path = unique_path("vortex-ingest-columnar-source", "parquet");
    let target_path = unique_path("vortex-ingest-columnar-target", "vortex");
    write_parquet_vortex_ingest_source(&source_path);

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-prepare",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--memory-gb",
            "4",
            "--max-parallelism",
            "2",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-prepare command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-prepare\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "parquet")));
    assert!(stdout.contains(&field("source_state_read_plan", "full_columns")));
    assert!(stdout.contains(&field(
        "source_state_materialization_layout",
        "streaming_arrow_record_batch_columnar_source_state"
    )));
    assert!(stdout.contains(&field(
        "source_state_parse_normalization",
        "structured_reader_to_streaming_arrow_record_batches"
    )));
    assert!(stdout.contains(&field("source_state_columnar_preserved", "true")));
    assert!(stdout.contains(&field("source_state_record_batch_count", "1")));
    assert!(stdout.contains(&field(
        "source_state_materialized_columns",
        "id,label,amount"
    )));
    assert!(stdout.contains(&field(
        "source_state_reader_projection_columns",
        "id,label,amount"
    )));
    assert!(stdout.contains(&field("compatibility_parse_millis", "0")));
    assert!(stdout.contains("\"key\":\"source_to_columnar_millis\""));
    assert!(stdout.contains("\"key\":\"vortex_array_build_millis\""));
    assert_ingest_array_build(&stdout, true);
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_status",
        "admitted_local_preparation_spine"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_vortex_first_decision",
        "use_vortex_native_provider"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_provider_kind",
        "vortex_array_kernel"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_runtime_decision_applied",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_selected_strategy",
        "single_vortex_artifact_embedded_olap_layout_statistics"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_provider_admitted",
        "true"
    )));
    assert!(stdout.contains(&field("vortex_layout_write_advisor_blocker", "none")));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_source_surface",
        "streaming_local_columnar_source_state_arrow_record_batches"
    )));
    assert!(stdout.contains(&field("vortex_preparation_spine_split_count", "1")));
    assert!(stdout.contains(&field("vortex_preparation_spine_source_split_count", "1")));
    assert!(stdout.contains(
        "\"key\":\"vortex_preparation_spine_source_split_refs\",\"value\":\"local-parquet-"
    ));
    assert!(stdout.contains(":split=1:bytes=0.."));
    assert!(stdout.contains(":rows=0..3"));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_native_io_certificate_status",
        "certified_local_vortex_preparation_spine"
    )));
    assert!(stdout.contains(&field("input_row_count", "3")));
    assert!(stdout.contains(&field("writer_row_count", "3")));
    assert!(stdout.contains(&field("reopen_row_count", "3")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_parquet_streaming_arrow_record_batch_columnar_source_state_to_vortex_prepared_state"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(target_path.exists());

    fs::remove_file(source_path).expect("remove source parquet");
    fs::remove_file(target_path).expect("remove target vortex");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_prepare_preserves_columnar_source_state_for_all_structured_formats() {
    let cases: [StructuredVortexIngestCase; 4] = [
        (
            "parquet",
            "parquet",
            "local_parquet_input_adapter",
            "shardloom.local_input_adapter.parquet.v1",
            ".parquet",
            "Parquet",
            write_parquet_smoke_source,
        ),
        (
            "arrow",
            "arrow_ipc",
            "local_arrow_ipc_input_adapter",
            "shardloom.local_input_adapter.arrow_ipc.v1",
            ".arrow,.ipc,.feather",
            "Arrow IPC",
            write_arrow_ipc_smoke_source,
        ),
        (
            "avro",
            "avro",
            "local_avro_input_adapter",
            "shardloom.local_input_adapter.avro.v1",
            ".avro",
            "Avro",
            write_avro_smoke_source,
        ),
        (
            "orc",
            "orc",
            "local_orc_input_adapter",
            "shardloom.local_input_adapter.orc.v1",
            ".orc",
            "ORC",
            write_orc_smoke_source,
        ),
    ];

    for (
        extension,
        source_format,
        adapter_id,
        registry_entry_id,
        admitted_extensions,
        _label,
        write_source,
    ) in cases
    {
        let source_path = unique_path("vortex-ingest-structured-source", extension);
        let target_path = unique_path("vortex-ingest-structured-target", "vortex");
        write_source(&source_path);

        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "vortex-prepare",
                &source_path.display().to_string(),
                &target_path.display().to_string(),
                "--allow-overwrite",
                "--memory-gb",
                "4",
                "--max-parallelism",
                "2",
                "--format",
                "json",
            ])
            .output()
            .expect("vortex-prepare command runs");

        assert!(
            output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains("\"command\":\"vortex-prepare\""));
        assert!(stdout.contains("\"status\":\"success\""));
        assert_inferred_adapter_evidence(
            &stdout,
            ExpectedAdapterEvidence {
                source_format,
                extension: &format!(".{extension}"),
                adapter_id,
                registry_entry_id,
                admitted_extensions,
                feature_gate: "universal-format-io",
                boundary: "local_columnar_source_state_adapter",
            },
        );
        assert!(stdout.contains(&field("source_state_read_plan", "full_columns")));
        assert!(stdout.contains(&field(
            "source_state_materialization_layout",
            "streaming_arrow_record_batch_columnar_source_state"
        )));
        assert!(stdout.contains(&field(
            "source_state_parse_normalization",
            "structured_reader_to_streaming_arrow_record_batches"
        )));
        assert!(stdout.contains(&field("source_state_columnar_preserved", "true")));
        assert!(stdout.contains(&field("source_state_record_batch_count", "1")));
        assert!(stdout.contains(&field(
            "source_state_materialized_columns",
            "id,label,amount,active"
        )));
        assert!(stdout.contains(&field(
            "source_state_reader_projection_columns",
            "id,label,amount,active"
        )));
        assert!(stdout.contains(&field("compatibility_parse_millis", "0")));
        assert_ingest_array_build(&stdout, true);
        assert!(stdout.contains(&field(
            "vortex_preparation_spine_vortex_first_decision",
            "use_vortex_native_provider"
        )));
        assert!(stdout.contains(&field(
            "vortex_preparation_spine_provider_kind",
            "vortex_array_kernel"
        )));
        assert!(stdout.contains(&field(
            "vortex_preparation_spine_source_surface",
            "streaming_local_columnar_source_state_arrow_record_batches"
        )));
        assert!(stdout.contains(&field("vortex_preparation_spine_split_count", "1")));
        assert!(stdout.contains(&field("vortex_preparation_spine_source_split_count", "1")));
        assert!(stdout.contains(&format!(
            "\"key\":\"vortex_preparation_spine_source_split_refs\",\"value\":\"local-{source_format}-"
        )));
        assert!(stdout.contains(&field("input_row_count", "4")));
        assert!(stdout.contains(&field(
            "column_family_summary",
            "id:int64,label:utf8,amount:int64,active:boolean"
        )));
        assert!(stdout.contains(&field("writer_row_count", "4")));
        assert!(stdout.contains(&field("reopen_row_count", "4")));
        assert!(stdout.contains(&field(
            "materialization_boundary",
            &format!("local_{source_format}_streaming_arrow_record_batch_columnar_source_state_to_vortex_prepared_state")
        )));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(target_path.exists());

        fs::remove_file(source_path).expect("remove source");
        fs::remove_file(target_path).expect("remove target vortex");
    }
}

#[cfg(feature = "universal-format-io")]
fn write_parquet_smoke_source(path: &std::path::Path) {
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma", "delta"])),
            Arc::new(Int64Array::from(vec![8, 15, 0, 21])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
        ],
    )
    .expect("record batch");
    let file = File::create(path).expect("create parquet source");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
fn write_parquet_vortex_ingest_source(path: &std::path::Path) {
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            Arc::new(Float64Array::from(vec![8.0, 15.5, 21.25])),
        ],
    )
    .expect("record batch");
    let file = File::create(path).expect("create parquet vortex ingest source");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write parquet batch");
    writer.close().expect("close parquet writer");
}

#[cfg(feature = "universal-format-io")]
fn write_arrow_ipc_smoke_source(path: &std::path::Path) {
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma", "delta"])),
            Arc::new(Int64Array::from(vec![8, 15, 0, 21])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
        ],
    )
    .expect("record batch");
    let file = File::create(path).expect("create arrow ipc source");
    let mut writer = FileWriter::try_new(file, &schema).expect("arrow ipc writer");
    writer.write(&batch).expect("write arrow ipc batch");
    writer.finish().expect("finish arrow ipc writer");
}

#[cfg(feature = "universal-format-io")]
fn write_avro_smoke_source(path: &std::path::Path) {
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma", "delta"])),
            Arc::new(Int64Array::from(vec![8, 15, 0, 21])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
        ],
    )
    .expect("record batch");
    let file = File::create(path).expect("create avro source");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer.write(&batch).expect("write avro batch");
    writer.finish().expect("finish avro writer");
}

#[cfg(feature = "universal-format-io")]
fn write_orc_smoke_source(path: &std::path::Path) {
    use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use orc_rust::ArrowWriterBuilder;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["alpha", "beta", "gamma", "delta"])),
            Arc::new(Int64Array::from(vec![8, 15, 0, 21])),
            Arc::new(BooleanArray::from(vec![true, false, true, true])),
        ],
    )
    .expect("record batch");
    let file = File::create(path).expect("create orc source");
    let mut writer = ArrowWriterBuilder::new(file, schema)
        .try_build()
        .expect("orc writer");
    writer.write(&batch).expect("write orc batch");
    writer.close().expect("close orc writer");
}
