use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

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
fn escaped_field(key: &str, value: &str) -> String {
    field(key, &value.replace('\\', "\\\\"))
}

fn run_sql_local_source_smoke_json(statement: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn assert_required_source_state_projection_evidence(stdout: &str, pushdown_status: &str) {
    assert!(stdout.contains(&field("source_state_read_plan", "required_columns")));
    assert!(stdout.contains(&field("source_state_requested_columns", "amount,id,label")));
    assert!(stdout.contains(&field(
        "source_state_projection_pushdown_status",
        pushdown_status
    )));
    assert!(stdout.contains(&field("source_state_materialized_column_count", "3")));
    assert!(stdout.contains(&field(
        "source_state_materialized_columns",
        "id,label,amount"
    )));
    assert!(stdout.contains(&field("source_state_reader_projection_column_count", "3")));
    assert!(stdout.contains(&field(
        "source_state_reader_projection_columns",
        "id,label,amount"
    )));
    assert!(stdout.contains(&field("source_state_pruned_column_count", "1")));
    assert!(stdout.contains(&field("source_state_column_pruning_applied", "true")));
}

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

#[cfg(feature = "universal-format-io")]
fn assert_zero_column_reader_projection_count_star<F>(
    extension: &str,
    source_format: &str,
    reader_projection_column_count: &str,
    reader_projection_columns: &str,
    write_source: F,
) where
    F: FnOnce(&Path),
{
    let source_path = unique_path("sql-local-source-count-star-projection", extension);
    write_source(&source_path);

    let statement = format!("SELECT count(*) FROM '{}' LIMIT 1", source_path.display());
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field("source_format", source_format)));
    assert!(stdout.contains(&field("source_state_read_plan", "required_columns")));
    assert!(stdout.contains(&field("source_state_requested_columns", "none")));
    assert!(stdout.contains(&field(
        "source_state_projection_pushdown_status",
        "reader_level_projection"
    )));
    assert!(stdout.contains(&field("source_state_materialized_column_count", "0")));
    assert!(stdout.contains(&field("source_state_materialized_columns", "none")));
    assert!(stdout.contains(&field(
        "source_state_reader_projection_column_count",
        reader_projection_column_count
    )));
    assert!(stdout.contains(&field(
        "source_state_reader_projection_columns",
        reader_projection_columns
    )));
    assert!(stdout.contains(&field("source_state_pruned_column_count", "4")));
    assert!(stdout.contains(&field("source_state_column_pruning_applied", "true")));
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*)")));
    assert!(stdout.contains("\"result_jsonl\",\"value\":\"{\\\"count_all\\\":4}\\n\""));

    fs::remove_file(source_path).expect("remove source");
}

#[cfg(not(feature = "vortex-write"))]
#[test]
fn vortex_ingest_smoke_blocks_without_vortex_write_feature() {
    let source_path = unique_path("vortex-ingest-source", "csv");
    let target_path = unique_path("vortex-ingest-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("schema_version", "shardloom.vortex_ingest_smoke.v1")));
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

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_ingest_smoke_writes_reopens_vortex_prepared_state() {
    let source_path = unique_path("vortex-ingest-source", "csv");
    let target_path = unique_path("vortex-ingest-target", "vortex");
    fs::write(
        &source_path,
        "id,label,amount,active\n1,alpha,8,true\n2,beta,15,false\n",
    )
    .expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("schema_version", "shardloom.vortex_ingest_smoke.v1")));
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
    assert!(stdout.contains(&field("certification_status", "fixture_smoke_certified")));
    assert!(stdout.contains(&field("preparation_included_in_timing", "true")));
    assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
    assert!(stdout.contains("\"key\":\"vortex_digest_millis\""));
    assert!(stdout.contains(&field(
        "vortex_array_build_provider_kind",
        "shardloom_kernel"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_provider_surface",
        "shardloom_scalar_rows_to_vortex_struct"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_strategy",
        "scalar_rows_to_vortex_struct"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_input_layout",
        "materialized_rows"
    )));
    assert!(stdout.contains(&field("vortex_array_build_record_batch_count", "0")));
    assert!(stdout.contains(&field(
        "vortex_array_build_manual_scalar_copy_avoided",
        "false"
    )));
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
        "single_local_vortex_artifact"
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
        "implement_shardloom_kernel"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_provider_kind",
        "shardloom_kernel"
    )));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_source_surface",
        "local_text_source_state_scalar_rows"
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
        "writer_and_reopen_row_count_verified"
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
        "reported_copy_budget_with_unmeasured_segments"
    )));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_buffer_reuse_status",
        "blocked_until_correctness_parity"
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
        "reopen_row_count_verified"
    )));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(target_path.exists());
    assert!(fs::metadata(&target_path).expect("metadata").len() > 0);

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(target_path).expect("remove target vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_ingest_smoke_blocks_nested_jsonl_with_scout_quarantine_plan() {
    let source_path = unique_path("vortex-ingest-nested-source", "jsonl");
    let target_path = unique_path("vortex-ingest-nested-target", "vortex");
    fs::write(&source_path, "{\"id\":1,\"payload\":{\"nested\":true}}\n")
        .expect("write nested source jsonl");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("vortex_ingest_status", "blocked_scout_ingress")));
    assert!(stdout.contains(&field(
        "vortex_ingest_blocker_id",
        "vortex_scout_ingress.unsupported_nested_shape"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_status",
        "blocked_unsupported_nested_shape"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_anomaly_families",
        "unsupported_nested_shape"
    )));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_unsupported_shape_status",
        "blocked_unsupported_nested_shape"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_quarantine_required", "true")));
    assert!(stdout.contains(&field(
        "vortex_scout_ingress_quarantine_output_plan_status",
        "planned_not_emitted_no_quarantine_sink_requested"
    )));
    assert!(stdout.contains(&field("vortex_scout_ingress_fallback_attempted", "false")));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_status",
        "blocked_layout_write_strategy"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_strategy_admitted",
        "false"
    )));
    assert!(stdout.contains(&field("vortex_copy_budget_status", "blocked_copy_budget")));
    assert!(stdout.contains(&field(
        "vortex_copy_budget_prepared_state_id",
        "not_created_scout_ingress_blocked"
    )));
    assert!(stdout.contains(&field("vortex_ingest_performed", "false")));
    assert!(stdout.contains(&field("prepared_state_created", "false")));
    assert!(
        !target_path.exists(),
        "scout blocker must not write {}",
        target_path.display()
    );

    fs::remove_file(source_path).expect("remove nested source jsonl");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_ingest_smoke_applies_append_only_differential_overlay() {
    let source_path = unique_path("vortex-ingest-delta-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n")
        .expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n3,gamma,21\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_preserves_declared_input_format_for_extensionless_delta() {
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
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--input-format",
            "csv",
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_automatically_refines_append_only_source_drift() {
    let root = unique_dir("vortex-ingest-auto-refinement");
    let source_path = root.join("input.csv");
    let target_path = root.join("prepared.vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n")
        .expect("write base source csv");

    let first = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("first vortex-ingest-smoke command runs");
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
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("second vortex-ingest-smoke command runs");

    assert!(
        second.status.success(),
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
        fs::read(&target_path).expect("read base artifact after refinement"),
        base_artifact,
        "automatic refinement must not rewrite the base prepared artifact"
    );

    let stdout = String::from_utf8(second.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "vortex_ingest_status",
        "prepared_state_refined_from_append_only_delta"
    )));
    assert!(stdout.contains(&field("vortex_ingest_base_reprepare_performed", "false")));
    assert!(stdout.contains(&field("vortex_ingest_delta_prepare_performed", "true")));
    assert!(stdout.contains(&field("prepared_state_refined", "true")));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_refinement_status",
        "admitted_append_only_refinement"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_automatic_detection_status",
        "append_only_delta_detected"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_status",
        "admitted_append_only_delta_overlay"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_base_row_count",
        "2"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_delta_row_count",
        "1"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_refinement_manifest_written",
        "true"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_overlay_consumer_family",
        "count"
    )));
    assert!(stdout.contains(&field(
        "vortex_differential_preparation_overlay_consumer_status",
        "admitted_base_manifest_plus_delta_reopen_row_count"
    )));
    assert!(stdout.contains(&field("refined_row_count", "3")));
    assert!(stdout.contains(&field("overlay_consumer_row_count", "3")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(root).expect("remove auto refinement root");
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_ingest_smoke_blocks_update_mode_differential_overlay() {
    let source_path = unique_path("vortex-ingest-delta-update-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-update-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-update-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-update-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n1,alpha-prime,9\n")
        .expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--delta-update-mode",
            "update",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_rejects_differential_minimal_certification_before_writes() {
    let source_path = unique_path("vortex-ingest-delta-minimal-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-minimal-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-minimal-base-target", "vortex");
    let delta_target_path = unique_path("vortex-ingest-delta-minimal-change-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n2,beta,15\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--certification-level",
            "ingest_minimal",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_rejects_shared_differential_target_before_writes() {
    let source_path = unique_path("vortex-ingest-delta-shared-target-base", "csv");
    let delta_source_path = unique_path("vortex-ingest-delta-shared-target-change", "csv");
    let target_path = unique_path("vortex-ingest-delta-shared-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n").expect("write base source csv");
    fs::write(&delta_source_path, "id,label,amount\n2,beta,15\n").expect("write delta source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_reports_delta_source_for_differential_scout_blocker() {
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
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--delta-source",
            &delta_source_path.display().to_string(),
            "--delta-target",
            &delta_target_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains("differential delta source"));
    assert!(stdout.contains(&escaped_field(
        "source_path",
        &delta_source_path.display().to_string()
    )));
    assert!(stdout.contains(&escaped_field(
        "target_vortex_path",
        &delta_target_path.display().to_string()
    )));
    assert!(stdout.contains(&field("vortex_ingest_status", "blocked_scout_ingress")));
    assert!(stdout.contains(&field(
        "vortex_ingest_blocker_id",
        "vortex_scout_ingress.unsupported_nested_shape"
    )));
    assert!(stdout.contains(&field("vortex_ingest_performed", "false")));
    assert!(stdout.contains(&field("prepared_state_created", "false")));
    assert!(
        target_path.exists(),
        "delta scout blocker happens after base preparation {}",
        target_path.display()
    );
    assert!(
        !delta_target_path.exists(),
        "delta scout blocker must not write delta {}",
        delta_target_path.display()
    );

    fs::remove_file(source_path).expect("remove base source csv");
    fs::remove_file(delta_source_path).expect("remove delta source jsonl");
    fs::remove_file(target_path).expect("remove base vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)]
fn vortex_ingest_smoke_prepares_json_jsonl_and_ndjson_through_text_adapter_registry() {
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
                "vortex-ingest-smoke",
                &source_path.display().to_string(),
                &target_path.display().to_string(),
                "--allow-overwrite",
                "--format",
                "json",
            ])
            .output()
            .expect("vortex-ingest-smoke command runs");

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
        assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
        assert!(stdout.contains(&field(
            "source_state_materialization_layout",
            "scalar_row_map"
        )));
        assert!(stdout.contains(&field(
            "source_state_parse_normalization",
            "local_text_to_scalar_rows"
        )));
        assert!(stdout.contains(&field("source_state_columnar_preserved", "false")));
        assert!(stdout.contains(&field("source_state_record_batch_count", "0")));
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

        fs::remove_file(source_path).expect("remove source");
        fs::remove_file(target_path).expect("remove target vortex");
    }
}

#[cfg(feature = "vortex-write")]
#[test]
fn vortex_ingest_smoke_minimal_certification_skips_reopen_scan() {
    let source_path = unique_path("vortex-ingest-minimal-source", "csv");
    let target_path = unique_path("vortex-ingest-minimal-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--certification-level",
            "ingest_minimal",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("certification_level", "ingest_minimal")));
    assert!(stdout.contains(&field(
        "certification_status",
        "minimal_ingest_evidence_reported"
    )));
    assert!(stdout.contains(&field("writer_row_count", "2")));
    assert!(stdout.contains(&field("reopen_row_count", "0")));
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
fn vortex_ingest_smoke_full_replay_requires_output_replay_evidence() {
    let source_path = unique_path("vortex-ingest-full-replay-source", "csv");
    let target_path = unique_path("vortex-ingest-full-replay-target", "vortex");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--certification-level",
            "ingest_full_replay",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
fn vortex_ingest_smoke_preserves_columnar_source_state_for_parquet() {
    let source_path = unique_path("vortex-ingest-columnar-source", "parquet");
    let target_path = unique_path("vortex-ingest-columnar-target", "vortex");
    write_parquet_vortex_ingest_source(&source_path);

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-ingest-smoke",
            &source_path.display().to_string(),
            &target_path.display().to_string(),
            "--allow-overwrite",
            "--format",
            "json",
        ])
        .output()
        .expect("vortex-ingest-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "parquet")));
    assert!(stdout.contains(&field("source_state_read_plan", "full_columns")));
    assert!(stdout.contains(&field(
        "source_state_materialization_layout",
        "arrow_record_batch_columnar_source_state"
    )));
    assert!(stdout.contains(&field(
        "source_state_parse_normalization",
        "structured_reader_to_arrow_record_batches"
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
    assert!(stdout.contains(&field(
        "vortex_array_build_provider_kind",
        "vortex_array_kernel"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_provider_surface",
        "ArrayRef::from_arrow(RecordBatch)"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_strategy",
        "vortex_from_arrow_record_batch"
    )));
    assert!(stdout.contains(&field(
        "vortex_array_build_input_layout",
        "arrow_record_batch_columnar_source_state"
    )));
    assert!(stdout.contains(&field("vortex_array_build_record_batch_count", "1")));
    assert!(stdout.contains(&field(
        "vortex_array_build_manual_scalar_copy_avoided",
        "true"
    )));
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
        "single_local_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_advisor_provider_admitted",
        "true"
    )));
    assert!(stdout.contains(&field("vortex_layout_write_advisor_blocker", "none")));
    assert!(stdout.contains(&field(
        "vortex_preparation_spine_source_surface",
        "local_columnar_source_state_arrow_record_batches"
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
        "local_parquet_arrow_record_batch_columnar_source_state_to_vortex_prepared_state"
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
fn vortex_ingest_smoke_preserves_columnar_source_state_for_all_structured_formats() {
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
                "vortex-ingest-smoke",
                &source_path.display().to_string(),
                &target_path.display().to_string(),
                "--allow-overwrite",
                "--format",
                "json",
            ])
            .output()
            .expect("vortex-ingest-smoke command runs");

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
        assert!(stdout.contains("\"command\":\"vortex-ingest-smoke\""));
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
            "arrow_record_batch_columnar_source_state"
        )));
        assert!(stdout.contains(&field(
            "source_state_parse_normalization",
            "structured_reader_to_arrow_record_batches"
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
        assert!(stdout.contains(&field(
            "vortex_array_build_provider_kind",
            "vortex_array_kernel"
        )));
        assert!(stdout.contains(&field(
            "vortex_array_build_provider_surface",
            "ArrayRef::from_arrow(RecordBatch)"
        )));
        assert!(stdout.contains(&field(
            "vortex_array_build_strategy",
            "vortex_from_arrow_record_batch"
        )));
        assert!(stdout.contains(&field(
            "vortex_array_build_input_layout",
            "arrow_record_batch_columnar_source_state"
        )));
        assert!(stdout.contains(&field("vortex_array_build_record_batch_count", "1")));
        assert!(stdout.contains(&field(
            "vortex_array_build_manual_scalar_copy_avoided",
            "true"
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
            "vortex_preparation_spine_source_surface",
            "local_columnar_source_state_arrow_record_batches"
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
            &format!("local_{source_format}_arrow_record_batch_columnar_source_state_to_vortex_prepared_state")
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
fn write_nested_arrow_ipc_smoke_source(path: &std::path::Path) {
    use arrow_array::{
        Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray,
        types::Int64Type,
    };
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};

    let values = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1), Some(2), None]),
        None,
        Some(vec![]),
    ])) as ArrayRef;
    let payload = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("label", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("empty")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("amount", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(8), Some(15), None])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("values", values.data_type().clone(), true),
        Field::new("payload", payload.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3])), values, payload],
    )
    .expect("nested record batch");
    let file = File::create(path).expect("create nested arrow ipc source");
    let mut writer = FileWriter::try_new(file, &schema).expect("arrow ipc writer");
    writer.write(&batch).expect("write nested arrow ipc batch");
    writer.finish().expect("finish nested arrow ipc writer");
}

#[cfg(feature = "universal-format-io")]
fn write_binary_arrow_ipc_smoke_source(path: &std::path::Path) {
    use arrow_array::{BinaryArray, Int64Array, RecordBatch};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Binary, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(BinaryArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                None,
                Some(&[0x01, 0x02][..]),
                Some(&b"raw"[..]),
            ])),
        ],
    )
    .expect("binary record batch");
    let file = File::create(path).expect("create binary arrow ipc source");
    let mut writer = FileWriter::try_new(file, &schema).expect("arrow ipc writer");
    writer.write(&batch).expect("write binary arrow ipc batch");
    writer.finish().expect("finish binary arrow ipc writer");
}

#[cfg(feature = "universal-format-io")]
fn write_all_null_binary_arrow_ipc_smoke_source(path: &std::path::Path) {
    use arrow_array::{BinaryArray, RecordBatch};
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Binary,
        true,
    )]));
    let values: Vec<Option<&[u8]>> = vec![None, None, None];
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from(values))],
    )
    .expect("all-null binary record batch");
    let file = File::create(path).expect("create all-null binary arrow ipc source");
    let mut writer = FileWriter::try_new(file, &schema).expect("arrow ipc writer");
    writer
        .write(&batch)
        .expect("write all-null binary arrow ipc batch");
    writer
        .finish()
        .expect("finish all-null binary arrow ipc writer");
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

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_csv_projection_filter_limit_without_fallback() {
    let source_path = unique_path("sql-local-source", "csv");
    fs::write(
        &source_path,
        "\u{feff}id,label,amount,active\n1,alpha,8,true\n2,beta,15,false\n3,gamma,,true\n4,delta,21,true\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.sql_local_source_smoke.v1"
    )));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
    assert!(stdout.contains(&field("execution_mode", "direct_compatibility_transient")));
    assert!(stdout.contains(&field("engine_mode", "batch")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("sql_parser_executed", "true")));
    assert!(stdout.contains(&field("sql_binder_executed", "true")));
    assert!(stdout.contains(&field("sql_planner_executed", "true")));
    assert!(stdout.contains(&field("sql_runtime_execution", "true")));
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
    assert!(stdout.contains(&field(
        "source_state_contract_schema_version",
        "shardloom.local_source_state.v1"
    )));
    assert!(stdout.contains(&field(
        "local_input_adapter_registry_version",
        "shardloom.local_input_adapter_registry.v1"
    )));
    assert_required_source_state_projection_evidence(&stdout, "local_text_parser_column_pruning");
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("limit", "1")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(&field("projected_columns", "id,label")));
    assert!(stdout.contains(&field("predicate_operator_family", "comparison")));
    assert!(stdout.contains(&field(
        "pushdown_status",
        "not_applicable_local_csv_transient"
    )));
    assert!(stdout.contains(&field(
        "source_native_io_certificate_status",
        "scoped_compatibility_import_certificate"
    )));
    assert!(stdout.contains(&field("execution_certificate_status", "certified")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_csv_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "not_requested"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("fallback_execution_allowed", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(&field("performance_claim_allowed", "false")));
    assert!(stdout.contains(&field("production_claim_allowed", "false")));
    assert!(stdout.contains(&field("sql_dataframe_runtime_claim_allowed", "false")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains("\"plan_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"source_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"fnv64:"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_executes_parquet_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source", "parquet");
    write_parquet_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert_inferred_adapter_evidence(
        &stdout,
        ExpectedAdapterEvidence {
            source_format: "parquet",
            extension: ".parquet",
            adapter_id: "local_parquet_input_adapter",
            registry_entry_id: "shardloom.local_input_adapter.parquet.v1",
            admitted_extensions: ".parquet",
            feature_gate: "universal-format-io",
            boundary: "local_columnar_source_state_adapter",
        },
    );
    assert!(stdout.contains(&field("source_adapter_status", "smoke_supported")));
    assert!(stdout.contains(&field("ingress_route", "direct_transient")));
    assert!(stdout.contains(&field(
        "vortex_ingest_status",
        "not_performed_direct_transient"
    )));
    assert!(stdout.contains(&field(
        "selected_execution_mode",
        "direct_compatibility_transient"
    )));
    assert!(stdout.contains(&field("timing_scope", "direct_one_shot")));
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "id,label")));
    assert_required_source_state_projection_evidence(&stdout, "reader_level_projection");
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.parquet.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_parquet_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
        )
    );

    fs::remove_file(source_path).expect("remove source parquet");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_executes_arrow_ipc_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source", "arrow");
    write_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "arrow_ipc")));
    assert!(stdout.contains(&field("source_adapter_id", "local_arrow_ipc_input_adapter")));
    assert!(stdout.contains(&field("source_adapter_status", "smoke_supported")));
    assert!(stdout.contains(&field("ingress_route", "direct_transient")));
    assert!(stdout.contains(&field(
        "vortex_ingest_status",
        "not_performed_direct_transient"
    )));
    assert!(stdout.contains(&field(
        "selected_execution_mode",
        "direct_compatibility_transient"
    )));
    assert!(stdout.contains(&field("timing_scope", "direct_one_shot")));
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "id,label")));
    assert_required_source_state_projection_evidence(&stdout, "reader_level_projection");
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.arrow_ipc.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_arrow_ipc_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
        )
    );

    fs::remove_file(source_path).expect("remove source arrow ipc");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_decodes_arrow_ipc_nested_source_to_jsonl_csv_boundary_without_fallback() {
    let source_path = unique_path("sql-local-source-nested", "arrow");
    let csv_output_path = unique_path("sql-local-source-nested", "csv");
    write_nested_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,values,payload FROM '{}' ORDER BY id ASC LIMIT 3",
        source_path.display()
    );
    let csv_target = format!("csv={}", csv_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &csv_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "arrow_ipc")));
    assert!(stdout.contains(&field("source_state_read_plan", "required_columns")));
    assert!(stdout.contains(&field(
        "source_state_projection_pushdown_status",
        "reader_level_projection"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,values,payload")));
    assert!(stdout.contains(&field(
        "result_batch_state_status",
        "shared_logical_columnar_boundary_available"
    )));
    assert!(stdout.contains(&field(
        "result_batch_state_layout",
        "logical_mixed_column_vectors_v1"
    )));
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout.contains(
            "{\\\"id\\\":1,\\\"values\\\":[1,2,null],\\\"payload\\\":{\\\"label\\\":\\\"alpha\\\",\\\"amount\\\":8}}"
        ),
        "{stdout}"
    );
    assert!(
        stdout.contains(
            "{\\\"id\\\":2,\\\"values\\\":null,\\\"payload\\\":{\\\"label\\\":null,\\\"amount\\\":15}}"
        ),
        "{stdout}"
    );

    let csv = fs::read_to_string(&csv_output_path).expect("read nested csv output");
    assert_eq!(
        csv,
        "id,values,payload\n1,\"[1,2,null]\",\"{\"\"label\"\":\"\"alpha\"\",\"\"amount\"\":8}\"\n2,,\"{\"\"label\"\":null,\"\"amount\"\":15}\"\n3,[],\"{\"\"label\"\":\"\"empty\"\",\"\"amount\"\":null}\"\n"
    );

    fs::remove_file(source_path).expect("remove nested source arrow ipc");
    fs::remove_file(csv_output_path).expect("remove nested csv output");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_arrow_ipc_nested_source_to_typed_parquet_without_fallback() {
    let source_path = unique_path("sql-local-source-nested-parquet", "arrow");
    let output_path = unique_path("sql-local-source-nested-parquet", "parquet");
    write_nested_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT values,payload FROM '{}' ORDER BY id ASC LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output-format",
            "parquet",
            "--output",
            &output_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""), "{stdout}");
    assert!(stdout.contains(&field("output_format", "parquet")));
    assert!(stdout.contains(&field(
        "output_plan_type_nullability_support",
        "flat_scalar_nullable_and_inferable_typed_nested_nullable_values_supported"
    )));
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        output_path.exists(),
        "typed nested parquet output should be written at {}",
        output_path.display()
    );

    let table =
        shardloom_vortex::read_flat_parquet_source(&output_path, 10).expect("read nested parquet");
    assert_eq!(
        table.column_dtypes,
        vec![
            Some(shardloom_core::LogicalDType::List),
            Some(shardloom_core::LogicalDType::Struct)
        ]
    );
    assert_eq!(table.rows.len(), 3);
    assert_eq!(
        table.rows[0].get("values"),
        Some(&shardloom_core::ScalarValue::List(vec![
            shardloom_core::ScalarValue::Int64(1),
            shardloom_core::ScalarValue::Int64(2),
            shardloom_core::ScalarValue::Null,
        ]))
    );
    assert_eq!(
        table.rows[1].get("values"),
        Some(&shardloom_core::ScalarValue::Null)
    );
    assert_eq!(
        table.rows[2].get("payload"),
        Some(&shardloom_core::ScalarValue::Struct(vec![
            (
                "label".to_string(),
                shardloom_core::ScalarValue::Utf8("empty".to_string())
            ),
            ("amount".to_string(), shardloom_core::ScalarValue::Null),
        ]))
    );

    fs::remove_file(source_path).expect("remove nested source arrow ipc");
    fs::remove_file(output_path).expect("remove nested parquet output");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_all_null_nested_source_schema_to_structured_sinks_without_fallback()
 {
    let source_path = unique_path("sql-local-source-nested-all-null-schema", "arrow");
    write_nested_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT values FROM '{}' WHERE id = 2 LIMIT 1",
        source_path.display()
    );
    for (format, extension) in [
        ("parquet", "parquet"),
        ("arrow_ipc", "arrow"),
        ("avro", "avro"),
    ] {
        let output_path = unique_path(
            &format!("sql-local-source-nested-all-null-schema-{format}"),
            extension,
        );
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "sql-local-source-smoke",
                &statement,
                "--output-format",
                format,
                "--output",
                &output_path.display().to_string(),
                "--format",
                "json",
            ])
            .output()
            .expect("sql-local-source-smoke command runs");

        assert!(
            output.status.success(),
            "format={format} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stderr.is_empty(),
            "format={format} stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
        assert!(stdout.contains("\"status\":\"success\""), "{stdout}");
        assert!(stdout.contains(&field("output_format", format)), "{stdout}");
        assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
        assert!(stdout.contains(&field(
            "typed_nested_child_schema_evidence_status",
            "present_from_source_schema_child_fields_for_all_null_typed_nested_column"
        )));
        assert!(stdout.contains(&field("typed_nested_child_schema_blocker", "none")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(
            output_path.exists(),
            "structured sink should write all-null nested output at {}",
            output_path.display()
        );

        let table = match format {
            "parquet" => shardloom_vortex::read_flat_parquet_source(&output_path, 10),
            "arrow_ipc" => shardloom_vortex::read_flat_arrow_ipc_source(&output_path, 10),
            "avro" => shardloom_vortex::read_flat_avro_source(&output_path, 10),
            _ => unreachable!("covered formats"),
        }
        .expect("read all-null nested output");
        assert_eq!(
            table.column_dtypes,
            vec![Some(shardloom_core::LogicalDType::List)]
        );
        assert_eq!(table.rows.len(), 1);
        assert_eq!(
            table.rows[0].get("values"),
            Some(&shardloom_core::ScalarValue::Null)
        );
        fs::remove_file(output_path).expect("remove all-null nested structured output");
    }

    fs::remove_file(source_path).expect("remove nested source arrow ipc");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_writes_all_null_nested_source_schema_to_vortex_without_fallback() {
    let source_path = unique_path("sql-local-source-nested-all-null-schema-vortex", "arrow");
    let output_path = unique_path("sql-local-source-nested-all-null-schema-vortex", "vortex");
    write_nested_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT values FROM '{}' WHERE id = 2 LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output-format",
            "vortex",
            "--output",
            &output_path.display().to_string(),
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""), "{stdout}");
    assert!(
        stdout.contains(&field("output_format", "vortex")),
        "{stdout}"
    );
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field(
        "typed_nested_child_schema_evidence_status",
        "present_from_source_schema_child_fields_for_all_null_typed_nested_column"
    )));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "true")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("vortex_output_column_families", "values:list")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        output_path.exists(),
        "Vortex sink should write all-null nested output at {}",
        output_path.display()
    );

    fs::remove_file(source_path).expect("remove nested source arrow ipc");
    fs::remove_file(output_path).expect("remove all-null nested vortex output");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_executes_avro_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source", "avro");
    write_avro_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "avro")));
    assert!(stdout.contains(&field("source_adapter_id", "local_avro_input_adapter")));
    assert!(stdout.contains(&field("source_adapter_status", "smoke_supported")));
    assert!(stdout.contains(&field("ingress_route", "direct_transient")));
    assert!(stdout.contains(&field(
        "vortex_ingest_status",
        "not_performed_direct_transient"
    )));
    assert!(stdout.contains(&field(
        "selected_execution_mode",
        "direct_compatibility_transient"
    )));
    assert!(stdout.contains(&field("timing_scope", "direct_one_shot")));
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "id,label")));
    assert_required_source_state_projection_evidence(&stdout, "reader_level_projection");
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.avro.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_avro_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
        )
    );

    fs::remove_file(source_path).expect("remove source avro");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_executes_orc_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source", "orc");
    write_orc_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "orc")));
    assert!(stdout.contains(&field("source_adapter_id", "local_orc_input_adapter")));
    assert!(stdout.contains(&field("source_adapter_status", "smoke_supported")));
    assert!(stdout.contains(&field("ingress_route", "direct_transient")));
    assert!(stdout.contains(&field(
        "vortex_ingest_status",
        "not_performed_direct_transient"
    )));
    assert!(stdout.contains(&field(
        "selected_execution_mode",
        "direct_compatibility_transient"
    )));
    assert!(stdout.contains(&field("timing_scope", "direct_one_shot")));
    assert!(stdout.contains(&field("input_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "id,label")));
    assert_required_source_state_projection_evidence(&stdout, "reader_level_projection");
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.orc.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_orc_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
        )
    );

    fs::remove_file(source_path).expect("remove source orc");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_uses_zero_column_reader_projection_for_count_star() {
    assert_zero_column_reader_projection_count_star(
        "parquet",
        "parquet",
        "0",
        "none",
        write_parquet_smoke_source,
    );
    assert_zero_column_reader_projection_count_star(
        "arrow",
        "arrow_ipc",
        "0",
        "none",
        write_arrow_ipc_smoke_source,
    );
    assert_zero_column_reader_projection_count_star(
        "avro",
        "avro",
        "1",
        "id",
        write_avro_smoke_source,
    );
    assert_zero_column_reader_projection_count_star(
        "orc",
        "orc",
        "0",
        "none",
        write_orc_smoke_source,
    );
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_parquet_without_universal_format_feature() {
    let source_path = unique_path("sql-local-source-blocked", "parquet");
    fs::write(&source_path, b"not a real parquet file").expect("write source parquet placeholder");

    let statement = format!("SELECT id FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source parquet");
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_arrow_ipc_without_universal_format_feature() {
    let source_path = unique_path("sql-local-source-blocked", "arrow");
    fs::write(&source_path, b"not a real arrow ipc file")
        .expect("write source arrow ipc placeholder");

    let statement = format!("SELECT id FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source arrow ipc");
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_avro_without_universal_format_feature() {
    let source_path = unique_path("sql-local-source-blocked", "avro");
    fs::write(&source_path, b"not a real avro file").expect("write source avro placeholder");

    let statement = format!("SELECT id FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source avro");
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_orc_without_universal_format_feature() {
    let source_path = unique_path("sql-local-source-blocked", "orc");
    fs::write(&source_path, b"not a real orc file").expect("write source orc placeholder");

    let statement = format!("SELECT id FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source orc");
}

#[test]
fn sql_local_source_smoke_executes_literal_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-literal-projection", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,'north' AS segment FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_literal_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("literal_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("literal_projection_columns", "segment")));
    assert!(stdout.contains(&field("literal_projection_count", "1")));
    assert!(stdout.contains(&field("projected_columns", "id,label,segment")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\",\\\"segment\\\":\\\"north\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\",\\\"segment\\\":\\\"north\\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_arithmetic_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-arithmetic-projection", "csv");
    fs::write(
        &source_path,
        "id,amount,ratio\n1,8,0.25\n2,15,0.5\n3,21,0.75\n4,,1.25\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,amount + 2.5 AS adjusted,ratio * 2 AS doubled FROM '{}' WHERE amount >= 10 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_operator",
        "add,multiply"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_source_column",
        "amount,ratio"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_output_column",
        "adjusted,doubled"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_rhs_dtype",
        "float64,int64"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,adjusted,doubled")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"adjusted\\\":17.5,\\\"doubled\\\":1.0}\\n{\\\"id\\\":3,\\\"adjusted\\\":23.5,\\\"doubled\\\":1.5}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,amount / 0 AS broken FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("numeric arithmetic projection division by zero is a runtime data error")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_decimal_arithmetic_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-decimal-arithmetic", "csv");
    fs::write(&source_path, "id,amount\n1,12.34\n2,15.50\n3,21.25\n").expect("write source csv");

    let statement = format!(
        "SELECT id,CAST(amount AS decimal128(10,2)) + CAST('1.25' AS decimal128(10,2)) AS adjusted,CAST(amount AS decimal128(10,2)) / 2 AS half,CAST(amount AS decimal128(10,2)) * CAST('1.50' AS decimal128(3,2)) AS scaled FROM '{}' WHERE CAST(amount AS decimal128(10,2)) + 0 >= CAST('12.34' AS decimal128(10,2)) LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_source_column",
        "amount,amount,amount"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_output_column",
        "adjusted,half,scaled"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_operator_family",
        "cast+numeric_binary,cast+numeric_binary,cast+numeric_binary"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_binary_operator_count",
        "3"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,adjusted,half,scaled")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"adjusted\\\":\\\"13.59\\\",\\\"half\\\":\\\"6.170000\\\",\\\"scaled\\\":\\\"18.5100\\\"}\\n{\\\"id\\\":2,\\\"adjusted\\\":\\\"16.75\\\",\\\"half\\\":\\\"7.750000\\\",\\\"scaled\\\":\\\"23.2500\\\"}\\n{\\\"id\\\":3,\\\"adjusted\\\":\\\"22.50\\\",\\\"half\\\":\\\"10.625000\\\",\\\"scaled\\\":\\\"31.8750\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,CAST(amount AS decimal128(10,2)) / 3 AS broken FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("decimal128 division requires an exact quotient"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_star_plus_computed_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-star-computed-projection", "csv");
    fs::write(
        &source_path,
        "id,amount,label\n1,8,Alpha\n2,15,Beta\n3,21,Gamma\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT *,amount + 2 AS adjusted,LOWER(label) AS normalized FROM '{}' WHERE amount >= 10 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "numeric_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "string_transform_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,amount,label,adjusted,normalized"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"amount\\\":15,\\\"label\\\":\\\"Beta\\\",\\\"adjusted\\\":17,\\\"normalized\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"amount\\\":21,\\\"label\\\":\\\"Gamma\\\",\\\"adjusted\\\":23,\\\"normalized\\\":\\\"gamma\\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_computed_projection_order_by_alias_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-computed-projection-topn", "csv");
    fs::write(
        &source_path,
        "id,amount,label\n1,8,alpha\n2,15,beta\n3,21,gamma\n4,13,delta\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,amount + 5 AS adjusted FROM '{}' WHERE amount >= 10 ORDER BY adjusted DESC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("computed_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "computed_projection_top_n_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "computed_projection_operator_family",
        "computed_projection_topn"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "single_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "adjusted")));
    assert!(stdout.contains(&field("sort_direction", "desc")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"adjusted\\\":26}\\n{\\\"id\\\":2,\\\"adjusted\\\":20}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.computed-projection-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_computed_projection_source_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-computed-projection-source-topn", "csv");
    fs::write(
        &source_path,
        "id,amount,label\n1,8,alpha\n2,15,beta\n3,21,gamma\n4,13,delta\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,amount + 5 AS adjusted FROM '{}' WHERE amount >= 10 ORDER BY label ASC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "computed_projection_top_n_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("sort_keys", "label")));
    assert!(stdout.contains(&field("sort_direction", "asc")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"adjusted\\\":20}\\n{\\\"id\\\":4,\\\"adjusted\\\":18}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_generic_expression_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-generic-expression-projection", "csv");
    fs::write(
        &source_path,
        "id,amount,tax\n1,8,2\n2,15,5\n3,21,4\n4,12,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,(amount + tax) * 2 AS gross,ABS(amount - tax) AS spread,ROUND((amount + tax) / 2.0) AS midpoint FROM '{}' WHERE amount >= 10 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_source_column",
        "amount+tax,amount+tax,amount+tax"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_output_column",
        "gross,spread,midpoint"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_operator_family",
        "numeric_binary,numeric_abs+numeric_binary,numeric_binary+numeric_rounding"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_binary_operator_count",
        "5"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,gross,spread,midpoint")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"gross\\\":40,\\\"spread\\\":10,\\\"midpoint\\\":10.0}\\n{\\\"id\\\":3,\\\"gross\\\":50,\\\"spread\\\":17,\\\"midpoint\\\":13.0}\\n{\\\"id\\\":4,\\\"gross\\\":null,\\\"spread\\\":null,\\\"midpoint\\\":null}\\n\""
    ));

    let division_by_zero_statement = format!(
        "SELECT id,(amount + tax) / 0 AS gross FROM '{}' LIMIT 10",
        source_path.display()
    );
    let division_by_zero = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &division_by_zero_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!division_by_zero.status.success());
    let division_by_zero_output = format!(
        "{}{}",
        String::from_utf8_lossy(&division_by_zero.stdout),
        String::from_utf8_lossy(&division_by_zero.stderr)
    );
    assert!(
        division_by_zero_output
            .contains("generic numeric expression division by zero is not admitted")
    );
    assert!(division_by_zero_output.contains("external_engine_invoked=false"));

    let blocked_statement = format!(
        "SELECT id,(amount + missing_tax) * 2 AS gross FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains(
        "generic expression projection source column \\\"missing_tax\\\" is not present"
    ));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_abs_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-abs-projection", "csv");
    fs::write(&source_path, "id,amount\n1,-5\n2,3\n3,-4\n4,\n").expect("write source csv");

    let statement = format!(
        "SELECT id,ABS(amount) AS magnitude FROM '{}' WHERE ABS(amount) >= 4 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "numeric_abs")));
    assert!(stdout.contains(&field("numeric_abs_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_abs_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_abs_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("numeric_abs_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_abs_projection_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_abs_projection_output_column", "magnitude")));
    assert!(stdout.contains(&field("projected_columns", "id,magnitude")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"magnitude\\\":5}\\n{\\\"id\\\":3,\\\"magnitude\\\":4}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,ABS(missing) AS magnitude FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("numeric abs projection source column \\\"missing\\\" is not present")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_rounding_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-rounding-projection", "csv");
    fs::write(
        &source_path,
        "id,amount,ratio\n1,3.2,1.2\n2,3.8,1.8\n3,-2.3,-1.2\n4,,2.0\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,FLOOR(amount) AS lower,CEIL(ratio) AS upper FROM '{}' WHERE ROUND(amount) >= 4 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "numeric_rounding")));
    assert!(stdout.contains(&field("numeric_rounding_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_rounding_operator", "round")));
    assert!(stdout.contains(&field("numeric_rounding_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_rounding_rhs_dtype", "int64")));
    assert!(stdout.contains(&field(
        "numeric_rounding_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("numeric_rounding_projection_operator", "floor,ceil")));
    assert!(stdout.contains(&field(
        "numeric_rounding_projection_source_column",
        "amount,ratio"
    )));
    assert!(stdout.contains(&field(
        "numeric_rounding_projection_output_column",
        "lower,upper"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,lower,upper")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"lower\\\":3.0,\\\"upper\\\":2.0}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,FLOOR(missing) AS lower FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("numeric rounding projection source column \\\"missing\\\" is not present")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_cast_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-cast-projection", "csv");
    fs::write(
        &source_path,
        "id,amount,active,event_date,event_ts\n\
         1,8,true,2026-05-19,2026-05-19T12:34:56Z\n\
         2,15,false,2027-01-02,2027-01-02T03:04:05Z\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,CAST(amount AS float64) AS amount_float,CAST(active AS utf8) AS active_text,CAST(event_date AS date32) AS event_day,CAST(event_ts AS timestamp_micros) AS event_time FROM '{}' WHERE id >= 1 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("cast_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "cast_projection_source_column",
        "amount,active,event_date,event_ts"
    )));
    assert!(stdout.contains(&field(
        "cast_projection_output_column",
        "amount_float,active_text,event_day,event_time"
    )));
    assert!(stdout.contains(&field(
        "cast_projection_target_dtype",
        "float64,utf8,date32,timestamp_micros"
    )));
    assert!(stdout.contains(&field(
        "cast_projection_mode",
        "strict,strict,strict,strict"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,amount_float,active_text,event_day,event_time"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"amount_float\\\":8.0,\\\"active_text\\\":\\\"true\\\",\\\"event_day\\\":\\\"2026-05-19\\\",\\\"event_time\\\":\\\"2026-05-19T12:34:56Z\\\"}\\n{\\\"id\\\":2,\\\"amount_float\\\":15.0,\\\"active_text\\\":\\\"false\\\",\\\"event_day\\\":\\\"2027-01-02\\\",\\\"event_time\\\":\\\"2027-01-02T03:04:05Z\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,CAST(label AS decimal128(39,2)) AS unsupported FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("decimal CAST precision/scale must satisfy"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_try_cast_projection_and_predicate_without_fallback() {
    let source_path = unique_path("sql-local-source-try-cast", "csv");
    fs::write(
        &source_path,
        "id,raw_amount\n\
         1,8\n\
         2,not_an_int\n\
         3,15\n",
    )
    .expect("write source csv");

    let projection_statement = format!(
        "SELECT id,TRY_CAST(raw_amount AS int64) AS amount_i64 FROM '{}' WHERE id >= 1 LIMIT 10",
        source_path.display()
    );
    let projection_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &projection_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        projection_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&projection_output.stdout),
        String::from_utf8_lossy(&projection_output.stderr)
    );
    let projection_stdout = String::from_utf8(projection_output.stdout).expect("stdout is utf8");
    assert!(projection_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(projection_stdout.contains(&field("cast_projection_runtime_execution", "true")));
    assert!(projection_stdout.contains(&field("cast_projection_source_column", "raw_amount")));
    assert!(projection_stdout.contains(&field("cast_projection_output_column", "amount_i64")));
    assert!(projection_stdout.contains(&field("cast_projection_target_dtype", "int64")));
    assert!(projection_stdout.contains(&field("cast_projection_mode", "try")));
    assert!(projection_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"amount_i64\\\":8}\\n{\\\"id\\\":2,\\\"amount_i64\\\":null}\\n{\\\"id\\\":3,\\\"amount_i64\\\":15}\\n\""
    ));
    assert!(projection_stdout.contains(&field("fallback_attempted", "false")));
    assert!(projection_stdout.contains(&field("external_engine_invoked", "false")));

    let predicate_statement = format!(
        "SELECT id,raw_amount FROM '{}' WHERE TRY_CAST(raw_amount AS int64) >= 10 LIMIT 10",
        source_path.display()
    );
    let predicate_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &predicate_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        predicate_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&predicate_output.stdout),
        String::from_utf8_lossy(&predicate_output.stderr)
    );
    let predicate_stdout = String::from_utf8(predicate_output.stdout).expect("stdout is utf8");
    assert!(predicate_stdout.contains(&field("predicate_operator_family", "cast")));
    assert!(predicate_stdout.contains(&field("cast_runtime_execution", "true")));
    assert!(predicate_stdout.contains(&field("cast_source_column", "raw_amount")));
    assert!(predicate_stdout.contains(&field("cast_target_dtype", "int64")));
    assert!(predicate_stdout.contains(&field("cast_mode", "try")));
    assert!(predicate_stdout.contains(&field("selected_row_count", "1")));
    assert!(
        predicate_stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"raw_amount\\\":15}\\n\"")
    );
    assert!(predicate_stdout.contains(&field("fallback_attempted", "false")));
    assert!(predicate_stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_transform_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-string-transform-projection", "csv");
    fs::write(&source_path, "id,label\n1, Alpha \n2,BETA\n3,gamma\n").expect("write source csv");

    let statement = format!(
        "SELECT id,LOWER(label) AS lowered,UPPER(label) AS raised,TRIM(label) AS trimmed FROM '{}' WHERE id >= 1 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "string_transform_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "string_transform_projection_operator",
        "lower,upper,trim"
    )));
    assert!(stdout.contains(&field(
        "string_transform_projection_source_column",
        "label,label,label"
    )));
    assert!(stdout.contains(&field(
        "string_transform_projection_output_column",
        "lowered,raised,trimmed"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,lowered,raised,trimmed")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(
        stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"lowered\\\":\\\" alpha \\\"")
    );
    assert!(stdout.contains("\\\"raised\\\":\\\" ALPHA \\\""));
    assert!(stdout.contains("\\\"trimmed\\\":\\\"Alpha\\\""));

    let blocked_statement = format!(
        "SELECT id,LOWER(missing) AS lowered FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains(
        "string transform projection source column \\\"missing\\\" is not present in the CSV header"
    ));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_length_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-string-length-projection", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n3,écho\n").expect("write source csv");

    let statement = format!(
        "SELECT id,LENGTH(label) AS label_len FROM '{}' WHERE LENGTH(label) >= 4 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "string_length")));
    assert!(stdout.contains(&field("string_length_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_length_source_column", "label")));
    assert!(stdout.contains(&field("string_length_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("string_length_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_length_projection_source_column", "label")));
    assert!(stdout.contains(&field(
        "string_length_projection_output_column",
        "label_len"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,label_len")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label_len\\\":5}\\n{\\\"id\\\":2,\\\"label_len\\\":4}\\n{\\\"id\\\":3,\\\"label_len\\\":4}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,LENGTH(missing) AS missing_len FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("string length projection source column \\\"missing\\\" is not present")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_function_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-string-function-projection", "csv");
    fs::write(
        &source_path,
        "id,label,segment\n1,alpha,north\n2,beta,east\n3,alpaca,north\n4,,west\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,CONCAT(label, '-', segment) AS label_key,SUBSTR(label, 2, 3) AS middle,LEFT(label, 2) AS prefix,RIGHT(label, 2) AS suffix,REPLACE(label, 'a', '') AS scrubbed FROM '{}' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "string_function")));
    assert!(stdout.contains(&field("string_function_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_function_operator", "concat")));
    assert!(stdout.contains(&field("string_function_source_column", "label+segment")));
    assert!(stdout.contains(&field("string_function_literal_count", "2")));
    assert!(stdout.contains(&field("string_function_rhs_dtype", "utf8")));
    assert!(stdout.contains(&field(
        "string_function_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_operator",
        "concat,substr,left,right,replace"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_source_column",
        "label+segment,label,label,label,label"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_output_column",
        "label_key,middle,prefix,suffix,scrubbed"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_literal_count",
        "1,2,1,1,2"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,label_key,middle,prefix,suffix,scrubbed"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label_key\\\":\\\"alpha-north\\\",\\\"middle\\\":\\\"lph\\\",\\\"prefix\\\":\\\"al\\\",\\\"suffix\\\":\\\"ha\\\",\\\"scrubbed\\\":\\\"lph\\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_composed_string_expressions_without_fallback() {
    let source_path = unique_path("sql-local-source-composed-string-expressions", "csv");
    fs::write(
        &source_path,
        "id,label,segment\n1, Alpha ,NORTH\n2,beta,east\n3,alpha,north\n4, alpha beta ,NORTH\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,CONCAT(LOWER(TRIM(label)), '-', LOWER(segment)) AS label_key,LENGTH(REPLACE(TRIM(label), ' ', '')) AS compact_len,SUBSTR(LOWER(TRIM(label)), 1, 5) AS prefix FROM '{}' WHERE CONCAT(LOWER(TRIM(label)), '-', LOWER(segment)) = 'alpha-north' AND LENGTH(REPLACE(TRIM(label), ' ', '')) >= 5 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("string_function_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_function_operator", "concat")));
    assert!(stdout.contains(&field("string_function_source_column", "label+segment")));
    assert!(stdout.contains(&field("string_function_literal_count", "2")));
    assert!(stdout.contains(&field("string_length_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_length_source_column", "label")));
    assert!(stdout.contains(&field("string_length_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_length_projection_source_column", "label")));
    assert!(stdout.contains(&field(
        "string_function_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_operator",
        "concat,substr"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_source_column",
        "label+segment,label"
    )));
    assert!(stdout.contains(&field("string_function_projection_literal_count", "1,2")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,label_key,compact_len,prefix"
    )));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label_key\\\":\\\"alpha-north\\\",\\\"compact_len\\\":5,\\\"prefix\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label_key\\\":\\\"alpha-north\\\",\\\"compact_len\\\":5,\\\"prefix\\\":\\\"alpha\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_temporal_extract_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-temporal-extract-projection", "csv");
    fs::write(
        &source_path,
        "id,event_date,event_ts\n\
         1,2026-05-19,2026-05-19T12:34:56Z\n\
         2,2027-01-02,2027-01-02T03:04:05Z\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,DATE_YEAR(CAST(event_date AS date32)) AS event_year,DATE_MONTH(event_date) AS event_month,TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) AS event_hour,TIMESTAMP_SECOND(event_ts) AS event_second FROM '{}' WHERE id >= 1 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("date_extract_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "date_extract_projection_operator",
        "date_year,date_month"
    )));
    assert!(stdout.contains(&field(
        "date_extract_projection_source_column",
        "event_date,event_date"
    )));
    assert!(stdout.contains(&field(
        "date_extract_projection_output_column",
        "event_year,event_month"
    )));
    assert!(stdout.contains(&field(
        "timestamp_extract_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "timestamp_extract_projection_operator",
        "timestamp_hour,timestamp_second"
    )));
    assert!(stdout.contains(&field(
        "timestamp_extract_projection_source_column",
        "event_ts,event_ts"
    )));
    assert!(stdout.contains(&field(
        "timestamp_extract_projection_output_column",
        "event_hour,event_second"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,event_year,event_month,event_hour,event_second"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"event_year\\\":2026,\\\"event_month\\\":5,\\\"event_hour\\\":12,\\\"event_second\\\":56}\\n{\\\"id\\\":2,\\\"event_year\\\":2027,\\\"event_month\\\":1,\\\"event_hour\\\":3,\\\"event_second\\\":5}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_arithmetic_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-date-arithmetic-projection", "csv");
    fs::write(
        &source_path,
        "id,event_date\n\
         1,2026-05-19\n\
         2,2027-01-02\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,DATE_ADD_DAYS(CAST(event_date AS date32), 7) AS next_week,DATE_SUB_DAYS(event_date, 1) AS prior_day FROM '{}' WHERE id >= 1 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_operator",
        "date_add_days,date_sub_days"
    )));
    assert!(stdout.contains(&field("date_arithmetic_projection_days", "7,1")));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_source_column",
        "event_date,event_date"
    )));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_output_column",
        "next_week,prior_day"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,next_week,prior_day")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"next_week\\\":\\\"2026-05-26\\\",\\\"prior_day\\\":\\\"2026-05-18\\\"}\\n{\\\"id\\\":2,\\\"next_week\\\":\\\"2027-01-09\\\",\\\"prior_day\\\":\\\"2027-01-01\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,DATE_ADD_DAYS(event_date, 366001) AS too_far FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("date arithmetic day count admits absolute values <= 366000"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_timestamp_arithmetic_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-timestamp-arithmetic-projection", "csv");
    fs::write(
        &source_path,
        "id,event_ts\n\
         1,2026-05-19T12:34:00Z\n\
         2,2026-05-19T12:34:45Z\n\
         3,\n\
         4,2026-05-19T12:35:00Z\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,TIMESTAMP_ADD_SECONDS(CAST(event_ts AS timestamp_micros), 90) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, 45) AS prior_ts FROM '{}' WHERE TIMESTAMP_ADD_SECONDS(event_ts, 60) >= TIMESTAMP '2026-05-19T12:35:45Z' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "timestamp_arithmetic")));
    assert!(stdout.contains(&field("timestamp_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("timestamp_arithmetic_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_operator",
        "timestamp_add_seconds"
    )));
    assert!(stdout.contains(&field("timestamp_arithmetic_seconds", "60")));
    assert!(stdout.contains(&field("timestamp_arithmetic_source_column", "event_ts")));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_operator",
        "timestamp_add_seconds,timestamp_sub_seconds"
    )));
    assert!(stdout.contains(&field("timestamp_arithmetic_projection_seconds", "90,45")));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_source_column",
        "event_ts,event_ts"
    )));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_output_column",
        "shifted_ts,prior_ts"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,shifted_ts,prior_ts")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"shifted_ts\\\":\\\"2026-05-19T12:36:15Z\\\",\\\"prior_ts\\\":\\\"2026-05-19T12:34:00Z\\\"}\\n{\\\"id\\\":4,\\\"shifted_ts\\\":\\\"2026-05-19T12:36:30Z\\\",\\\"prior_ts\\\":\\\"2026-05-19T12:34:15Z\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,TIMESTAMP_ADD_SECONDS(event_ts, 31622400001) AS too_far FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("timestamp arithmetic second count admits absolute values <= 31622400000")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_temporal_difference_generic_expressions_without_fallback() {
    let source_path = unique_path("sql-local-source-temporal-difference", "csv");
    fs::write(
        &source_path,
        "id,start_date,end_date,start_ts,end_ts\n\
         1,2026-05-19,2026-05-20,2026-05-19T12:00:00Z,2026-05-19T12:01:30Z\n\
         2,2026-05-19,2026-05-21,2026-05-19T12:00:00Z,2026-05-19T12:03:05Z\n\
         3,2026-05-19,2026-05-23,,2026-05-19T12:05:00Z\n\
         4,2026-05-20,2026-05-24,2026-05-19T12:00:10Z,2026-05-19T12:10:10Z\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,DATE_DIFF_DAYS(CAST(end_date AS date32), start_date) AS age_days,TIMESTAMP_DIFF_SECONDS(CAST(end_ts AS timestamp_micros), start_ts) AS elapsed_seconds FROM '{}' WHERE DATE_DIFF_DAYS(end_date, start_date) >= 2 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "generic_expression")));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_source_column",
        "end_date+start_date"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_operator_family",
        "temporal_difference"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_binary_operator_count",
        "0"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_comparison_operator",
        "gte"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_source_column",
        "end_date+start_date,end_ts+start_ts"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_output_column",
        "age_days,elapsed_seconds"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_operator_family",
        "cast+temporal_difference,cast+temporal_difference"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_binary_operator_count",
        "0"
    )));
    assert!(stdout.contains(&field("projected_columns", "id,age_days,elapsed_seconds")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"age_days\\\":2,\\\"elapsed_seconds\\\":185}\\n{\\\"id\\\":3,\\\"age_days\\\":4,\\\"elapsed_seconds\\\":null}\\n{\\\"id\\\":4,\\\"age_days\\\":4,\\\"elapsed_seconds\\\":600}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,DATE_DIFF_DAYS(start_date) AS bad_delta FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output.contains("temporal difference expressions require exactly two arguments")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_null_coalesce_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-null-coalesce-projection", "csv");
    fs::write(
        &source_path,
        "id,label,amount,event_date\n\
         1,alpha,8,2026-05-19\n\
         2,,,\n\
         3,beta,15,2027-01-02\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,COALESCE(label, 'unknown') AS label_clean,COALESCE(amount, 0) AS amount_clean,COALESCE(event_date, DATE '2026-01-01') AS event_day FROM '{}' WHERE id >= 1 LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("null_coalesce_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "null_coalesce_projection_source_column",
        "label,amount,event_date"
    )));
    assert!(stdout.contains(&field(
        "null_coalesce_projection_output_column",
        "label_clean,amount_clean,event_day"
    )));
    assert!(stdout.contains(&field(
        "null_coalesce_projection_fallback_dtype",
        "utf8,int64,date32"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,label_clean,amount_clean,event_day"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label_clean\\\":\\\"alpha\\\",\\\"amount_clean\\\":8,\\\"event_day\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":2,\\\"label_clean\\\":\\\"unknown\\\",\\\"amount_clean\\\":0,\\\"event_day\\\":\\\"2026-01-01\\\"}\\n{\\\"id\\\":3,\\\"label_clean\\\":\\\"beta\\\",\\\"amount_clean\\\":15,\\\"event_day\\\":\\\"2027-01-02\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,COALESCE(label, 0) AS label_clean FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("scoped null coalesce requires matching non-null source and fallback dtypes")
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_nullif_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-nullif-projection", "csv");
    fs::write(
        &source_path,
        "id,label,amount,event_date\n\
         1,alpha,8,2026-05-19\n\
         2,missing,0,2026-01-01\n\
         3,beta,15,2027-01-02\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,NULLIF(label, 'missing') AS label_clean,NULLIF(amount, 0) AS amount_clean,NULLIF(CAST(event_date AS date32), DATE '2026-01-01') AS event_day FROM '{}' WHERE id >= 1 LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("nullif_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "nullif_projection_source_column",
        "label,amount,event_date"
    )));
    assert!(stdout.contains(&field(
        "nullif_projection_output_column",
        "label_clean,amount_clean,event_day"
    )));
    assert!(stdout.contains(&field(
        "nullif_projection_sentinel_dtype",
        "utf8,int64,date32"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,label_clean,amount_clean,event_day"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label_clean\\\":\\\"alpha\\\",\\\"amount_clean\\\":8,\\\"event_day\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":2,\\\"label_clean\\\":null,\\\"amount_clean\\\":null,\\\"event_day\\\":null}\\n{\\\"id\\\":3,\\\"label_clean\\\":\\\"beta\\\",\\\"amount_clean\\\":15,\\\"event_day\\\":\\\"2027-01-02\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,NULLIF(label, 0) AS label_clean FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(
        blocked_output
            .contains("scoped nullif requires matching non-null source and sentinel dtypes"),
        "{blocked_output}"
    );
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

fn assert_sql_local_source_smoke_rejects(statement: &str, expected_fragments: &[&str]) {
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    for fragment in expected_fragments {
        assert!(blocked_output.contains(fragment), "{blocked_output}");
    }
    assert!(blocked_output.contains("external_engine_invoked=false"));
}

#[test]
fn sql_local_source_smoke_executes_conditional_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-conditional-projection", "csv");
    fs::write(
        &source_path,
        "id,label,amount,event_date,preferred_label,fallback_label,empty_label\n\
         1,alpha,8,2025-12-31,preferred-alpha,fallback-alpha,\n\
         2,beta,15,2026-05-19,preferred-beta,fallback-beta,\n\
         3,gamma,,2026-06-01,preferred-gamma,fallback-gamma,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 'small' END AS size_band,CASE WHEN event_date >= DATE '2026-01-01' THEN DATE '2026-12-31' ELSE DATE '2025-12-31' END AS cutoff_day,CASE WHEN amount >= 10 THEN preferred_label ELSE fallback_label END AS label_choice FROM '{}' WHERE id >= 1 LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("conditional_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "conditional_projection_predicate_family",
        "comparison,comparison,comparison"
    )));
    assert!(stdout.contains(&field(
        "conditional_projection_source_column",
        "amount,event_date,amount+fallback_label+preferred_label"
    )));
    assert!(stdout.contains(&field(
        "conditional_projection_output_column",
        "size_band,cutoff_day,label_choice"
    )));
    assert!(stdout.contains(&field(
        "conditional_projection_then_dtype",
        "utf8,date32,utf8"
    )));
    assert!(stdout.contains(&field(
        "conditional_projection_else_dtype",
        "utf8,date32,utf8"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,size_band,cutoff_day,label_choice"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"size_band\\\":\\\"small\\\",\\\"cutoff_day\\\":\\\"2025-12-31\\\",\\\"label_choice\\\":\\\"fallback-alpha\\\"}\\n{\\\"id\\\":2,\\\"size_band\\\":\\\"large\\\",\\\"cutoff_day\\\":\\\"2026-12-31\\\",\\\"label_choice\\\":\\\"preferred-beta\\\"}\\n{\\\"id\\\":3,\\\"size_band\\\":\\\"small\\\",\\\"cutoff_day\\\":\\\"2026-12-31\\\",\\\"label_choice\\\":\\\"fallback-gamma\\\"}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,CASE WHEN amount >= 10 THEN 'large' ELSE 0 END AS size_band FROM '{}' LIMIT 10",
        source_path.display()
    );
    assert_sql_local_source_smoke_rejects(
        &blocked_statement,
        &["CASE projection THEN/ELSE branches must have matching dtypes"],
    );

    let mixed_source_statement = format!(
        "SELECT id,CASE WHEN amount >= 10 THEN amount ELSE fallback_label END AS mixed_case FROM '{}' LIMIT 10",
        source_path.display()
    );
    assert_sql_local_source_smoke_rejects(
        &mixed_source_statement,
        &["THEN/ELSE branches must have matching dtypes after source-column binding"],
    );

    let all_null_source_statement = format!(
        "SELECT id,CASE WHEN amount >= 10 THEN empty_label ELSE fallback_label END AS label_or_empty FROM '{}' LIMIT 10",
        source_path.display()
    );
    assert_sql_local_source_smoke_rejects(
        &all_null_source_statement,
        &["empty_label", "has no non-NULL values"],
    );

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_predicate_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-predicate-projection", "csv");
    fs::write(
        &source_path,
        "id,label,amount,active,event_date\n\
         1,alpha,8,true,2025-12-31\n\
         2,,15,false,2026-05-19\n\
         3,gamma,,,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,amount >= 10 AS is_large,label IS NULL AS missing_label,active IS NOT TRUE AS inactive_or_unknown,event_date >= DATE '2026-01-01' AS current_year FROM '{}' WHERE id >= 1 LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("predicate_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "predicate_projection_predicate_family",
        "comparison,null_predicate,boolean_predicate,comparison"
    )));
    assert!(stdout.contains(&field(
        "predicate_projection_source_column",
        "amount,label,active,event_date"
    )));
    assert!(stdout.contains(&field(
        "predicate_projection_output_column",
        "is_large,missing_label,inactive_or_unknown,current_year"
    )));
    assert!(stdout.contains(&field(
        "predicate_projection_null_semantics",
        "sql_three_valued_boolean_or_null_projection,sql_is_null_is_not_null,sql_boolean_is_not_true_false_null_matches,sql_three_valued_boolean_or_null_projection"
    )));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,is_large,missing_label,inactive_or_unknown,current_year"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"is_large\\\":false,\\\"missing_label\\\":false,\\\"inactive_or_unknown\\\":false,\\\"current_year\\\":false}\\n{\\\"id\\\":2,\\\"is_large\\\":true,\\\"missing_label\\\":true,\\\"inactive_or_unknown\\\":true,\\\"current_year\\\":true}\\n{\\\"id\\\":3,\\\"is_large\\\":null,\\\"missing_label\\\":false,\\\"inactive_or_unknown\\\":true,\\\"current_year\\\":null}\\n\""
    ));

    let blocked_statement = format!(
        "SELECT id,missing >= 10 AS missing_flag FROM '{}' LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("predicate projection source column"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_correlated_subquery_projection_without_fallback() {
    let source_path = unique_path("sql-local-source-correlated-projection-source", "csv");
    let allowed_path = unique_path("sql-local-source-correlated-projection-allowed", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,10\n2,beta,20\n3,gamma,30\n4,delta,40\n",
    )
    .expect("write source csv");
    fs::write(
        &allowed_path,
        "id,min_amount,active\n1,5,true\n2,25,true\n3,20,true\n4,99,true\n",
    )
    .expect("write allowed csv");

    let statement = format!(
        "SELECT id,id IN (SELECT id FROM '{}' WHERE id = outer.id AND active IS TRUE AND outer.amount >= min_amount ORDER BY min_amount ASC LIMIT 10) AS matched FROM '{}' ORDER BY id ASC LIMIT 4",
        allowed_path.display(),
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "predicate_projection_predicate_family",
        "in_subquery"
    )));
    assert!(stdout.contains(&field("predicate_projection_source_column", "amount+id")));
    assert!(stdout.contains(&field("in_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_source_column", "id")));
    assert!(stdout.contains(&field("correlated_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("correlated_subquery_outer_column", "amount,id")));
    assert!(stdout.contains(&field(
        "correlated_subquery_outer_row_evaluation_count",
        "4"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"matched\\\":true}\\n{\\\"id\\\":2,\\\"matched\\\":false}\\n{\\\"id\\\":3,\\\"matched\\\":true}\\n{\\\"id\\\":4,\\\"matched\\\":false}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
}

#[test]
fn sql_local_source_smoke_executes_csv_projection_limit_without_predicate() {
    let source_path = unique_path("sql-local-source-no-filter", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_projection_limit"
    )));
    assert!(stdout.contains(&field("filter_runtime_execution", "false")));
    assert!(stdout.contains(&field("predicate_operator_family", "none")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.projection-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_select_distinct_before_limit_without_fallback() {
    let source_path = unique_path("sql-local-source-select-distinct", "csv");
    fs::write(
        &source_path,
        "id,region,label,amount\n\
         1,east,alpha,10\n\
         2,east,alpha,12\n\
         3,west,beta,8\n\
         4,west,beta,14\n\
         5,north,gamma,20\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT DISTINCT region,label FROM '{}' WHERE amount >= 8 ORDER BY region,label LIMIT 2",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_distinct_projection_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("distinct_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_output_columns", "region,label")));
    assert!(stdout.contains(&field("distinct_projection_input_row_count", "5")));
    assert!(stdout.contains(&field("distinct_projection_output_row_count", "2")));
    assert!(stdout.contains(&field(
        "distinct_projection_limit_applied_after_deduplication",
        "true"
    )));
    assert!(stdout.contains(&field(
        "distinct_projection_null_semantics",
        "sql_select_distinct_groups_nulls"
    )));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"label\\\":\\\"alpha\\\"}\\n{\\\"region\\\":\\\"north\\\",\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_select_distinct_aggregate_having_without_fallback() {
    let source_path = unique_path("sql-local-source-select-distinct-aggregate", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,east,12\n3,west,8\n4,west,14\n5,north,3\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT DISTINCT region,count(*) AS rows FROM '{}' GROUP BY region HAVING count(*) >= 2 LIMIT 5",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_distinct_group_by_aggregate_limit_having"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_output_columns", "region,rows")));
    assert!(stdout.contains(&field("distinct_projection_input_row_count", "2")));
    assert!(stdout.contains(&field("distinct_projection_output_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_select_distinct_join_output_before_limit_without_fallback() {
    let fact_path = unique_path("sql-local-source-select-distinct-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-select-distinct-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,region,amount\n1,10,east,5\n2,10,east,7\n3,20,west,9\n",
    )
    .expect("write fact csv");
    fs::write(&dim_path, "customer_id,segment\n10,retail\n20,enterprise\n").expect("write dim csv");

    let statement = format!(
        "SELECT DISTINCT f.region,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id LIMIT 2",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_distinct_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "distinct_projection_output_columns",
        "f.region,d.segment"
    )));
    assert!(stdout.contains(&field("distinct_projection_input_row_count", "3")));
    assert!(stdout.contains(&field("distinct_projection_output_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.region\\\":\\\"east\\\",\\\"d.segment\\\":\\\"retail\\\"}\\n{\\\"f.region\\\":\\\"west\\\",\\\"d.segment\\\":\\\"enterprise\\\"}\\n\""
    ));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_select_distinct_window_output_before_limit_without_fallback() {
    let source_path = unique_path("sql-local-source-select-distinct-window", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,east,10\n3,east,5\n4,west,7\n5,west,7\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT DISTINCT region,RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r FROM '{}' LIMIT 2",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_distinct_window_limit"
    )));
    assert!(stdout.contains(&field("window_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_rank_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("distinct_projection_output_columns", "region,r")));
    assert!(stdout.contains(&field("distinct_projection_input_row_count", "5")));
    assert!(stdout.contains(&field("distinct_projection_output_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"r\\\":1}\\n{\\\"region\\\":\\\"east\\\",\\\"r\\\":3}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_writes_local_jsonl_output_with_certificate_fields() {
    let source_path = unique_path("sql-local-source-output", "csv");
    let output_path = unique_path("sql-local-source-output", "jsonl");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let written = fs::read_to_string(&output_path).expect("read output jsonl");
    assert_eq!(
        written,
        "{\"id\":2,\"label\":\"beta\"}\n{\"id\":3,\"label\":\"gamma\"}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field(
        "output_certificate_ref",
        "sql-local-source.csv.local-jsonl-output.native-io.v1"
    )));
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("output_workspace_path_safety_status", "enforced")));
    assert!(stdout.contains(&field("output_within_workspace", "true")));
    assert!(stdout.contains(&field("output_symlink_followed", "false")));
    assert!(stdout.contains(&field("output_overwrite_allowed", "false")));
    assert!(stdout.contains(&field("output_commit_mode", "atomic_rename_same_directory")));
    assert!(stdout.contains(&field("output_commit_status", "committed")));
    assert!(stdout.contains(&field(
        "output_cleanup_status",
        "no_staging_artifacts_remaining"
    )));
    assert!(stdout.contains(&field("output_fallback_attempted", "false")));
    assert!(stdout.contains(&field("output_external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_output_capillary_skips_small_local_csv_output() {
    let source_path = unique_path("sql-local-source-csv-output", "csv");
    let output_path = unique_path("sql-local-source-csv-output", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,\"beta, quoted\",15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let written = fs::read_to_string(&output_path).expect("read output csv");
    assert_eq!(written, "id,label\n2,\"beta, quoted\"\n3,gamma\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("result_format", "inline_jsonl")));
    assert!(stdout.contains(&field(
        "result_batch_state_status",
        "shared_flat_scalar_columnar_boundary_available"
    )));
    assert!(stdout.contains("\"result_batch_state_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field(
        "result_batch_state_layout",
        "flat_scalar_column_vectors_v1"
    )));
    assert!(stdout.contains(&field("result_batch_state_row_count", "2")));
    assert!(stdout.contains(&field("result_batch_state_column_count", "2")));
    assert!(stdout.contains(&field(
        "result_batch_state_materialization_required",
        "terminal_text_materialization_required"
    )));
    assert!(stdout.contains(&field("result_batch_state_decode_required", "false")));
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains("\"output_conversion_millis\",\"value\":\""));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_csv_sink"
    )));
    assert!(stdout.contains(&field(
        "output_certificate_ref",
        "sql-local-source.csv.local-csv-output.native-io.v1"
    )));
    assert!(stdout.contains("\"output_plan_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("output_plan_status", "smoke_supported")));
    assert!(stdout.contains(&field(
        "output_plan_materialization_required",
        "terminal_text_materialization_required"
    )));
    assert!(stdout.contains(&field("output_plan_required_columns", "id,label")));
    assert!(stdout.contains(&field("output_plan_ordering_required", "false")));
    assert!(stdout.contains(&field(
        "output_plan_statistics_required",
        "not_required_for_text_sink"
    )));
    assert!(stdout.contains(&field(
        "output_plan_text_materialization_boundary",
        "csv_terminal_encoder"
    )));
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field(
        "output_plan_type_nullability_support",
        "flat_scalar_and_nested_json_text_values_null_as_empty_boundary"
    )));
    assert!(stdout.contains(&field(
        "output_plan_compression_encoding_posture",
        "csv_uncompressed_text_terminal_encoder"
    )));
    assert!(stdout.contains(&field("output_plan_replay_depth", "write_digest_replay")));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_status",
        "advisory_only_compatibility_targets"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_selected_strategy",
        "advisory_only_no_runtime_write_knob_applied"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_runtime_decision_applied",
        "false"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_target_strategies",
        "csv_streaming_text_chunk_advisory"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_blocker",
        "compatibility_targets_advisory_only_no_writer_knob_applied"
    )));
    assert!(
        stdout.contains(
            "\"output_layout_write_advisor_strategy_decision_digest\",\"value\":\"fnv64:"
        )
    );
    assert!(stdout.contains(
        "csv:column_names=preserved,row_order=preserved,row_count=digest_replay_verified,nested_values=json_text_when_present,static_types=dropped"
    ));
    assert!(stdout.contains(
        "csv:static_types_nullability_nested_type_metadata_and_vortex_layout_metadata_lost_json_text_values_preserved"
    ));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains(&field(
        "output_replay_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&field(
        "output_fidelity_report_status",
        "scoped_local_output_fidelity_reported"
    )));
    assert!(stdout.contains("csv:csv_text_roundtrip_loses_static_and_nested_type_metadata"));
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("sink_artifact_count", "1")));
    assert!(stdout.contains(&field(
        "sink_artifact_ref",
        &output_path.display().to_string()
    )));
    assert!(stdout.contains("\"sink_artifact_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("sink_artifact_formats", "csv")));
    assert!(stdout.contains("\"sink_artifact_conversion_millis\",\"value\":\""));
    assert!(stdout.contains(&field(
        "output_capillary_status",
        "not_requested_below_threshold"
    )));
    assert!(stdout.contains(&field(
        "output_capillary_activation_reason",
        "below_threshold_small_local_output"
    )));
    assert!(stdout.contains(&field("output_capillary_task_roles", "none")));
    assert!(stdout.contains(&field("output_capillary_window_count", "0")));
    assert!(stdout.contains(&field(
        "output_sink_pressure_status",
        "below_threshold_small_local_output"
    )));
    assert!(stdout.contains(&field(
        "output_memory_pressure_status",
        "below_threshold_small_local_output"
    )));
    assert!(stdout.contains(&field("pulseweave_output_policy_applied", "false")));
    assert!(stdout.contains(&field(
        "sink_artifact_manifest_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"csv:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        output_path.display(),
        output_path.display()
    )));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_output_capillary_writes_local_jsonl_csv_fanout_with_evidence() {
    let source_path = unique_path("sql-local-source-jsonl-csv-fanout", "csv");
    let jsonl_output_path = unique_path("sql-local-source-jsonl-csv-fanout", "jsonl");
    let csv_output_path = unique_path("sql-local-source-jsonl-csv-fanout", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let jsonl_target = format!("jsonl={}", jsonl_output_path.display());
    let csv_target = format!("csv={}", csv_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &jsonl_target,
            "--fanout-output",
            &csv_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let jsonl = fs::read_to_string(&jsonl_output_path).expect("read jsonl fanout");
    assert!(jsonl.contains(r#""label":"beta""#));
    assert!(jsonl.contains(r#""label":"gamma""#));
    let csv = fs::read_to_string(&csv_output_path).expect("read csv fanout");
    assert!(csv.starts_with("id,label,amount\n"));
    assert!(csv.contains("2,beta,15"));
    assert!(csv.contains("3,gamma,21"));

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_route", "local_fanout")));
    assert!(stdout.contains(&field(
        "result_batch_state_status",
        "shared_flat_scalar_columnar_boundary_available"
    )));
    assert!(stdout.contains("\"result_batch_state_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field(
        "result_batch_state_layout",
        "flat_scalar_column_vectors_v1"
    )));
    assert!(stdout.contains(&field("result_batch_state_row_count", "2")));
    assert!(stdout.contains(&field("result_batch_state_column_count", "3")));
    assert!(stdout.contains(&field(
        "result_batch_state_materialization_required",
        "terminal_text_materialization_required"
    )));
    assert!(stdout.contains(&field("result_batch_state_decode_required", "false")));
    assert!(stdout.contains(&field("output_fanout_performed", "true")));
    assert!(stdout.contains(&field("fanout_output_count", "2")));
    assert!(stdout.contains(&field("fanout_output_formats", "jsonl,csv")));
    assert!(stdout.contains(&field(
        "output_plan_materialization_required",
        "jsonl:terminal_text_materialization_required,csv:terminal_text_materialization_required"
    )));
    assert!(stdout.contains(&field("output_plan_required_columns", "id,label,amount")));
    assert!(stdout.contains(&field(
        "output_plan_ordering_required",
        "jsonl:false,csv:false"
    )));
    assert!(stdout.contains(&field(
        "output_plan_statistics_required",
        "jsonl:not_required_for_text_sink,csv:not_required_for_text_sink"
    )));
    assert!(stdout.contains(&field(
        "output_plan_text_materialization_boundary",
        "jsonl:jsonl_terminal_encoder,csv:csv_terminal_encoder"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "jsonl:none,csv:none"
    )));
    assert!(stdout.contains(&field(
        "fanout_conversion_dag_status",
        "shared_fanout_conversion_dag_applied"
    )));
    assert!(stdout.contains(&field("fanout_shared_stage_count", "3")));
    assert!(stdout.contains(&field("fanout_terminal_sink_count", "2")));
    assert!(stdout.contains("\"fanout_shared_conversion_millis\",\"value\":\""));
    assert!(stdout.contains("\"fanout_terminal_conversion_millis\",\"value\":\""));
    assert!(stdout.contains(&field("fanout_duplicate_conversion_avoided", "true")));
    assert!(stdout.contains(&field(
        "output_capillary_status",
        "applied_output_pulseweave_control"
    )));
    assert!(stdout.contains(&field(
        "output_capillary_task_roles",
        "schema_map,columnar_export,terminal_encode,compression,local_write,digest,replay,evidence_render"
    )));
    assert!(stdout.contains(&field("output_capillary_task_count", "13")));
    assert!(stdout.contains(&field("output_capillary_window_count", "13")));
    assert!(stdout.contains(&field("output_capillary_window_size", "1")));
    assert!(stdout.contains(&field(
        "output_sink_pressure_status",
        "bounded_by_output_sink_pressure"
    )));
    assert!(stdout.contains(&field(
        "output_memory_pressure_status",
        "within_declared_output_memory_budget"
    )));
    assert!(stdout.contains(&field("pulseweave_output_policy_applied", "true")));
    assert!(stdout.contains("\"output_conversion_millis\",\"value\":\""));
    assert!(stdout.contains("\"sink_artifact_conversion_millis\",\"value\":\"jsonl:"));
    assert!(stdout.contains("\"fanout_output_conversion_millis\",\"value\":\""));
    assert!(stdout.contains(&field("sink_artifact_count", "2")));
    assert!(stdout.contains(&field(
        "sink_artifact_refs",
        &format!(
            "jsonl:{},csv:{}",
            jsonl_output_path.display(),
            csv_output_path.display()
        )
    )));
    assert!(stdout.contains("\"sink_artifact_digests\",\"value\":\"jsonl:fnv64:"));
    assert!(stdout.contains("csv:fnv64:"));
    assert!(stdout.contains(&field("sink_artifact_formats", "jsonl,csv")));
    assert!(stdout.contains(&field(
        "sink_artifact_manifest_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"jsonl:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        jsonl_output_path.display(),
        jsonl_output_path.display()
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"csv:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        csv_output_path.display(),
        csv_output_path.display()
    )));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field("result_reuse_for_fanout", "true")));
    assert!(stdout.contains(&field("fanout_result_reuse_hit", "true")));
    assert!(stdout.contains("\"fanout_output_digests\",\"value\":\"jsonl:fnv64:"));
    assert!(stdout.contains("csv:fnv64:"));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_fanout_sinks"
    )));
    assert!(stdout.contains("jsonl:sql-local-source.csv.local-jsonl-output.native-io.v1"));
    assert!(stdout.contains("csv:sql-local-source.csv.local-csv-output.native-io.v1"));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains(&field(
        "output_replay_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains("jsonl:verified_local_file_digest"));
    assert!(stdout.contains("csv:verified_local_file_digest"));
    assert!(stdout.contains("jsonl:logical_rows_replay_verified"));
    assert!(stdout.contains("csv:logical_rows_replay_verified_type_metadata_not_preserved"));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "fanout_output_workspace_path_safety_statuses",
        "jsonl:true,csv:true"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_commit_modes",
        "jsonl:atomic_rename_same_directory,csv:atomic_rename_same_directory"
    )));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(jsonl_output_path).expect("remove jsonl fanout");
    fs::remove_file(csv_output_path).expect("remove csv fanout");
}

#[test]
fn sql_local_source_smoke_writes_complex_jsonl_csv_fanout_without_fallback() {
    let source_path = unique_path("sql-local-source-complex-fanout", "csv");
    let jsonl_output_path = unique_path("sql-local-source-complex-fanout", "jsonl");
    let csv_output_path = unique_path("sql-local-source-complex-fanout", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!(
        "SELECT id,ARRAY[1,2,NULL] AS values FROM '{}' LIMIT 1",
        source_path.display()
    );
    let jsonl_target = format!("jsonl={}", jsonl_output_path.display());
    let csv_target = format!("csv={}", csv_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &jsonl_target,
            "--fanout-output",
            &csv_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "complex_projection_output_boundary",
        "jsonl_nested_result_boundary_and_csv_json_text_sink"
    )));
    assert!(stdout.contains(&field(
        "output_plan_type_nullability_support",
        "jsonl:logical_values_including_nested_json_boundary,csv:flat_scalar_and_nested_json_text_values_null_as_empty_boundary"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "jsonl:none,csv:none"
    )));
    assert!(stdout.contains(&field(
        "fanout_conversion_dag_status",
        "shared_fanout_conversion_dag_applied"
    )));
    assert!(stdout.contains(&field(
        "output_fidelity_loss",
        "jsonl:jsonl_text_roundtrip_not_full_type_metadata_fidelity,csv:csv_text_roundtrip_loses_static_and_nested_type_metadata"
    )));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert_eq!(
        fs::read_to_string(&jsonl_output_path).expect("read jsonl fanout"),
        "{\"id\":1,\"values\":[1,2,null]}\n"
    );
    assert_eq!(
        fs::read_to_string(&csv_output_path).expect("read csv fanout"),
        "id,values\n1,\"[1,2,null]\"\n"
    );

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(jsonl_output_path).expect("remove jsonl fanout");
    fs::remove_file(csv_output_path).expect("remove csv fanout");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_local_parquet_output_with_certificate_fields() {
    let source_path = unique_path("sql-local-source-parquet-output", "csv");
    let output_path = unique_path("sql-local-source-parquet-output", "parquet");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "parquet",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let table =
        shardloom_vortex::read_flat_parquet_source(&output_path, 10).expect("read parquet output");
    assert_eq!(table.header, vec!["id", "label", "amount"]);
    assert_eq!(table.rows.len(), 2);
    assert_eq!(
        table.rows[0].get("label"),
        Some(&shardloom_core::ScalarValue::Utf8("beta".to_string()))
    );
    assert_eq!(
        table.rows[1].get("label"),
        Some(&shardloom_core::ScalarValue::Utf8("gamma".to_string()))
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("result_format", "inline_jsonl")));
    assert!(stdout.contains(&field("output_format", "parquet")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_parquet_sink"
    )));
    assert!(stdout.contains(&field(
        "output_certificate_ref",
        "sql-local-source.local-parquet-output.native-io.v1"
    )));
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output parquet");
}

#[cfg(feature = "universal-format-io")]
fn assert_sql_local_source_writes_feature_gated_output(
    name: &str,
    extension: &str,
    output_format_arg: &str,
    output_format_field: &str,
    certificate_status: &str,
    certificate_ref: &str,
    read_output: fn(
        &std::path::Path,
        usize,
    ) -> shardloom_core::Result<shardloom_vortex::FlatLocalSourceTable>,
) {
    let source_path = unique_path(name, "csv");
    let output_path = unique_path(name, extension);
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            output_format_arg,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let table = read_output(&output_path, 10).expect("read local output");
    assert_eq!(table.header, vec!["id", "label", "amount"]);
    assert_eq!(table.rows.len(), 2);
    assert_eq!(
        table.rows[0].get("label"),
        Some(&shardloom_core::ScalarValue::Utf8("beta".to_string()))
    );
    assert_eq!(
        table.rows[1].get("label"),
        Some(&shardloom_core::ScalarValue::Utf8("gamma".to_string()))
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("result_format", "inline_jsonl")));
    assert!(stdout.contains(&field("output_format", output_format_field)));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        certificate_status
    )));
    assert!(stdout.contains(&field("output_certificate_ref", certificate_ref)));
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output file");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_local_arrow_ipc_output_with_certificate_fields() {
    assert_sql_local_source_writes_feature_gated_output(
        "sql-local-source-arrow-ipc-output",
        "arrow",
        "arrow-ipc",
        "arrow_ipc",
        "certified_local_arrow_ipc_sink",
        "sql-local-source.local-arrow-ipc-output.native-io.v1",
        shardloom_vortex::read_flat_arrow_ipc_source,
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_local_avro_output_with_certificate_fields() {
    assert_sql_local_source_writes_feature_gated_output(
        "sql-local-source-avro-output",
        "avro",
        "avro",
        "avro",
        "certified_local_avro_sink",
        "sql-local-source.local-avro-output.native-io.v1",
        shardloom_vortex::read_flat_avro_source,
    );
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_local_orc_output_with_certificate_fields() {
    assert_sql_local_source_writes_feature_gated_output(
        "sql-local-source-orc-output",
        "orc",
        "orc",
        "orc",
        "certified_local_orc_sink",
        "sql-local-source.local-orc-output.native-io.v1",
        shardloom_vortex::read_flat_orc_source,
    );
}

#[cfg(feature = "vortex-write")]
#[test]
fn sql_local_source_smoke_writes_local_vortex_output_with_certificate_fields() {
    let source_path = unique_path("sql-local-source-vortex-output", "csv");
    let output_path = unique_path("sql-local-source-vortex-output", "vortex");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.exists(), "local Vortex output was written");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("result_format", "inline_jsonl")));
    assert!(stdout.contains(&field("output_format", "vortex")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_vortex_sink"
    )));
    assert!(stdout.contains(&field(
        "output_certificate_ref",
        "sql-local-source.local-vortex-output.native-io.v1"
    )));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "true")));
    assert!(stdout.contains(&field("vortex_output_count", "1")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("vortex_output_row_count", "2")));
    assert!(stdout.contains(&field("vortex_output_column_count", "3")));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_status",
        "applied_local_vortex_layout_write_strategy"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_selected_strategy",
        "single_local_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_runtime_decision_applied",
        "true"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_target_strategies",
        "vortex_single_local_artifact_writer_default_stats_reopen"
    )));
    assert!(stdout.contains(&field("output_layout_write_advisor_blocker", "none")));
    assert!(stdout.contains(
        "\"output_layout_write_advisor_strategy_decision_digest\",\"value\":\"vortex:fnv64:"
    ));
    assert!(stdout.contains(
        "vortex:schema=flat_scalar_or_inferable_typed_nested_preserved,dtypes=preserved,row_count=reopen_verified,statistics=writer_default,layout_intent=writer_default_vortex"
    ));
    assert!(stdout.contains("vortex:none_for_scoped_flat_scalar_or_typed_nested_vortex_output"));
    assert!(stdout.contains("\"vortex_artifact_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains("\"output_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains(&field(
        "output_replay_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(
        "vortex:no_broad_vortex_writer_fidelity_claim_beyond_scoped_flat_scalar_or_inferable_typed_nested"
    ));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
fn sql_local_source_smoke_writes_non_null_binary_vortex_output() {
    let source_path = unique_path("sql-local-source-vortex-binary-output", "csv");
    let output_path = unique_path("sql-local-source-vortex-binary-output", "vortex");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let statement = format!(
        "SELECT id,X'00ff10' AS payload FROM '{}' ORDER BY id ASC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.exists(), "local Vortex output was written");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_format", "vortex")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "true")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("vortex_output_row_count", "2")));
    assert!(stdout.contains(&field("vortex_output_column_count", "2")));
    assert!(stdout.contains(&field(
        "vortex_output_column_families",
        "id:int64,payload:binary"
    )));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output vortex");
}

#[cfg(feature = "vortex-write")]
#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_writes_local_vortex_fanout_with_evidence() {
    let source_path = unique_path("sql-local-source-vortex-fanout", "csv");
    let csv_output_path = unique_path("sql-local-source-vortex-fanout", "csv");
    let vortex_output_path = unique_path("sql-local-source-vortex-fanout", "vortex");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let csv_target = format!("csv={}", csv_output_path.display());
    let vortex_target = format!("vortex={}", vortex_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &csv_target,
            "--fanout-output",
            &vortex_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let csv = fs::read_to_string(&csv_output_path).expect("read csv fanout");
    assert!(csv.starts_with("id,label,amount\n"));
    assert!(
        vortex_output_path.exists(),
        "local Vortex fanout was written"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_route", "local_fanout")));
    assert!(stdout.contains(&field("fanout_output_count", "2")));
    assert!(stdout.contains(&field("fanout_output_formats", "csv,vortex")));
    assert!(stdout.contains(&field(
        "output_plan_materialization_required",
        "csv:terminal_text_materialization_required,vortex:flat_scalar_or_inferable_typed_nested_vortex_writer_bridge_required_no_text_rendering"
    )));
    assert!(stdout.contains(&field("output_plan_required_columns", "id,label,amount")));
    assert!(stdout.contains(&field(
        "output_plan_statistics_required",
        "csv:not_required_for_text_sink,vortex:row_count_reopen_statistics_required"
    )));
    assert!(stdout.contains(&field(
        "output_plan_text_materialization_boundary",
        "csv:csv_terminal_encoder,vortex:not_required_for_requested_sink"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "csv:none,vortex:none"
    )));
    assert!(stdout.contains(&field(
        "fanout_conversion_dag_status",
        "shared_fanout_conversion_dag_applied"
    )));
    assert!(stdout.contains(&field("fanout_shared_stage_count", "3")));
    assert!(stdout.contains(&field("fanout_terminal_sink_count", "2")));
    assert!(stdout.contains(&field("fanout_duplicate_conversion_avoided", "true")));
    assert!(stdout.contains("csv:sql-local-source.csv.local-csv-output.native-io.v1"));
    assert!(stdout.contains("vortex:sql-local-source.local-vortex-output.native-io.v1"));
    assert!(stdout.contains("vortex:certified_local_vortex_sink"));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "true")));
    assert!(stdout.contains(&field("vortex_output_count", "1")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("vortex_output_row_count", "2")));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_status",
        "applied_local_vortex_layout_write_strategy"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_selected_strategy",
        "csv:advisory_only_no_runtime_write_knob_applied,vortex:single_local_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "output_layout_write_advisor_runtime_decision_applied",
        "true"
    )));
    assert!(stdout.contains(
        "csv:csv_streaming_text_chunk_advisory,vortex:vortex_single_local_artifact_writer_default_stats_reopen"
    ));
    assert!(stdout.contains(
        "csv:static_types_nullability_nested_type_metadata_and_vortex_layout_metadata_lost_json_text_values_preserved,vortex:none_for_scoped_flat_scalar_or_typed_nested_vortex_output"
    ));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains("csv:verified_local_file_digest"));
    assert!(stdout.contains("vortex:verified_vortex_reopen_row_count"));
    assert!(stdout.contains("vortex:vortex_flat_scalar_or_typed_nested_reopen_verified"));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(csv_output_path).expect("remove csv fanout");
    fs::remove_file(vortex_output_path).expect("remove vortex fanout");
}

#[cfg(not(feature = "vortex-write"))]
#[test]
fn sql_local_source_smoke_blocks_vortex_output_without_vortex_write_feature() {
    let source_path = unique_path("sql-local-source-vortex-output-blocked", "csv");
    let output_path = unique_path("sql-local-source-vortex-output-blocked", "vortex");
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("output_plan_conversion_blocker=vortex_write_feature_not_enabled"));
    assert!(stdout.contains("requires building shardloom-cli with --features vortex-write"));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(!output_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(not(feature = "vortex-write"))]
#[test]
fn sql_local_source_smoke_blocks_vortex_fanout_without_partial_writes() {
    let source_path = unique_path("sql-local-source-vortex-fanout-blocked", "csv");
    let csv_output_path = unique_path("sql-local-source-vortex-fanout-blocked", "csv");
    let vortex_output_path = unique_path("sql-local-source-vortex-fanout-blocked", "vortex");
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", source_path.display());
    let csv_target = format!("csv={}", csv_output_path.display());
    let vortex_target = format!("vortex={}", vortex_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &csv_target,
            "--fanout-output",
            &vortex_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("output_plan_conversion_blocker=vortex_write_feature_not_enabled"));
    assert!(stdout.contains("requires building shardloom-cli with --features vortex-write"));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(!csv_output_path.exists());
    assert!(!vortex_output_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_writes_feature_gated_structured_fanout_outputs() {
    let source_path = unique_path("sql-local-source-structured-fanout", "csv");
    let parquet_output_path = unique_path("sql-local-source-structured-fanout", "parquet");
    let arrow_output_path = unique_path("sql-local-source-structured-fanout", "arrow");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label,amount FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let parquet_target = format!("parquet={}", parquet_output_path.display());
    let arrow_target = format!("arrow-ipc={}", arrow_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &parquet_target,
            "--fanout-output",
            &arrow_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let parquet =
        shardloom_vortex::read_flat_parquet_source(&parquet_output_path, 10).expect("read parquet");
    let arrow =
        shardloom_vortex::read_flat_arrow_ipc_source(&arrow_output_path, 10).expect("read arrow");
    for table in [&parquet, &arrow] {
        assert_eq!(table.header, vec!["id", "label", "amount"]);
        assert_eq!(table.rows.len(), 2);
        assert_eq!(
            table.rows[0].get("label"),
            Some(&shardloom_core::ScalarValue::Utf8("beta".to_string()))
        );
        assert_eq!(
            table.rows[1].get("label"),
            Some(&shardloom_core::ScalarValue::Utf8("gamma".to_string()))
        );
    }

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_route", "local_fanout")));
    assert!(stdout.contains(&field(
        "result_batch_state_status",
        "shared_flat_scalar_columnar_boundary_available"
    )));
    assert!(stdout.contains("\"result_batch_state_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("result_batch_state_row_count", "2")));
    assert!(stdout.contains(&field("result_batch_state_column_count", "3")));
    assert!(stdout.contains(&field("fanout_output_count", "2")));
    assert!(stdout.contains(&field("fanout_output_formats", "parquet,arrow_ipc")));
    assert!(stdout.contains(&field(
        "output_plan_materialization_required",
        "parquet:flat_scalar_or_inferable_typed_nested_row_bridge_required_no_text_rendering,arrow_ipc:flat_scalar_or_inferable_typed_nested_row_bridge_required_no_text_rendering"
    )));
    assert!(stdout.contains(&field("output_plan_required_columns", "id,label,amount")));
    assert!(stdout.contains(&field(
        "output_plan_statistics_required",
        "parquet:schema_and_row_count_replay_required,arrow_ipc:schema_and_row_count_replay_required"
    )));
    assert!(stdout.contains(&field(
        "output_plan_text_materialization_boundary",
        "parquet:not_required_for_requested_sink,arrow_ipc:not_required_for_requested_sink"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "parquet:none,arrow_ipc:none"
    )));
    assert!(stdout.contains(&field(
        "fanout_conversion_dag_status",
        "shared_fanout_conversion_dag_applied"
    )));
    assert!(stdout.contains(&field("fanout_shared_stage_count", "3")));
    assert!(stdout.contains(&field("fanout_terminal_sink_count", "2")));
    assert!(stdout.contains(&field("fanout_duplicate_conversion_avoided", "true")));
    assert!(stdout.contains("\"fanout_output_conversion_millis\",\"value\":\""));
    assert!(stdout.contains("parquet:sql-local-source.local-parquet-output.native-io.v1"));
    assert!(stdout.contains("arrow_ipc:sql-local-source.local-arrow-ipc-output.native-io.v1"));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains("parquet:verified_local_file_digest"));
    assert!(stdout.contains("arrow_ipc:verified_local_file_digest"));
    assert!(
        stdout.contains("parquet:flat_scalar_or_inferable_typed_nested_schema_replay_verified")
    );
    assert!(
        stdout.contains("arrow_ipc:flat_scalar_or_inferable_typed_nested_schema_replay_verified")
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(parquet_output_path).expect("remove parquet fanout");
    fs::remove_file(arrow_output_path).expect("remove arrow fanout");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_preserves_binary_structured_sinks() {
    let source_path = unique_path("sql-local-source-binary-sink-source", "arrow");
    let parquet_output_path = unique_path("sql-local-source-binary-sink", "parquet");
    let arrow_output_path = unique_path("sql-local-source-binary-sink", "arrow");
    let avro_output_path = unique_path("sql-local-source-binary-sink", "avro");
    let orc_output_path = unique_path("sql-local-source-binary-sink", "orc");
    write_binary_arrow_ipc_smoke_source(&source_path);

    let statement = format!(
        "SELECT id,payload FROM '{}' WHERE payload >= X'00' ORDER BY payload ASC LIMIT 3",
        source_path.display()
    );
    let parquet_target = format!("parquet={}", parquet_output_path.display());
    let arrow_target = format!("arrow-ipc={}", arrow_output_path.display());
    let avro_target = format!("avro={}", avro_output_path.display());
    let orc_target = format!("orc={}", orc_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &parquet_target,
            "--fanout-output",
            &arrow_target,
            "--fanout-output",
            &avro_target,
            "--fanout-output",
            &orc_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let parquet =
        shardloom_vortex::read_flat_parquet_source(&parquet_output_path, 10).expect("read parquet");
    let arrow =
        shardloom_vortex::read_flat_arrow_ipc_source(&arrow_output_path, 10).expect("read arrow");
    let avro = shardloom_vortex::read_flat_avro_source(&avro_output_path, 10).expect("read avro");
    let orc = shardloom_vortex::read_flat_orc_source(&orc_output_path, 10).expect("read orc");
    for table in [&parquet, &arrow, &avro, &orc] {
        assert_eq!(table.header, vec!["id", "payload"]);
        assert_eq!(table.rows.len(), 3);
        assert_eq!(
            table.rows[0].get("payload"),
            Some(&shardloom_core::ScalarValue::Binary(vec![0x00, 0xff, 0x10]))
        );
        assert_eq!(
            table.rows[1].get("payload"),
            Some(&shardloom_core::ScalarValue::Binary(vec![0x01, 0x02]))
        );
        assert_eq!(
            table.rows[2].get("payload"),
            Some(&shardloom_core::ScalarValue::Binary(b"raw".to_vec()))
        );
    }

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("source_format", "arrow_ipc")));
    assert!(stdout.contains(&field("output_route", "local_fanout")));
    assert!(stdout.contains(&field(
        "fanout_output_formats",
        "parquet,arrow_ipc,avro,orc"
    )));
    assert!(stdout.contains(&field("predicate_operator_family", "comparison")));
    assert!(stdout.contains(&field("binary_source_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "binary_source_ordering_predicate_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "parquet:none,arrow_ipc:none,avro:none,orc:none"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source arrow");
    fs::remove_file(parquet_output_path).expect("remove parquet output");
    fs::remove_file(arrow_output_path).expect("remove arrow output");
    fs::remove_file(avro_output_path).expect("remove avro output");
    fs::remove_file(orc_output_path).expect("remove orc output");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn sql_local_source_smoke_preserves_all_null_binary_source_schema_sinks() {
    use arrow_schema::DataType;
    use std::io::BufReader;

    let source_path = unique_path("sql-local-source-all-null-binary-source", "arrow");
    let parquet_output_path = unique_path("sql-local-source-all-null-binary-sink", "parquet");
    let arrow_output_path = unique_path("sql-local-source-all-null-binary-sink", "arrow");
    let avro_output_path = unique_path("sql-local-source-all-null-binary-sink", "avro");
    let orc_output_path = unique_path("sql-local-source-all-null-binary-sink", "orc");
    write_all_null_binary_arrow_ipc_smoke_source(&source_path);

    let statement = format!("SELECT payload FROM '{}' LIMIT 3", source_path.display());
    let parquet_target = format!("parquet={}", parquet_output_path.display());
    let arrow_target = format!("arrow-ipc={}", arrow_output_path.display());
    let avro_target = format!("avro={}", avro_output_path.display());
    let orc_target = format!("orc={}", orc_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &parquet_target,
            "--fanout-output",
            &arrow_target,
            "--fanout-output",
            &avro_target,
            "--fanout-output",
            &orc_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let parquet_file = File::open(&parquet_output_path).expect("open parquet output");
    let parquet_builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(parquet_file)
            .expect("parquet reader builder");
    let parquet_schema = parquet_builder.schema();
    assert_eq!(parquet_schema.field(0).data_type(), &DataType::Binary);

    let arrow_file = File::open(&arrow_output_path).expect("open arrow output");
    let arrow_reader =
        arrow_ipc::reader::FileReader::try_new(arrow_file, None).expect("arrow ipc reader");
    assert_eq!(
        arrow_reader.schema().field(0).data_type(),
        &DataType::Binary
    );
    let avro_file = File::open(&avro_output_path).expect("open avro output");
    let avro_reader = arrow_avro::reader::ReaderBuilder::new()
        .build(BufReader::new(avro_file))
        .expect("avro reader");
    assert_eq!(avro_reader.schema().field(0).data_type(), &DataType::Binary);

    let orc_file = File::open(&orc_output_path).expect("open orc output");
    let orc_builder = orc_rust::ArrowReaderBuilder::try_new(orc_file).expect("orc reader builder");
    assert_eq!(orc_builder.schema().field(0).data_type(), &DataType::Binary);

    let parquet =
        shardloom_vortex::read_flat_parquet_source(&parquet_output_path, 10).expect("read parquet");
    let arrow =
        shardloom_vortex::read_flat_arrow_ipc_source(&arrow_output_path, 10).expect("read arrow");
    let avro = shardloom_vortex::read_flat_avro_source(&avro_output_path, 10).expect("read avro");
    let orc = shardloom_vortex::read_flat_orc_source(&orc_output_path, 10).expect("read orc");
    for table in [&parquet, &arrow, &avro, &orc] {
        assert_eq!(table.header, vec!["payload"]);
        assert_eq!(table.rows.len(), 3);
        for row in &table.rows {
            assert_eq!(row.get("payload"), Some(&shardloom_core::ScalarValue::Null));
        }
    }

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("source_format", "arrow_ipc")));
    assert!(stdout.contains(&field("output_route", "local_fanout")));
    assert!(stdout.contains(&field(
        "fanout_output_formats",
        "parquet,arrow_ipc,avro,orc"
    )));
    assert!(stdout.contains(&field(
        "output_plan_conversion_blocker",
        "parquet:none,arrow_ipc:none,avro:none,orc:none"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove all-null binary source arrow");
    fs::remove_file(parquet_output_path).expect("remove all-null parquet output");
    fs::remove_file(arrow_output_path).expect("remove all-null arrow output");
    fs::remove_file(avro_output_path).expect("remove all-null avro output");
    fs::remove_file(orc_output_path).expect("remove all-null orc output");
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_writes_all_null_binary_vortex_output_with_source_schema() {
    let source_path = unique_path("sql-local-source-all-null-binary-vortex-source", "arrow");
    let output_path = unique_path("sql-local-source-all-null-binary-vortex-output", "vortex");
    write_all_null_binary_arrow_ipc_smoke_source(&source_path);

    let statement = format!("SELECT payload FROM '{}' LIMIT 3", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output_path.exists(),
        "all-null binary Vortex output was written"
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_format", "vortex")));
    assert!(stdout.contains(&field("output_plan_conversion_blocker", "none")));
    assert!(stdout.contains(&field("vortex_output_column_families", "payload:binary")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove all-null binary source arrow");
    fs::remove_file(output_path).expect("remove all-null binary vortex output");
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_parquet_output_without_universal_format_feature() {
    let source_path = unique_path("sql-local-source-parquet-output-blocked", "csv");
    let output_path = unique_path("sql-local-source-parquet-output-blocked", "parquet");
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "parquet",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(!output_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(not(feature = "universal-format-io"))]
fn assert_sql_local_source_blocks_feature_gated_output(
    name: &str,
    extension: &str,
    output_format: &str,
    expected_format_label: &str,
) {
    let source_path = unique_path(name, "csv");
    let output_path = unique_path(name, extension);
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            output_format,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(&format!(
        "local {expected_format_label} output runtime requires building shardloom-cli with --features universal-format-io"
    )));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(!output_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_arrow_ipc_output_without_universal_format_feature() {
    assert_sql_local_source_blocks_feature_gated_output(
        "sql-local-source-arrow-ipc-output-blocked",
        "arrow",
        "arrow-ipc",
        "Arrow IPC",
    );
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_avro_output_without_universal_format_feature() {
    assert_sql_local_source_blocks_feature_gated_output(
        "sql-local-source-avro-output-blocked",
        "avro",
        "avro",
        "Avro",
    );
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_orc_output_without_universal_format_feature() {
    assert_sql_local_source_blocks_feature_gated_output(
        "sql-local-source-orc-output-blocked",
        "orc",
        "orc",
        "ORC",
    );
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn sql_local_source_smoke_blocks_feature_gated_fanout_without_partial_writes() {
    let source_path = unique_path("sql-local-source-fanout-output-blocked", "csv");
    let csv_output_path = unique_path("sql-local-source-fanout-output-blocked", "csv");
    let parquet_output_path = unique_path("sql-local-source-fanout-output-blocked", "parquet");
    fs::write(&source_path, "id,label\n1,alpha\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", source_path.display());
    let csv_target = format!("csv={}", csv_output_path.display());
    let parquet_target = format!("parquet={}", parquet_output_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--fanout-output",
            &csv_target,
            "--fanout-output",
            &parquet_target,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(
        stdout.contains("output_plan_conversion_blocker=universal_format_io_feature_not_enabled")
    );
    assert!(stdout.contains(
        "local Parquet output runtime requires building shardloom-cli with --features universal-format-io"
    ));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(!csv_output_path.exists());
    assert!(!parquet_output_path.exists());

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_writes_literal_projection_csv_header() {
    let source_path = unique_path("sql-local-source-literal-csv-output", "csv");
    let output_path = unique_path("sql-local-source-literal-csv-output", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,'north' AS segment FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let written = fs::read_to_string(&output_path).expect("read output csv");
    assert_eq!(written, "id,segment\n2,north\n3,north\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field("projected_columns", "id,segment")));
    assert!(stdout.contains(&field("literal_projection_columns", "segment")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_csv_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output csv");
}

#[test]
fn sql_local_source_smoke_writes_csv_header_for_empty_output() {
    let source_path = unique_path("sql-local-source-empty-csv-output", "csv");
    let output_path = unique_path("sql-local-source-empty-csv-output", "csv");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount > 100 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output",
            output_path.to_str().expect("utf8 output path"),
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let written = fs::read_to_string(&output_path).expect("read output csv");
    assert_eq!(written, "id,label\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("selected_row_count", "0")));
    assert!(stdout.contains(&field("output_row_count", "0")));
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_csv_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output csv");
}

#[test]
fn sql_local_source_smoke_blocks_csv_output_without_local_output_path() {
    let source_path = unique_path("sql-local-source-csv-output-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", source_path.display());
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &statement,
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "SQL local-source CSV, Parquet, Arrow IPC, Avro, ORC, or Vortex output requires --output <local path>"
    ));
    assert!(stdout.contains("\"attempted\":false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-order-by", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 ORDER BY amount DESC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "single_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "amount")));
    assert!(stdout.contains(&field("sort_direction", "desc")));
    assert!(stdout.contains(&field(
        "sort_null_ordering",
        "nulls_blocked_for_fixture_smoke"
    )));
    assert!(stdout.contains(&field("top_n_limit", "2")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_explicit_null_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-order-by-null-ordering", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,missing_a,\n2,beta,10\n3,gamma,7\n4,missing_b,\n5,delta,12\n",
    )
    .expect("write source csv");

    let nulls_first_statement = format!(
        "SELECT id,label FROM '{}' ORDER BY amount ASC NULLS FIRST LIMIT 4",
        source_path.display()
    );
    let nulls_first_stdout = run_sql_local_source_smoke_json(&nulls_first_statement);

    assert!(nulls_first_stdout.contains("\"status\":\"success\""));
    assert!(nulls_first_stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(nulls_first_stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(nulls_first_stdout.contains(&field("sort_operator_family", "single_key_scalar_topn")));
    assert!(nulls_first_stdout.contains(&field("sort_keys", "amount")));
    assert!(nulls_first_stdout.contains(&field("sort_direction", "asc")));
    assert!(nulls_first_stdout.contains(&field("sort_null_ordering", "nulls_first")));
    assert!(nulls_first_stdout.contains(&field("selected_row_count", "5")));
    assert!(nulls_first_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"missing_a\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"missing_b\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
    ));
    assert!(nulls_first_stdout.contains(&field("fallback_attempted", "false")));
    assert!(nulls_first_stdout.contains(&field("external_engine_invoked", "false")));

    let nulls_last_statement = format!(
        "SELECT id,label FROM '{}' ORDER BY amount DESC NULLS LAST LIMIT 5",
        source_path.display()
    );
    let nulls_last_stdout = run_sql_local_source_smoke_json(&nulls_last_statement);

    assert!(nulls_last_stdout.contains("\"status\":\"success\""));
    assert!(nulls_last_stdout.contains(&field("sort_direction", "desc")));
    assert!(nulls_last_stdout.contains(&field("sort_null_ordering", "nulls_last")));
    assert!(nulls_last_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":5,\\\"label\\\":\\\"delta\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":1,\\\"label\\\":\\\"missing_a\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"missing_b\\\"}\\n\""
    ));
    assert!(nulls_last_stdout.contains(&field("fallback_attempted", "false")));
    assert!(nulls_last_stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_window_row_number_without_fallback() {
    let source_path = unique_path("sql-local-source-window-row-number", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,east,30\n3,west,15\n4,east,20\n5,west,5\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,region,amount,ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM '{}' WHERE amount >= 10 LIMIT 4",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_window_filter_limit"
    )));
    assert!(stdout.contains(&field("window_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_operator_family", "row_number")));
    assert!(stdout.contains(&field("window_function", "row_number")));
    assert!(stdout.contains(&field("window_partition_columns", "region")));
    assert!(stdout.contains(&field("window_order_by_columns", "amount")));
    assert!(stdout.contains(&field("window_order_by_directions", "desc")));
    assert!(stdout.contains(&field("window_output_columns", "rn")));
    assert!(stdout.contains(&field("window_row_number_runtime_execution", "true")));
    assert!(stdout.contains(&field("projected_columns", "id,region,amount,rn")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"region\\\":\\\"east\\\",\\\"amount\\\":10,\\\"rn\\\":3}\\n{\\\"id\\\":2,\\\"region\\\":\\\"east\\\",\\\"amount\\\":30,\\\"rn\\\":1}\\n{\\\"id\\\":3,\\\"region\\\":\\\"west\\\",\\\"amount\\\":15,\\\"rn\\\":1}\\n{\\\"id\\\":4,\\\"region\\\":\\\"east\\\",\\\"amount\\\":20,\\\"rn\\\":2}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.window-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_window_rank_dense_rank_without_fallback() {
    let source_path = unique_path("sql-local-source-window-rank-dense-rank", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,30\n2,east,30\n3,east,20\n4,east,10\n5,west,10\n6,west,5\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,region,amount,RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS r,DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS dr FROM '{}' LIMIT 6",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "local_source_window_limit")));
    assert!(stdout.contains(&field("window_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_operator_family", "ranking")));
    assert!(stdout.contains(&field("window_function", "rank,dense_rank")));
    assert!(stdout.contains(&field("window_partition_columns", "region;region")));
    assert!(stdout.contains(&field("window_order_by_columns", "amount;amount")));
    assert!(stdout.contains(&field("window_order_by_directions", "desc;desc")));
    assert!(stdout.contains(&field("window_output_columns", "r,dr")));
    assert!(stdout.contains(&field("window_row_number_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_rank_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_dense_rank_runtime_execution", "true")));
    assert!(stdout.contains(&field("projected_columns", "id,region,amount,r,dr")));
    assert!(stdout.contains(&field("selected_row_count", "6")));
    assert!(stdout.contains(&field("output_row_count", "6")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"region\\\":\\\"east\\\",\\\"amount\\\":30,\\\"r\\\":1,\\\"dr\\\":1}\\n{\\\"id\\\":2,\\\"region\\\":\\\"east\\\",\\\"amount\\\":30,\\\"r\\\":1,\\\"dr\\\":1}\\n{\\\"id\\\":3,\\\"region\\\":\\\"east\\\",\\\"amount\\\":20,\\\"r\\\":3,\\\"dr\\\":2}\\n{\\\"id\\\":4,\\\"region\\\":\\\"east\\\",\\\"amount\\\":10,\\\"r\\\":4,\\\"dr\\\":3}\\n{\\\"id\\\":5,\\\"region\\\":\\\"west\\\",\\\"amount\\\":10,\\\"r\\\":1,\\\"dr\\\":1}\\n{\\\"id\\\":6,\\\"region\\\":\\\"west\\\",\\\"amount\\\":5,\\\"r\\\":2,\\\"dr\\\":2}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_window_lag_lead_without_fallback() {
    let source_path = unique_path("sql-local-source-window-lag-lead", "csv");
    fs::write(
        &source_path,
        "id,region,amount,label\n\
         1,east,10,alpha\n\
         2,east,30,gamma\n\
         3,west,15,kappa\n\
         4,east,20,beta\n\
         5,west,5,omega\n\
         6,west,25,zeta\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,region,label,LAG(label) OVER (PARTITION BY region ORDER BY amount ASC) AS previous_label,LEAD(label, 2) OVER (PARTITION BY region ORDER BY amount ASC) AS next2_label FROM '{}' LIMIT 6",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "local_source_window_limit")));
    assert!(stdout.contains(&field("window_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_operator_family", "offset")));
    assert!(stdout.contains(&field("window_function", "lag,lead")));
    assert!(stdout.contains(&field("window_partition_columns", "region;region")));
    assert!(stdout.contains(&field("window_order_by_columns", "amount;amount")));
    assert!(stdout.contains(&field("window_order_by_directions", "asc;asc")));
    assert!(stdout.contains(&field(
        "window_output_columns",
        "previous_label,next2_label"
    )));
    assert!(stdout.contains(&field("window_value_columns", "label,label")));
    assert!(stdout.contains(&field("window_offset_rows", "1,2")));
    assert!(stdout.contains(&field("window_row_number_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_rank_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_dense_rank_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_lag_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_lead_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,region,label,previous_label,next2_label"
    )));
    assert!(stdout.contains(&field("selected_row_count", "6")));
    assert!(stdout.contains(&field("output_row_count", "6")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"region\\\":\\\"east\\\",\\\"label\\\":\\\"alpha\\\",\\\"previous_label\\\":null,\\\"next2_label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":2,\\\"region\\\":\\\"east\\\",\\\"label\\\":\\\"gamma\\\",\\\"previous_label\\\":\\\"beta\\\",\\\"next2_label\\\":null}\\n{\\\"id\\\":3,\\\"region\\\":\\\"west\\\",\\\"label\\\":\\\"kappa\\\",\\\"previous_label\\\":\\\"omega\\\",\\\"next2_label\\\":null}\\n{\\\"id\\\":4,\\\"region\\\":\\\"east\\\",\\\"label\\\":\\\"beta\\\",\\\"previous_label\\\":\\\"alpha\\\",\\\"next2_label\\\":null}\\n{\\\"id\\\":5,\\\"region\\\":\\\"west\\\",\\\"label\\\":\\\"omega\\\",\\\"previous_label\\\":null,\\\"next2_label\\\":\\\"zeta\\\"}\\n{\\\"id\\\":6,\\\"region\\\":\\\"west\\\",\\\"label\\\":\\\"zeta\\\",\\\"previous_label\\\":\\\"kappa\\\",\\\"next2_label\\\":null}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let blocked_statement = format!(
        "SELECT id,LEAD(label, 0) OVER (PARTITION BY region ORDER BY amount ASC) AS bad_lead FROM '{}' LIMIT 6",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("LEAD window offset must be between 1 and 50000"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_window_distribution_without_fallback() {
    let source_path = unique_path("sql-local-source-window-distribution", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n\
         1,east,30\n\
         2,east,30\n\
         3,east,20\n\
         4,east,10\n\
         5,west,10\n\
         6,west,5\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,region,amount,NTILE(2) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket,PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS percent_rank,CUME_DIST() OVER (PARTITION BY region ORDER BY amount DESC) AS cume_dist FROM '{}' LIMIT 6",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "local_source_window_limit")));
    assert!(stdout.contains(&field("window_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_operator_family", "distribution")));
    assert!(stdout.contains(&field("window_function", "ntile,percent_rank,cume_dist")));
    assert!(stdout.contains(&field("window_partition_columns", "region;region;region")));
    assert!(stdout.contains(&field("window_order_by_columns", "amount;amount;amount")));
    assert!(stdout.contains(&field("window_order_by_directions", "desc;desc;desc")));
    assert!(stdout.contains(&field(
        "window_output_columns",
        "bucket,percent_rank,cume_dist"
    )));
    assert!(stdout.contains(&field("window_bucket_counts", "2,none,none")));
    assert!(stdout.contains(&field("window_ntile_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_percent_rank_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_cume_dist_runtime_execution", "true")));
    assert!(stdout.contains(&field("window_row_number_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_rank_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_dense_rank_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_lag_runtime_execution", "false")));
    assert!(stdout.contains(&field("window_lead_runtime_execution", "false")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,region,amount,bucket,percent_rank,cume_dist"
    )));
    assert!(stdout.contains(&field("selected_row_count", "6")));
    assert!(stdout.contains(&field("output_row_count", "6")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"region\\\":\\\"east\\\",\\\"amount\\\":30,\\\"bucket\\\":1,\\\"percent_rank\\\":0.0,\\\"cume_dist\\\":0.5}\\n{\\\"id\\\":2,\\\"region\\\":\\\"east\\\",\\\"amount\\\":30,\\\"bucket\\\":1,\\\"percent_rank\\\":0.0,\\\"cume_dist\\\":0.5}\\n{\\\"id\\\":3,\\\"region\\\":\\\"east\\\",\\\"amount\\\":20,\\\"bucket\\\":2,\\\"percent_rank\\\":0.6666666666666666,\\\"cume_dist\\\":0.75}\\n{\\\"id\\\":4,\\\"region\\\":\\\"east\\\",\\\"amount\\\":10,\\\"bucket\\\":2,\\\"percent_rank\\\":1.0,\\\"cume_dist\\\":1.0}\\n{\\\"id\\\":5,\\\"region\\\":\\\"west\\\",\\\"amount\\\":10,\\\"bucket\\\":1,\\\"percent_rank\\\":0.0,\\\"cume_dist\\\":0.5}\\n{\\\"id\\\":6,\\\"region\\\":\\\"west\\\",\\\"amount\\\":5,\\\"bucket\\\":2,\\\"percent_rank\\\":1.0,\\\"cume_dist\\\":1.0}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let blocked_statement = format!(
        "SELECT id,NTILE(0) OVER (PARTITION BY region ORDER BY amount DESC) AS bucket FROM '{}' LIMIT 6",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("NTILE window bucket count must be between 1 and 50000"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_utf8_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-utf8-order-by", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,delta,8\n2,beta,15\n3,gamma,21\n4,alpha,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 ORDER BY label ASC LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "single_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "label")));
    assert!(stdout.contains(&field("sort_direction", "asc")));
    assert!(stdout.contains(&field("top_n_limit", "3")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":4,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_multi_key_scalar_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-multi-key-order-by", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,10\n2,beta,10\n3,gamma,21\n4,delta,21\n5,epsilon,8\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 ORDER BY amount DESC,id ASC LIMIT 3",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "amount,id")));
    assert!(stdout.contains(&field("sort_direction", "desc,asc")));
    assert!(stdout.contains(&field("top_n_limit", "3")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_scalar_aggregates_without_fallback() {
    let source_path = unique_path("sql-local-source-aggregate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,\n4,delta,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT count(*),sum(amount),avg(amount),min(amount),max(amount) FROM '{}' WHERE amount >= 10 LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field(
        "aggregate_functions",
        "count(*),sum(amount),avg(amount),min(amount),max(amount)"
    )));
    assert!(stdout.contains(&field(
        "aggregate_output_columns",
        "count_all,sum_amount,avg_amount,min_amount,max_amount"
    )));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "false")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "count_all,sum_amount,avg_amount,min_amount,max_amount"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"count_all\\\":2,\\\"sum_amount\\\":36,\\\"avg_amount\\\":18.0,\\\"min_amount\\\":15,\\\"max_amount\\\":21}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_aggregate_aliases_without_fallback() {
    let source_path = unique_path("sql-local-source-aggregate-alias", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,21\n4,west,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT count(*) AS rows,sum(amount) AS total_amount,avg(amount) AS mean_amount FROM '{}' WHERE amount >= 10 LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field(
        "aggregate_functions",
        "count(*),sum(amount),avg(amount)"
    )));
    assert!(stdout.contains(&field(
        "aggregate_output_columns",
        "rows,total_amount,mean_amount"
    )));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_aliases", "rows,total_amount,mean_amount")));
    assert!(stdout.contains(&field("projected_columns", "rows,total_amount,mean_amount")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"rows\\\":2,\\\"total_amount\\\":31,\\\"mean_amount\\\":15.5}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_scalar_aggregate_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-aggregate-order-by", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,21\n4,west,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 10 ORDER BY total_amount DESC,rows DESC LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_aggregate_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_aliases", "rows,total_amount")));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "total_amount,rows")));
    assert!(stdout.contains(&field("sort_direction", "desc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "1")));
    assert!(stdout.contains(&field("projected_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(
        stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"rows\\\":2,\\\"total_amount\\\":31}\\n\"")
    );
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.aggregate-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_count_distinct_aggregates_without_fallback() {
    let source_path = unique_path("sql-local-source-count-distinct", "csv");
    fs::write(
        &source_path,
        "id,region,customer_id,amount\n\
         1,east,c1,10\n\
         2,east,c1,12\n\
         3,east,c2,14\n\
         4,east,,16\n\
         5,west,c3,7\n\
         6,west,c4,8\n\
         7,west,c3,9\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,count(DISTINCT customer_id) AS unique_customers,count(*) AS rows FROM '{}' WHERE amount >= 8 GROUP BY region LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field(
        "aggregate_functions",
        "count(DISTINCT customer_id),count(*)"
    )));
    assert!(stdout.contains(&field("aggregate_output_columns", "unique_customers,rows")));
    assert!(stdout.contains(&field("distinct_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "distinct_aggregate_function",
        "count(DISTINCT customer_id)"
    )));
    assert!(stdout.contains(&field("distinct_aggregate_column", "customer_id")));
    assert!(stdout.contains(&field(
        "distinct_aggregate_null_semantics",
        "sql_count_distinct_ignores_nulls"
    )));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "region")));
    assert!(stdout.contains(&field("group_by_group_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "region,unique_customers,rows")));
    assert!(stdout.contains(&field("selected_row_count", "6")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"unique_customers\\\":2,\\\"rows\\\":4}\\n{\\\"region\\\":\\\"west\\\",\\\"unique_customers\\\":2,\\\"rows\\\":2}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let scalar_statement = format!(
        "SELECT count(DISTINCT customer_id) AS unique_customers FROM '{}' WHERE amount >= 8 LIMIT 1",
        source_path.display()
    );
    let scalar = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &scalar_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        scalar.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&scalar.stdout),
        String::from_utf8_lossy(&scalar.stderr)
    );
    let scalar_stdout = String::from_utf8(scalar.stdout).expect("stdout is utf8");
    assert!(scalar_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_aggregate_filter_limit"
    )));
    assert!(scalar_stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(scalar_stdout.contains(&field("distinct_aggregate_runtime_execution", "true")));
    assert!(
        scalar_stdout.contains("\"result_jsonl\",\"value\":\"{\\\"unique_customers\\\":4}\\n\"")
    );
    assert!(scalar_stdout.contains(&field("fallback_attempted", "false")));
    assert!(scalar_stdout.contains(&field("external_engine_invoked", "false")));

    let blocked_statement = format!(
        "SELECT sum(DISTINCT amount) FROM '{}' LIMIT 1",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("COUNT(DISTINCT <column>) only"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_group_by_aggregates_without_fallback() {
    let source_path = unique_path("sql-local-source-group-by", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,12\n4,west,\n5,north,3\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,count(*),sum(amount) FROM '{}' WHERE amount >= 0 GROUP BY region LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "region")));
    assert!(stdout.contains(&field("group_by_key_arity", "1")));
    assert!(stdout.contains(&field("group_by_multi_key_runtime_execution", "false")));
    assert!(stdout.contains(&field("group_by_group_count", "3")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(amount)")));
    assert!(stdout.contains(&field("projected_columns", "region,count_all,sum_amount")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"count_all\\\":2,\\\"sum_amount\\\":22}\\n{\\\"region\\\":\\\"north\\\",\\\"count_all\\\":1,\\\"sum_amount\\\":3}\\n{\\\"region\\\":\\\"west\\\",\\\"count_all\\\":1,\\\"sum_amount\\\":5}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_aggregate_having_without_fallback() {
    let source_path = unique_path("sql-local-source-aggregate-having", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,12\n4,west,14\n5,north,3\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 0 GROUP BY region HAVING total_amount >= 10 AND rows >= 2 ORDER BY total_amount DESC LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_order_by_topn_filter_limit_having"
    )));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.group-by-aggregate-order-by-topn-filter-limit-having.execution.v1"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "region")));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("having_source_column", "total_amount,rows")));
    assert!(stdout.contains(&field("having_input_row_count", "3")));
    assert!(stdout.contains(&field("having_selected_row_count", "2")));
    assert!(stdout.contains(&field("selected_row_count", "5")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total_amount\\\":22}\\n{\\\"region\\\":\\\"west\\\",\\\"rows\\\":2,\\\"total_amount\\\":19}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let scalar_statement = format!(
        "SELECT count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 0 HAVING total_amount >= 40 LIMIT 1",
        source_path.display()
    );
    let scalar = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &scalar_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        scalar.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&scalar.stdout),
        String::from_utf8_lossy(&scalar.stderr)
    );
    let scalar_stdout = String::from_utf8(scalar.stdout).expect("stdout is utf8");
    assert!(scalar_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_aggregate_filter_limit_having"
    )));
    assert!(scalar_stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(scalar_stdout.contains(&field("having_runtime_execution", "true")));
    assert!(scalar_stdout.contains(&field("having_operator_family", "comparison")));
    assert!(scalar_stdout.contains(&field("having_source_column", "total_amount")));
    assert!(scalar_stdout.contains(&field("having_input_row_count", "1")));
    assert!(scalar_stdout.contains(&field("having_selected_row_count", "1")));
    assert!(
        scalar_stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"rows\\\":5,\\\"total_amount\\\":44}\\n\"")
    );

    let unprojected_statement = format!(
        "SELECT region,count(*) AS rows FROM '{}' WHERE amount >= 0 GROUP BY region HAVING sum(amount) >= 10 AND count(*) >= 2 AND count(DISTINCT id) >= 2 ORDER BY rows DESC LIMIT 10",
        source_path.display()
    );
    let unprojected = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &unprojected_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        unprojected.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&unprojected.stdout),
        String::from_utf8_lossy(&unprojected.stderr)
    );
    let unprojected_stdout = String::from_utf8(unprojected.stdout).expect("stdout is utf8");
    assert!(unprojected_stdout.contains(&field("having_runtime_execution", "true")));
    assert!(unprojected_stdout.contains(&field("having_operator_family", "logical_predicate")));
    assert!(unprojected_stdout.contains(&field(
        "having_source_column",
        "sum(amount),count(*),count(DISTINCT id)"
    )));
    assert!(unprojected_stdout.contains(&field("having_aggregate_runtime_execution", "true")));
    assert!(unprojected_stdout.contains(&field(
        "having_aggregate_function",
        "sum(amount),count(*),count(DISTINCT id)"
    )));
    assert!(unprojected_stdout.contains(&field(
        "having_aggregate_output_column",
        "__having_sum_amount_1,__having_count_all_2,__having_count_distinct_id_3"
    )));
    assert!(unprojected_stdout.contains(&field("output_row_count", "2")));
    assert!(unprojected_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2}\\n{\\\"region\\\":\\\"west\\\",\\\"rows\\\":2}\\n\""
    ));
    assert!(unprojected_stdout.contains(&field("fallback_attempted", "false")));
    assert!(unprojected_stdout.contains(&field("external_engine_invoked", "false")));

    let blocked_statement = format!(
        "SELECT region,count(*) AS rows FROM '{}' GROUP BY region HAVING amount >= 10 LIMIT 10",
        source_path.display()
    );
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked.status.success());
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    assert!(blocked_output.contains("HAVING column \\\"amount\\\" is not present"));
    assert!(blocked_output.contains("external_engine_invoked=false"));

    let blocked_distinct_statement = format!(
        "SELECT region,count(*) AS rows FROM '{}' GROUP BY region HAVING sum(DISTINCT amount) >= 10 LIMIT 10",
        source_path.display()
    );
    let blocked_distinct = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &blocked_distinct_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!blocked_distinct.status.success());
    let blocked_distinct_output = format!(
        "{}{}",
        String::from_utf8_lossy(&blocked_distinct.stdout),
        String::from_utf8_lossy(&blocked_distinct.stderr)
    );
    assert!(blocked_distinct_output.contains("COUNT(DISTINCT <column>) only"));
    assert!(blocked_distinct_output.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_group_by_aggregate_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-group-by-order-by", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,12\n4,west,14\n5,north,3\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 0 GROUP BY region ORDER BY total_amount DESC,rows DESC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_group_count", "2")));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "total_amount,rows")));
    assert!(stdout.contains(&field("sort_direction", "desc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "2")));
    assert!(stdout.contains(&field("selected_row_count", "5")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total_amount\\\":22}\\n{\\\"region\\\":\\\"west\\\",\\\"rows\\\":2,\\\"total_amount\\\":19}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.group-by-aggregate-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_group_by_aggregate_utf8_order_by_topn_without_fallback() {
    let source_path = unique_path("sql-local-source-group-by-utf8-order-by", "csv");
    fs::write(
        &source_path,
        "id,region,amount\n1,east,10\n2,west,5\n3,east,12\n4,west,14\n5,north,3\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 0 GROUP BY region ORDER BY region ASC,total_amount DESC LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "region,total_amount")));
    assert!(stdout.contains(&field("sort_direction", "asc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "2")));
    assert!(stdout.contains(&field("selected_row_count", "5")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total_amount\\\":22}\\n{\\\"region\\\":\\\"north\\\",\\\"rows\\\":1,\\\"total_amount\\\":3}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_multi_key_group_by_aggregates_without_fallback() {
    let source_path = unique_path("sql-local-source-multi-key-group-by", "csv");
    fs::write(
        &source_path,
        "\
id,region,segment,amount
1,east,retail,10
2,east,retail,12
3,east,enterprise,7
4,west,retail,5
5,west,enterprise,
6,west,enterprise,3
",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,segment,count(*),sum(amount) FROM '{}' WHERE amount >= 0 GROUP BY region,segment LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "region,segment")));
    assert!(stdout.contains(&field("group_by_key_arity", "2")));
    assert!(stdout.contains(&field("group_by_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_group_count", "4")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "count_all,sum_amount")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "region,segment,count_all,sum_amount"
    )));
    assert!(stdout.contains(&field("selected_row_count", "5")));
    assert!(stdout.contains(&field("output_row_count", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"segment\\\":\\\"enterprise\\\",\\\"count_all\\\":1,\\\"sum_amount\\\":7}\\n{\\\"region\\\":\\\"east\\\",\\\"segment\\\":\\\"retail\\\",\\\"count_all\\\":2,\\\"sum_amount\\\":22}\\n{\\\"region\\\":\\\"west\\\",\\\"segment\\\":\\\"enterprise\\\",\\\"count_all\\\":1,\\\"sum_amount\\\":3}\\n{\\\"region\\\":\\\"west\\\",\\\"segment\\\":\\\"retail\\\",\\\"count_all\\\":1,\\\"sum_amount\\\":5}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_group_by_aggregate_aliases_without_fallback() {
    let source_path = unique_path("sql-local-source-group-by-aggregate-alias", "csv");
    fs::write(
        &source_path,
        "\
id,region,segment,amount
1,east,retail,10
2,east,retail,12
3,east,enterprise,7
4,west,retail,5
5,west,enterprise,
6,west,enterprise,3
",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT region,segment,count(*) AS rows,sum(amount) AS total_amount FROM '{}' WHERE amount >= 0 GROUP BY region,segment LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_group_by_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_aliases", "rows,total_amount")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "region,segment")));
    assert!(stdout.contains(&field("group_by_key_arity", "2")));
    assert!(stdout.contains(&field("group_by_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_group_count", "4")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "region,segment,rows,total_amount"
    )));
    assert!(stdout.contains(&field("selected_row_count", "5")));
    assert!(stdout.contains(&field("output_row_count", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"east\\\",\\\"segment\\\":\\\"enterprise\\\",\\\"rows\\\":1,\\\"total_amount\\\":7}\\n{\\\"region\\\":\\\"east\\\",\\\"segment\\\":\\\"retail\\\",\\\"rows\\\":2,\\\"total_amount\\\":22}\\n{\\\"region\\\":\\\"west\\\",\\\"segment\\\":\\\"enterprise\\\",\\\"rows\\\":1,\\\"total_amount\\\":3}\\n{\\\"region\\\":\\\"west\\\",\\\"segment\\\":\\\"retail\\\",\\\"rows\\\":1,\\\"total_amount\\\":5}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.group-by-aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_duplicate_aggregate_output_names_without_fallback() {
    let source_path = unique_path("sql-local-source-aggregate-duplicate-output", "csv");
    fs::write(&source_path, "id,region,amount\n1,east,10\n2,west,5\n").expect("write source csv");

    let duplicate_alias_statement = format!(
        "SELECT count(*) AS metric,sum(amount) AS metric FROM '{}' LIMIT 1",
        source_path.display()
    );
    let duplicate_alias_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &duplicate_alias_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !duplicate_alias_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&duplicate_alias_output.stdout),
        String::from_utf8_lossy(&duplicate_alias_output.stderr)
    );
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&duplicate_alias_output.stdout),
        String::from_utf8_lossy(&duplicate_alias_output.stderr)
    );
    assert!(blocked_output.contains("aggregate smoke requires unique output column names"));
    assert!(blocked_output.contains("\"fallback\":{\"attempted\":false"));

    let group_column_alias_statement = format!(
        "SELECT region,count(*) AS region FROM '{}' GROUP BY region LIMIT 10",
        source_path.display()
    );
    let group_column_alias_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &group_column_alias_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !group_column_alias_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&group_column_alias_output.stdout),
        String::from_utf8_lossy(&group_column_alias_output.stderr)
    );
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&group_column_alias_output.stdout),
        String::from_utf8_lossy(&group_column_alias_output.stderr)
    );
    assert!(blocked_output.contains("aggregate smoke requires unique output column names"));
    assert!(blocked_output.contains("\"fallback\":{\"attempted\":false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_local_source_smoke_executes_string_like_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-string-like", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,alpine,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let prefix_statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE 'al%' LIMIT 10",
        source_path.display()
    );
    let prefix_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &prefix_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        prefix_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&prefix_output.stdout),
        String::from_utf8_lossy(&prefix_output.stderr)
    );
    let stdout = String::from_utf8(prefix_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "starts_with")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"alpine\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let contains_statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE '%ta%' LIMIT 10",
        source_path.display()
    );
    let contains_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &contains_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        contains_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&contains_output.stdout),
        String::from_utf8_lossy(&contains_output.stderr)
    );
    let stdout = String::from_utf8(contains_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_operator", "contains")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let suffix_statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE '%ta' LIMIT 10",
        source_path.display()
    );
    let suffix_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &suffix_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        suffix_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&suffix_output.stdout),
        String::from_utf8_lossy(&suffix_output.stderr)
    );
    let stdout = String::from_utf8(suffix_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_operator", "ends_with")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let mixed_wildcard_statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE 'a%a' LIMIT 10",
        source_path.display()
    );
    let mixed_wildcard_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &mixed_wildcard_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        mixed_wildcard_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&mixed_wildcard_output.stdout),
        String::from_utf8_lossy(&mixed_wildcard_output.stderr)
    );
    let stdout = String::from_utf8(mixed_wildcard_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "like_pattern")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let underscore_wildcard_statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE '_l%' LIMIT 10",
        source_path.display()
    );
    let underscore_wildcard_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &underscore_wildcard_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        underscore_wildcard_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&underscore_wildcard_output.stdout),
        String::from_utf8_lossy(&underscore_wildcard_output.stderr)
    );
    let stdout = String::from_utf8(underscore_wildcard_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("string_predicate_operator", "like_pattern")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"alpine\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let negated_wildcard_statement = format!(
        "SELECT id,label FROM '{}' WHERE label NOT LIKE 'a_p%' LIMIT 10",
        source_path.display()
    );
    let negated_wildcard_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &negated_wildcard_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        negated_wildcard_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&negated_wildcard_output.stdout),
        String::from_utf8_lossy(&negated_wildcard_output.stderr)
    );
    let stdout = String::from_utf8(negated_wildcard_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("string_predicate_operator", "like_pattern")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_regex_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-regex", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let regex_statement = format!(
        "SELECT id,label FROM '{}' WHERE label RLIKE '^(alpha|gamma)$' LIMIT 10",
        source_path.display()
    );
    let regex_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &regex_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        regex_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&regex_output.stdout),
        String::from_utf8_lossy(&regex_output.stderr)
    );
    let stdout = String::from_utf8(regex_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "regex_match")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let invalid_statement = format!(
        "SELECT id,label FROM '{}' WHERE label REGEXP '[' LIMIT 10",
        source_path.display()
    );
    let invalid_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &invalid_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !invalid_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&invalid_output.stdout),
        String::from_utf8_lossy(&invalid_output.stderr)
    );
    let blocked_output = format!(
        "{}{}",
        String::from_utf8_lossy(&invalid_output.stdout),
        String::from_utf8_lossy(&invalid_output.stderr)
    );
    assert!(blocked_output.contains("regex pattern is invalid"));
    assert!(blocked_output.contains("\"fallback\":{\"attempted\":false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_transform_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-string-transform", "csv");
    fs::write(
        &source_path,
        "id,label\n1,Alpha\n2,BETA\n3, gamma \n4,delta\n",
    )
    .expect("write source csv");

    let lower_statement = format!(
        "SELECT id,label FROM '{}' WHERE LOWER(label) = 'alpha' LIMIT 10",
        source_path.display()
    );
    let lower_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &lower_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        lower_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&lower_output.stdout),
        String::from_utf8_lossy(&lower_output.stderr)
    );
    let stdout = String::from_utf8(lower_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_transform")));
    assert!(stdout.contains(&field("string_transform_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_transform_operator", "lower")));
    assert!(stdout.contains(&field("string_transform_source_column", "label")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"Alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let upper_statement = format!(
        "SELECT id,label FROM '{}' WHERE UPPER(label) = 'BETA' LIMIT 10",
        source_path.display()
    );
    let upper_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &upper_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        upper_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&upper_output.stdout),
        String::from_utf8_lossy(&upper_output.stderr)
    );
    let stdout = String::from_utf8(upper_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("string_transform_operator", "upper")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"BETA\\\"}\\n\""
        )
    );

    let trim_statement = format!(
        "SELECT id,label FROM '{}' WHERE TRIM(label) = 'gamma' LIMIT 10",
        source_path.display()
    );
    let trim_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &trim_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        trim_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&trim_output.stdout),
        String::from_utf8_lossy(&trim_output.stderr)
    );
    let stdout = String::from_utf8(trim_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("string_transform_operator", "trim")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"label\\\":\\\" gamma \\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_function_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-string-function", "csv");
    fs::write(
        &source_path,
        "id,label,segment\n1,alpha,north\n2,beta,east\n3,alpaca,north\n4,,west\n",
    )
    .expect("write source csv");

    let concat_statement = format!(
        "SELECT id,label FROM '{}' WHERE CONCAT(label, '-', segment) = 'alpha-north' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&concat_statement);
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_function")));
    assert!(stdout.contains(&field("string_function_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_function_operator", "concat")));
    assert!(stdout.contains(&field("string_function_source_column", "label+segment")));
    assert!(stdout.contains(&field("string_function_literal_count", "2")));
    assert!(stdout.contains(&field("string_function_rhs_dtype", "utf8")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let substr_statement = format!(
        "SELECT id,label FROM '{}' WHERE SUBSTR(label, 2, 2) = 'et' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&substr_statement);
    assert!(stdout.contains(&field("string_function_operator", "substr")));
    assert!(stdout.contains(&field("string_function_source_column", "label")));
    assert!(stdout.contains(&field("string_function_literal_count", "3")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let left_statement = format!(
        "SELECT id,label FROM '{}' WHERE LEFT(label, 2) = 'al' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&left_statement);
    assert!(stdout.contains(&field("string_function_operator", "left")));
    assert!(stdout.contains(&field("string_function_source_column", "label")));
    assert!(stdout.contains(&field("string_function_literal_count", "2")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"alpaca\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let right_statement = format!(
        "SELECT id,label FROM '{}' WHERE RIGHT(label, 2) = 'ta' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&right_statement);
    assert!(stdout.contains(&field("string_function_operator", "right")));
    assert!(stdout.contains(&field("string_function_source_column", "label")));
    assert!(stdout.contains(&field("string_function_literal_count", "2")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let replace_statement = format!(
        "SELECT id,label FROM '{}' WHERE REPLACE(label, 'a', '') = 'lph' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&replace_statement);
    assert!(stdout.contains(&field("string_function_operator", "replace")));
    assert!(stdout.contains(&field("string_function_source_column", "label")));
    assert!(stdout.contains(&field("string_function_literal_count", "3")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_arithmetic_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-arithmetic", "csv");
    fs::write(
        &source_path,
        "id,amount,ratio\n1,8,0.25\n2,15,0.5\n3,21,0.75\n4,,1.25\n",
    )
    .expect("write source csv");

    let add_statement = format!(
        "SELECT id,amount FROM '{}' WHERE amount + 5 >= 20 LIMIT 10",
        source_path.display()
    );
    let add_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &add_statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        add_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&add_output.stdout),
        String::from_utf8_lossy(&add_output.stderr)
    );
    let stdout = String::from_utf8(add_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "numeric_arithmetic")));
    assert!(stdout.contains(&field("numeric_arithmetic_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_arithmetic_operator", "add")));
    assert!(stdout.contains(&field("numeric_arithmetic_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_arithmetic_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"amount\\\":15}\\n{\\\"id\\\":3,\\\"amount\\\":21}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let float_statement = format!(
        "SELECT id,ratio FROM '{}' WHERE ratio * 2.0 > 1.0 LIMIT 10",
        source_path.display()
    );
    let float_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &float_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        float_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&float_output.stdout),
        String::from_utf8_lossy(&float_output.stderr)
    );
    let stdout = String::from_utf8(float_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("numeric_arithmetic_operator", "multiply")));
    assert!(stdout.contains(&field("numeric_arithmetic_rhs_dtype", "float64")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"ratio\\\":0.75}\\n{\\\"id\\\":4,\\\"ratio\\\":1.25}\\n\""
    ));

    let divide_by_zero_statement = format!(
        "SELECT id FROM '{}' WHERE amount / 0 >= 1 LIMIT 10",
        source_path.display()
    );
    let divide_by_zero_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &divide_by_zero_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!divide_by_zero_output.status.success());
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&divide_by_zero_output.stdout),
        String::from_utf8_lossy(&divide_by_zero_output.stderr)
    );
    assert!(output.contains("numeric arithmetic division by zero is a runtime data error"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_mixed_numeric_arithmetic_predicate_without_fallback() {
    let source_path = unique_path("sql-local-source-mixed-numeric-arithmetic", "csv");
    fs::write(&source_path, "id,amount\n1,8\n2,15\n3,21\n4,\n").expect("write source csv");

    let statement = format!(
        "SELECT id,amount FROM '{}' WHERE amount + 2.5 >= 17.5 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("numeric_arithmetic_operator", "add")));
    assert!(stdout.contains(&field("numeric_arithmetic_rhs_dtype", "float64")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"amount\\\":15}\\n{\\\"id\\\":3,\\\"amount\\\":21}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_generic_expression_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-generic-expression-predicate", "csv");
    fs::write(
        &source_path,
        "id,amount,tax\n1,8,2\n2,15,5\n3,21,4\n4,12,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,amount FROM '{}' WHERE (amount + tax) * 2 >= 40 AND ABS(amount - tax) > 8 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_source_column",
        "amount+tax,amount+tax"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_operator_family",
        "numeric_binary,numeric_abs+numeric_binary"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_binary_operator_count",
        "3"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_predicate_comparison_operator",
        "gte,gt"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"amount\\\":15}\\n{\\\"id\\\":3,\\\"amount\\\":21}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let divide_by_zero_statement = format!(
        "SELECT id FROM '{}' WHERE (amount + tax) / 0 >= 1 LIMIT 10",
        source_path.display()
    );
    let divide_by_zero_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &divide_by_zero_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!divide_by_zero_output.status.success());
    let blocked = format!(
        "{}{}",
        String::from_utf8_lossy(&divide_by_zero_output.stdout),
        String::from_utf8_lossy(&divide_by_zero_output.stderr)
    );
    assert!(blocked.contains("generic numeric expression division by zero is not admitted"));
    assert!(blocked.contains("external_engine_invoked=false"));

    let missing_statement = format!(
        "SELECT id FROM '{}' WHERE (amount + missing_tax) * 2 >= 1 LIMIT 10",
        source_path.display()
    );
    let missing_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &missing_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!missing_output.status.success());
    let missing = format!(
        "{}{}",
        String::from_utf8_lossy(&missing_output.stdout),
        String::from_utf8_lossy(&missing_output.stderr)
    );
    assert!(missing.contains("predicate column \\\"missing_tax\\\" is not present"));
    assert!(missing.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_in_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-in-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label IN ('alpha','gamma') LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "in_predicate")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "2")));
    assert!(stdout.contains(&field("in_list_null_value_count", "0")));
    assert!(stdout.contains(&field("in_predicate_null_semantics", "not_applicable")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_null_aware_in_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-null-aware-in-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,,21\n4,gamma,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label IN ('alpha', NULL) LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "in_predicate")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "2")));
    assert!(stdout.contains(&field("in_list_null_value_count", "1")));
    assert!(stdout.contains(&field(
        "in_predicate_null_semantics",
        "sql_three_valued_where_filter"
    )));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_row_value_in_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-row-value-in-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,alpha,13\n5,,34\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE (id,label) IN ((1,'alpha'),(3,'gamma'),(5,NULL)) LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "predicate_operator_family",
        "row_value_in_predicate"
    )));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "3")));
    assert!(stdout.contains(&field("in_list_null_value_count", "1")));
    assert!(stdout.contains(&field("row_value_in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("row_value_in_source_columns", "id,label")));
    assert!(stdout.contains(&field("row_value_in_column_groups", "id+label")));
    assert!(stdout.contains(&field("row_value_in_column_count", "2")));
    assert!(stdout.contains(&field("row_value_in_tuple_count", "3")));
    assert!(stdout.contains(&field("row_value_in_null_value_count", "1")));
    assert!(stdout.contains(&field(
        "row_value_in_null_semantics",
        "sql_row_value_three_valued_where_filter"
    )));
    assert!(stdout.contains(&field(
        "in_predicate_null_semantics",
        "sql_three_valued_where_filter"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_in_subquery_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-in-subquery-predicate", "csv");
    let allowed_path = unique_path("sql-local-source-in-subquery-allowed", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");
    fs::write(&allowed_path, "id\n1\n3\nNULL\n").expect("write allowed csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id IN (SELECT id FROM '{}') LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "in_subquery")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "3")));
    assert!(stdout.contains(&field("in_list_null_value_count", "1")));
    assert!(stdout.contains(&field("in_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_source_column", "id")));
    assert!(stdout.contains(&field("in_subquery_source_format", "csv")));
    assert!(stdout.contains(&field("in_subquery_materialized_value_count", "3")));
    assert!(stdout.contains(&field("in_subquery_materialized_null_value_count", "1")));
    assert!(stdout.contains(&field(
        "in_predicate_null_semantics",
        "sql_three_valued_where_filter"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    assert_in_subquery_missing_column_blocks(&source_path, &allowed_path);
    let oversized_path = assert_in_subquery_oversized_blocks(&source_path);

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
    fs::remove_file(oversized_path).expect("remove oversized csv");
}

#[test]
fn sql_local_source_smoke_executes_filtered_ordered_limited_in_subquery_without_fallback() {
    let source_path = unique_path("sql-local-source-filtered-in-subquery-source", "csv");
    let allowed_path = unique_path("sql-local-source-filtered-in-subquery-allowed", "csv");
    fs::write(
        &source_path,
        "id,label\n1,alpha\n2,beta\n3,gamma\n4,delta\n",
    )
    .expect("write source csv");
    fs::write(
        &allowed_path,
        "id,active,score\n1,true,10\n2,false,100\n3,true,30\n4,true,20\n",
    )
    .expect("write allowed csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id IN (SELECT id FROM '{}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "in_subquery")));
    assert!(stdout.contains(&field("in_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_source_column", "id")));
    assert!(stdout.contains(&field("in_subquery_input_row_count", "4")));
    assert!(stdout.contains(&field("in_subquery_filtered_row_count", "3")));
    assert!(stdout.contains(&field("in_subquery_materialized_value_count", "2")));
    assert!(stdout.contains(&field("in_subquery_materialized_null_value_count", "0")));
    assert!(stdout.contains(&field("in_subquery_materialization_bound", "32")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
}

#[test]
fn sql_local_source_smoke_executes_exists_subquery_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-exists-subquery-source", "csv");
    let allowed_path = unique_path("sql-local-source-exists-subquery-allowed", "csv");
    let blocked_path = unique_path("sql-local-source-not-exists-subquery-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n3,gamma\n").expect("write source csv");
    fs::write(&allowed_path, "active,score\nfalse,10\ntrue,30\ntrue,20\n")
        .expect("write allowed csv");
    fs::write(&blocked_path, "active\nfalse\nfalse\n").expect("write blocked csv");

    let exists_statement = format!(
        "SELECT id,label FROM '{}' WHERE EXISTS (SELECT * FROM '{}' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let exists_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &exists_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        exists_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&exists_output.stdout),
        String::from_utf8_lossy(&exists_output.stderr)
    );
    let stdout = String::from_utf8(exists_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "exists_subquery")));
    assert!(stdout.contains(&field("exists_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_projection_kind", "wildcard")));
    assert!(stdout.contains(&field("exists_subquery_source_format", "csv")));
    assert!(stdout.contains(&field("exists_subquery_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_input_row_count", "3")));
    assert!(stdout.contains(&field("exists_subquery_filtered_row_count", "2")));
    assert!(stdout.contains(&field("exists_subquery_bounded_row_count", "1")));
    assert!(stdout.contains(&field("exists_subquery_result", "true")));
    assert!(stdout.contains(&field(
        "exists_subquery_null_semantics",
        "sql_exists_two_valued_presence_test"
    )));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let not_exists_statement = format!(
        "SELECT id,label FROM '{}' WHERE NOT EXISTS (SELECT 1 FROM '{}' WHERE active IS TRUE LIMIT 1) LIMIT 10",
        source_path.display(),
        blocked_path.display()
    );
    let not_exists_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &not_exists_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        not_exists_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&not_exists_output.stdout),
        String::from_utf8_lossy(&not_exists_output.stderr)
    );
    let stdout = String::from_utf8(not_exists_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("exists_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_projection_kind", "literal")));
    assert!(stdout.contains(&field("exists_subquery_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_filtered_row_count", "0")));
    assert!(stdout.contains(&field("exists_subquery_bounded_row_count", "0")));
    assert!(stdout.contains(&field("exists_subquery_result", "false")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
    fs::remove_file(blocked_path).expect("remove blocked csv");
}

#[test]
fn sql_local_source_smoke_executes_having_in_subquery_without_fallback() {
    let source_path = unique_path("sql-local-source-having-in-subquery-source", "csv");
    let allowed_path = unique_path("sql-local-source-having-in-subquery-allowed", "csv");
    fs::write(
        &source_path,
        "region,id,amount\n\
         east,1,10\n\
         east,2,13\n\
         west,3,20\n\
         north,4,12\n\
         north,5,15\n\
         north,6,18\n",
    )
    .expect("write source csv");
    fs::write(
        &allowed_path,
        "rows,active,score\n2,true,10\n3,true,20\n1,false,30\n",
    )
    .expect("write allowed csv");

    let statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{}' GROUP BY region HAVING rows IN (SELECT rows FROM '{}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY total DESC LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_operator_family", "in_subquery")));
    assert!(stdout.contains(&field("having_source_column", "rows")));
    assert!(stdout.contains(&field("having_in_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_subquery_source_column", "rows")));
    assert!(stdout.contains(&field("in_subquery_input_row_count", "3")));
    assert!(stdout.contains(&field("in_subquery_filtered_row_count", "2")));
    assert!(stdout.contains(&field("in_subquery_materialized_value_count", "2")));
    assert!(stdout.contains(&field("having_input_row_count", "3")));
    assert!(stdout.contains(&field("having_selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"north\\\",\\\"rows\\\":3,\\\"total\\\":45}\\n{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total\\\":23}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
}

fn write_having_subquery_csv_fixtures() -> (PathBuf, PathBuf, PathBuf) {
    let source_path = unique_path("sql-local-source-having-subquery-source", "csv");
    let allowed_path = unique_path("sql-local-source-having-exists-subquery-allowed", "csv");
    let thresholds_path = unique_path("sql-local-source-having-quantified-thresholds", "csv");
    fs::write(
        &source_path,
        "region,id,amount\n\
         east,1,10\n\
         east,2,13\n\
         west,3,20\n\
         north,4,12\n\
         north,5,15\n\
         north,6,18\n",
    )
    .expect("write source csv");
    fs::write(&allowed_path, "active,score\nfalse,10\ntrue,30\ntrue,20\n")
        .expect("write allowed csv");
    fs::write(
        &thresholds_path,
        "threshold,active,score\n20,true,10\n22,true,20\n99,false,30\n",
    )
    .expect("write thresholds csv");
    (source_path, allowed_path, thresholds_path)
}

#[test]
fn sql_local_source_smoke_executes_having_exists_subquery_without_fallback() {
    let (source_path, allowed_path, thresholds_path) = write_having_subquery_csv_fixtures();

    let exists_statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{}' GROUP BY region HAVING EXISTS (SELECT * FROM '{}' WHERE active IS TRUE ORDER BY score DESC LIMIT 1) ORDER BY total DESC LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let exists_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &exists_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        exists_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&exists_output.stdout),
        String::from_utf8_lossy(&exists_output.stderr)
    );
    let stdout = String::from_utf8(exists_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_operator_family", "exists_subquery")));
    assert!(stdout.contains(&field("having_exists_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_projection_kind", "wildcard")));
    assert!(stdout.contains(&field("exists_subquery_source_format", "csv")));
    assert!(stdout.contains(&field("exists_subquery_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("exists_subquery_input_row_count", "3")));
    assert!(stdout.contains(&field("exists_subquery_filtered_row_count", "2")));
    assert!(stdout.contains(&field("exists_subquery_bounded_row_count", "1")));
    assert!(stdout.contains(&field("exists_subquery_result", "true")));
    assert!(stdout.contains(&field("having_input_row_count", "3")));
    assert!(stdout.contains(&field("having_selected_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"north\\\",\\\"rows\\\":3,\\\"total\\\":45}\\n{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total\\\":23}\\n{\\\"region\\\":\\\"west\\\",\\\"rows\\\":1,\\\"total\\\":20}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
    fs::remove_file(thresholds_path).expect("remove thresholds csv");
}

#[test]
fn sql_local_source_smoke_executes_having_quantified_subquery_without_fallback() {
    let (source_path, allowed_path, thresholds_path) = write_having_subquery_csv_fixtures();

    let quantified_statement = format!(
        "SELECT region,count(*) AS rows,sum(amount) AS total FROM '{}' GROUP BY region HAVING total > ALL (SELECT threshold FROM '{}' WHERE active IS TRUE ORDER BY score DESC LIMIT 2) ORDER BY total DESC LIMIT 10",
        source_path.display(),
        thresholds_path.display()
    );
    let quantified_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &quantified_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        quantified_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&quantified_output.stdout),
        String::from_utf8_lossy(&quantified_output.stderr)
    );
    let stdout = String::from_utf8(quantified_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_operator_family", "quantified_subquery")));
    assert!(stdout.contains(&field("having_source_column", "total")));
    assert!(stdout.contains(&field(
        "having_quantified_subquery_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("quantified_subquery_runtime_execution", "true")));
    assert!(stdout.contains(&field("quantified_subquery_quantifier", "all")));
    assert!(stdout.contains(&field("quantified_subquery_comparison_operator", "gt")));
    assert!(stdout.contains(&field("quantified_subquery_source_column", "threshold")));
    assert!(stdout.contains(&field("quantified_subquery_source_format", "csv")));
    assert!(stdout.contains(&field(
        "quantified_subquery_filter_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "quantified_subquery_order_by_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "quantified_subquery_limit_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("quantified_subquery_input_row_count", "3")));
    assert!(stdout.contains(&field("quantified_subquery_filtered_row_count", "2")));
    assert!(stdout.contains(&field("quantified_subquery_materialized_value_count", "2")));
    assert!(stdout.contains(&field("having_input_row_count", "3")));
    assert!(stdout.contains(&field("having_selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"region\\\":\\\"north\\\",\\\"rows\\\":3,\\\"total\\\":45}\\n{\\\"region\\\":\\\"east\\\",\\\"rows\\\":2,\\\"total\\\":23}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(allowed_path).expect("remove allowed csv");
    fs::remove_file(thresholds_path).expect("remove thresholds csv");
}

fn assert_in_subquery_missing_column_blocks(source_path: &Path, allowed_path: &Path) {
    let statement = format!(
        "SELECT id FROM '{}' WHERE id IN (SELECT missing_id FROM '{}') LIMIT 10",
        source_path.display(),
        allowed_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!output.status.success());
    let error = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        error.contains("IN subquery source column \\\"missing_id\\\" is not present"),
        "{error}"
    );
    assert!(error.contains("external_engine_invoked=false"));
}

fn assert_in_subquery_oversized_blocks(source_path: &Path) -> PathBuf {
    let oversized_path = unique_path("sql-local-source-in-subquery-oversized", "csv");
    let oversized_rows = (1..=33)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(&oversized_path, format!("id\n{oversized_rows}\n"))
        .expect("write oversized allowed csv");
    let statement = format!(
        "SELECT id FROM '{}' WHERE id IN (SELECT id FROM '{}') LIMIT 10",
        source_path.display(),
        oversized_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(!output.status.success());
    let error = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        error.contains("IN subquery predicates admit at most 32 materialized values"),
        "{error}"
    );
    assert!(error.contains("external_engine_invoked=false"));
    oversized_path
}

#[test]
fn sql_local_source_smoke_executes_direct_not_in_and_not_like_without_fallback() {
    let source_path = unique_path("sql-local-source-not-in-not-like-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label NOT IN ('alpha','gamma') AND label NOT LIKE '%lt%' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "2")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "contains")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_preserves_not_in_sql_three_valued_null_semantics() {
    let source_path = unique_path("sql-local-source-not-in-null-semantics", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,,21\n4,gamma,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label NOT IN ('alpha', NULL) LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "2")));
    assert!(stdout.contains(&field("in_list_null_value_count", "1")));
    assert!(stdout.contains(&field(
        "in_predicate_null_semantics",
        "sql_three_valued_where_filter"
    )));
    assert!(stdout.contains(&field("selected_row_count", "0")));
    assert!(stdout.contains("\"result_jsonl\",\"value\":\"\""));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_is_null_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-is-null-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,,15\n3,gamma,21\n4,NULL,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label IS NULL LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "null_predicate")));
    assert!(stdout.contains(&field("filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("null_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("null_predicate_operator", "is_null")));
    assert!(stdout.contains(&field("null_predicate_source_column", "label")));
    assert!(stdout.contains(&field(
        "null_predicate_null_semantics",
        "sql_is_null_is_not_null"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":null}\\n{\\\"id\\\":4,\\\"label\\\":null}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_is_not_null_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-is-not-null-predicate", "jsonl");
    fs::write(
        &source_path,
        "{\"id\":1,\"closed_at\":null}\n{\"id\":2,\"closed_at\":\"2026-05-19\"}\n{\"id\":3,\"closed_at\":null}\n",
    )
    .expect("write source jsonl");

    let statement = format!(
        "SELECT id,closed_at FROM '{}' WHERE closed_at IS NOT NULL LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert_inferred_adapter_evidence(
        &stdout,
        ExpectedAdapterEvidence {
            source_format: "jsonl",
            extension: ".jsonl",
            adapter_id: "local_jsonl_input_adapter",
            registry_entry_id: "shardloom.local_input_adapter.jsonl.v1",
            admitted_extensions: ".jsonl,.ndjson",
            feature_gate: "default",
            boundary: "local_text_source_state_adapter",
        },
    );
    assert!(stdout.contains(&field("predicate_operator_family", "null_predicate")));
    assert!(stdout.contains(&field("filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("null_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("null_predicate_operator", "is_not_null")));
    assert!(stdout.contains(&field("null_predicate_source_column", "closed_at")));
    assert!(stdout.contains(&field(
        "null_predicate_null_semantics",
        "sql_is_null_is_not_null"
    )));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"closed_at\\\":\\\"2026-05-19\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_executes_null_safe_comparison_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-null-safe-comparison-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,peer\n1,alpha,alpha\n2,alpha,beta\n3,,beta\n4,beta,\n5,,\n",
    )
    .expect("write source csv");

    let distinct_statement = format!(
        "SELECT id FROM '{}' WHERE label IS DISTINCT FROM peer ORDER BY id ASC LIMIT 10",
        source_path.display()
    );
    let distinct_stdout = run_sql_local_source_smoke_json(&distinct_statement);
    assert!(distinct_stdout.contains("\"status\":\"success\""));
    assert!(distinct_stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(distinct_stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(distinct_stdout.contains(&field("null_predicate_runtime_execution", "true")));
    assert!(distinct_stdout.contains(&field("selected_row_count", "3")));
    assert!(distinct_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2}\\n{\\\"id\\\":3}\\n{\\\"id\\\":4}\\n\""
    ));
    assert!(distinct_stdout.contains(&field("fallback_attempted", "false")));
    assert!(distinct_stdout.contains(&field("external_engine_invoked", "false")));

    let not_distinct_statement = format!(
        "SELECT id FROM '{}' WHERE label IS NOT DISTINCT FROM peer ORDER BY id ASC LIMIT 10",
        source_path.display()
    );
    let not_distinct_stdout = run_sql_local_source_smoke_json(&not_distinct_statement);
    assert!(not_distinct_stdout.contains("\"status\":\"success\""));
    assert!(not_distinct_stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(not_distinct_stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(not_distinct_stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(not_distinct_stdout.contains(&field("selected_row_count", "2")));
    assert!(
        not_distinct_stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":1}\\n{\\\"id\\\":5}\\n\"")
    );
    assert!(not_distinct_stdout.contains(&field("fallback_attempted", "false")));
    assert!(not_distinct_stdout.contains(&field("external_engine_invoked", "false")));

    let null_literal_statement = format!(
        "SELECT id FROM '{}' WHERE label IS NOT DISTINCT FROM NULL ORDER BY id ASC LIMIT 10",
        source_path.display()
    );
    let null_literal_stdout = run_sql_local_source_smoke_json(&null_literal_statement);
    assert!(null_literal_stdout.contains("\"status\":\"success\""));
    assert!(null_literal_stdout.contains(&field("selected_row_count", "2")));
    assert!(
        null_literal_stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":3}\\n{\\\"id\\\":5}\\n\"")
    );
    assert!(null_literal_stdout.contains(&field("fallback_attempted", "false")));
    assert!(null_literal_stdout.contains(&field("external_engine_invoked", "false")));

    let projection_statement = format!(
        "SELECT id,label IS NOT DISTINCT FROM peer AS same_null_safe FROM '{}' WHERE label IS DISTINCT FROM peer ORDER BY id ASC LIMIT 10",
        source_path.display()
    );
    let projection_stdout = run_sql_local_source_smoke_json(&projection_statement);
    assert!(projection_stdout.contains("\"status\":\"success\""));
    assert!(projection_stdout.contains(&field("predicate_projection_runtime_execution", "true")));
    assert!(projection_stdout.contains(&field(
        "predicate_projection_predicate_family",
        "logical_predicate"
    )));
    assert!(projection_stdout.contains(&field("predicate_projection_source_column", "label+peer")));
    assert!(projection_stdout.contains(&field("selected_row_count", "3")));
    assert!(projection_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"same_null_safe\\\":false}\\n{\\\"id\\\":3,\\\"same_null_safe\\\":false}\\n{\\\"id\\\":4,\\\"same_null_safe\\\":false}\\n\""
    ));
    assert!(projection_stdout.contains(&field("fallback_attempted", "false")));
    assert!(projection_stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_boolean_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-boolean-predicate", "csv");
    fs::write(
        &source_path,
        "id,active,label\n1,true,alpha\n2,false,beta\n3,,missing\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE active LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "boolean_predicate")));
    assert!(stdout.contains(&field("filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_true")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field(
        "boolean_predicate_null_semantics",
        "sql_where_true_only_null_filters_out"
    )));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE active IS FALSE LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "boolean_predicate")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_false")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE NOT active LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(stdout.contains(&field("boolean_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_true")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_preserves_is_true_null_semantics_under_not_without_fallback() {
    let source_path = unique_path("sql-local-source-boolean-is-true-null-semantics", "csv");
    fs::write(
        &source_path,
        "id,active,label\n1,true,alpha\n2,false,beta\n3,,missing\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE NOT (active IS TRUE) LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(stdout.contains(&field("boolean_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_true")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"missing\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_is_not_boolean_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-boolean-is-not-predicate", "csv");
    fs::write(
        &source_path,
        "id,active,label\n1,true,alpha\n2,false,beta\n3,,missing\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE active IS NOT TRUE LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "boolean_predicate")));
    assert!(stdout.contains(&field("filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_not_true")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field(
        "boolean_predicate_null_semantics",
        "sql_boolean_is_not_true_false_null_matches"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"missing\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE active IS NOT FALSE LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "boolean_predicate")));
    assert!(stdout.contains(&field("boolean_predicate_operator", "is_not_false")));
    assert!(stdout.contains(&field("boolean_predicate_source_column", "active")));
    assert!(stdout.contains(&field(
        "boolean_predicate_null_semantics",
        "sql_boolean_is_not_true_false_null_matches"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"missing\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_between_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-between-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount BETWEEN 10 AND 20 AND label LIKE '%ta' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "3")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"delta\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_not_between_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-not-between-predicate", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,13\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount NOT BETWEEN 10 AND 20 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_logical_and_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-logical-and", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,delta,5\n4,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 AND label LIKE '%ta' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "ends_with")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_logical_or_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-logical-or", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,delta,5\n4,gamma,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 OR label LIKE '%ta' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "or")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "ends_with")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"delta\\\"}\\n{\\\"id\\\":4,\\\"label\\\":\\\"gamma\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_parenthesized_logical_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-logical-parentheses", "csv");
    fs::write(
        &source_path,
        "id,label,amount\n1,alpha,8\n2,beta,15\n3,gamma,21\n4,delta,5\n5,zeta,10\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 AND (label LIKE '%ta' OR label LIKE 'gam%') LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "3")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "ends_with,starts_with")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\"}\\n{\\\"id\\\":5,\\\"label\\\":\\\"zeta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_literal_filters_without_fallback() {
    let source_path = unique_path("sql-local-source-date-literal", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-05-18,old\n2,2026-05-19,today\n3,2026-05-20,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE event_date >= DATE '2026-05-19' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "comparison")));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_between_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-date-between-predicate", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-05-18,old\n2,2026-05-19,today\n3,2026-05-20,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE event_date BETWEEN DATE '2026-05-19' AND DATE '2026-05-20' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "and")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "2")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_string_length_predicate_without_fallback() {
    let source_path = unique_path("sql-local-source-string-length", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n3,écho\n").expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE LENGTH(label) >= 5 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "string_length")));
    assert!(stdout.contains(&field("string_length_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_length_source_column", "label")));
    assert!(stdout.contains(&field("string_length_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"label\\\":\\\"alpha\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_abs_predicate_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-abs", "csv");
    fs::write(&source_path, "id,amount\n1,-5\n2,3\n3,-4\n4,\n").expect("write source csv");

    let statement = format!(
        "SELECT id,amount FROM '{}' WHERE ABS(amount) >= 4 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "numeric_abs")));
    assert!(stdout.contains(&field("numeric_abs_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_abs_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_abs_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"amount\\\":-5}\\n{\\\"id\\\":3,\\\"amount\\\":-4}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_numeric_rounding_predicate_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-rounding", "csv");
    fs::write(&source_path, "id,amount\n1,3.2\n2,3.8\n3,-2.3\n4,\n").expect("write source csv");

    let statement = format!(
        "SELECT id,amount FROM '{}' WHERE CEIL(amount) >= 4 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "numeric_rounding")));
    assert!(stdout.contains(&field("numeric_rounding_runtime_execution", "true")));
    assert!(stdout.contains(&field("numeric_rounding_operator", "ceil")));
    assert!(stdout.contains(&field("numeric_rounding_source_column", "amount")));
    assert!(stdout.contains(&field("numeric_rounding_rhs_dtype", "int64")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"amount\\\":3.2}\\n{\\\"id\\\":2,\\\"amount\\\":3.8}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_interval_literal_temporal_arithmetic_without_fallback() {
    let source_path = unique_path("sql-local-source-interval-temporal-arithmetic", "csv");
    fs::write(
        &source_path,
        "id,event_date,event_ts\n\
         1,2026-05-19,2026-05-19T12:34:45Z\n\
         2,2026-01-01,2026-01-01T00:00:00Z\n\
         3,,\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,DATE_ADD_DAYS(event_date, INTERVAL '1' DAY) AS next_day,DATE_SUB_DAYS(event_date, INTERVAL '2' DAYS) AS prior_two,TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '90' SECOND) AS shifted_ts,TIMESTAMP_SUB_SECONDS(event_ts, INTERVAL '1' MINUTE) AS prior_minute FROM '{}' WHERE TIMESTAMP_ADD_SECONDS(event_ts, INTERVAL '1' HOUR) >= TIMESTAMP '2026-01-01T01:00:00Z' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("predicate_operator_family", "timestamp_arithmetic")));
    assert!(stdout.contains(&field("timestamp_arithmetic_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_operator",
        "timestamp_add_seconds"
    )));
    assert!(stdout.contains(&field("timestamp_arithmetic_seconds", "3600")));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "date_arithmetic_projection_operator",
        "date_add_days,date_sub_days"
    )));
    assert!(stdout.contains(&field("date_arithmetic_projection_days", "1,2")));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "timestamp_arithmetic_projection_operator",
        "timestamp_add_seconds,timestamp_sub_seconds"
    )));
    assert!(stdout.contains(&field("timestamp_arithmetic_projection_seconds", "90,60")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "id,next_day,prior_two,shifted_ts,prior_minute"
    )));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"next_day\\\":\\\"2026-05-20\\\",\\\"prior_two\\\":\\\"2026-05-17\\\",\\\"shifted_ts\\\":\\\"2026-05-19T12:36:15Z\\\",\\\"prior_minute\\\":\\\"2026-05-19T12:33:45Z\\\"}\\n{\\\"id\\\":2,\\\"next_day\\\":\\\"2026-01-02\\\",\\\"prior_two\\\":\\\"2025-12-30\\\",\\\"shifted_ts\\\":\\\"2026-01-01T00:01:30Z\\\",\\\"prior_minute\\\":\\\"2025-12-31T23:59:00Z\\\"}\\n\""
    ));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_arithmetic_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-date-arithmetic", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-05-18,old\n2,2026-05-19,today\n3,2026-05-20,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE DATE_ADD_DAYS(CAST(event_date AS date32), 1) >= DATE '2026-05-20' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "date_arithmetic")));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("date_arithmetic_runtime_execution", "true")));
    assert!(stdout.contains(&field("date_arithmetic_operator", "date_add_days")));
    assert!(stdout.contains(&field("date_arithmetic_days", "1")));
    assert!(stdout.contains(&field("date_arithmetic_source_column", "event_date")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_extract_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-date-extract", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-04-18,old\n2,2026-05-19,today\n3,2026-05-20,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE DATE_YEAR(CAST(event_date AS date32)) = 2026 AND DATE_MONTH(event_date) = 5 AND DATE_DAY(event_date) >= 19 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("date_extract_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "date_extract_operator",
        "date_year,date_month,date_day"
    )));
    assert!(stdout.contains(&field(
        "date_extract_source_column",
        "event_date,event_date,event_date"
    )));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "3")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_timestamp_literal_and_extract_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-timestamp-literal", "csv");
    fs::write(
        &source_path,
        "id,event_ts,label\n1,2026-05-19T11:00:00Z,old\n2,2026-05-19T12:30:45.123456Z,target\n3,2026-05-20T12:00:00Z,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_ts FROM '{}' WHERE event_ts >= TIMESTAMP '2026-05-19T12:00:00Z' AND TIMESTAMP_HOUR(CAST(event_ts AS timestamp_micros)) = 12 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("timestamp_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("timestamp_extract_runtime_execution", "true")));
    assert!(stdout.contains(&field("timestamp_extract_operator", "timestamp_hour")));
    assert!(stdout.contains(&field("timestamp_extract_source_column", "event_ts")));
    assert!(stdout.contains(&field("cast_runtime_execution", "false")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_ts\\\":\\\"2026-05-19T12:30:45.123456Z\\\"}\\n{\\\"id\\\":3,\\\"event_ts\\\":\\\"2026-05-20T12:00:00Z\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_date_in_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-date-in-predicate", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-05-18,old\n2,2026-05-19,today\n3,2026-05-20,next\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE event_date IN (DATE '2026-05-18', DATE '2026-05-20') LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "in_predicate")));
    assert!(stdout.contains(&field("in_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("in_list_value_count", "2")));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "true")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":1,\\\"event_date\\\":\\\"2026-05-18\\\"}\\n{\\\"id\\\":3,\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_cast_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-cast-predicate", "jsonl");
    fs::write(
        &source_path,
        "{\"id\":1,\"amount\":\"8\",\"label\":\"low\"}\n\
         {\"id\":2,\"amount\":\"15\",\"label\":\"mid\"}\n\
         {\"id\":3,\"amount\":\"21\",\"label\":\"high\"}\n",
    )
    .expect("write source jsonl");

    let statement = format!(
        "SELECT id,amount,label FROM '{}' WHERE CAST(amount AS int64) >= 10 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field("predicate_operator_family", "cast")));
    assert!(stdout.contains(&field("cast_runtime_execution", "true")));
    assert!(stdout.contains(&field("cast_source_column", "amount")));
    assert!(stdout.contains(&field("cast_target_dtype", "int64")));
    assert!(stdout.contains(&field("cast_mode", "strict")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"amount\\\":\\\"15\\\",\\\"label\\\":\\\"mid\\\"}\\n{\\\"id\\\":3,\\\"amount\\\":\\\"21\\\",\\\"label\\\":\\\"high\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_preserves_iso_csv_strings_for_quoted_equality() {
    let source_path = unique_path("sql-local-source-iso-string-equality", "csv");
    fs::write(
        &source_path,
        "id,event_date,label\n1,2026-05-18,alpha\n2,2026-05-19,beta\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE event_date = '2026-05-19' LIMIT 5",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_jsonl_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source-jsonl", "jsonl");
    fs::write(
        &source_path,
        "{\"id\":1,\"label\":\"alpha\",\"amount\":8,\"event_date\":\"2026-05-18\"}\n\
         {\"id\":2,\"label\":\"beta\",\"amount\":15,\"event_date\":\"2026-05-19\"}\n\
         {\"id\":3,\"label\":\"gamma\",\"amount\":21,\"event_date\":\"2026-05-20\"}\n",
    )
    .expect("write source jsonl");

    let statement = format!(
        "SELECT id,label,event_date FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field(
        "source_fingerprint_kind",
        "local_file_content_digest"
    )));
    assert!(stdout.contains("\"source_state_id\",\"value\":\"local-jsonl-fnv64-"));
    assert!(stdout.contains("\"source_state_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("source_state_reuse_allowed", "false")));
    assert!(stdout.contains(&field("source_state_reuse_hit", "false")));
    assert!(stdout.contains(&field(
        "source_state_reuse_reason",
        "not_cached_sql_local_source_smoke"
    )));
    assert!(stdout.contains(&field("source_columns", "id,label,amount,event_date")));
    assert!(stdout.contains(&field(
        "pushdown_status",
        "not_applicable_local_jsonl_transient"
    )));
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.jsonl.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.jsonl.projection-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_jsonl_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_jsonl_sql_projection_filter_limit_smoke"
    )));
    assert!(stdout.contains(&field("input_row_count", "3")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\",\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\",\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_infers_ndjson_as_jsonl_adapter_without_fallback() {
    let source_path = unique_path("sql-local-source-ndjson", "ndjson");
    fs::write(
        &source_path,
        "{\"id\":1,\"label\":\"alpha\",\"amount\":8}\n\
         {\"id\":2,\"label\":\"beta\",\"amount\":15}\n",
    )
    .expect("write source ndjson");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE amount >= 10 LIMIT 1",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert_inferred_adapter_evidence(
        &stdout,
        ExpectedAdapterEvidence {
            source_format: "jsonl",
            extension: ".ndjson",
            adapter_id: "local_jsonl_input_adapter",
            registry_entry_id: "shardloom.local_input_adapter.jsonl.v1",
            admitted_extensions: ".jsonl,.ndjson",
            feature_gate: "default",
            boundary: "local_text_source_state_adapter",
        },
    );
    assert!(stdout.contains(&field("input_row_count", "2")));
    assert!(
        stdout.contains(
            "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\"}\\n\""
        )
    );
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source ndjson");
}

#[test]
fn sql_local_source_smoke_executes_json_projection_filter_limit_with_source_state_evidence() {
    let source_path = unique_path("sql-local-source-json", "json");
    fs::write(
        &source_path,
        "[\n\
          {\"id\":1,\"label\":\"alpha\",\"amount\":8,\"event_date\":\"2026-05-18\"},\n\
          {\"id\":2,\"label\":\"beta\",\"amount\":15,\"event_date\":\"2026-05-19\"},\n\
          {\"id\":3,\"label\":\"gamma\",\"amount\":21,\"event_date\":\"2026-05-20\"}\n\
        ]\n",
    )
    .expect("write source json");

    let statement = format!(
        "SELECT id,label,event_date FROM '{}' WHERE amount >= 10 LIMIT 2",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert_inferred_adapter_evidence(
        &stdout,
        ExpectedAdapterEvidence {
            source_format: "json",
            extension: ".json",
            adapter_id: "local_json_input_adapter",
            registry_entry_id: "shardloom.local_input_adapter.json.v1",
            admitted_extensions: ".json",
            feature_gate: "default",
            boundary: "local_text_source_state_adapter",
        },
    );
    assert!(stdout.contains(&field("source_kind", "local_non_vortex_file")));
    assert!(stdout.contains(&field("source_adapter_status", "smoke_supported")));
    assert!(stdout.contains(&field("ingress_route", "direct_transient")));
    assert!(stdout.contains(&field("vortex_ingest_performed", "false")));
    assert!(stdout.contains(&field("prepared_state_created", "false")));
    assert!(stdout.contains(&field(
        "selected_execution_mode",
        "direct_compatibility_transient"
    )));
    assert!(stdout.contains(&field("timing_scope", "direct_one_shot")));
    assert!(stdout.contains(&field(
        "source_fingerprint_kind",
        "local_file_content_digest"
    )));
    assert!(stdout.contains("\"source_state_id\",\"value\":\"local-json-fnv64-"));
    assert!(stdout.contains("\"source_state_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("source_state_reuse_allowed", "false")));
    assert!(stdout.contains(&field("source_state_reuse_hit", "false")));
    assert!(stdout.contains(&field("source_columns", "id,label,amount,event_date")));
    assert!(stdout.contains(&field(
        "pushdown_status",
        "not_applicable_local_json_transient"
    )));
    assert!(stdout.contains(&field(
        "source_certificate_ref",
        "sql-local-source.json.compatibility-source.v1"
    )));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.json.projection-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_json_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_json_sql_projection_filter_limit_smoke"
    )));
    assert!(stdout.contains(&field("input_row_count", "3")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"label\\\":\\\"beta\\\",\\\"event_date\\\":\\\"2026-05-19\\\"}\\n{\\\"id\\\":3,\\\"label\\\":\\\"gamma\\\",\\\"event_date\\\":\\\"2026-05-20\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source json");
}

#[test]
fn sql_local_source_smoke_blocks_unregistered_extension_before_reading_without_fallback() {
    let source_path = unique_path("sql-local-source-unregistered-adapter", "sqlite");
    let statement = format!("SELECT id FROM '{}' LIMIT 1", source_path.display());

    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "local input adapter registry cannot infer a supported source adapter from extension '.sqlite'"
    ));
    assert!(stdout.contains(
        "admitted local source extensions are .csv,.json,.jsonl,.ndjson,.parquet,.arrow,.ipc,.feather,.avro,.orc"
    ));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("external_engine_invoked=false"));
    assert!(
        !source_path.exists(),
        "format blocker should not require the source file to exist"
    );
}

#[test]
fn sql_local_source_smoke_jsonl_scalar_aggregate_uses_jsonl_evidence_labels() {
    let source_path = unique_path("sql-local-source-jsonl-aggregate", "jsonl");
    fs::write(
        &source_path,
        "{\"id\":1,\"label\":\"alpha\",\"amount\":8}\n\
         {\"id\":2,\"label\":\"beta\",\"amount\":15}\n\
         {\"id\":3,\"label\":\"beta\",\"amount\":21}\n",
    )
    .expect("write source jsonl");

    let statement = format!(
        "SELECT count(*),sum(amount),avg(amount) FROM '{}' WHERE amount >= 10 LIMIT 1",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.jsonl.aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_jsonl_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_jsonl_sql_scalar_aggregate_filter_limit_smoke"
    )));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"count_all\\\":2,\\\"sum_amount\\\":36,\\\"avg_amount\\\":18.0}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_preserves_iso_jsonl_strings_for_quoted_equality() {
    let source_path = unique_path("sql-local-source-jsonl-iso-string-equality", "jsonl");
    fs::write(
        &source_path,
        "{\"id\":1,\"event_date\":\"2026-05-18\"}\n\
         {\"id\":2,\"event_date\":\"2026-05-19\"}\n",
    )
    .expect("write source jsonl");

    let statement = format!(
        "SELECT id,event_date FROM '{}' WHERE event_date = '2026-05-19' LIMIT 5",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"id\\\":2,\\\"event_date\\\":\\\"2026-05-19\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("date_literal_runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_executes_inner_equi_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,amount\n1,10,8\n2,20,15\n3,30,21\n4,99,13\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,segment\n10,seed\n20,enterprise\n30,startup\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_equi")));
    assert!(stdout.contains(&field("source_alias", "f")));
    assert!(stdout.contains(&field("right_source_alias", "d")));
    assert!(stdout.contains(&field("left_input_row_count", "4")));
    assert!(stdout.contains(&field("right_input_row_count", "3")));
    assert!(stdout.contains(&field("join_left_key", "f.customer_id")));
    assert!(stdout.contains(&field("join_right_key", "d.customer_id")));
    assert!(stdout.contains(&field("join_left_keys", "f.customer_id")));
    assert!(stdout.contains(&field("join_right_keys", "d.customer_id")));
    assert!(stdout.contains(&field("join_key_arity", "1")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "false")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_left_rows_scanned", "4")));
    assert!(stdout.contains(&field("join_right_rows_scanned", "3")));
    assert!(stdout.contains(&field("join_rows_output", "2")));
    assert!(stdout.contains(&field("join_memory_estimate_bytes", "2240")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(&field("projected_columns", "f.id,d.segment")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"startup\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_left_outer_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-left-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-left-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,amount\n1,10,8\n2,20,15\n3,30,21\n4,99,13\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,segment\n10,seed\n20,enterprise\n30,startup\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f LEFT JOIN '{}' AS d ON f.customer_id = d.customer_id ORDER BY f.id ASC LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_left_outer_equi_join_order_by_topn_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "left_outer_equi")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_unmatched_left_row_count", "1")));
    assert!(stdout.contains(&field("join_unmatched_right_row_count", "0")));
    assert!(stdout.contains(&field("join_rows_output", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":1,\\\"d.segment\\\":\\\"seed\\\"}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"startup\\\"}\\n{\\\"f.id\\\":4,\\\"d.segment\\\":null}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.left-outer-equi-join-order-by-topn-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_full_outer_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-full-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-full-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,amount\n1,10,8\n2,20,15\n3,30,21\n4,77,13\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,segment\n10,seed\n20,enterprise\n30,startup\n99,orphan\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f FULL OUTER JOIN '{}' AS d ON f.customer_id = d.customer_id LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_full_outer_equi_join_limit"
    )));
    assert!(stdout.contains(&field("join_type", "full_outer_equi")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_unmatched_left_row_count", "1")));
    assert!(stdout.contains(&field("join_unmatched_right_row_count", "1")));
    assert!(stdout.contains(&field("join_rows_output", "5")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":1,\\\"d.segment\\\":\\\"seed\\\"}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"startup\\\"}\\n{\\\"f.id\\\":4,\\\"d.segment\\\":null}\\n{\\\"f.id\\\":null,\\\"d.segment\\\":\\\"orphan\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.full-outer-equi-join-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_right_outer_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-right-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-right-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,amount\n1,10,8\n2,20,15\n3,30,21\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,segment\n10,seed\n20,enterprise\n30,startup\n99,orphan\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f RIGHT JOIN '{}' AS d ON f.customer_id = d.customer_id LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_right_outer_equi_join_limit"
    )));
    assert!(stdout.contains(&field("join_type", "right_outer_equi")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_unmatched_left_row_count", "0")));
    assert!(stdout.contains(&field("join_unmatched_right_row_count", "1")));
    assert!(stdout.contains(&field("join_rows_output", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":1,\\\"d.segment\\\":\\\"seed\\\"}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"startup\\\"}\\n{\\\"f.id\\\":null,\\\"d.segment\\\":\\\"orphan\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.right-outer-equi-join-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_left_semi_and_anti_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-existence-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-existence-join-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,amount\n1,10,8\n2,20,15\n3,30,21\n4,99,13\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,segment\n10,seed\n20,enterprise\n30,startup\n",
    )
    .expect("write dim csv");

    let semi_statement = format!(
        "SELECT f.id FROM '{}' AS f LEFT SEMI JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let semi_stdout = run_sql_local_source_smoke_json(&semi_statement);
    assert!(semi_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_left_semi_equi_join_filter_limit"
    )));
    assert!(semi_stdout.contains(&field("join_type", "left_semi_equi")));
    assert!(semi_stdout.contains(&field("join_matched_row_count", "3")));
    assert!(semi_stdout.contains(&field("join_unmatched_left_row_count", "0")));
    assert!(semi_stdout.contains(&field("selected_row_count", "2")));
    assert!(
        semi_stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2}\\n{\\\"f.id\\\":3}\\n\"")
    );
    assert!(semi_stdout.contains(&field("fallback_attempted", "false")));
    assert!(semi_stdout.contains(&field("external_engine_invoked", "false")));

    let anti_statement = format!(
        "SELECT f.id FROM '{}' AS f LEFT ANTI JOIN '{}' AS d ON f.customer_id = d.customer_id LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let anti_stdout = run_sql_local_source_smoke_json(&anti_statement);
    assert!(anti_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_left_anti_equi_join_limit"
    )));
    assert!(anti_stdout.contains(&field("join_type", "left_anti_equi")));
    assert!(anti_stdout.contains(&field("join_matched_row_count", "3")));
    assert!(anti_stdout.contains(&field("join_unmatched_left_row_count", "1")));
    assert!(anti_stdout.contains(&field("join_rows_output", "1")));
    assert!(anti_stdout.contains("\"result_jsonl\",\"value\":\"{\\\"f.id\\\":4}\\n\""));
    assert!(anti_stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.left-anti-equi-join-limit.execution.v1"
    )));
    assert!(anti_stdout.contains(&field("fallback_attempted", "false")));
    assert!(anti_stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_cross_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-cross-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-cross-join-dim", "csv");
    fs::write(&fact_path, "id,amount\n1,8\n2,15\n").expect("write fact csv");
    fs::write(&dim_path, "segment\nseed\nenterprise\n").expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f CROSS JOIN '{}' AS d WHERE f.id = 2 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_cross_join_filter_limit"
    )));
    assert!(stdout.contains(&field("join_type", "cross")));
    assert!(stdout.contains(&field("join_key_arity", "0")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("join_candidate_row_count", "4")));
    assert!(stdout.contains(&field("join_rows_output", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"seed\\\"}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.cross-join-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_inner_expression_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-expression-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-expression-join-dim", "csv");
    fs::write(&fact_path, "id,amount\n1,8\n2,15\n3,21\n").expect("write fact csv");
    fs::write(&dim_path, "threshold,segment\n10,base\n20,premium\n").expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.amount > d.threshold ORDER BY f.id ASC,d.threshold ASC LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_expression_join_order_by_topn_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_expression")));
    assert!(stdout.contains(&field("join_on_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_on_predicate_operator_family",
        "column_compare"
    )));
    assert!(stdout.contains(&field(
        "join_on_predicate_source_column",
        "f.amount,d.threshold"
    )));
    assert!(stdout.contains(&field("join_key_arity", "0")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_candidate_row_count", "6")));
    assert!(stdout.contains(&field("join_unmatched_left_row_count", "0")));
    assert!(stdout.contains(&field("join_unmatched_right_row_count", "0")));
    assert!(stdout.contains(&field("join_rows_output", "3")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"base\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"base\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"premium\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-expression-join-order-by-topn-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let generic_statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.amount + d.threshold >= 35 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let generic_stdout = run_sql_local_source_smoke_json(&generic_statement);

    assert!(generic_stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_expression_join_limit"
    )));
    assert!(generic_stdout.contains(&field("join_type", "inner_expression")));
    assert!(generic_stdout.contains(&field("join_on_predicate_runtime_execution", "true")));
    assert!(generic_stdout.contains(&field(
        "join_on_predicate_operator_family",
        "generic_expression"
    )));
    assert!(generic_stdout.contains(&field(
        "join_on_predicate_source_column",
        "d.threshold,f.amount"
    )));
    assert!(generic_stdout.contains(&field("join_candidate_row_count", "6")));
    assert!(generic_stdout.contains(&field("join_matched_row_count", "2")));
    assert!(generic_stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"premium\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"premium\\\"}\\n\""
    ));
    assert!(generic_stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-expression-join-limit.execution.v1"
    )));
    assert!(generic_stdout.contains(&field("fallback_attempted", "false")));
    assert!(generic_stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_left_outer_expression_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-left-expression-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-left-expression-join-dim", "csv");
    fs::write(&fact_path, "id,amount\n1,8\n2,15\n3,21\n").expect("write fact csv");
    fs::write(&dim_path, "threshold,segment\n10,base\n20,premium\n").expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f LEFT JOIN '{}' AS d ON f.amount > d.threshold ORDER BY f.id ASC LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);

    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_left_outer_expression_join_order_by_topn_limit"
    )));
    assert!(stdout.contains(&field("join_type", "left_outer_expression")));
    assert!(stdout.contains(&field("join_on_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_on_predicate_operator_family",
        "column_compare"
    )));
    assert!(stdout.contains(&field("join_key_arity", "0")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_candidate_row_count", "6")));
    assert!(stdout.contains(&field("join_unmatched_left_row_count", "1")));
    assert!(stdout.contains(&field("join_unmatched_right_row_count", "0")));
    assert!(stdout.contains(&field("join_rows_output", "4")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":1,\\\"d.segment\\\":null}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"base\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"base\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"premium\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.left-outer-expression-join-order-by-topn-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_multi_key_inner_equi_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-multi-key-join-fact", "csv");
    let dim_path = unique_path("sql-local-source-multi-key-join-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
99,east,orphan
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_equi")));
    assert!(stdout.contains(&field("join_left_key", "f.customer_id,f.region")));
    assert!(stdout.contains(&field("join_right_key", "d.customer_id,d.region")));
    assert!(stdout.contains(&field("join_left_keys", "f.customer_id,f.region")));
    assert!(stdout.contains(&field("join_right_keys", "d.customer_id,d.region")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_left_rows_scanned", "5")));
    assert!(stdout.contains(&field("join_right_rows_scanned", "4")));
    assert!(stdout.contains(&field("join_rows_output", "3")));
    assert!(stdout.contains(&field("join_memory_estimate_bytes", "4032")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "3")));
    assert!(stdout.contains(&field("projected_columns", "f.id,d.segment")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"consumer\\\"}\\n{\\\"f.id\\\":5,\\\"d.segment\\\":\\\"startup\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_order_by_topn_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-topn-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-topn-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
99,east,orphan
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 2",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("join_rows_output", "2")));
    assert!(stdout.contains(&field(
        "join_computed_projection_runtime_execution",
        "false"
    )));
    assert!(stdout.contains(&field("join_order_by_top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_projection_operator_family",
        "raw_projection_topn"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "single_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "f.amount")));
    assert!(stdout.contains(&field("sort_direction", "desc")));
    assert!(stdout.contains(&field("top_n_limit", "2")));
    assert!(stdout.contains(&field("projected_columns", "f.id,d.segment")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":5,\\\"d.segment\\\":\\\"startup\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"consumer\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_utf8_order_by_topn_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-utf8-topn-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-utf8-topn-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
30,east,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY d.segment ASC,f.amount DESC LIMIT 3",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("join_order_by_top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "d.segment,f.amount")));
    assert!(stdout.contains(&field("sort_direction", "asc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "3")));
    assert!(stdout.contains(&field("projected_columns", "f.id,d.segment")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"consumer\\\"}\\n{\\\"f.id\\\":4,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n\""
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_computed_projection_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-computed-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-computed-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,region,amount\n1,10,na,8\n2,20,na,15\n3,30,eu,21\n4,40,na,12\n5,50,eu,23\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,region,segment,discount\n10,na,seed,1\n20,na,enterprise,2\n30,eu,consumer,3\n40,na,enterprise,4\n50,eu,startup,5\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment,f.amount + d.discount AS adjusted FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 LIMIT 2",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(
        stdout.contains("\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\",\\\"adjusted\\\":17")
    );
    assert!(
        stdout.contains("\\\"f.id\\\":3,\\\"d.segment\\\":\\\"consumer\\\",\\\"adjusted\\\":24")
    );
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_computed_projection_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_matched_row_count", "5")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("join_rows_output", "2")));
    assert!(stdout.contains(&field("join_computed_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_order_by_top_n_runtime_execution", "false")));
    assert!(stdout.contains(&field(
        "join_projection_operator_family",
        "computed_projection"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "false")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "false")));
    assert!(stdout.contains(&field("projected_columns", "f.id,d.segment,adjusted")));
    assert!(stdout.contains(&field(
        "generic_expression_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_source_column",
        "d.discount+f.amount"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_output_column",
        "adjusted"
    )));
    assert!(stdout.contains(
        "sql-local-source.csv.inner-equi-join-computed-projection-filter-limit.execution.v1"
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_computed_projection_topn_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-computed-topn-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-computed-topn-dim", "csv");
    fs::write(
        &fact_path,
        "id,customer_id,region,amount\n1,10,na,8\n2,20,na,15\n3,30,eu,21\n4,40,na,12\n5,50,eu,23\n",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "customer_id,region,segment,discount\n10,na,seed,1\n20,na,enterprise,2\n30,eu,consumer,3\n40,na,enterprise,4\n50,eu,startup,5\n",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT f.id,CONCAT(d.segment,'-',f.region) AS segment_region,d.segment,f.amount + d.discount AS adjusted FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 ORDER BY f.amount DESC LIMIT 3",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(
        "\\\"f.id\\\":5,\\\"segment_region\\\":\\\"startup-eu\\\",\\\"d.segment\\\":\\\"startup\\\",\\\"adjusted\\\":28"
    ));
    assert!(stdout.contains(
        "\\\"f.id\\\":3,\\\"segment_region\\\":\\\"consumer-eu\\\",\\\"d.segment\\\":\\\"consumer\\\",\\\"adjusted\\\":24"
    ));
    assert!(stdout.contains(
        "\\\"f.id\\\":2,\\\"segment_region\\\":\\\"enterprise-na\\\",\\\"d.segment\\\":\\\"enterprise\\\",\\\"adjusted\\\":17"
    ));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_computed_projection_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "5")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("join_rows_output", "3")));
    assert!(stdout.contains(&field("join_computed_projection_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_order_by_top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_projection_operator_family",
        "computed_projection_topn"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_keys", "f.amount")));
    assert!(stdout.contains(&field("sort_direction", "desc")));
    assert!(stdout.contains(&field("top_n_limit", "3")));
    assert!(stdout.contains(&field(
        "projected_columns",
        "f.id,segment_region,d.segment,adjusted"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_source_column",
        "d.discount+f.amount"
    )));
    assert!(stdout.contains(&field(
        "generic_expression_projection_output_column",
        "adjusted"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_source_column",
        "d.segment+f.region"
    )));
    assert!(stdout.contains(&field(
        "string_function_projection_output_column",
        "segment_region"
    )));
    assert!(stdout.contains(
        "sql-local-source.csv.inner-equi-join-computed-projection-order-by-topn-filter-limit.execution.v1"
    ));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_scalar_aggregate_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-scalar-aggregate-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-scalar-aggregate-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,amount
1,10,8
2,20,15
3,30,21
4,99,13
5,20,5
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,segment
10,seed
20,enterprise
30,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT count(*) AS rows,sum(f.amount) AS total_amount FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE d.segment = 'enterprise' LIMIT 1",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_equi")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(f.amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_aliases", "rows,total_amount")));
    assert!(stdout.contains(&field("join_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_aggregate_operator_family",
        "scalar_join_aggregate"
    )));
    assert!(stdout.contains(&field("join_aggregate_group_count", "0")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(&field("projected_columns", "rows,total_amount")));
    assert!(
        stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"rows\\\":3,\\\"total_amount\\\":41}\\n\"")
    );
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_scalar_aggregate_order_by_topn_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-scalar-aggregate-order-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-scalar-aggregate-order-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,amount
1,10,8
2,20,15
3,30,21
4,99,13
5,20,5
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,segment
10,seed
20,enterprise
30,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT count(*) AS rows,sum(f.amount) AS total_amount FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE d.segment = 'enterprise' ORDER BY total_amount DESC,rows DESC LIMIT 1",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_aggregate_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_equi")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "scalar_aggregate")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(f.amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("aggregate_alias_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_aliases", "rows,total_amount")));
    assert!(stdout.contains(&field("join_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_aggregate_operator_family",
        "scalar_join_aggregate"
    )));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "total_amount,rows")));
    assert!(stdout.contains(&field("sort_direction", "desc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "1")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("selected_row_count", "3")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(&field("projected_columns", "rows,total_amount")));
    assert!(
        stdout
            .contains("\"result_jsonl\",\"value\":\"{\\\"rows\\\":3,\\\"total_amount\\\":41}\\n\"")
    );
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-aggregate-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_multi_key_join_group_by_aggregate_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-group-by-aggregate-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-group-by-aggregate-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
6,99,west,50
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
30,east,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_group_by_aggregate_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("aggregate_functions", "count(*),sum(f.amount)")));
    assert!(stdout.contains(&field("aggregate_output_columns", "rows,total_amount")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_columns", "d.segment")));
    assert!(stdout.contains(&field("group_by_key_arity", "1")));
    assert!(stdout.contains(&field("group_by_group_count", "3")));
    assert!(stdout.contains(&field("join_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "join_aggregate_operator_family",
        "grouped_join_aggregate"
    )));
    assert!(stdout.contains(&field("join_aggregate_group_count", "3")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "3")));
    assert!(stdout.contains(&field("projected_columns", "d.segment,rows,total_amount")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"d.segment\\\":\\\"consumer\\\",\\\"rows\\\":1,\\\"total_amount\\\":21}\\n{\\\"d.segment\\\":\\\"enterprise\\\",\\\"rows\\\":2,\\\"total_amount\\\":37}\\n{\\\"d.segment\\\":\\\"startup\\\",\\\"rows\\\":1,\\\"total_amount\\\":23}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-group-by-aggregate-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_group_by_aggregate_order_by_topn_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-aggregate-order-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-aggregate-order-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
6,99,west,50
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
30,east,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment ORDER BY total_amount DESC,rows DESC LIMIT 2",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_group_by_aggregate_order_by_topn_filter_limit"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("group_by_group_count", "2")));
    assert!(stdout.contains(&field("join_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field("sort_operator_family", "multi_key_scalar_topn")));
    assert!(stdout.contains(&field("sort_keys", "total_amount,rows")));
    assert!(stdout.contains(&field("sort_direction", "desc,desc")));
    assert!(stdout.contains(&field("top_n_limit", "2")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"d.segment\\\":\\\"enterprise\\\",\\\"rows\\\":2,\\\"total_amount\\\":37}\\n{\\\"d.segment\\\":\\\"startup\\\",\\\"rows\\\":1,\\\"total_amount\\\":23}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-group-by-aggregate-order-by-topn-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_join_group_by_aggregate_having_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-aggregate-having-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-aggregate-having-dim", "csv");
    fs::write(
        &fact_path,
        "\
id,customer_id,region,amount
1,10,east,8
2,20,west,15
3,20,east,21
4,30,east,22
5,30,west,23
6,99,west,50
",
    )
    .expect("write fact csv");
    fs::write(
        &dim_path,
        "\
customer_id,region,segment
20,west,enterprise
20,east,consumer
30,west,startup
30,east,enterprise
",
    )
    .expect("write dim csv");

    let statement = format!(
        "SELECT d.segment,count(*) AS rows,sum(f.amount) AS total_amount FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.region = d.region WHERE f.amount >= 10 GROUP BY d.segment HAVING rows >= 2 AND max(f.amount) >= 22 ORDER BY total_amount DESC LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "sql_statement_kind",
        "local_source_inner_equi_join_group_by_aggregate_order_by_topn_filter_limit_having"
    )));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_key_arity", "2")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_matched_row_count", "4")));
    assert!(stdout.contains(&field("aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("aggregate_operator_family", "grouped_aggregate")));
    assert!(stdout.contains(&field("group_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_aggregate_group_count", "1")));
    assert!(stdout.contains(&field("having_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("having_source_column", "rows,max(f.amount)")));
    assert!(stdout.contains(&field("having_aggregate_runtime_execution", "true")));
    assert!(stdout.contains(&field("having_aggregate_function", "max(f.amount)")));
    assert!(stdout.contains(&field(
        "having_aggregate_output_column",
        "__having_max_f_amount_1"
    )));
    assert!(stdout.contains(&field("having_input_row_count", "3")));
    assert!(stdout.contains(&field("having_selected_row_count", "1")));
    assert!(stdout.contains(&field("selected_row_count", "4")));
    assert!(stdout.contains(&field("output_row_count", "1")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"d.segment\\\":\\\"enterprise\\\",\\\"rows\\\":2,\\\"total_amount\\\":37}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.csv.inner-equi-join-group-by-aggregate-order-by-topn-filter-limit-having.execution.v1"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_executes_jsonl_inner_equi_join_without_fallback() {
    let fact_path = unique_path("sql-local-source-jsonl-join-fact", "jsonl");
    let dim_path = unique_path("sql-local-source-jsonl-join-dim", "jsonl");
    fs::write(
        &fact_path,
        "{\"id\":1,\"customer_id\":10,\"amount\":8}\n{\"id\":2,\"customer_id\":20,\"amount\":15}\n{\"id\":3,\"customer_id\":30,\"amount\":21}\n{\"id\":4,\"customer_id\":99,\"amount\":13}\n",
    )
    .expect("write fact jsonl");
    fs::write(
        &dim_path,
        "{\"customer_id\":10,\"segment\":\"seed\"}\n{\"customer_id\":20,\"segment\":\"enterprise\"}\n{\"customer_id\":30,\"segment\":\"startup\"}\n",
    )
    .expect("write dim jsonl");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f INNER JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 10 LIMIT 10",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field("right_source_format", "jsonl")));
    assert!(stdout.contains(&field("join_source_formats", "jsonl,jsonl")));
    assert!(stdout.contains(&field("source_adapter_id", "local_jsonl_input_adapter")));
    assert!(stdout.contains(&field("join_runtime_execution", "true")));
    assert!(stdout.contains(&field("join_type", "inner_equi")));
    assert!(stdout.contains(&field("join_left_keys", "f.customer_id")));
    assert!(stdout.contains(&field("join_right_keys", "d.customer_id")));
    assert!(stdout.contains(&field("join_key_arity", "1")));
    assert!(stdout.contains(&field("join_multi_key_runtime_execution", "false")));
    assert!(stdout.contains(&field("join_matched_row_count", "3")));
    assert!(stdout.contains(&field("join_rows_output", "2")));
    assert!(stdout.contains(&field("selected_row_count", "2")));
    assert!(stdout.contains(
        "\"result_jsonl\",\"value\":\"{\\\"f.id\\\":2,\\\"d.segment\\\":\\\"enterprise\\\"}\\n{\\\"f.id\\\":3,\\\"d.segment\\\":\\\"startup\\\"}\\n\""
    ));
    assert!(stdout.contains(&field(
        "execution_certificate_ref",
        "sql-local-source.jsonl.inner-equi-join-filter-limit.execution.v1"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "local_jsonl_row_materialization_to_expression_semantics"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(fact_path).expect("remove fact jsonl");
    fs::remove_file(dim_path).expect("remove dim jsonl");
}

#[test]
fn sql_local_source_smoke_blocks_duplicate_key_join_explosion_without_materializing() {
    let fact_path = unique_path("sql-local-source-join-explosion-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-explosion-dim", "csv");
    let mut fact = String::from("id,customer_id,amount\n");
    let mut dim = String::from("customer_id,segment\n");
    for index in 0..225 {
        writeln!(fact, "{index},42,10").expect("write fact row");
        writeln!(dim, "42,segment_{index}").expect("write dim row");
    }
    fs::write(&fact_path, fact).expect("write fact csv");
    fs::write(&dim_path, dim).expect("write dim csv");

    let statement = format!(
        "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 1 LIMIT 1",
        fact_path.display(),
        dim_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("JOIN candidate row count exceeds scoped smoke cap"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_join_shapes_without_fallback() {
    let fact_path = unique_path("sql-local-source-join-blocked-fact", "csv");
    let dim_path = unique_path("sql-local-source-join-blocked-dim", "csv");
    let unsupported_fact_path = unique_path("sql-local-source-join-blocked-fact", "sqlite");
    let unsupported_dim_path = unique_path("sql-local-source-join-blocked-dim", "sqlite");
    fs::write(&fact_path, "id,customer_id,amount\n1,10,8\n2,20,15\n").expect("write fact csv");
    fs::write(&dim_path, "customer_id,segment\n10,seed\n20,enterprise\n").expect("write dim csv");

    let cases = [
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f SEMI JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "left_semi_equi emits the left source only",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f CROSS JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "CROSS JOIN smoke does not admit an ON clause",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = 10 WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "SQL identifiers must start with an ASCII letter or underscore",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke requires unique key columns on each side",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.amount > f.id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN expression ON predicates must reference both left and right sources",
        ),
        (
            format!(
                "SELECT count(*) AS rows FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 ORDER BY f.amount DESC LIMIT 1",
                fact_path.display(),
                dim_path.display()
            ),
            "ORDER BY join aggregate output column \\\"f.amount\\\" is not present in the row",
        ),
        (
            format!(
                "SELECT id,segment FROM '{}' JOIN '{}' ON customer_id = customer_id WHERE amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke requires left source syntax <local-source> AS <alias>",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                unsupported_fact_path.display(),
                unsupported_dim_path.display()
            ),
            "local input adapter registry cannot infer a supported source adapter from extension '.sqlite'",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(fact_path).expect("remove fact csv");
    fs::remove_file(dim_path).expect("remove dim csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_order_by_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-order-by-blocked", "csv");
    fs::write(
        &source_path,
        "id,label,amount,active,mixed\n1,alpha,8,true,8\n2,beta,,false,beta\n3,gamma,21,true,21\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY amount DESC LIMIT 2",
        source_path.display()
    );
    let null_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !null_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&null_output.stdout),
        String::from_utf8_lossy(&null_output.stderr)
    );
    let stdout = String::from_utf8(null_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("ORDER BY NULL ordering is not admitted"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY active DESC LIMIT 2",
        source_path.display()
    );
    let boolean_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !boolean_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&boolean_output.stdout),
        String::from_utf8_lossy(&boolean_output.stderr)
    );
    let stdout = String::from_utf8(boolean_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "ORDER BY top-N smoke admits numeric, UTF-8, binary, or scoped ARRAY/STRUCT result-boundary sort columns only"
    ));
    assert!(stdout.contains("external_engine_invoked=false"));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY mixed DESC LIMIT 2",
        source_path.display()
    );
    let mixed_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !mixed_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&mixed_output.stdout),
        String::from_utf8_lossy(&mixed_output.stderr)
    );
    let stdout = String::from_utf8(mixed_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(
        stdout.contains(
            "ORDER BY mixed numeric, UTF-8, binary, and scoped ARRAY/STRUCT values within one sort key are not admitted in this scoped top-N smoke"
        )
    );
    assert!(stdout.contains("external_engine_invoked=false"));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY amount DESC,amount ASC LIMIT 2",
        source_path.display()
    );
    let duplicate_key_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !duplicate_key_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&duplicate_key_output.stdout),
        String::from_utf8_lossy(&duplicate_key_output.stderr)
    );
    let stdout = String::from_utf8(duplicate_key_output.stdout).expect("stdout is utf8");
    assert!(
        stdout.contains("ORDER BY duplicate sort keys are not admitted in this scoped top-N smoke")
    );

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_like_escape_clause_without_fallback() {
    let source_path = unique_path("sql-local-source-like-escape", "csv");
    fs::write(
        &source_path,
        "id,label\n1,alpha\n2,al_pha\n3,al%pha\n4,alxpha\n",
    )
    .expect("write source csv");

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE label LIKE 'al!_%' ESCAPE '!' LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);
    assert!(
        stdout.contains("{\\\"id\\\":2,\\\"label\\\":\\\"al_pha\\\"}\\n"),
        "statement={statement} stdout={stdout}"
    );
    assert!(stdout.contains(&field("predicate_operator_family", "string_predicate")));
    assert!(stdout.contains(&field("string_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("string_predicate_operator", "like_pattern")));
    assert!(stdout.contains(&field(
        "string_predicate_like_escape_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("string_predicate_like_escape_character", "!")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_string_transform_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-string-transform-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE LOWER(label) = 1 LIMIT 10",
                source_path.display()
            ),
            "SQL string literals must be single quoted",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE LOWER(label, id) = 'alpha' LIMIT 10",
                source_path.display()
            ),
            "string transform expressions require exactly one argument",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_string_length_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-string-length-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE LENGTH(label) >= '4' LIMIT 10",
                source_path.display()
            ),
            "string length predicates compare against int64 literals only",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE LENGTH(label, id) >= 4 LIMIT 10",
                source_path.display()
            ),
            "string length expressions require exactly one argument",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_string_function_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-string-function-blocked", "csv");
    fs::write(&source_path, "id,label,amount\n1,alpha,10\n2,beta,20\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE CONCAT('a', 'b') = 'ab' LIMIT 10",
                source_path.display()
            ),
            "string function predicates require at least one source column argument",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE SUBSTR(label, 0, 2) = 'al' LIMIT 10",
                source_path.display()
            ),
            "SUBSTR/SUBSTRING string function expressions require a 1-based start index >= 1",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE REPLACE(label, '', 'x') = 'alpha' LIMIT 10",
                source_path.display()
            ),
            "REPLACE string function expressions require a non-empty search literal",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE LEFT(label, -1) = 'a' LIMIT 10",
                source_path.display()
            ),
            "LEFT/RIGHT string function expressions require a non-negative count",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE RIGHT(label) = 'a' LIMIT 10",
                source_path.display()
            ),
            "RIGHT string function expressions require exactly two arguments: <column>, <count>",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE CONCAT(label, amount) = 'alpha10' LIMIT 10",
                source_path.display()
            ),
            "supports UTF-8/null operands only",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(
            stdout.contains("no fallback execution was attempted")
                || stdout.contains("ShardLoom prohibits Spark")
        );
        assert!(
            stdout.contains("external_engine_invoked=false")
                || stdout.contains("\"attempted\":false")
        );
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_numeric_abs_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-abs-blocked", "csv");
    fs::write(&source_path, "id,amount\n1,-5\n2,3\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE ABS(amount) >= 'large' LIMIT 10",
                source_path.display()
            ),
            "numeric arithmetic expressions admit int64 or finite float64 literals only",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE ABS(amount, id) >= 4 LIMIT 10",
                source_path.display()
            ),
            "SQL identifiers may contain only ASCII letters, numbers, and underscores",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_numeric_rounding_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-numeric-rounding-blocked", "csv");
    fs::write(&source_path, "id,amount\n1,3.2\n2,3.8\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE ROUND(amount) >= 'large' LIMIT 10",
                source_path.display()
            ),
            "numeric arithmetic expressions admit int64 or finite float64 literals only",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE ROUND(amount, 2) >= 4 LIMIT 10",
                source_path.display()
            ),
            "SQL identifiers may contain only ASCII letters, numbers, and underscores",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_in_predicate_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-in-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let too_many_values = (0..33)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE label IN () LIMIT 10",
                source_path.display()
            ),
            "IN predicates require at least one literal value",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE id IN ({too_many_values}) LIMIT 10",
                source_path.display()
            ),
            "IN predicates admit at most 32 literal values",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE label IN ('alpha',) LIMIT 10",
                source_path.display()
            ),
            "IN predicates require non-empty literal values",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE (id,label) IN () LIMIT 10",
                source_path.display()
            ),
            "row-value IN predicates require at least one literal tuple",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE (id,label) IN ((1,'alpha'),) LIMIT 10",
                source_path.display()
            ),
            "row-value IN predicates require non-empty literal tuples",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE (id,label) IN ((1,'alpha',10)) LIMIT 10",
                source_path.display()
            ),
            "row-value IN literal tuple arity must match the source column count",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE id IN (SELECT id,label FROM '{}') LIMIT 10",
                source_path.display(),
                source_path.display(),
            ),
            "multi-column IN subqueries require row-value source columns",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_between_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-between-blocked", "csv");
    fs::write(&source_path, "id,amount\n1,8\n2,15\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE amount BETWEEN 10 20 LIMIT 10",
                source_path.display()
            ),
            "BETWEEN predicates require an AND separator between lower and upper bounds",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE amount BETWEEN 10 AND LIMIT 10",
                source_path.display()
            ),
            "BETWEEN predicates require non-empty lower and upper literal bounds",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE amount BETWEEN DATE '2026-05-19' AND 20 LIMIT 10",
                source_path.display()
            ),
            "requires ISO date strings or nulls",
        ),
    ];

    for (statement, expected_reason) in cases {
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(["sql-local-source-smoke", &statement, "--format", "json"])
            .output()
            .expect("sql-local-source-smoke command runs");
        assert!(
            !output.status.success(),
            "statement={statement} stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(
            stdout.contains(expected_reason),
            "statement={statement} expected={expected_reason} stdout={stdout}"
        );
        assert!(stdout.contains("no fallback execution was attempted"));
        assert!(stdout.contains("external_engine_invoked=false"));
    }

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_executes_logical_not_predicates_without_fallback() {
    let source_path = unique_path("sql-local-source-logical-not", "csv");
    fs::write(&source_path, "id,label,amount\n1,alpha,8\n2,beta,15\n").expect("write source csv");

    let statement = format!(
        "SELECT id FROM '{}' WHERE NOT amount >= 10 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("predicate_operator_family", "logical_predicate")));
    assert!(stdout.contains(&field("logical_predicate_runtime_execution", "true")));
    assert!(stdout.contains(&field("logical_predicate_operator", "not")));
    assert!(stdout.contains(&field("logical_predicate_leaf_count", "1")));
    assert!(stdout.contains(&field("selected_row_count", "1")));
    assert!(stdout.contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":1}\\n\""));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_invalid_date_literals_without_fallback() {
    let source_path = unique_path("sql-local-source-date-literal-blocked", "csv");
    fs::write(&source_path, "id,event_date\n1,2026-05-19\n").expect("write source csv");

    let statement = format!(
        "SELECT id FROM '{}' WHERE event_date >= DATE '2026-02-30' LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("DATE literals must use DATE 'YYYY-MM-DD'"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_skips_unselected_nested_jsonl_values_without_fallback() {
    let source_path = unique_path("sql-local-source-jsonl-nested-blocked", "jsonl");
    fs::write(&source_path, "{\"id\":1,\"payload\":{\"x\":1}}\n").expect("write source jsonl");

    let statement = format!(
        "SELECT id FROM '{}' WHERE id >= 1 LIMIT 10",
        source_path.display()
    );
    let stdout = run_sql_local_source_smoke_json(&statement);
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("source_format", "jsonl")));
    assert!(stdout.contains(&field("source_columns", "id,payload")));
    assert!(stdout.contains(&field("source_state_read_plan", "required_columns")));
    assert!(stdout.contains(&field("source_state_requested_columns", "id")));
    assert!(stdout.contains(&field("source_state_reader_projection_columns", "id")));
    assert!(stdout.contains(&field("source_state_pruned_column_count", "1")));
    assert!(stdout.contains(&field("source_state_column_pruning_applied", "true")));
    assert!(stdout.contains("\"result_jsonl\",\"value\":\"{\\\"id\\\":1}\\n\""));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    let selected_statement = format!(
        "SELECT payload FROM '{}' WHERE id >= 1 LIMIT 10",
        source_path.display()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            &selected_statement,
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("JSON source runtime admits scalar values only"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("external_engine_invoked=false"));

    fs::remove_file(source_path).expect("remove source jsonl");
}

#[test]
fn sql_local_source_smoke_blocks_remote_sources_before_execution() {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "sql-local-source-smoke",
            "SELECT id FROM 's3://bucket/input.csv' WHERE id = 1 LIMIT 1",
            "--format",
            "json",
        ])
        .output()
        .expect("sql-local-source-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"sql-local-source-smoke\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains(
        "local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet/Arrow IPC/Avro/ORC file paths only"
    ));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
}
