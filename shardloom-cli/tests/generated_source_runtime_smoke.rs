use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn unique_output_path(name: &str) -> PathBuf {
    unique_output_path_with_extension(name, "jsonl")
}

fn unique_output_path_with_extension(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{nanos}.{extension}",
        std::process::id(),
    ))
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn assert_generated_vortex_writer_pressure_fields(stdout: &str) {
    assert!(stdout.contains(&field(
        "vortex_write_timing_split_schema_version",
        "shardloom.vortex_write_timing_split.v1"
    )));
    assert!(stdout.contains("\"vortex_segment_write_millis\",\"value\":\""));
    assert!(stdout.contains("\"vortex_workspace_stage_millis\",\"value\":\""));
    assert!(
        stdout.contains(&field(
            "vortex_writer_context_reuse_status",
            "thread_local_write_context_opened_for_first_artifact"
        )) || stdout.contains(&field(
            "vortex_writer_context_reuse_status",
            "thread_local_write_context_reused_for_artifact"
        ))
    );
    assert!(stdout.contains(&field(
        "vortex_writer_layout_strategy_applied",
        "vortex_write_strategy_upstream_default"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_coalescing_policy_status",
        "upstream_vortex_default_writer_coalescing_policy"
    )));
    assert!(stdout.contains(&field("vortex_writer_layout_row_block_size", "8192")));
    assert!(stdout.contains(&field(
        "vortex_writer_compression_policy",
        "vortex_default_btrblocks_available_parallelism"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_profile_selection_reason",
        "upstream_vortex_default_writer_profile"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_profile_regression_guard",
        "not_applicable"
    )));
    assert!(stdout.contains(&field("vortex_layout_write_decision_applied", "false")));
    assert!(stdout.contains(&field(
        "vortex_layout_write_decision_strategy",
        "not_requested"
    )));
    assert!(stdout.contains(&field(
        "vortex_layout_write_decision_blocker",
        "layout_write_advisor_not_attached_to_writer"
    )));
}

#[test]
#[allow(clippy::too_many_lines)]
fn user_rows_smoke_writes_local_jsonl_and_emits_generated_source_evidence() {
    let output_path = unique_output_path("generated-user-rows");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "id:int64,label:utf8,active:bool,score:float64",
            "id=1,label=alpha,active=true,score=1.5;id=2,label=beta,active=false,score=2.25",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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

    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"id\":1,\"label\":\"alpha\",\"active\":true,\"score\":1.5}\n\
         {\"id\":2,\"label\":\"beta\",\"active\":false,\"score\":2.25}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-user-rows-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.generated_source_user_rows_smoke.v1"
    )));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
    assert!(stdout.contains(&field("execution_mode", "source_free_generated_output")));
    assert!(stdout.contains(&field("engine_mode", "batch")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("input_dataset_count", "0")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field(
        "source_native_io_certificate_status",
        "not_applicable_no_source_dataset"
    )));
    assert!(stdout.contains(&field("generated_source_created", "true")));
    assert!(stdout.contains(&field("generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("output_workspace_path_safety_status", "enforced")));
    assert!(stdout.contains(&field("output_within_workspace", "true")));
    assert!(stdout.contains(&field("output_path_traversal_checked", "true")));
    assert!(stdout.contains(&field("output_symlink_followed", "false")));
    assert!(stdout.contains(&field("output_overwrite_allowed", "false")));
    assert!(stdout.contains(&field("output_overwrite_performed", "false")));
    assert!(stdout.contains(&field("output_commit_mode", "atomic_rename_same_directory")));
    assert!(stdout.contains(&field("output_commit_status", "committed")));
    assert!(stdout.contains(&field(
        "output_cleanup_status",
        "no_staging_artifacts_remaining"
    )));
    assert!(stdout.contains(&field("output_rollback_status", "not_required_new_target")));
    assert!(stdout.contains(&field("output_fallback_attempted", "false")));
    assert!(stdout.contains(&field("output_external_engine_invoked", "false")));
    assert!(stdout.contains("\"output_staging_path\",\"value\":\""));
    assert!(stdout.contains(&field("prepared_state_created", "false")));
    assert!(stdout.contains(&field("prepared_state_reuse_hit", "false")));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_scope",
        "not_applicable_non_vortex_generated_output"
    )));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_reason",
        "not_requested_non_vortex_generated_output"
    )));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "false")));
    assert!(stdout.contains(&field(
        "vortex_write_timing_split_schema_version",
        "shardloom.vortex_write_timing_split.v1"
    )));
    assert!(stdout.contains(&field("vortex_segment_write_millis", "0")));
    assert!(stdout.contains(&field("vortex_workspace_stage_millis", "0")));
    assert!(stdout.contains(&field(
        "vortex_writer_layout_strategy_applied",
        "not_applicable"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_profile_selection_reason",
        "not_applicable"
    )));
    assert!(stdout.contains(&field(
        "vortex_writer_profile_regression_guard",
        "not_applicable"
    )));
    assert!(stdout.contains(&field("vortex_layout_write_decision_applied", "false")));
    assert!(stdout.contains(&field("upstream_vortex_write_called", "false")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "false")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("sink_artifact_count", "1")));
    assert!(stdout.contains(&field(
        "sink_artifact_ref",
        &output_path.display().to_string()
    )));
    assert!(stdout.contains("\"sink_artifact_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains(&field("sink_artifact_formats", "jsonl")));
    assert!(stdout.contains(&field(
        "sink_artifact_manifest_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"jsonl:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        output_path.display(),
        output_path.display()
    )));
    assert!(stdout.contains(&field("execution_certificate_status", "certified")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("data_decoded", "false")));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("network_probe", "false")));
    assert!(stdout.contains(&field("catalog_probe", "false")));
    assert!(stdout.contains(&field("foundry_runtime_invoked", "false")));
    assert!(stdout.contains(&field("foundry_spark_invoked", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("fallback_execution_allowed", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(&field("performance_claim_allowed", "false")));
    assert!(stdout.contains(&field("production_claim_allowed", "false")));
    assert!(stdout.contains(&field("sql_dataframe_runtime_claim_allowed", "false")));
    assert!(stdout.contains(&field("object_store_lakehouse_claim_allowed", "false")));
    assert!(stdout.contains("\"generated_source_schema_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"generated_source_plan_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"output_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"sha256:"));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
#[cfg(not(feature = "vortex-write"))]
fn generated_source_vortex_output_blocks_without_vortex_write_feature() {
    let output_path =
        unique_output_path_with_extension("generated-user-rows-vortex-blocked", "vortex");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "id:int64,label:utf8",
            "id=1,label=alpha",
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    assert!(!output_path.exists());
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-user-rows-smoke\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("local Vortex generated-source output runtime requires"));
    assert!(stdout.contains("--features vortex-write"));
    assert!(stdout.contains("deterministic blocked sink"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
}

#[test]
#[cfg(feature = "vortex-write")]
fn generated_source_vortex_output_writes_local_artifact_and_emits_vortex_evidence() {
    let output_path = unique_output_path_with_extension("generated-user-rows-vortex", "vortex");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "id:int64,label:utf8,score:float64",
            "id=1,label=alpha,score=1.5;id=2,label=beta,score=2.25",
            "--output-format",
            "vortex",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    assert!(output_path.exists());
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-user-rows-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("output_format", "vortex")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_vortex_sink"
    )));
    assert!(stdout.contains(&field("vortex_output_runtime_execution", "true")));
    assert!(stdout.contains(&field("vortex_output_reopen_verified", "true")));
    assert!(stdout.contains(&field("vortex_output_row_count", "2")));
    assert!(stdout.contains(&field("vortex_output_column_count", "3")));
    assert!(stdout.contains(&field(
        "vortex_prepared_olap_layout_inventory_status",
        "opened_single_vortex_artifact_footer"
    )));
    assert!(stdout.contains(&field("vortex_prepared_olap_layout_footer_row_count", "2")));
    assert!(stdout.contains(&field(
        "vortex_prepared_olap_layout_footer_statistics_status",
        "available"
    )));
    assert!(stdout.contains(&field(
        "vortex_prepared_olap_layout_footer_encoding_layout_status",
        "segment_map_available"
    )));
    assert!(stdout.contains(&field(
        "vortex_prepared_olap_layout_metadata_persisted_in_artifact",
        "true"
    )));
    assert!(stdout.contains(&field("prepared_state_created", "true")));
    assert!(stdout.contains(&field("prepared_state_reused", "false")));
    assert!(stdout.contains(&field("prepared_state_reuse_hit", "false")));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_scope",
        "single_vortex_artifact_no_sidecar"
    )));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_reason",
        "generated_source_vortex_output_writes_single_vortex_artifact_without_sidecar"
    )));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_manifest_path",
        "not_applicable_single_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "prepared_state_reuse_manifest_digest",
        "not_applicable_single_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "prepared_state_invalidation_reason",
        "not_applicable_single_vortex_artifact"
    )));
    assert!(stdout.contains(&field(
        "vortex_output_timing_scope",
        "vortex_ingest_prepare_once"
    )));
    assert!(stdout.contains(&field(
        "vortex_output_certification_level",
        "ingest_certified"
    )));
    assert_generated_vortex_writer_pressure_fields(&stdout);
    assert!(stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(stdout.contains(&field("upstream_vortex_scan_called", "false")));
    assert!(stdout.contains("\"vortex_artifact_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    fs::remove_file(output_path).expect("remove vortex output");
}

#[test]
#[cfg(feature = "vortex-write")]
fn generated_source_vortex_output_keeps_single_artifact_without_reuse_manifest() {
    let output_path =
        unique_output_path_with_extension("generated-user-rows-vortex-reuse", "vortex");
    let args = [
        "generated-source-user-rows-smoke",
        output_path.to_str().expect("temp path is utf8"),
        "id:int64,label:utf8,score:float64",
        "id=1,label=alpha,score=1.5;id=2,label=beta,score=2.25",
        "--output-format",
        "vortex",
        "--format",
        "json",
    ];
    let first = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("first generated-source Vortex command runs");
    assert!(
        first.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    let first_stdout = String::from_utf8(first.stdout).expect("stdout is utf8");
    assert!(first_stdout.contains(&field("prepared_state_created", "true")));
    assert!(first_stdout.contains(&field("prepared_state_reused", "false")));
    assert!(first_stdout.contains(&field("prepared_state_reuse_hit", "false")));
    assert!(first_stdout.contains(&field(
        "prepared_state_reuse_reason",
        "generated_source_vortex_output_writes_single_vortex_artifact_without_sidecar"
    )));
    assert!(first_stdout.contains(&field(
        "prepared_state_reuse_manifest_path",
        "not_applicable_single_vortex_artifact"
    )));
    assert!(first_stdout.contains(&field("upstream_vortex_write_called", "true")));
    assert!(first_stdout.contains(&field("upstream_vortex_scan_called", "false")));

    let second = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("second generated-source Vortex command runs");
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
    let second_stdout = String::from_utf8(second.stdout).expect("stdout is utf8");
    assert!(second_stdout.contains("\"status\":\"error\""));
    assert!(second_stdout.contains("output target already exists and overwrite is disabled"));
    assert!(second_stdout.contains("no fallback execution was attempted"));
    assert!(second_stdout.contains("\"attempted\":false"));
    assert!(second_stdout.contains("\"allowed\":false"));

    let manifest_path = output_path
        .parent()
        .expect("output path parent")
        .join(".shardloom")
        .join(format!(
            "{}.prepared-state-reuse.manifest",
            output_path
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .expect("file name")
        ));
    assert!(!manifest_path.exists());
    fs::remove_file(output_path).expect("remove vortex output");
}

#[test]
fn generated_source_smokes_write_local_csv_outputs() {
    let user_rows_path = unique_output_path_with_extension("generated-user-rows-csv", "csv");
    let user_rows_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            user_rows_path.to_str().expect("temp path is utf8"),
            "id:int64,label:utf8,active:bool",
            "id=1,label=alpha,active=true;id=2,label=comma%2Cquote%22,active=false",
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");
    assert!(
        user_rows_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&user_rows_output.stdout),
        String::from_utf8_lossy(&user_rows_output.stderr)
    );
    let user_rows_written = fs::read_to_string(&user_rows_path).expect("csv output was written");
    assert_eq!(
        user_rows_written,
        "id,label,active\n1,alpha,true\n2,\"comma,quote\"\"\",false\n"
    );
    let user_rows_stdout = String::from_utf8(user_rows_output.stdout).expect("stdout is utf8");
    assert!(user_rows_stdout.contains(&field("output_format", "csv")));
    assert!(user_rows_stdout.contains(&field(
        "materialization_boundary",
        "python_user_rows_to_local_csv_sink"
    )));
    assert!(user_rows_stdout.contains(&field("fallback_attempted", "false")));
    assert!(user_rows_stdout.contains(&field("external_engine_invoked", "false")));
    fs::remove_file(user_rows_path).expect("remove csv output");

    let range_path = unique_output_path_with_extension("generated-range-csv", "csv");
    let range_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-range-smoke",
            range_path.to_str().expect("temp path is utf8"),
            "1",
            "4",
            "--column",
            "id",
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-range-smoke command runs");
    assert!(
        range_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&range_output.stdout),
        String::from_utf8_lossy(&range_output.stderr)
    );
    let range_written = fs::read_to_string(&range_path).expect("csv output was written");
    assert_eq!(range_written, "id\n1\n2\n3\n");
    let range_stdout = String::from_utf8(range_output.stdout).expect("stdout is utf8");
    assert!(range_stdout.contains(&field("output_format", "csv")));
    assert!(range_stdout.contains(&field(
        "materialization_boundary",
        "engine_native_range_generator_to_local_csv_sink"
    )));
    assert!(range_stdout.contains(&field("fallback_attempted", "false")));
    assert!(range_stdout.contains(&field("external_engine_invoked", "false")));
    fs::remove_file(range_path).expect("remove csv output");

    let sql_path = unique_output_path_with_extension("generated-sql-csv", "csv");
    let sql_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            sql_path.to_str().expect("temp path is utf8"),
            "VALUES (1, 'alpha'), (2, 'beta')",
            "--output-format",
            "csv",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");
    assert!(
        sql_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&sql_output.stdout),
        String::from_utf8_lossy(&sql_output.stderr)
    );
    let sql_written = fs::read_to_string(&sql_path).expect("csv output was written");
    assert_eq!(sql_written, "column_1,column_2\n1,alpha\n2,beta\n");
    let sql_stdout = String::from_utf8(sql_output.stdout).expect("stdout is utf8");
    assert!(sql_stdout.contains(&field("output_format", "csv")));
    assert!(sql_stdout.contains(&field(
        "materialization_boundary",
        "sql_values_to_local_csv_sink"
    )));
    assert!(sql_stdout.contains(&field("fallback_attempted", "false")));
    assert!(sql_stdout.contains(&field("external_engine_invoked", "false")));
    fs::remove_file(sql_path).expect("remove csv output");
}

#[test]
#[cfg(not(feature = "universal-format-io"))]
fn generated_source_structured_outputs_fail_closed_without_feature() {
    let output_path =
        unique_output_path_with_extension("generated-user-rows-parquet-blocked", "parquet");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "id:int64,label:utf8",
            "id=1,label=alpha",
            "--output-format",
            "parquet",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    assert!(
        !output_path.exists(),
        "blocked structured output should not create a sink"
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-user-rows-smoke\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("requires building shardloom-cli with --features universal-format-io"));
    assert!(stdout.contains("deterministic blocked sink"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
}

#[test]
#[cfg(feature = "universal-format-io")]
fn generated_source_smokes_write_feature_gated_structured_outputs() {
    for (name, extension, command_args, expected_format, expected_certificate, expected_boundary) in [
        (
            "generated-user-rows-parquet",
            "parquet",
            vec![
                "generated-source-user-rows-smoke".to_string(),
                "id:int64,label:utf8,active:bool,score:float64".to_string(),
                "id=1,label=alpha,active=true,score=1.5;id=2,label=beta,active=false,score=2.25"
                    .to_string(),
                "--output-format".to_string(),
                "parquet".to_string(),
            ],
            "parquet",
            "certified_local_parquet_sink",
            "python_user_rows_to_local_parquet_sink",
        ),
        (
            "generated-range-arrow-ipc",
            "arrow",
            vec![
                "generated-source-range-smoke".to_string(),
                "1".to_string(),
                "4".to_string(),
                "--column".to_string(),
                "id".to_string(),
                "--output-format".to_string(),
                "arrow-ipc".to_string(),
            ],
            "arrow_ipc",
            "certified_local_arrow_ipc_sink",
            "engine_native_range_generator_to_local_arrow_ipc_sink",
        ),
        (
            "generated-sql-avro",
            "avro",
            vec![
                "generated-source-sql-smoke".to_string(),
                "VALUES (1, 'alpha'), (2, 'beta')".to_string(),
                "--output-format".to_string(),
                "avro".to_string(),
            ],
            "avro",
            "certified_local_avro_sink",
            "sql_values_to_local_avro_sink",
        ),
        (
            "generated-sequence-orc",
            "orc",
            vec![
                "generated-source-sequence-smoke".to_string(),
                "1".to_string(),
                "4".to_string(),
                "--column".to_string(),
                "seq".to_string(),
                "--output-format".to_string(),
                "orc".to_string(),
            ],
            "orc",
            "certified_local_orc_sink",
            "engine_native_sequence_generator_to_local_orc_sink",
        ),
    ] {
        let output_path = unique_output_path_with_extension(name, extension);
        let mut args = vec![
            command_args[0].as_str(),
            output_path.to_str().expect("temp path is utf8"),
        ];
        args.extend(command_args.iter().skip(1).map(String::as_str));
        args.extend(["--format", "json"]);

        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args(args)
            .output()
            .expect("generated-source command runs");

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
        let written = fs::read(&output_path).expect("structured output was written");
        assert!(!written.is_empty(), "structured sink must not be empty");
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains(&field("output_format", expected_format)));
        assert!(stdout.contains(&field(
            "output_native_io_certificate_status",
            expected_certificate
        )));
        assert!(stdout.contains(&field("materialization_boundary", expected_boundary)));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
        fs::remove_file(output_path).expect("remove structured output");
    }
}

#[test]
fn user_rows_smoke_supports_literal_table_and_calendar_source_kinds() {
    for (name, source_kind, schema, rows, expected_written, expected_boundary, expected_reason) in [
        (
            "generated-literal-table",
            "literal_table",
            "code:utf8,weight:float64",
            "code=A,weight=1.5;code=B,weight=2.0",
            "{\"code\":\"A\",\"weight\":1.5}\n{\"code\":\"B\",\"weight\":2}\n",
            "python_literal_table_to_local_jsonl_sink",
            "one_scoped_local_literal_table_generated_output_smoke",
        ),
        (
            "generated-calendar",
            "calendar",
            "dt:utf8,year:int64,month:int64,day:int64",
            "dt=2026-05-18,year=2026,month=5,day=18;dt=2026-05-19,year=2026,month=5,day=19",
            "{\"dt\":\"2026-05-18\",\"year\":2026,\"month\":5,\"day\":18}\n{\"dt\":\"2026-05-19\",\"year\":2026,\"month\":5,\"day\":19}\n",
            "python_calendar_generator_to_local_jsonl_sink",
            "one_scoped_local_calendar_generated_output_smoke",
        ),
    ] {
        let output_path = unique_output_path(name);
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "generated-source-user-rows-smoke",
                output_path.to_str().expect("temp path is utf8"),
                schema,
                rows,
                "--source-kind",
                source_kind,
                "--format",
                "json",
            ])
            .output()
            .expect("generated-source-user-rows-smoke command runs");

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
        let written = fs::read_to_string(&output_path).expect("output jsonl was written");
        assert_eq!(written, expected_written);

        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains(&field("generated_source_kind", source_kind)));
        assert!(stdout.contains(&field("generated_source_row_count", "2")));
        assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
        assert!(stdout.contains(&field(
            "output_native_io_certificate_status",
            "certified_local_file_sink"
        )));
        assert!(stdout.contains(&field("materialization_boundary", expected_boundary)));
        assert!(stdout.contains(&field("claim_gate_reason", expected_reason)));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

        fs::remove_file(output_path).expect("remove output jsonl");
    }
}

#[test]
fn user_rows_smoke_supports_dataframe_source_free_projection_source_kind() {
    let output_path = unique_output_path("generated-dataframe-projection");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "value:int64,label:utf8",
            "value=1,label=alpha",
            "--source-kind",
            "dataframe_source_free_projection",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"value\":1,\"label\":\"alpha\"}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "generated_source_kind",
        "dataframe_source_free_projection"
    )));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "python_dataframe_source_free_projection_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_dataframe_source_free_projection_generated_output_smoke"
    )));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn user_rows_smoke_supports_dataframe_generated_with_column_source_kind() {
    let output_path = unique_output_path("generated-dataframe-with-column");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "value:int64",
            "value=1",
            "--source-kind",
            "dataframe_generated_with_column",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"value\":1}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field(
        "generated_source_kind",
        "dataframe_generated_with_column"
    )));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "python_dataframe_generated_with_column_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_dataframe_generated_with_column_generated_output_smoke"
    )));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn user_rows_smoke_blocks_remote_object_store_outputs() {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-user-rows-smoke",
            "s3://bucket/out.jsonl",
            "id:int64",
            "id=1",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-user-rows-smoke command runs");

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
    assert!(stdout.contains("\"command\":\"generated-source-user-rows-smoke\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("support local file output only"));
    assert!(stdout.contains("object-store and remote URI writes remain blocked"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
}

#[test]
fn range_smoke_writes_local_jsonl_and_emits_engine_native_generated_source_evidence() {
    let output_path = unique_output_path("generated-range");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-range-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "2",
            "8",
            "--step",
            "2",
            "--column",
            "id",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-range-smoke command runs");

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

    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"id\":2}\n{\"id\":4}\n{\"id\":6}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-range-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.generated_source_range_smoke.v1"
    )));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
    assert!(stdout.contains(&field("execution_mode", "source_free_generated_output")));
    assert!(stdout.contains(&field("engine_mode", "batch")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("input_dataset_count", "0")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field(
        "source_native_io_certificate_status",
        "not_applicable_no_source_dataset"
    )));
    assert!(stdout.contains(&field("generated_source_created", "true")));
    assert!(stdout.contains(&field("generated_source_kind", "range")));
    assert!(stdout.contains(&field("generated_source_range_start", "2")));
    assert!(stdout.contains(&field("generated_source_range_end", "8")));
    assert!(stdout.contains(&field("generated_source_range_step", "2")));
    assert!(stdout.contains(&field("generated_source_range_column", "id")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("execution_certificate_status", "certified")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "engine_native_range_generator_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("data_decoded", "false")));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("network_probe", "false")));
    assert!(stdout.contains(&field("catalog_probe", "false")));
    assert!(stdout.contains(&field("foundry_runtime_invoked", "false")));
    assert!(stdout.contains(&field("foundry_spark_invoked", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_range_generated_output_smoke"
    )));
    assert!(stdout.contains(&field("performance_claim_allowed", "false")));
    assert!(stdout.contains(&field("production_claim_allowed", "false")));
    assert!(stdout.contains(&field("sql_dataframe_runtime_claim_allowed", "false")));
    assert!(stdout.contains(&field("object_store_lakehouse_claim_allowed", "false")));
    assert!(stdout.contains("\"generated_source_schema_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"generated_source_plan_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"output_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"sha256:"));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sequence_smoke_writes_local_jsonl_and_emits_sequence_evidence() {
    let output_path = unique_output_path("generated-sequence");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sequence-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "1",
            "6",
            "--step",
            "2",
            "--column",
            "seq",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sequence-smoke command runs");

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

    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"seq\":1}\n{\"seq\":3}\n{\"seq\":5}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sequence-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.generated_source_sequence_smoke.v1"
    )));
    assert!(stdout.contains(&field("generated_source_kind", "sequence")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "6")));
    assert!(stdout.contains(&field("generated_source_range_step", "2")));
    assert!(stdout.contains(&field("generated_source_range_column", "seq")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "engine_native_sequence_generator_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_sequence_generated_output_smoke"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn range_smoke_blocks_remote_outputs_and_zero_step() {
    let remote = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-range-smoke",
            "s3://bucket/out.jsonl",
            "0",
            "3",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-range-smoke command runs");

    assert!(
        !remote.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&remote.stdout),
        String::from_utf8_lossy(&remote.stderr)
    );
    let remote_stdout = String::from_utf8(remote.stdout).expect("stdout is utf8");
    assert!(remote_stdout.contains("\"command\":\"generated-source-range-smoke\""));
    assert!(remote_stdout.contains("\"status\":\"error\""));
    assert!(remote_stdout.contains("support local file output only"));
    assert!(remote_stdout.contains("object-store and remote URI writes remain blocked"));
    assert!(remote_stdout.contains("\"attempted\":false"));

    let output_path = unique_output_path("generated-range-zero-step");
    let zero_step = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-range-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "0",
            "3",
            "--step",
            "0",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-range-smoke command runs");

    assert!(
        !zero_step.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&zero_step.stdout),
        String::from_utf8_lossy(&zero_step.stderr)
    );
    let zero_step_stdout = String::from_utf8(zero_step.stdout).expect("stdout is utf8");
    assert!(zero_step_stdout.contains("\"command\":\"generated-source-range-smoke\""));
    assert!(zero_step_stdout.contains("\"status\":\"error\""));
    assert!(zero_step_stdout.contains("step must not be zero"));
    assert!(zero_step_stdout.contains("\"attempted\":false"));
}

#[test]
fn sql_smoke_writes_literal_select_jsonl_and_emits_generated_source_evidence() {
    let output_path = unique_output_path("generated-sql-select");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT 1 AS id, 'alpha' AS label, true AS active, 1.5 AS score",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

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

    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"id\":1,\"label\":\"alpha\",\"active\":true,\"score\":1.5}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.generated_source_sql_smoke.v1"
    )));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
    assert!(stdout.contains(&field("execution_mode", "source_free_generated_output")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("input_dataset_count", "0")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("sql_parser_executed", "true")));
    assert!(stdout.contains(&field("sql_binder_executed", "true")));
    assert!(stdout.contains(&field("sql_planner_executed", "true")));
    assert!(stdout.contains(&field("sql_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_statement_kind", "sql_literal_select")));
    assert!(stdout.contains(&field("generated_source_created", "true")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_literal_select")));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("execution_certificate_status", "certified")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "sql_literal_select_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(stdout.contains(&field(
        "claim_gate_reason",
        "one_scoped_local_sql_literal_select_generated_output_smoke"
    )));
    assert!(stdout.contains(&field("sql_source_free_runtime_smoke_supported", "true")));
    assert!(stdout.contains(&field("sql_production_runtime_claim_allowed", "false")));
    assert!(stdout.contains(&field("performance_claim_allowed", "false")));
    assert!(stdout.contains("\"generated_source_schema_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"generated_source_plan_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"output_digest\",\"value\":\"sha256:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"sha256:"));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_writes_values_jsonl_and_rejects_broader_sql() {
    let output_path = unique_output_path("generated-sql-values");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "VALUES (1, 'alpha'), (2, 'beta')",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"column_1\":1,\"column_2\":\"alpha\"}\n{\"column_1\":2,\"column_2\":\"beta\"}\n"
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("generated_source_kind", "sql_values")));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field(
        "materialization_boundary",
        "sql_values_to_local_jsonl_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));
    fs::remove_file(output_path).expect("remove output jsonl");

    let blocked_path = unique_output_path("generated-sql-blocked");
    let blocked = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            blocked_path.to_str().expect("temp path is utf8"),
            "SELECT id FROM events",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");
    assert!(
        !blocked.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&blocked.stdout),
        String::from_utf8_lossy(&blocked.stderr)
    );
    let blocked_stdout = String::from_utf8(blocked.stdout).expect("stdout is utf8");
    assert!(blocked_stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(blocked_stdout.contains("\"status\":\"error\""));
    assert!(blocked_stdout.contains("does not admit FROM clauses"));
    assert!(blocked_stdout.contains("no fallback engine was invoked"));
    assert!(blocked_stdout.contains("\"attempted\":false"));
}

#[test]
fn sql_smoke_writes_generate_series_and_range_jsonl() {
    for (
        name,
        statement,
        expected_written,
        expected_function,
        expected_end_inclusive,
        expected_rows,
    ) in [
        (
            "generated-sql-generate-series",
            "SELECT * FROM generate_series(2, 8, 2)",
            "{\"value\":2}\n{\"value\":4}\n{\"value\":6}\n{\"value\":8}\n",
            "generate_series",
            "true",
            "4",
        ),
        (
            "generated-sql-range",
            "SELECT * FROM range(2, 8, 2)",
            "{\"value\":2}\n{\"value\":4}\n{\"value\":6}\n",
            "range",
            "false",
            "3",
        ),
    ] {
        let output_path = unique_output_path(name);
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "generated-source-sql-smoke",
                output_path.to_str().expect("temp path is utf8"),
                statement,
                "--format",
                "json",
            ])
            .output()
            .expect("generated-source-sql-smoke command runs");

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
        let written = fs::read_to_string(&output_path).expect("output jsonl was written");
        assert_eq!(written, expected_written);

        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("sql_statement_kind", "sql_generate_series_range")));
        assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
        assert!(stdout.contains(&field("generated_source_range_start", "2")));
        assert!(stdout.contains(&field("generated_source_range_end", "8")));
        assert!(stdout.contains(&field("generated_source_range_step", "2")));
        assert!(stdout.contains(&field("generated_source_range_column", "value")));
        assert!(stdout.contains(&field(
            "generated_source_sql_generator_function",
            expected_function
        )));
        assert!(stdout.contains(&field(
            "generated_source_range_end_inclusive",
            expected_end_inclusive
        )));
        assert!(stdout.contains(&field("generated_source_row_count", expected_rows)));
        assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
        assert!(stdout.contains(&field(
            "output_native_io_certificate_status",
            "certified_local_file_sink"
        )));
        assert!(stdout.contains(&field(
            "materialization_boundary",
            "sql_generate_series_range_to_local_jsonl_sink"
        )));
        assert!(stdout.contains(&field(
            "claim_gate_reason",
            "one_scoped_local_sql_generate_series_range_generated_output_smoke"
        )));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
        assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

        fs::remove_file(output_path).expect("remove output jsonl");
    }
}

#[test]
fn sql_smoke_writes_generate_series_projection_jsonl() {
    let output_path = unique_output_path("generated-sql-range-projection");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value AS id, value + 10 AS shifted, value * 2 AS doubled, CASE WHEN value >= 3 THEN 1 ELSE 0 END AS is_high FROM range(2, 5)",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

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
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"id\":2,\"shifted\":12,\"doubled\":4,\"is_high\":0}\n{\"id\":3,\"shifted\":13,\"doubled\":6,\"is_high\":1}\n{\"id\":4,\"shifted\":14,\"doubled\":8,\"is_high\":1}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_range_start", "2")));
    assert!(stdout.contains(&field("generated_source_range_end", "5")));
    assert!(stdout.contains(&field("generated_source_range_step", "1")));
    assert!(stdout.contains(&field("generated_source_range_column", "value")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("sql_source_free_projection_source_column", "value")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_columns",
        "id,shifted,doubled,is_high"
    )));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_expressions",
        "value,value+10,value*2,case(value>=3?1:0)"
    )));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_writes_generate_series_filter_limit_projection_jsonl() {
    let output_path = unique_output_path("generated-sql-range-filter-limit");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value AS id, value + 10 AS shifted, CASE WHEN value >= 5 THEN 1 ELSE 0 END AS is_high FROM range(1, 8) WHERE value >= 3 LIMIT 3",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

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
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"id\":3,\"shifted\":13,\"is_high\":0}\n{\"id\":4,\"shifted\":14,\"is_high\":0}\n{\"id\":5,\"shifted\":15,\"is_high\":1}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "8")));
    assert!(stdout.contains(&field("generated_source_range_step", "1")));
    assert!(stdout.contains(&field("generated_source_range_column", "value")));
    assert!(stdout.contains(&field("sql_source_free_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_filter_source_column", "value")));
    assert!(stdout.contains(&field("sql_source_free_filter_predicate", "value>=3")));
    assert!(stdout.contains(&field("sql_source_free_filter_selected_row_count", "5")));
    assert!(stdout.contains(&field("sql_source_free_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_limit_count", "3")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("sql_source_free_projection_source_column", "value")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_columns",
        "id,shifted,is_high"
    )));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_expressions",
        "value,value+10,case(value>=5?1:0)"
    )));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("generated_source_certificate_status", "present")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("claim_gate_status", "fixture_smoke_only")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_writes_generate_series_projection_order_by_topn_jsonl() {
    let output_path = unique_output_path("generated-sql-range-projection-topn");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value AS id, value * 2 AS doubled FROM range(1, 6) WHERE value >= 2 ORDER BY doubled DESC LIMIT 2",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

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
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(
        written,
        "{\"id\":5,\"doubled\":10}\n{\"id\":4,\"doubled\":8}\n"
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("sql_statement_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "6")));
    assert!(stdout.contains(&field("generated_source_range_step", "1")));
    assert!(stdout.contains(&field("sql_source_free_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_filter_selected_row_count", "4")));
    assert!(stdout.contains(&field("sql_source_free_order_by_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_top_n_runtime_execution", "true")));
    assert!(stdout.contains(&field(
        "sql_source_free_sort_operator_family",
        "single_key_int64_topn"
    )));
    assert!(stdout.contains(&field("sql_source_free_sort_keys", "doubled")));
    assert!(stdout.contains(&field("sql_source_free_sort_direction", "desc")));
    assert!(stdout.contains(&field("sql_source_free_sort_input_row_count", "4")));
    assert!(stdout.contains(&field("sql_source_free_top_n_limit", "2")));
    assert!(stdout.contains(&field("sql_source_free_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_limit_count", "2")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_runtime_execution",
        "true"
    )));
    assert!(stdout.contains(&field("sql_source_free_projection_columns", "id,doubled")));
    assert!(stdout.contains(&field(
        "sql_source_free_projection_expressions",
        "value,value*2"
    )));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_smoke_writes_generate_series_topn_fanout_and_replay_evidence() {
    let output_path = unique_output_path("generated-sql-range-fanout-primary");
    let fanout_path = unique_output_path_with_extension("generated-sql-range-fanout-csv", "csv");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value AS id, value * 2 AS doubled FROM range(1, 6) ORDER BY doubled DESC LIMIT 2",
            "--output-format",
            "jsonl",
            "--fanout-output",
        ])
        .arg(format!("csv={}", fanout_path.display()))
        .args(["--format", "json"])
        .output()
        .expect("generated-source-sql-smoke command runs");

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

    let written = fs::read_to_string(&output_path).expect("primary jsonl was written");
    assert_eq!(
        written,
        "{\"id\":5,\"doubled\":10}\n{\"id\":4,\"doubled\":8}\n"
    );
    let fanout = fs::read_to_string(&fanout_path).expect("fanout csv was written");
    assert_eq!(fanout, "id,doubled\n5,10\n4,8\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("output_route", "local_sink_and_fanout")));
    assert!(stdout.contains(&field("result_reuse_for_fanout", "true")));
    assert!(stdout.contains(&field("fanout_result_reuse_hit", "true")));
    assert!(stdout.contains(&field("result_replay_verified", "true")));
    assert!(stdout.contains(&field(
        "output_replay_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&field(
        "output_fidelity_report_status",
        "scoped_local_output_fidelity_reported"
    )));
    assert!(stdout.contains(&field("output_fanout_performed", "true")));
    assert!(stdout.contains(&field("sink_artifact_count", "2")));
    assert!(stdout.contains(&field(
        "sink_artifact_refs",
        &format!(
            "jsonl:{},csv:{}",
            output_path.display(),
            fanout_path.display()
        )
    )));
    assert!(stdout.contains("\"sink_artifact_digests\",\"value\":\"jsonl:sha256:"));
    assert!(stdout.contains("csv:sha256:"));
    assert!(stdout.contains(&field("sink_artifact_formats", "jsonl,csv")));
    assert!(stdout.contains(&field(
        "sink_artifact_manifest_status",
        "verified_local_sink_artifacts"
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"jsonl:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        output_path.display(),
        output_path.display()
    )));
    assert!(stdout.contains(&format!(
        "{{\"id\":\"csv:{}\",\"kind\":\"sink_artifact\",\"status\":\"available\",\"uri\":\"{}\"}}",
        fanout_path.display(),
        fanout_path.display()
    )));
    assert!(stdout.contains(&field("fanout_output_count", "1")));
    assert!(stdout.contains(&field("fanout_output_formats", "csv")));
    assert!(stdout.contains("\"fanout_output_paths\",\"value\":\""));
    assert!(stdout.contains("\"fanout_output_bytes\",\"value\":\"csv:"));
    assert!(stdout.contains("\"fanout_output_digests\",\"value\":\"csv:sha256:"));
    assert!(stdout.contains(&field(
        "fanout_output_native_io_certificate_statuses",
        "csv:certified_local_file_sink"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_replay_statuses",
        "csv:verified_local_file_digest"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_fidelity_statuses",
        "csv:logical_rows_replay_verified_type_metadata_not_preserved"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_fidelity_loss",
        "csv:csv_text_roundtrip_loses_static_type_metadata"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_workspace_path_safety_statuses",
        "csv:true"
    )));
    assert!(stdout.contains(&field(
        "fanout_output_commit_modes",
        "csv:atomic_rename_same_directory"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
    fs::remove_file(fanout_path).expect("remove fanout csv");
}

#[test]
fn generated_source_fanout_rejects_duplicate_paths_before_writes() {
    let output_path = unique_output_path("generated-sql-range-fanout-duplicate");
    let duplicate_path = output_path
        .parent()
        .expect("temp path has parent")
        .join(".")
        .join(output_path.file_name().expect("temp path has file name"));
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT * FROM range(1, 3)",
            "--fanout-output",
        ])
        .arg(format!("csv={}", duplicate_path.display()))
        .args(["--format", "json"])
        .output()
        .expect("generated-source-sql-smoke command runs");

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
    assert!(!output_path.exists());
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("generated-source fanout output path is duplicated"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
}

#[test]
fn sql_smoke_writes_generate_series_source_order_by_topn_jsonl() {
    let output_path = unique_output_path("generated-sql-range-source-topn");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value * 2 AS doubled FROM range(1, 6) ORDER BY value DESC LIMIT 2",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"doubled\":10}\n{\"doubled\":8}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("sql_source_free_sort_keys", "value")));
    assert!(stdout.contains(&field("sql_source_free_sort_direction", "desc")));
    assert!(stdout.contains(&field("sql_source_free_top_n_limit", "2")));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_prefers_projected_order_by_alias_over_source_column() {
    let output_path = unique_output_path("generated-sql-range-alias-precedence-topn");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value * -1 AS value FROM range(1, 6) ORDER BY value DESC LIMIT 2",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"value\":-1}\n{\"value\":-2}\n");

    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains(&field("sql_source_free_sort_keys", "value")));
    assert!(stdout.contains(&field("sql_source_free_sort_direction", "desc")));
    assert!(stdout.contains(&field("sql_source_free_top_n_limit", "2")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_allows_from_in_projection_alias_identifier() {
    let output_path = unique_output_path("generated-sql-range-from-alias");
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "generated-source-sql-smoke",
            output_path.to_str().expect("temp path is utf8"),
            "SELECT value AS from_col FROM range(1, 3)",
            "--format",
            "json",
        ])
        .output()
        .expect("generated-source-sql-smoke command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let written = fs::read_to_string(&output_path).expect("output jsonl was written");
    assert_eq!(written, "{\"from_col\":1}\n{\"from_col\":2}\n");

    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_smoke_blocks_unadmitted_generate_series_forms() {
    for (name, statement, expected_error) in [
        (
            "generated-sql-generate-series-zero-step",
            "SELECT * FROM generate_series(1, 5, 0)",
            "step must not be zero",
        ),
        (
            "generated-sql-generate-series-one-arg",
            "SELECT * FROM generate_series(1)",
            "require start, end, and optional step",
        ),
        (
            "generated-sql-generate-series-case-string-branch",
            "SELECT CASE WHEN value >= 3 THEN 'high' ELSE 0 END AS bucket FROM range(1, 4)",
            "CASE projection THEN branch must be an int64 literal",
        ),
        (
            "generated-sql-generate-series-case-unsupported-predicate",
            "SELECT CASE WHEN value BETWEEN 1 AND 2 THEN 1 ELSE 0 END AS bucket FROM range(1, 4)",
            "CASE projection predicate must use =, !=, <>, <, <=, >, or >= against an int64 literal",
        ),
        (
            "generated-sql-generate-series-filter-wrong-column",
            "SELECT * FROM range(1, 4) WHERE other >= 2",
            "predicate must compare the range column",
        ),
        (
            "generated-sql-generate-series-limit-negative",
            "SELECT * FROM range(1, 4) LIMIT -1",
            "LIMIT requires a single non-negative integer literal",
        ),
        (
            "generated-sql-generate-series-limit-trailing-clause",
            "SELECT * FROM range(1, 4) LIMIT 1 ORDER BY value",
            "LIMIT requires a single non-negative integer literal",
        ),
        (
            "generated-sql-generate-series-order-by-missing-column",
            "SELECT value AS id FROM range(1, 4) ORDER BY missing LIMIT 1",
            "ORDER BY keys must resolve to the range source column or projected int64 output aliases",
        ),
        (
            "generated-sql-generate-series-order-by-duplicate-key",
            "SELECT * FROM range(1, 4) ORDER BY value, value LIMIT 1",
            "ORDER BY keys must be unique",
        ),
        (
            "generated-sql-generate-series-project",
            "SELECT value + 1 FROM generate_series(1, 5)",
            "computed projections require an explicit AS alias",
        ),
        (
            "generated-sql-generate-series-aggregate",
            "SELECT SUM(value) AS total FROM generate_series(1, 5)",
            "range projection admits only the range column",
        ),
    ] {
        let output_path = unique_output_path(name);
        let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
            .args([
                "generated-source-sql-smoke",
                output_path.to_str().expect("temp path is utf8"),
                statement,
                "--format",
                "json",
            ])
            .output()
            .expect("generated-source-sql-smoke command runs");

        assert!(
            !output.status.success(),
            "stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
        assert!(stdout.contains("\"command\":\"generated-source-sql-smoke\""));
        assert!(stdout.contains("\"status\":\"error\""));
        assert!(stdout.contains(expected_error));
        assert!(stdout.contains("no fallback engine was invoked"));
        assert!(stdout.contains("\"attempted\":false"));
    }
}
