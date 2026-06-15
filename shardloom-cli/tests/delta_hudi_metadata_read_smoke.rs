use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

fn run_shardloom_json(args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn temp_path(prefix: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-{prefix}-{}-{nanos}.{extension}",
        std::process::id(),
    ))
}

fn temp_dir(prefix: &str) -> PathBuf {
    let path = temp_path(prefix, "dir");
    fs::create_dir(&path).expect("create temp dir");
    path
}

fn write_safe_delta_log_fixture() -> PathBuf {
    let path = temp_path("delta-safe-log", "json");
    let log = r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"delta-orders-fixture","name":"orders","schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}","partitionColumns":["region"],"configuration":{"delta.appendOnly":"true"}}}
{"add":{"path":"region=us/part-000.parquet","partitionValues":{"region":"us"},"size":42,"modificationTime":1770000000000,"dataChange":true,"stats":"{\"numRecords\":2}"}}
{"commitInfo":{"operation":"WRITE","operationParameters":{"mode":"Append"}}}
"#;
    fs::write(&path, log).expect("write safe delta fixture");
    path
}

fn write_blocked_delta_log_fixture() -> PathBuf {
    let path = temp_path("delta-blocked-log", "json");
    let log = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}
{"metaData":{"id":"delta-dv-fixture","name":"orders","schemaString":"{}","partitionColumns":[],"configuration":{}}}
{"add":{"path":"part-000.parquet","deletionVector":{"storageType":"u","pathOrInlineDv":"abc","offset":1,"sizeInBytes":3,"cardinality":1}}}
{"remove":{"path":"part-old.parquet","deletionTimestamp":1770000000000,"dataChange":true}}
{"cdc":{"path":"_change_data/cdc-000.parquet","size":12,"dataChange":false}}
"#;
    fs::write(&path, log).expect("write blocked delta fixture");
    path
}

fn write_malformed_delta_protocol_fixture() -> PathBuf {
    let path = temp_path("delta-malformed-protocol-log", "json");
    let log = r#"{"protocol":{"minReaderVersion":1}}
{"metaData":{"id":"delta-malformed-protocol-fixture","name":"orders","schemaString":"{}","partitionColumns":[],"configuration":{}}}
"#;
    fs::write(&path, log).expect("write malformed delta fixture");
    path
}

fn write_hudi_safe_timeline_fixture() -> (PathBuf, PathBuf) {
    let timeline = temp_dir("hudi-safe-timeline");
    write_empty_file(&timeline.join("20260101010101000.commit"));
    write_empty_file(&timeline.join("20260102020202000.commit"));
    let metadata = temp_path("hudi-safe-metadata", "json");
    fs::write(
        &metadata,
        r#"{
  "metadataTableEnabled": true,
  "partitions": ["files", "column_stats", "record_index"],
  "filesCount": 4,
  "columnStatsFileCount": 2,
  "recordIndexFileCount": 1
}"#,
    )
    .expect("write safe hudi metadata summary");
    (timeline, metadata)
}

fn write_hudi_blocked_timeline_fixture() -> (PathBuf, PathBuf) {
    let timeline = temp_dir("hudi-blocked-timeline");
    write_empty_file(&timeline.join("20260101010101000.commit.requested"));
    write_empty_file(&timeline.join("20260102020202000.deltacommit"));
    write_empty_file(&timeline.join("20260103030303000.compaction.inflight"));
    let metadata = temp_path("hudi-blocked-metadata", "json");
    fs::write(
        &metadata,
        r#"{
  "metadataTableEnabled": true,
  "partitions": ["files", "mystery_partition"],
  "filesCount": 4
}"#,
    )
    .expect("write blocked hudi metadata summary");
    (timeline, metadata)
}

fn write_empty_file(path: &Path) {
    fs::write(path, "").expect("write empty timeline file");
}

fn write_non_empty_file(path: &Path) {
    fs::write(path, "marker-body-not-read").expect("write non-empty timeline marker");
}

#[test]
fn delta_log_metadata_read_smoke_exposes_safe_local_log_summary() {
    let path = write_safe_delta_log_fixture();
    let path_arg = path.to_string_lossy().to_string();
    let args = vec![
        "delta-log-metadata-read-smoke".to_string(),
        path_arg.clone(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
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

    assert!(stdout.contains("\"command\":\"delta-log-metadata-read-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("mode", "delta_log_metadata_read_smoke")));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.delta_log_metadata_read_smoke.v1"
    )));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.delta_transaction_log_metadata_smoke"
    )));
    assert!(stdout.contains(&field("phase_id", "PROD-READY-1C")));
    assert!(stdout.contains(&field("support_status", "runtime_supported")));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_delta_transaction_log_metadata_smoke_only"
    )));
    assert!(stdout.contains(&field("source_protocol", "delta_transaction_log_protocol")));
    assert!(stdout.contains(&field("log_path", &path_arg)));
    assert!(stdout.contains(&field("min_reader_version", "1")));
    assert!(stdout.contains(&field("min_writer_version", "2")));
    assert!(stdout.contains(&field("table_id", "delta-orders-fixture")));
    assert!(stdout.contains(&field("table_name", "orders")));
    assert!(stdout.contains(&field("schema_string_present", "true")));
    assert!(stdout.contains(&field("partition_column_order", "region")));
    assert!(stdout.contains(&field("configuration_key_order", "delta.appendOnly")));
    assert!(stdout.contains(&field("action_line_count", "4")));
    assert!(stdout.contains(&field("add_action_count", "1")));
    assert!(stdout.contains(&field("remove_action_count", "0")));
    assert!(stdout.contains(&field("deletion_vector_action_count", "0")));
    assert!(stdout.contains(&field("unsupported_feature_order", "none")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("lakehouse_claim_allowed", "false")));
}

#[test]
fn delta_log_metadata_read_smoke_blocks_unadmitted_runtime_features() {
    let path = write_blocked_delta_log_fixture();
    let args = vec![
        "delta-log-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("support_status", "unsupported_delta_log_features")));
    assert!(stdout.contains(&field("min_reader_version", "3")));
    assert!(stdout.contains(&field("min_writer_version", "7")));
    assert!(stdout.contains(&field("reader_feature_order", "deletionVectors")));
    assert!(stdout.contains(&field("writer_feature_order", "deletionVectors")));
    assert!(stdout.contains(&field("remove_action_count", "1")));
    assert!(stdout.contains(&field("deletion_vector_action_count", "1")));
    assert!(stdout.contains(&field("cdc_action_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "delta_min_reader_version_gt_1,delta_min_writer_version_gt_2,delta_reader_features_present,delta_writer_features_present,delta_remove_actions_present,delta_deletion_vectors_present,delta_cdc_actions_present"
    )));
    assert!(stdout.contains(&field("runtime_supported", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn delta_log_metadata_read_smoke_blocks_protocol_actions_missing_versions() {
    let path = write_malformed_delta_protocol_fixture();
    let args = vec![
        "delta-log-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("min_reader_version", "1")));
    assert!(stdout.contains(&field("min_writer_version", "none")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "missing_delta_min_writer_version"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn hudi_timeline_metadata_read_smoke_exposes_safe_local_metadata_summary() {
    let (timeline, metadata) = write_hudi_safe_timeline_fixture();
    let timeline_arg = timeline.to_string_lossy().to_string();
    let metadata_arg = metadata.to_string_lossy().to_string();
    let args = vec![
        "hudi-timeline-metadata-read-smoke".to_string(),
        timeline_arg.clone(),
        "--metadata-json".to_string(),
        metadata_arg.clone(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
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

    assert!(stdout.contains("\"command\":\"hudi-timeline-metadata-read-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("mode", "hudi_timeline_metadata_read_smoke")));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.hudi_timeline_metadata_read_smoke.v1"
    )));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.hudi_timeline_metadata_smoke"
    )));
    assert!(stdout.contains(&field("phase_id", "PROD-READY-1C")));
    assert!(stdout.contains(&field("support_status", "runtime_supported")));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_hudi_timeline_metadata_smoke_only"
    )));
    assert!(stdout.contains(&field(
        "source_protocol",
        "apache_hudi_timeline_and_metadata_table"
    )));
    assert!(stdout.contains(&field("timeline_dir", &timeline_arg)));
    assert!(stdout.contains(&field("timeline_entry_count", "2")));
    assert!(stdout.contains(&field("completed_instant_count", "2")));
    assert!(stdout.contains(&field("pending_instant_count", "0")));
    assert!(stdout.contains(&field("commit_action_count", "2")));
    assert!(stdout.contains(&field("metadata_json_path", &metadata_arg)));
    assert!(stdout.contains(&field("metadata_table_summary_json_read_performed", "true")));
    assert!(stdout.contains(&field("metadata_table_enabled", "true")));
    assert!(stdout.contains(&field(
        "metadata_partition_order",
        "files,column_stats,record_index"
    )));
    assert!(stdout.contains(&field("unknown_metadata_partition_count", "0")));
    assert!(stdout.contains(&field("metadata_files_count", "4")));
    assert!(stdout.contains(&field("metadata_column_stats_file_count", "2")));
    assert!(stdout.contains(&field("metadata_record_index_file_count", "1")));
    assert!(stdout.contains(&field("timeline_bytes_read", "0")));
    assert!(stdout.contains(&field("unsupported_feature_order", "none")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains(&field("lakehouse_claim_allowed", "false")));
}

#[test]
fn hudi_timeline_metadata_read_smoke_does_not_count_marker_sizes_as_bytes_read() {
    let timeline = temp_dir("hudi-non-empty-marker-timeline");
    write_non_empty_file(&timeline.join("20260101010101000.commit"));
    let args = vec![
        "hudi-timeline-metadata-read-smoke".to_string(),
        timeline.to_string_lossy().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("timeline_entry_count", "1")));
    assert!(stdout.contains(&field("timeline_bytes_read", "0")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn hudi_timeline_metadata_read_smoke_blocks_pending_and_table_service_paths() {
    let (timeline, metadata) = write_hudi_blocked_timeline_fixture();
    let args = vec![
        "hudi-timeline-metadata-read-smoke".to_string(),
        timeline.to_string_lossy().to_string(),
        "--metadata-json".to_string(),
        metadata.to_string_lossy().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_shardloom_json(&args);
    assert!(
        !output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field(
        "support_status",
        "unsupported_hudi_timeline_features"
    )));
    assert!(stdout.contains(&field("timeline_entry_count", "3")));
    assert!(stdout.contains(&field("requested_instant_count", "1")));
    assert!(stdout.contains(&field("inflight_instant_count", "1")));
    assert!(stdout.contains(&field("pending_instant_count", "2")));
    assert!(stdout.contains(&field("delta_commit_action_count", "1")));
    assert!(stdout.contains(&field("compaction_action_count", "1")));
    assert!(stdout.contains(&field("unknown_metadata_partition_count", "1")));
    assert!(stdout.contains(&field(
        "unknown_metadata_partition_order",
        "mystery_partition"
    )));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "hudi_pending_instants_present,hudi_delta_commit_requires_log_merge,hudi_table_service_actions_present,hudi_unknown_metadata_table_partitions_present"
    )));
    assert!(stdout.contains(&field("runtime_supported", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
