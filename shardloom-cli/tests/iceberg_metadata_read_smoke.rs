use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

fn run_iceberg_metadata_read_smoke_json(args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("iceberg-metadata-read-smoke command runs")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn temp_metadata_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-iceberg-metadata-{name}-{}-{nanos}.json",
        std::process::id()
    ))
}

fn write_metadata_fixture(name: &str, delete_files: u64) -> PathBuf {
    let path = temp_metadata_path(name);
    let metadata = format!(
        r#"{{
  "format-version": 2,
  "table-uuid": "iceberg-orders-fixture",
  "location": "file:///warehouse/orders",
  "last-sequence-number": 2,
  "current-schema-id": 1,
  "schemas": [
    {{
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {{"id": 1, "name": "order_id", "required": true, "type": "long"}},
        {{"id": 2, "name": "region", "required": false, "type": "string"}},
        {{"id": 3, "name": "amount", "required": false, "type": "double"}}
      ]
    }}
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {{
      "spec-id": 0,
      "fields": [
        {{"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"}}
      ]
    }}
  ],
  "last-partition-id": 1000,
  "default-sort-order-id": 0,
  "sort-orders": [
    {{"order-id": 0, "fields": []}}
  ],
  "current-snapshot-id": 2002,
  "snapshots": [
    {{
      "snapshot-id": 2001,
      "sequence-number": 1,
      "timestamp-ms": 1770000000000,
      "manifest-list": "file:///warehouse/orders/metadata/snap-2001.avro",
      "summary": {{"operation": "append", "total-records": "10", "total-data-files": "1"}}
    }},
    {{
      "snapshot-id": 2002,
      "sequence-number": 2,
      "timestamp-ms": 1770000001000,
      "manifest-list": "file:///warehouse/orders/metadata/snap-2002.avro",
      "summary": {{
        "operation": "append",
        "total-records": "20",
        "total-data-files": "2",
        "total-delete-files": "{delete_files}"
      }}
    }}
  ]
}}"#
    );
    fs::write(&path, metadata).expect("metadata fixture write");
    path
}

#[test]
fn iceberg_metadata_read_smoke_exposes_scoped_metadata_summary() {
    let path = write_metadata_fixture("summary", 0);
    let path_arg = path.to_string_lossy().to_string();
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path_arg.clone(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_iceberg_metadata_read_smoke_json(&args);
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

    assert!(stdout.contains("\"command\":\"iceberg-metadata-read-smoke\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("mode", "iceberg_metadata_read_smoke")));
    assert!(stdout.contains(&field(
        "schema_version",
        "shardloom.iceberg_metadata_read_smoke.v1"
    )));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.iceberg_metadata_json_read_smoke"
    )));
    assert!(stdout.contains(&field("phase_id", "PROD-READY-1C")));
    assert!(stdout.contains(&field("support_status", "runtime_supported")));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_iceberg_metadata_json_smoke_only"
    )));
    assert!(stdout.contains(&field("source_protocol", "apache_iceberg_table_metadata")));
    assert!(stdout.contains(&field("metadata_path", &path_arg)));
    assert!(stdout.contains(&field("format_version", "2")));
    assert!(stdout.contains(&field("table_uuid", "iceberg-orders-fixture")));
    assert!(stdout.contains(&field("table_location", "file:///warehouse/orders")));
    assert!(stdout.contains(&field("current_schema_id", "1")));
    assert!(stdout.contains(&field("schema_count", "1")));
    assert!(stdout.contains(&field("current_schema_field_count", "3")));
    assert!(stdout.contains(&field("schema_field_ids_present", "true")));
    assert!(stdout.contains(&field("partition_spec_count", "1")));
    assert!(stdout.contains(&field("default_partition_spec_id", "0")));
    assert!(stdout.contains(&field("sort_order_count", "1")));
    assert!(stdout.contains(&field("snapshot_count", "2")));
    assert!(stdout.contains(&field("current_snapshot_id", "2002")));
    assert!(stdout.contains(&field("selected_snapshot_id", "2002")));
    assert!(stdout.contains(&field("selected_snapshot_sequence_number", "2")));
    assert!(stdout.contains(&field("selected_snapshot_timestamp_ms", "1770000001000")));
    assert!(stdout.contains(&field("snapshot_selector_kind", "current_snapshot")));
    assert!(stdout.contains(&field("manifest_list_ref_count", "2")));
    assert!(stdout.contains(&field("last_sequence_number", "2")));
    assert!(stdout.contains("\"metadata_summary_digest\",\"value\":\"fnv1a64:"));
}

#[test]
fn iceberg_metadata_read_smoke_selects_explicit_snapshot_and_time_travel() {
    let path = write_metadata_fixture("snapshot-selection", 0);
    let path_arg = path.to_string_lossy().to_string();

    let snapshot_args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path_arg.clone(),
        "--snapshot-id".to_string(),
        "2001".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    let snapshot_output = run_iceberg_metadata_read_smoke_json(&snapshot_args);
    assert!(
        snapshot_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&snapshot_output.stdout),
        String::from_utf8_lossy(&snapshot_output.stderr)
    );
    let snapshot_stdout = String::from_utf8(snapshot_output.stdout).expect("stdout is utf8");
    assert!(snapshot_stdout.contains(&field("selected_snapshot_id", "2001")));
    assert!(snapshot_stdout.contains(&field("snapshot_selector_kind", "snapshot_id")));
    assert!(snapshot_stdout.contains(&field("time_travel_selection_performed", "false")));

    let time_args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path_arg,
        "--as-of-timestamp-ms".to_string(),
        "1770000000500".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    let time_output = run_iceberg_metadata_read_smoke_json(&time_args);
    assert!(
        time_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&time_output.stdout),
        String::from_utf8_lossy(&time_output.stderr)
    );
    let time_stdout = String::from_utf8(time_output.stdout).expect("stdout is utf8");
    assert!(time_stdout.contains(&field("selected_snapshot_id", "2001")));
    assert!(time_stdout.contains(&field("snapshot_selector_kind", "as_of_timestamp_ms")));
    assert!(time_stdout.contains(&field("time_travel_selection_performed", "true")));
}

#[test]
fn iceberg_metadata_read_smoke_blocks_delete_file_runtime_without_fallback() {
    let path = write_metadata_fixture("delete-files", 1);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];

    let output = run_iceberg_metadata_read_smoke_json(&args);
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

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("support_status", "unsupported_metadata_features")));
    assert!(stdout.contains(&field("unsupported_feature_count", "1")));
    assert!(stdout.contains(&field("unsupported_feature_order", "delete_files_present")));
    assert!(stdout.contains(&field("selected_snapshot_delete_file_count", "1")));
    assert!(stdout.contains(&field("runtime_supported", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("fallback_execution_allowed", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(stdout.contains("\"feature\":\"delete_files_present\""));
}
