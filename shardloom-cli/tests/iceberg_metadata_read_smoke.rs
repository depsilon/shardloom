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
    temp_iceberg_path(name, "json")
}

fn temp_manifest_list_path(name: &str) -> PathBuf {
    temp_iceberg_path(name, "avro")
}

fn temp_manifest_file_path(name: &str) -> PathBuf {
    temp_iceberg_path(name, "manifest.avro")
}

fn temp_iceberg_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-iceberg-{name}-{}-{nanos}.{extension}",
        std::process::id(),
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

fn write_metadata_evolution_fixture(name: &str) -> PathBuf {
    let path = temp_metadata_path(name);
    let metadata = r#"{
  "format-version": 2,
  "table-uuid": "iceberg-orders-evolution-fixture",
  "location": "file:///warehouse/orders",
  "last-sequence-number": 2,
  "current-schema-id": 2,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {"id": 1, "name": "order_id", "required": true, "type": "long"},
        {"id": 2, "name": "region", "required": false, "type": "string"}
      ]
    },
    {
      "type": "struct",
      "schema-id": 2,
      "fields": [
        {"id": 1, "name": "order_id", "required": true, "type": "long"},
        {"id": 2, "name": "market", "required": false, "type": "string"},
        {"id": 3, "name": "amount", "required": false, "type": "double"}
      ]
    }
  ],
  "default-spec-id": 1,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"}
      ]
    },
    {
      "spec-id": 1,
      "fields": [
        {"source-id": 2, "field-id": 1000, "name": "region", "transform": "identity"},
        {"source-id": 1, "field-id": 1001, "name": "order_bucket", "transform": "bucket[16]"}
      ]
    }
  ],
  "last-partition-id": 1001,
  "default-sort-order-id": 0,
  "sort-orders": [
    {"order-id": 0, "fields": []}
  ],
  "current-snapshot-id": 2002,
  "snapshots": [
    {
      "snapshot-id": 2002,
      "sequence-number": 2,
      "timestamp-ms": 1770000001000,
      "manifest-list": "file:///warehouse/orders/metadata/snap-2002.avro",
      "summary": {"operation": "append", "total-records": "20", "total-data-files": "2"}
    }
  ]
}"#;
    fs::write(&path, metadata).expect("metadata evolution fixture write");
    path
}

fn write_metadata_unsafe_schema_fixture(name: &str) -> PathBuf {
    let path = temp_metadata_path(name);
    let metadata = r#"{
  "format-version": 2,
  "table-uuid": "iceberg-orders-unsafe-schema-fixture",
  "location": "file:///warehouse/orders",
  "last-sequence-number": 2,
  "current-schema-id": 2,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 1,
      "fields": [
        {"id": 1, "name": "order_id", "required": true, "type": "long"},
        {"id": 2, "name": "amount", "required": false, "type": "double"}
      ]
    },
    {
      "type": "struct",
      "schema-id": 2,
      "fields": [
        {"id": 1, "name": "order_id", "required": true, "type": "long"},
        {"id": 2, "name": "amount", "required": false, "type": "string"}
      ]
    }
  ],
  "default-spec-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 1, "field-id": 1000, "name": "order_id", "transform": "identity"}
      ]
    }
  ],
  "last-partition-id": 1000,
  "default-sort-order-id": 0,
  "sort-orders": [
    {"order-id": 0, "fields": []}
  ],
  "current-snapshot-id": 2002,
  "snapshots": [
    {
      "snapshot-id": 2002,
      "sequence-number": 2,
      "timestamp-ms": 1770000001000,
      "manifest-list": "file:///warehouse/orders/metadata/snap-2002.avro",
      "summary": {"operation": "append", "total-records": "20", "total-data-files": "2"}
    }
  ]
}"#;
    fs::write(&path, metadata).expect("unsafe schema fixture write");
    path
}

#[cfg(feature = "universal-format-io")]
fn write_manifest_list_fixture(path: &std::path::Path, include_delete_manifest: bool) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let content = if include_delete_manifest {
        vec![0, 1]
    } else {
        vec![0, 0]
    };
    let added_delete_files = if include_delete_manifest {
        vec![0, 1]
    } else {
        vec![0, 0]
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("manifest_path", DataType::Utf8, false),
        Field::new("manifest_length", DataType::Int64, false),
        Field::new("partition_spec_id", DataType::Int64, false),
        Field::new("content", DataType::Int64, false),
        Field::new("sequence_number", DataType::Int64, false),
        Field::new("min_sequence_number", DataType::Int64, false),
        Field::new("added_snapshot_id", DataType::Int64, false),
        Field::new("added_data_files_count", DataType::Int64, false),
        Field::new("existing_data_files_count", DataType::Int64, false),
        Field::new("deleted_data_files_count", DataType::Int64, false),
        Field::new("added_delete_files_count", DataType::Int64, false),
        Field::new("existing_delete_files_count", DataType::Int64, false),
        Field::new("deleted_delete_files_count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![
                "file:///warehouse/orders/metadata/manifest-a.avro",
                "file:///warehouse/orders/metadata/manifest-b.avro",
            ])),
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(Int64Array::from(vec![0, 0])),
            Arc::new(Int64Array::from(content)),
            Arc::new(Int64Array::from(vec![2, 2])),
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![2002, 2002])),
            Arc::new(Int64Array::from(vec![2, 0])),
            Arc::new(Int64Array::from(vec![0, 1])),
            Arc::new(Int64Array::from(vec![0, 0])),
            Arc::new(Int64Array::from(added_delete_files)),
            Arc::new(Int64Array::from(vec![0, 0])),
            Arc::new(Int64Array::from(vec![0, 0])),
        ],
    )
    .expect("manifest-list record batch");
    let file = File::create(path).expect("create manifest-list avro");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer.write(&batch).expect("write manifest-list batch");
    writer.finish().expect("finish manifest-list writer");
}

#[cfg(feature = "universal-format-io")]
fn write_manifest_file_fixture(path: &std::path::Path, include_deleted_data_file: bool) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let statuses = if include_deleted_data_file {
        vec![1, 2]
    } else {
        vec![1, 0]
    };
    let data_file = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("content", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_path", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![
                "file:///warehouse/orders/data/orders-a.parquet",
                "file:///warehouse/orders/data/orders-b.parquet",
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_format", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["PARQUET", "PARQUET"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("record_count", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_size_in_bytes", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1000, 2000])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("data_file", data_file.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(statuses)),
            Arc::new(Int64Array::from(vec![2002, 2002])),
            data_file,
        ],
    )
    .expect("manifest-file record batch");
    let file = File::create(path).expect("create manifest-file avro");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer.write(&batch).expect("write manifest-file batch");
    writer.finish().expect("finish manifest-file writer");
}

#[cfg(feature = "universal-format-io")]
fn write_manifest_file_scan_fixture(
    path: &std::path::Path,
    first_data_path: &std::path::Path,
    second_data_path: &std::path::Path,
) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let first_size = i64::try_from(
        fs::metadata(first_data_path)
            .expect("first parquet metadata")
            .len(),
    )
    .expect("first parquet size fits i64");
    let second_size = i64::try_from(
        fs::metadata(second_data_path)
            .expect("second parquet metadata")
            .len(),
    )
    .expect("second parquet size fits i64");
    let data_file = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("content", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![0, 0])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_path", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![
                first_data_path.to_string_lossy().to_string(),
                second_data_path.to_string_lossy().to_string(),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_format", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["PARQUET", "PARQUET"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("record_count", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![2, 3])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_size_in_bytes", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![first_size, second_size])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("data_file", data_file.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 0])),
            Arc::new(Int64Array::from(vec![2002, 2002])),
            data_file,
        ],
    )
    .expect("manifest-file scan record batch");
    let file = File::create(path).expect("create manifest-file scan avro");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer
        .write(&batch)
        .expect("write manifest-file scan batch");
    writer.finish().expect("finish manifest-file scan writer");
}

#[cfg(feature = "universal-format-io")]
fn write_manifest_file_remote_data_path_fixture(path: &std::path::Path) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let data_file = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("content", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_path", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![
                "s3://warehouse/orders/data-a.parquet",
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_format", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["PARQUET"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("record_count", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![10])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_size_in_bytes", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1000])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("data_file", data_file.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2002])),
            data_file,
        ],
    )
    .expect("manifest-file remote record batch");
    let file = File::create(path).expect("create manifest-file remote avro");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer
        .write(&batch)
        .expect("write manifest-file remote batch");
    writer.finish().expect("finish manifest-file remote writer");
}

#[cfg(feature = "universal-format-io")]
fn write_iceberg_parquet_data_file(
    path: &std::path::Path,
    order_ids: Vec<i64>,
    regions: Vec<&str>,
    amounts: Vec<f64>,
) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(order_ids)),
            Arc::new(StringArray::from(regions)),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .expect("iceberg data record batch");
    let file = File::create(path).expect("create iceberg parquet data file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
    writer.write(&batch).expect("write iceberg parquet batch");
    writer.close().expect("close iceberg parquet writer");
}

#[cfg(feature = "universal-format-io")]
fn write_manifest_file_delete_semantics_fixture(path: &std::path::Path) {
    use std::{fs::File, sync::Arc};

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow_avro::writer::AvroWriter;
    use arrow_schema::{DataType, Field, Schema};

    let data_file = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("content", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1, 2, 1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_path", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![
                "file:///warehouse/orders/deletes/pos-a.parquet",
                "file:///warehouse/orders/deletes/eq-a.parquet",
                "file:///warehouse/orders/deletes/dv-a.puffin",
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_format", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["PARQUET", "PARQUET", "PUFFIN"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("record_count", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![3, 4, 5])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("file_size_in_bytes", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![300, 400, 500])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("referenced_data_file", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![
                None,
                None,
                Some("file:///warehouse/orders/data/orders-a.parquet"),
            ])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("content_offset", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![None, None, Some(64)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("content_size_in_bytes", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![None, None, Some(4096)])) as ArrayRef,
        ),
    ])) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("status", DataType::Int64, false),
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("data_file", data_file.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            Arc::new(Int64Array::from(vec![2002, 2002, 2002])),
            data_file,
        ],
    )
    .expect("manifest-file delete record batch");
    let file = File::create(path).expect("create manifest-file delete avro");
    let mut writer = AvroWriter::new(file, schema.as_ref().clone()).expect("avro writer");
    writer
        .write(&batch)
        .expect("write manifest-file delete batch");
    writer.finish().expect("finish manifest-file delete writer");
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
fn iceberg_metadata_read_smoke_reports_safe_schema_partition_evolution() {
    let path = write_metadata_evolution_fixture("safe-evolution");
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
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
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains(&field("schema_count", "2")));
    assert!(stdout.contains(&field("schema_evolution_present", "true")));
    assert!(stdout.contains(&field("schema_id_order", "1,2")));
    assert!(stdout.contains(&field("schema_added_field_id_count", "1")));
    assert!(stdout.contains(&field("schema_renamed_field_id_count", "1")));
    assert!(stdout.contains(&field(
        "schema_evolution_admission_status",
        "metadata_only_id_based_schema_evolution_admitted_no_data_projection"
    )));
    assert!(stdout.contains(&field("partition_spec_count", "2")));
    assert!(stdout.contains(&field("partition_evolution_present", "true")));
    assert!(stdout.contains(&field("partition_spec_id_order", "0,1")));
    assert!(stdout.contains(&field("partition_added_field_count", "1")));
    assert!(stdout.contains(&field(
        "partition_evolution_admission_status",
        "metadata_only_partition_evolution_admitted_no_filter_execution"
    )));
    assert!(stdout.contains(&field("unsupported_feature_order", "none")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
}

#[test]
fn iceberg_metadata_read_smoke_blocks_unsafe_schema_evolution_without_fallback() {
    let path = write_metadata_unsafe_schema_fixture("unsafe-schema-evolution");
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
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("schema_type_changed_field_id_count", "1")));
    assert!(stdout.contains(&field(
        "schema_evolution_admission_status",
        "blocked_requires_schema_projection_semantics"
    )));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "schema_evolution_projection_required"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
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

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn iceberg_metadata_read_smoke_blocks_manifest_list_without_feature() {
    let path = write_metadata_fixture("manifest-list-feature-disabled", 0);
    let manifest_list_path = temp_manifest_list_path("manifest-list-feature-disabled");
    let manifest_list_arg = manifest_list_path.to_string_lossy().to_string();
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest-list".to_string(),
        manifest_list_arg.clone(),
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
    assert!(stdout.contains(&field("manifest_list_requested", "true")));
    assert!(stdout.contains(&field("manifest_list_reader_feature_enabled", "false")));
    assert!(stdout.contains(&field("manifest_list_path", &manifest_list_arg)));
    assert!(stdout.contains(&field("manifest_list_read_performed", "false")));
    assert!(stdout.contains(&field("unsupported_feature_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "manifest_list_reader_feature_disabled"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn iceberg_metadata_read_smoke_blocks_manifest_file_without_feature() {
    let path = write_metadata_fixture("manifest-file-feature-disabled", 0);
    let manifest_file_path = temp_manifest_file_path("manifest-file-feature-disabled");
    let manifest_file_arg = manifest_file_path.to_string_lossy().to_string();
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_arg.clone(),
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
    assert!(stdout.contains(&field("manifest_file_requested", "true")));
    assert!(stdout.contains(&field("manifest_file_reader_feature_enabled", "false")));
    assert!(stdout.contains(&field("manifest_file_path", &manifest_file_arg)));
    assert!(stdout.contains(&field("manifest_file_read_performed", "false")));
    assert!(stdout.contains(&field("unsupported_feature_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "manifest_file_reader_feature_disabled"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(not(feature = "universal-format-io"))]
#[test]
fn iceberg_metadata_read_smoke_blocks_data_file_scan_without_feature() {
    let path = write_metadata_fixture("data-file-scan-feature-disabled", 0);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--execute-data-file-scan".to_string(),
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
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("data_file_scan_requested", "true")));
    assert!(stdout.contains(&field("data_file_scan_reader_feature_enabled", "false")));
    assert!(stdout.contains(&field("data_file_scan_execution_performed", "false")));
    assert!(stdout.contains(&field("data_file_read_performed", "false")));
    assert!(stdout.contains("data_file_scan_reader_feature_disabled"));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_reads_manifest_list_summary_with_feature() {
    let path = write_metadata_fixture("manifest-list-summary", 0);
    let manifest_list_path = temp_manifest_list_path("manifest-list-summary");
    write_manifest_list_fixture(&manifest_list_path, false);
    let manifest_list_arg = manifest_list_path.to_string_lossy().to_string();
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest-list".to_string(),
        manifest_list_arg.clone(),
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

    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.iceberg_manifest_list_summary_smoke"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_iceberg_metadata_manifest_list_summary_smoke"
    )));
    assert!(stdout.contains(&field("manifest_list_requested", "true")));
    assert!(stdout.contains(&field("manifest_list_reader_feature_enabled", "true")));
    assert!(stdout.contains(&field("manifest_list_path", &manifest_list_arg)));
    assert!(stdout.contains(&field("manifest_list_read_performed", "true")));
    assert!(stdout.contains(&field("manifest_list_entry_count", "2")));
    assert!(stdout.contains(&field("manifest_list_data_manifest_count", "2")));
    assert!(stdout.contains(&field("manifest_list_delete_manifest_count", "0")));
    assert!(stdout.contains(&field("manifest_list_total_manifest_bytes", "300")));
    assert!(stdout.contains(&field("manifest_list_added_data_file_count", "2")));
    assert!(stdout.contains(&field("manifest_list_existing_data_file_count", "1")));
    assert!(stdout.contains(&field("manifest_summary_pruning_performed", "true")));
    assert!(stdout.contains(&field("manifest_split_planning_performed", "true")));
    assert!(stdout.contains(&field("planned_manifest_split_count", "2")));
    assert!(stdout.contains(&field("planned_data_file_count", "3")));
    assert!(stdout.contains(&field("manifest_file_read_performed", "false")));
    assert!(stdout.contains(&field("data_file_read_performed", "false")));
    assert!(stdout.contains(&field(
        "side_effect_free_except_declared_local_table_reads",
        "true"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_reads_manifest_file_split_plan_with_feature() {
    let path = write_metadata_fixture("manifest-file-split-plan", 0);
    let manifest_list_path = temp_manifest_list_path("manifest-file-split-plan-list");
    let manifest_file_path = temp_manifest_file_path("manifest-file-split-plan");
    write_manifest_list_fixture(&manifest_list_path, false);
    write_manifest_file_fixture(&manifest_file_path, false);
    let manifest_file_arg = manifest_file_path.to_string_lossy().to_string();
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest-list".to_string(),
        manifest_list_path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_arg.clone(),
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

    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.iceberg_manifest_file_split_plan_smoke"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_iceberg_manifest_file_split_plan_smoke"
    )));
    assert!(stdout.contains(&field("manifest_list_read_performed", "true")));
    assert!(stdout.contains(&field("manifest_file_requested", "true")));
    assert!(stdout.contains(&field("manifest_file_reader_feature_enabled", "true")));
    assert!(stdout.contains(&field("manifest_file_path", &manifest_file_arg)));
    assert!(stdout.contains(&field("manifest_file_read_performed", "true")));
    assert!(stdout.contains(&field("manifest_file_entry_count", "2")));
    assert!(stdout.contains(&field("manifest_file_added_data_file_count", "1")));
    assert!(stdout.contains(&field("manifest_file_existing_data_file_count", "1")));
    assert!(stdout.contains(&field("manifest_file_deleted_data_file_count", "0")));
    assert!(stdout.contains(&field("manifest_file_total_record_count", "30")));
    assert!(stdout.contains(&field("manifest_file_total_file_size_bytes", "3000")));
    assert!(stdout.contains(&field("data_file_split_planning_performed", "true")));
    assert!(stdout.contains(&field("planned_data_file_split_count", "2")));
    assert!(stdout.contains(&field("planned_data_file_split_bytes", "3000")));
    assert!(stdout.contains(&field("data_file_read_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_executes_declared_local_data_file_scan() {
    let path = write_metadata_fixture("local-data-file-scan", 0);
    let first_data_path = temp_iceberg_path("local-data-a", "parquet");
    let second_data_path = temp_iceberg_path("local-data-b", "parquet");
    write_iceberg_parquet_data_file(
        &first_data_path,
        vec![1, 2],
        vec!["east", "west"],
        vec![10.5, 20.25],
    );
    write_iceberg_parquet_data_file(
        &second_data_path,
        vec![3, 4, 5],
        vec!["east", "north", "south"],
        vec![30.0, 40.0, 50.0],
    );
    let manifest_file_path = temp_manifest_file_path("local-data-file-scan");
    write_manifest_file_scan_fixture(&manifest_file_path, &first_data_path, &second_data_path);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_path.to_string_lossy().to_string(),
        "--execute-data-file-scan".to_string(),
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

    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "report_id",
        "prod-ready-1c.iceberg_local_data_file_scan_smoke"
    )));
    assert!(stdout.contains(&field(
        "claim_gate_status",
        "scoped_iceberg_local_data_file_scan_smoke"
    )));
    assert!(stdout.contains(&field("data_file_scan_requested", "true")));
    assert!(stdout.contains(&field("data_file_scan_reader_feature_enabled", "true")));
    assert!(stdout.contains(&field("data_file_scan_execution_performed", "true")));
    assert!(stdout.contains(&field("data_file_scan_support_status", "runtime_supported")));
    assert!(stdout.contains(&field(
        "data_file_scan_provider_decision",
        "implement_shardloom_kernel"
    )));
    assert!(stdout.contains(&field(
        "data_file_scan_provider_kind",
        "compatibility_import"
    )));
    assert!(stdout.contains(&field(
        "data_file_scan_native_io_certificate_status",
        "certified_local_iceberg_parquet_data_file_scan"
    )));
    assert!(stdout.contains(&field("data_file_scan_split_count", "2")));
    assert!(stdout.contains(&field("data_file_scan_files_read_count", "2")));
    assert!(stdout.contains(&field("data_file_scan_manifest_record_count", "5")));
    assert!(stdout.contains(&field("data_file_scan_actual_row_count", "5")));
    assert!(stdout.contains(&field(
        "data_file_scan_schema_projection_columns",
        "order_id,region,amount"
    )));
    assert!(stdout.contains(&field("data_file_read_performed", "true")));
    assert!(stdout.contains(&field("unsupported_feature_order", "none")));
    assert!(stdout.contains(&field(
        "side_effect_free_except_declared_local_table_reads",
        "true"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
    assert!(!stdout.contains("\"feature\":\"data_file_scan\""));

    fs::remove_file(first_data_path).expect("remove first data file");
    fs::remove_file(second_data_path).expect("remove second data file");
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_blocks_remote_data_file_scan_without_fallback() {
    let path = write_metadata_fixture("remote-data-file-scan", 0);
    let manifest_file_path = temp_manifest_file_path("remote-data-file-scan");
    write_manifest_file_remote_data_path_fixture(&manifest_file_path);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_path.to_string_lossy().to_string(),
        "--execute-data-file-scan".to_string(),
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
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("planned_data_file_non_local_path_count", "1")));
    assert!(stdout.contains(&field("data_file_scan_requested", "true")));
    assert!(stdout.contains(&field("data_file_scan_execution_performed", "false")));
    assert!(stdout.contains(&field("data_file_read_performed", "false")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "remote_data_file_paths_present"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_blocks_delete_manifest_summary_without_fallback() {
    let path = write_metadata_fixture("delete-manifest-summary", 0);
    let manifest_list_path = temp_manifest_list_path("delete-manifest-summary");
    write_manifest_list_fixture(&manifest_list_path, true);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest-list".to_string(),
        manifest_list_path.to_string_lossy().to_string(),
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
    assert!(stdout.contains(&field("manifest_list_read_performed", "true")));
    assert!(stdout.contains(&field("manifest_list_data_manifest_count", "1")));
    assert!(stdout.contains(&field("manifest_list_delete_manifest_count", "1")));
    assert!(stdout.contains(&field("manifest_list_added_delete_file_count", "1")));
    assert!(stdout.contains(&field("unsupported_feature_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "delete_manifests_present"
    )));
    assert!(stdout.contains(&field(
        "delete_tombstone_deletion_vector_admission_status",
        "delete_manifests_or_delete_files_blocked"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_blocks_deleted_manifest_file_entries_without_fallback() {
    let path = write_metadata_fixture("deleted-manifest-file-entry", 0);
    let manifest_file_path = temp_manifest_file_path("deleted-manifest-file-entry");
    write_manifest_file_fixture(&manifest_file_path, true);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_path.to_string_lossy().to_string(),
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
    assert!(stdout.contains(&field("manifest_file_read_performed", "true")));
    assert!(stdout.contains(&field("manifest_file_added_data_file_count", "1")));
    assert!(stdout.contains(&field("manifest_file_deleted_data_file_count", "1")));
    assert!(stdout.contains(&field("planned_data_file_split_count", "1")));
    assert!(stdout.contains(&field("unsupported_feature_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "deleted_data_file_entries_present"
    )));
    assert!(stdout.contains(&field(
        "delete_tombstone_deletion_vector_admission_status",
        "deleted_data_file_entries_blocked"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "universal-format-io")]
#[test]
fn iceberg_metadata_read_smoke_classifies_delete_file_types_without_fallback() {
    let path = write_metadata_fixture("delete-file-types", 0);
    let manifest_file_path = temp_manifest_file_path("delete-file-types");
    write_manifest_file_delete_semantics_fixture(&manifest_file_path);
    let args = vec![
        "iceberg-metadata-read-smoke".to_string(),
        path.to_string_lossy().to_string(),
        "--manifest".to_string(),
        manifest_file_path.to_string_lossy().to_string(),
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
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");

    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("manifest_file_delete_file_entry_count", "3")));
    assert!(stdout.contains(&field(
        "manifest_file_position_delete_file_entry_count",
        "1"
    )));
    assert!(stdout.contains(&field(
        "manifest_file_equality_delete_file_entry_count",
        "1"
    )));
    assert!(stdout.contains(&field("manifest_file_deletion_vector_entry_count", "1")));
    assert!(stdout.contains(&field(
        "delete_tombstone_deletion_vector_admission_status",
        "deletion_vectors_blocked_requires_puffin_vector_application"
    )));
    assert!(stdout.contains(&field(
        "delete_manifest_file_position_delete_file_count",
        "1"
    )));
    assert!(stdout.contains(&field(
        "delete_manifest_file_equality_delete_file_count",
        "1"
    )));
    assert!(stdout.contains(&field("delete_manifest_file_deletion_vector_count", "1")));
    assert!(stdout.contains(&field(
        "unsupported_feature_order",
        "deletion_vector_entries_present,position_delete_file_entries_present,equality_delete_file_entries_present"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
