use std::process::Command;

fn run_input_adapters_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["input-adapters", "--format", "json"])
        .output()
        .expect("input-adapters command runs");

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

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_input_plan_json(uri: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["input-plan", uri, "--format", "json"])
        .output()
        .expect("input-plan command runs");

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

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn input_adapters_json_exposes_common_universal_io_formats() {
    let output = run_input_adapters_json();

    assert!(output.contains("\"command\":\"input-adapters\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "input_adapters")));
    assert!(output.contains(&field(
        "adapter_order",
        "native_vortex,parquet,arrow_ipc,csv,jsonl,iceberg_compatible,delta_compatible,avro,orc,hive_partition_discovery,table_snapshot_import_export,schema_evolution_adapter,local_filesystem,s3_compatible,gcs,azure_blob_adls,http_range,local_catalog,hive_compatible_catalog,iceberg_rest_compatible_catalog,glue_like_catalog,nessie_like_catalog,sqlite,postgres_mysql,jdbc_odbc,snowflake,bigquery,databricks_sql,unstructured_text,document,image,audio,video,binary_blob,api,llm,embeddings,vector_index"
    )));
    assert!(output.contains(&field(
        "common_structured_adapter_order",
        "native_vortex,parquet,arrow_ipc,csv,jsonl,avro,orc"
    )));
    assert!(output.contains(&field(
        "critical_structured_adapter_order",
        "native_vortex,parquet,arrow_ipc,csv,jsonl,iceberg_compatible,delta_compatible"
    )));
    assert!(output.contains(&field(
        "lakehouse_adapter_order",
        "iceberg_compatible,delta_compatible,hive_partition_discovery,table_snapshot_import_export,schema_evolution_adapter"
    )));
    assert!(output.contains(&field(
        "object_store_adapter_order",
        "local_filesystem,s3_compatible,gcs,azure_blob_adls,http_range"
    )));
    assert!(output.contains(&field(
        "catalog_adapter_order",
        "local_catalog,hive_compatible_catalog,iceberg_rest_compatible_catalog,glue_like_catalog,nessie_like_catalog"
    )));
    assert!(output.contains(&field(
        "database_adapter_order",
        "sqlite,postgres_mysql,jdbc_odbc,snowflake,bigquery,databricks_sql"
    )));
    assert!(output.contains(&field(
        "unstructured_adapter_order",
        "unstructured_text,document,image,audio,video,binary_blob"
    )));
    assert!(output.contains(&field("adapter_count", "38")));
    assert!(output.contains(&field("supported_adapter_count", "1")));
    assert!(output.contains(&field("planned_adapter_count", "22")));
    assert!(output.contains(&field("explicit_enablement_adapter_count", "10")));
}

#[test]
fn input_adapters_json_keeps_format_statuses_planned_and_no_fallback() {
    let output = run_input_adapters_json();

    assert!(output.contains(&field("native_vortex_status", "planned")));
    assert!(output.contains(&field("parquet_status", "planned")));
    assert!(output.contains(&field("arrow_ipc_status", "planned")));
    assert!(output.contains(&field("csv_status", "planned")));
    assert!(output.contains(&field("jsonl_status", "planned")));
    assert!(output.contains(&field("avro_status", "planned")));
    assert!(output.contains(&field("orc_status", "planned")));
    assert!(output.contains(&field("iceberg_compatible_status", "planned")));
    assert!(output.contains(&field("delta_compatible_status", "planned")));
    assert!(output.contains(&field("s3_compatible_status", "planned")));
    assert!(output.contains(&field("hive_compatible_catalog_status", "planned")));
    assert!(output.contains(&field("sqlite_status", "supported")));
    assert!(output.contains(&field("postgres_mysql_status", "requires_credentials")));
    assert!(output.contains(&field("jdbc_odbc_status", "requires_credentials")));
    assert!(output.contains(&field("snowflake_status", "requires_credentials")));
    assert!(output.contains(&field("bigquery_status", "requires_credentials")));
    assert!(output.contains(&field("databricks_sql_status", "requires_credentials")));
    assert!(output.contains(&field(
        "unstructured_text_status",
        "requires_explicit_enablement"
    )));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn input_plan_json_recognizes_common_structured_formats_without_reading() {
    let parquet = run_input_plan_json("s3://bucket/fact.parquet");
    assert!(parquet.contains(&field("source_kind", "parquet")));
    assert!(parquet.contains(&field("dataset_format", "parquet")));
    assert!(parquet.contains(&field("uri_scheme", "s3")));
    assert!(parquet.contains(&field("capability_status", "planned")));
    assert!(parquet.contains(&field("compatibility_structured", "true")));
    assert!(parquet.contains(&field("data_read", "false")));
    assert!(parquet.contains("\"artifact_kind\":\"source_report\""));
    assert!(parquet.contains("\"artifact_id\":\"input-plan.source\""));

    let avro = run_input_plan_json("file://tmp/events.avro");
    assert!(avro.contains(&field("source_kind", "avro")));
    assert!(avro.contains(&field("dataset_format", "avro")));
    assert!(avro.contains(&field("capability_status", "planned")));

    let orc = run_input_plan_json("file://tmp/events.orc");
    assert!(orc.contains(&field("source_kind", "orc")));
    assert!(orc.contains(&field("dataset_format", "orc")));
    assert!(orc.contains(&field("capability_status", "planned")));
    assert!(orc.contains(&field("fallback_execution_allowed", "false")));
}
