use std::{
    fmt::Write as _,
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "universal-format-io")]
use std::{fs::File, sync::Arc};

fn unique_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{nanos}.{extension}",
        std::process::id()
    ))
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
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

#[test]
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
    assert!(stdout.contains(&field("source_format", "csv")));
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
    assert!(stdout.contains(&field("source_format", "parquet")));
    assert!(stdout.contains(&field("source_adapter_id", "local_parquet_input_adapter")));
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

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output jsonl");
}

#[test]
fn sql_local_source_smoke_writes_local_csv_output_with_certificate_fields() {
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
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("write_io", "true")));
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_csv_sink"
    )));
    assert!(stdout.contains(&field(
        "output_certificate_ref",
        "sql-local-source.csv.local-csv-output.native-io.v1"
    )));
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains(&field("object_store_io", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));

    fs::remove_file(source_path).expect("remove source csv");
    fs::remove_file(output_path).expect("remove output csv");
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
    assert!(stdout.contains("SQL local-source CSV output requires --output <local.csv>"));
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
    assert!(stdout.contains(&field("sort_operator_family", "single_key_numeric_topn")));
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
    assert!(stdout.contains(&field("source_format", "json")));
    assert!(stdout.contains(&field("source_kind", "local_non_vortex_file")));
    assert!(stdout.contains(&field("source_adapter_id", "local_json_input_adapter")));
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
    fs::write(&fact_path, "id,customer_id,amount\n1,10,8\n2,20,15\n").expect("write fact csv");
    fs::write(&dim_path, "customer_id,segment\n10,seed\n20,enterprise\n").expect("write dim csv");

    let cases = [
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f LEFT JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "outer/cross/semi/anti joins remain blocked",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f SEMI JOIN '{}' AS d ON f.customer_id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "outer/cross/semi/anti joins remain blocked",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id <> d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke admits equi-join ON predicates only",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id = d.customer_id AND f.id = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke ON clause must be",
        ),
        (
            format!(
                "SELECT f.id,d.segment FROM '{}' AS f JOIN '{}' AS d ON f.customer_id + 1 = d.customer_id WHERE f.amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke ON clause must be",
        ),
        (
            format!(
                "SELECT id,segment FROM '{}' JOIN '{}' ON customer_id = customer_id WHERE amount >= 0 LIMIT 10",
                fact_path.display(),
                dim_path.display()
            ),
            "JOIN smoke requires left source syntax",
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
        "id,label,amount\n1,alpha,8\n2,beta,\n3,gamma,21\n",
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
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY label DESC LIMIT 2",
        source_path.display()
    );
    let string_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !string_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&string_output.stdout),
        String::from_utf8_lossy(&string_output.stderr)
    );
    let stdout = String::from_utf8(string_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("ORDER BY top-N smoke admits numeric sort columns only"));
    assert!(stdout.contains("external_engine_invoked=false"));

    let statement = format!(
        "SELECT id,label FROM '{}' WHERE id >= 1 ORDER BY amount,label LIMIT 2",
        source_path.display()
    );
    let multi_key_output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["sql-local-source-smoke", &statement, "--format", "json"])
        .output()
        .expect("sql-local-source-smoke command runs");
    assert!(
        !multi_key_output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&multi_key_output.stdout),
        String::from_utf8_lossy(&multi_key_output.stderr)
    );
    let stdout = String::from_utf8(multi_key_output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("ORDER BY top-N smoke admits exactly one sort key"));

    fs::remove_file(source_path).expect("remove source csv");
}

#[test]
fn sql_local_source_smoke_blocks_unsupported_like_shapes_without_fallback() {
    let source_path = unique_path("sql-local-source-like-blocked", "csv");
    fs::write(&source_path, "id,label\n1,alpha\n2,beta\n").expect("write source csv");

    let cases = [
        (
            format!(
                "SELECT id FROM '{}' WHERE label LIKE 'a_ph%' LIMIT 10",
                source_path.display()
            ),
            "LIKE '_' wildcards are not admitted",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE label LIKE 'alpha' LIMIT 10",
                source_path.display()
            ),
            "use = for exact string equality",
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
            "IN predicates require at least one non-null literal value",
        ),
        (
            format!(
                "SELECT id FROM '{}' WHERE label IN ('alpha', NULL) LIMIT 10",
                source_path.display()
            ),
            "IN predicates do not admit NULL list values",
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
fn sql_local_source_smoke_blocks_nested_jsonl_values_without_fallback() {
    let source_path = unique_path("sql-local-source-jsonl-nested-blocked", "jsonl");
    fs::write(&source_path, "{\"id\":1,\"payload\":{\"x\":1}}\n").expect("write source jsonl");

    let statement = format!(
        "SELECT id FROM '{}' WHERE id >= 1 LIMIT 10",
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
    assert!(
        stdout.contains(
            "local CSV, JSONL/NDJSON, flat JSON, and feature-gated Parquet file paths only"
        )
    );
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
}
