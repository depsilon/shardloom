use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

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
    assert!(stdout.contains("local CSV file paths only"));
    assert!(stdout.contains("no fallback execution was attempted"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
}
