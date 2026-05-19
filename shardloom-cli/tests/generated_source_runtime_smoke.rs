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

#[test]
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
    assert!(stdout.contains(&field(
        "output_native_io_certificate_status",
        "certified_local_file_sink"
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
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"fnv64:"));

    fs::remove_file(output_path).expect("remove output jsonl");
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
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"fnv64:"));

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
    assert!(stdout.contains("\"output_digest\",\"value\":\"fnv64:"));
    assert!(stdout.contains("\"correctness_digest\",\"value\":\"fnv64:"));

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
            "generated-sql-generate-series-project",
            "SELECT value FROM generate_series(1, 5)",
            "does not admit FROM clauses",
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
