use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn unique_output_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-{name}-{}-{nanos}.jsonl",
        std::process::id()
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
    assert!(stdout.contains("supports local file output only"));
    assert!(stdout.contains("object-store and remote URI writes remain blocked"));
    assert!(stdout.contains("\"attempted\":false"));
    assert!(stdout.contains("\"allowed\":false"));
    assert!(stdout.contains(&field("command_family", "workflow_planning")));
}
