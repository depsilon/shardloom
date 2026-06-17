use std::process::Command;

#[cfg(feature = "vortex-local-primitives")]
fn local_primitive_struct_fixture() -> String {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("shardloom-vortex")
        .join("tests")
        .join("fixtures")
        .join("local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

fn field(key: &str, value: &str) -> String {
    format!("\"key\":\"{key}\",\"value\":\"{value}\"")
}

fn run_route(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("run shardloom route");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn run_facade(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("run shardloom facade");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

#[test]
fn public_route_blocks_local_file_auto_without_vortex_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv) -> select(id) -> limit(10)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_schema_version",
        "shardloom.public_workflow_route.v1"
    )));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.local_file_vortex_middle_required"
    )));
    assert!(stdout.contains(&field("route_support_status", "unsupported_boundary")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("underlying_runtime_command", "not_resolved")));
    assert!(stdout.contains(&field("local_workflow_runtime_profile", "not_applicable")));
    assert!(stdout.contains(&field("surface", "dataframe")));
    assert!(stdout.contains(&field("source_format", "csv")));
    assert!(stdout.contains(&field("start_state", "blocked")));
    assert!(stdout.contains(&field("vortex_normalization_point", "not_applicable")));
    assert!(stdout.contains(&field("vortex_middle_status", "blocked_or_unsupported")));
    assert!(stdout.contains(&field("execution_mode", "blocked")));
    assert!(stdout.contains(&field("preparation_included", "false")));
    assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_explicit_direct_local_file_policy() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv) -> select(id) -> limit(10)",
        "--request",
        "collect",
        "--execution-policy",
        "direct",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.direct_local_file_blocked")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("underlying_runtime_command", "not_resolved")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_unbounded_collect_before_execution() {
    let stdout = run_route(&[
        "route",
        "python",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.unbounded_collect_blocked")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_does_not_infer_scalar_path_literals_as_sources() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT 'target/input.csv' AS label LIMIT 1",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.input_not_declared")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_ignores_limit_inside_sql_comments() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT id FROM 'target/input.csv' -- LIMIT 1\nWHERE id > 0",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.unbounded_collect_blocked")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_unresolved_newline_from_source_without_declared_input() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--sql",
        "SELECT *\nFROM events",
        "--request",
        "write_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.input_not_declared")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_requires_vortex_input_for_native_vortex_policy() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.native_vortex_input_required"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_collect_fanout_before_execution() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--fanout-output",
        "csv=target/out.csv",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.collect_fanout_blocked")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_native_vortex_write_payloads() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "project",
        "--vortex-columns",
        "metric",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    )));
    assert!(stdout.contains(&field("native_vortex_operation_family", "sink")));
    assert!(stdout.contains(&field(
        "typed_sink_contract",
        "blocked_until_native_vortex_typed_sink_contract"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_blocks_local_sql_auto_without_vortex_middle() {
    let workspace = std::path::Path::new("target/public-workflow-run-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n3,gamma\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &statement,
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(!success);
    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field(
        "public_workflow_facade_schema_version",
        "shardloom.public_workflow_execution_facade.v1"
    )));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
    assert!(stdout.contains(&field(
        "public_workflow_blocker_id",
        "cg21.route.local_file_vortex_middle_required"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "not_resolved"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_underlying_runtime_command",
        "not_resolved"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_workflow_runtime_profile",
        "not_applicable"
    )));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_blocks_extensionless_local_sql_source_but_preserves_declared_format() {
    let workspace = std::path::Path::new("target/public-workflow-extensionless-source");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact");
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write extensionless csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 1", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "sql",
        "--sql",
        &statement,
        "--input-format",
        "csv",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(!success);
    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
    assert!(stdout.contains(&field(
        "public_workflow_blocker_id",
        "cg21.route.local_file_vortex_middle_required"
    )));
    assert!(stdout.contains(&field("public_workflow_source_format", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_blocks_local_write_without_vortex_middle_but_preserves_intent() {
    let workspace = std::path::Path::new("target/public-workflow-write-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let output = workspace.join("out.csv");
    let _ = std::fs::remove_file(&output);
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let (success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--sql",
        &statement,
        "--plan",
        "read_csv(fact.csv) -> select(id,label) -> limit(2)",
        "--request",
        "write_csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(!success);
    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
    assert!(stdout.contains(&field(
        "public_workflow_blocker_id",
        "cg21.route.local_file_vortex_middle_required"
    )));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_csv")));
    assert!(stdout.contains(&field("public_workflow_allow_overwrite", "true")));
    assert!(stdout.contains(&field(
        "public_workflow_output_ref",
        output.to_str().expect("utf8 output path")
    )));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_blocks_local_fanout_without_vortex_middle_but_preserves_intent() {
    let workspace = std::path::Path::new("target/public-workflow-fanout-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let primary = workspace.join("out.jsonl");
    let fanout = workspace.join("out.csv");
    let _ = std::fs::remove_file(&primary);
    let _ = std::fs::remove_file(&fanout);
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let statement = format!("SELECT id,label FROM '{}' LIMIT 2", input.display());
    let fanout_arg = format!("csv={}", fanout.to_str().expect("utf8 fanout path"));
    let (success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--sql",
        &statement,
        "--plan",
        "read_csv(fact.csv) -> select(id,label) -> limit(2)",
        "--request",
        "write_jsonl",
        "--output",
        primary.to_str().expect("utf8 primary path"),
        "--fanout-output",
        &fanout_arg,
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(!success);
    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "not_resolved"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_underlying_runtime_command",
        "not_resolved"
    )));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("public_workflow_fanout_output_count", "1")));
    assert!(stdout.contains(&field("public_workflow_fanout_outputs", &fanout_arg)));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_generated_user_rows_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("user-rows.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "user_rows",
        "--generated-schema",
        "id:int64,label:utf8",
        "--generated-rows",
        "id=1,label=alpha",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_user_rows_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-user-rows-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("public_workflow_allow_overwrite", "true")));
    assert!(stdout.contains(&field("generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field(
        "output_path",
        output.to_str().expect("utf8 output path")
    )));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_forwards_generated_fanout_payload_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-fanout-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let primary = workspace.join("user-rows.jsonl");
    let fanout = workspace.join("user-rows.csv");
    let _ = std::fs::remove_file(&primary);
    let _ = std::fs::remove_file(&fanout);
    let fanout_arg = format!("csv={}", fanout.to_str().expect("utf8 fanout path"));
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        primary.to_str().expect("utf8 primary path"),
        "--fanout-output",
        &fanout_arg,
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "user_rows",
        "--generated-schema",
        "id:int64,label:utf8",
        "--generated-rows",
        "id=1,label=alpha",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_user_rows_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-user-rows-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("public_workflow_fanout_output_count", "1")));
    assert!(stdout.contains(&field("public_workflow_fanout_outputs", &fanout_arg)));
    assert!(stdout.contains(&field("generated_source_kind", "user_rows")));
    assert!(stdout.contains(&field("output_fanout_performed", "true")));
    assert!(stdout.contains(&field("fanout_output_count", "1")));
    assert!(stdout.contains(&field("fanout_output_formats", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_generated_range_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("range.csv");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "range",
        "--generated-range-start",
        "1",
        "--generated-range-end",
        "4",
        "--generated-range-step",
        "1",
        "--generated-range-column",
        "id",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_range_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-range-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "range")));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_csv")));
    assert!(stdout.contains(&field("generated_source_kind", "range")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "4")));
    assert!(stdout.contains(&field("generated_source_range_step", "1")));
    assert!(stdout.contains(&field("generated_source_range_column", "id")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("output_format", "csv")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_generated_sequence_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("sequence.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "python",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--bounded",
        "true",
        "--allow-overwrite",
        "--generated-source-kind",
        "sequence",
        "--generated-range-start",
        "1",
        "--generated-range-end",
        "6",
        "--generated-range-step",
        "2",
        "--generated-range-column",
        "seq",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "generated_sequence_direct_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sequence-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_generated_source_kind", "sequence")));
    assert!(stdout.contains(&field("generated_source_kind", "sequence")));
    assert!(stdout.contains(&field("generated_source_range_start", "1")));
    assert!(stdout.contains(&field("generated_source_range_end", "6")));
    assert!(stdout.contains(&field("generated_source_range_step", "2")));
    assert!(stdout.contains(&field("generated_source_range_column", "seq")));
    assert!(stdout.contains(&field("generated_source_row_count", "3")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_filter_project_payload() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "zero_decode",
        "--bounded",
        "true",
        "--vortex-primitive",
        "filter_project",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric,value",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_filter_project")));
    assert!(stdout.contains(&field("resolved_internal_command", "vortex-filter-project")));
    assert!(stdout.contains(&field("start_state", "native_vortex_file")));
    assert!(stdout.contains(&field("execution_mode", "native_vortex")));
    assert!(stdout.contains(&field("vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("vortex_columns", "metric,value")));
    assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("memory_gb", "1")));
    assert!(stdout.contains(&field("max_parallelism", "2")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_native_vortex_missing_required_payload() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--vortex-primitive",
        "count_where",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field(
        "blocker_id",
        "cg21.route.native_vortex_payload_invalid"
    )));
    assert!(stdout.contains(&field("vortex_primitive", "count_where")));
    assert!(stdout.contains("\"feature\":\"public_workflow_route.vortex_predicate\""));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_filter_project_payload_with_attached_route_envelope() {
    let fixture = local_primitive_struct_fixture();
    let stdout = run_route(&[
        "run",
        "cli",
        "--input",
        fixture.as_str(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--materialization-policy",
        "zero_decode",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "filter_project",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_filter_project"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-filter-project"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("mode", "vortex_filter_project")));
    assert!(stdout.contains(&field("primitive", "filter_and_project")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_projected_columns",
        "metric"
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_source_order_limit_requested",
        "2"
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_source_order_limit_applied",
        "true"
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_fallback_attempted",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_execution_certificate_fallback_attempted",
        "false"
    )));
}

#[test]
fn public_run_executes_source_free_values_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("values.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "sql",
        "--sql",
        "VALUES (1, 'alpha')",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "source_free_generated_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sql-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("generated_source_kind", "sql_values")));
    assert!(stdout.contains(&field("generated_source_row_count", "1")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_source_free_range_sql_with_attached_route_envelope() {
    let workspace = std::path::Path::new("target/public-workflow-generated-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let output = workspace.join("range-sql.jsonl");
    let _ = std::fs::remove_file(&output);
    let stdout = run_route(&[
        "run",
        "sql",
        "--sql",
        "SELECT value AS id FROM range(1, 5, 1) WHERE value >= 2 LIMIT 2",
        "--request",
        "write_jsonl",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "source_free_generated_output"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "generated-source-sql-smoke"
    )));
    assert!(stdout.contains(&field("generated_source_kind", "sql_generate_series_range")));
    assert!(stdout.contains(&field("generated_source_row_count", "2")));
    assert!(stdout.contains(&field("generated_source_sql_generator_function", "range")));
    assert!(stdout.contains(&field("sql_source_free_filter_runtime_execution", "true")));
    assert!(stdout.contains(&field("sql_source_free_limit_runtime_execution", "true")));
    assert!(stdout.contains(&field("output_format", "jsonl")));
    assert!(stdout.contains(&field("runtime_execution", "true")));
    assert!(stdout.contains(&field("output_io_performed", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_prepare_attaches_route_envelope_to_ingest_path_or_gate() {
    let workspace = std::path::Path::new("target/public-workflow-prepare-facade");
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    let output = workspace.join("fact.vortex");
    std::fs::write(&input, "id,label\n1,alpha\n2,beta\n").expect("write csv");
    let (_success, stdout) = run_facade(&[
        "prepare",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--output",
        output.to_str().expect("utf8 output path"),
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"prepare\""));
    assert!(stdout.contains(&field(
        "public_workflow_facade_schema_version",
        "shardloom.public_workflow_execution_facade.v1"
    )));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "local_file_prepare_once"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-ingest-smoke"
    )));
    assert!(stdout.contains(&field("public_workflow_preparation_included", "true")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
