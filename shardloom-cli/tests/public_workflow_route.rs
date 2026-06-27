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

#[cfg(feature = "vortex-local-primitives")]
fn unique_vortex_binding_dir(name: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "shardloom-public-{name}-{}-{nanos}",
        std::process::id()
    ))
}

#[cfg(feature = "vortex-local-primitives")]
fn copy_partitioned_vortex_fixture(name: &str) -> std::path::PathBuf {
    let dir = unique_vortex_binding_dir(name);
    std::fs::create_dir_all(&dir).expect("create partitioned fixture dir");
    let fixture = std::path::PathBuf::from(local_primitive_struct_fixture());
    std::fs::copy(&fixture, dir.join("part-000.vortex")).expect("copy first partition");
    std::fs::copy(&fixture, dir.join("part-001.vortex")).expect("copy second partition");
    dir
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

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_aggregate_emits_state_budget_and_pulseweave_evidence() {
    let fixture = local_primitive_struct_fixture();
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &fixture,
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "aggregate",
        "--vortex-aggregate",
        aggregate,
        "--format",
        "json",
    ]);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_aggregate"
    )));
    assert!(stdout.contains(&field("public_workflow_timing_surface", "hot_runtime")));
    assert!(stdout.contains(&field(
        "public_workflow_actual_evidence_tier",
        "metadata_sink"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_timing_claim_boundary",
        "runtime_route_evidence_only_no_benchmark_or_publication_claim"
    )));
    assert!(stdout.contains(&field("timing_surface", "hot_runtime")));
    assert!(stdout.contains(&field("actual_evidence_tier", "metadata_sink")));
    assert!(stdout.contains(&field("route_total_timing_reported", "false")));
    assert!(stdout.contains(&field(
        "result_sink_timing_included_in_route_total",
        "false"
    )));
    assert!(stdout.contains(&field(
        "evidence_render_timing_included_in_route_total",
        "false"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_budget_schema_version",
        "shardloom.local_vortex_state_budget.v2"
    )));
    assert!(stdout.contains(&field("local_primitive_state_budget_required", "true")));
    assert!(stdout.contains(&field(
        "local_primitive_state_family",
        "scalar_aggregate_state+direct_dictionary_or_typed"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_pressure_class",
        "low_cardinality_pressure"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_state_budget_status",
        "bounded_in_memory_low_pressure_spill_not_required"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_capillary_work_units",
        "vortex_scan,aggregate_state,dictionary_or_typed_direct_scalar_aggregate"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_pulseweave_pressure_signals",
        "aggregate_measure_count,aggregate_input_rows,row_materialization_bypass"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_spill_policy",
        "fail_closed_before_uncertified_spill"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_fail_closed_if_spill_required",
        "true"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_directory_count_uses_partitioned_binding() {
    let dir = copy_partitioned_vortex_fixture("directory-count");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &dir.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_count_all"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_schema_version",
        "shardloom.native_vortex_input_binding.v1"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_mode",
        "local_directory"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_strategy",
        "sequential_capillary_parts"
    )));
    assert!(stdout.contains(&field("native_vortex_partitioned_input_binding", "true")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_selected", "10")));
    assert!(stdout.contains(&field("data_read", "true")));
    assert!(stdout.contains(&field("data_decoded", "false")));
    assert!(stdout.contains(&field("data_materialized", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_directory_count_accepts_vtx_parts() {
    let dir = copy_partitioned_vortex_fixture("directory-count-vtx");
    std::fs::rename(dir.join("part-001.vortex"), dir.join("part-001.vtx"))
        .expect("rename partition to .vtx");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &dir.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_mode",
        "local_directory"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_manifest_aggregate_uses_partitioned_state() {
    let dir = copy_partitioned_vortex_fixture("manifest-aggregate");
    let manifest = dir.join("parts.vortex-manifest");
    std::fs::write(
        &manifest,
        r#"{"inputs":["part-000.vortex","part-001.vortex"]}"#,
    )
    .expect("write manifest");
    let expected_sources = format!(
        "{},{}",
        dir.join("part-000.vortex").display(),
        dir.join("part-001.vortex").display()
    );
    let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &manifest.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "aggregate",
        "--vortex-aggregate",
        aggregate,
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_aggregate"
    )));
    assert!(stdout.contains(&field("native_vortex_input_binding_mode", "manifest")));
    assert!(stdout.contains(&field("native_vortex_input_binding_count", "2")));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_sources",
        &expected_sources
    )));
    assert!(stdout.contains(&field(
        "native_vortex_input_binding_strategy",
        "sequential_capillary_parts"
    )));
    assert!(stdout.contains(&field("native_vortex_partitioned_input_binding", "true")));
    assert!(stdout.contains(&field("local_primitive_rows_scanned", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_selected", "10")));
    assert!(stdout.contains(&field("local_primitive_rows_projected", "1")));
    assert!(stdout.contains(&field(
        "local_primitive_capillary_work_units",
        "partitioned_vortex_source,vortex_scan,aggregate_state"
    )));
    assert!(stdout.contains(&field(
        "local_primitive_pulseweave_pressure_signals",
        "partition_count,aggregate_measure_count,aggregate_input_rows"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_native_vortex_manifest_rejects_duplicate_entries() {
    let dir = copy_partitioned_vortex_fixture("manifest-duplicate");
    let manifest = dir.join("parts.vortex-manifest");
    std::fs::write(
        &manifest,
        r#"{"inputs":["part-000.vortex","part-000.vortex"]}"#,
    )
    .expect("write manifest");
    let (ok, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        &manifest.display().to_string(),
        "--input-format",
        "vortex",
        "--request",
        "collect",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--vortex-primitive",
        "count",
        "--format",
        "json",
    ]);
    let _ = std::fs::remove_dir_all(&dir);

    assert!(!ok, "{stdout}");
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("native Vortex input manifest contains duplicate entry"));
    assert!(stdout.contains("no fallback execution was attempted"));
}

#[test]
fn public_route_routes_local_file_vortex_middle_without_direct_runtime() {
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
    assert!(stdout.contains(&field(
        "public_workflow_route_schema_version",
        "shardloom.public_workflow_route.v1"
    )));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "local_file_prepare_once_first_query")));
        assert!(stdout.contains(&field("route_support_status", "global_runtime_supported")));
        assert!(stdout.contains(&field("native_vortex_plan_contract_status", "admitted")));
        assert!(stdout.contains(&field(
            "native_vortex_plan_route_family",
            "native_vortex_unified_plan"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_plan_payload_kind",
            "prepared_compatibility_source"
        )));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-prepare->vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field(
            "underlying_runtime_command",
            "vortex-prepare->vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field("start_state", "compatibility_local_source")));
        assert!(stdout.contains(&field("vortex_normalization_point", "VortexPreparedState")));
        assert!(stdout.contains(&field("vortex_middle_status", "prepared_vortex_state")));
        assert!(stdout.contains(&field("execution_mode", "prepared_vortex")));
        assert!(stdout.contains(&field("preparation_included", "true")));
        assert!(stdout.contains(&field("query_timing_starts_after_preparation", "true")));
        assert!(stdout.contains(&field("blocker_id", "none")));
        assert!(stdout.contains(&field(
            "local_workflow_runtime_profile",
            "product_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "cg21.route.local_file_vortex_primitive_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("blocker_id", expected_blocker)));
        assert!(stdout.contains(&field(
            "native_vortex_plan_contract_status",
            "blocked_before_execution"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_plan_route_family",
            "native_vortex_unified_plan"
        )));
        assert!(stdout.contains(&field("route_support_status", "unsupported_boundary")));
        assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
        assert!(stdout.contains(&field("underlying_runtime_command", "not_resolved")));
        assert!(stdout.contains(&field("start_state", "blocked")));
        assert!(stdout.contains(&field("vortex_normalization_point", "not_applicable")));
        assert!(stdout.contains(&field("vortex_middle_status", "blocked_or_unsupported")));
        assert!(stdout.contains(&field("execution_mode", "blocked")));
        assert!(stdout.contains(&field("preparation_included", "false")));
        assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
        assert!(stdout.contains(&field("local_workflow_runtime_profile", "not_applicable")));
    }
    assert!(stdout.contains(&field("surface", "dataframe")));
    assert!(stdout.contains(&field("source_format", "csv")));
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
fn public_route_rejects_native_vortex_sql_non_where_limit_tail() {
    let stdout = run_route(&[
        "route",
        "sql",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--sql",
        "SELECT id FROM 'target/fact.vortex' OFFSET 10 LIMIT 2",
        "--request",
        "collect",
        "--execution-policy",
        "native_vortex",
        "--bounded",
        "true",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(!stdout.contains(&field("route_id", "native_vortex_project")));
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
fn public_route_admits_native_vortex_primitive_row_export_payloads() {
    let _ = std::fs::remove_file("target/native-vortex-output.jsonl");
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
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-local-primitive-row-export"
        )));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sink")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
        assert!(stdout.contains(&field(
            "decode_materialization_boundary",
            "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_tail_row_export_payloads() {
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
        "target/native-vortex-tail-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "tail",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "tail")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_native_vortex_tail_collect_without_explicit_count() {
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
        "tail",
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
        "cg21.route.native_vortex_payload_invalid"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sample_row_export_payloads() {
    let stdout = run_route(&[
        "route",
        "cli",
        "--input",
        "shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex",
        "--input-format",
        "vortex",
        "--request",
        "write_csv",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-vortex-sample-output.csv",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sample-seed",
        "7",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "none")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sort_row_export_payloads() {
    let _ = std::fs::remove_file("target/native-vortex-sort-output.jsonl");
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
        "target/native-vortex-sort-output.jsonl",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sort_rows",
        "--vortex-columns",
        "value,metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sort-rows",
        r#"{"order_by":[{"column":"metric","descending":true}]}"#,
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_primitive_row_export")));
        assert!(stdout.contains(&field("vortex_primitive", "sort_rows")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
        assert!(stdout.contains(&field("vortex_sort_rows_present", "true")));
        assert!(stdout.contains(&field(
            "typed_sink_contract",
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_native_vortex_sample_fraction_payloads() {
    let stdout = run_route(&[
        "route",
        "dataframe",
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
        "sample",
        "--vortex-columns",
        "metric",
        "--vortex-sample-fraction",
        "0.5",
        "--vortex-sample-seed",
        "7",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "none")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "0.5")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-production-runtime")]
#[test]
fn public_route_admits_provider_backed_native_vortex_jsonl_result_sink() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> with_column(amount_float,CAST(dirty_numeric AS float64)) -> filter(amount_float >= 0) -> limit(1000)",
        "--request",
        "write_jsonl",
        "--execution-policy",
        "native_vortex",
        "--output",
        "target/native-provider-result.jsonl",
        "--allow-overwrite",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_user_sink")));
    assert!(stdout.contains(&field(
        "resolved_internal_command",
        "vortex-production-runtime-run"
    )));
    assert!(stdout.contains(&field("requested_output", "write_jsonl")));
    assert!(stdout.contains(&field("native_vortex_operation_family", "sink")));
    assert!(stdout.contains(&field(
        "native_vortex_provider_scenario",
        "clean-cast-filter-write"
    )));
    assert!(stdout.contains(&field(
        "typed_sink_contract",
        "native_vortex_provider_result_json_export_with_workspace_safe_sink"
    )));
    assert!(stdout.contains(&field(
        "decode_materialization_boundary",
        "native_vortex_zero_decode_runtime_with_bounded_result_json_sink_materialization"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-production-runtime")]
#[test]
fn public_route_admits_provider_backed_native_vortex_cast_collect_shapes() {
    for (plan, scenario) in [
        (
            "read_vortex(target/fact.vortex) -> with_column(amount_float,CAST(dirty_numeric AS float64)) -> filter(amount_float >= 0) -> limit(1000)",
            "clean-cast-filter-write",
        ),
        (
            "read_vortex(target/fact.vortex) -> with_column(event_day,CAST(raw_event_time AS date32)) -> limit(1000)",
            "malformed-timestamp-dirty-csv",
        ),
    ] {
        let stdout = run_route(&[
            "route",
            "dataframe",
            "--input",
            "target/fact.vortex",
            "--input-format",
            "vortex",
            "--plan",
            plan,
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
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_user_cast")));
        assert!(stdout.contains(&field(
            "resolved_internal_command",
            "vortex-production-runtime-run"
        )));
        assert!(stdout.contains(&field("native_vortex_operation_family", "cast")));
        assert!(stdout.contains(&field("native_vortex_provider_scenario", scenario)));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
}

#[test]
fn public_route_infers_native_vortex_distinct_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,group_key) -> distinct() -> limit(10)",
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
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_distinct")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "distinct")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "distinct")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_infers_native_vortex_sample_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> filter(gte:value:3) -> select(id,group_key) -> sample(n=10,seed=7)",
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
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sample")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_infers_native_vortex_sample_fraction_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> filter(gte:value:3) -> select(id,group_key) -> sample(fraction,0.5,7)",
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
    if cfg!(feature = "vortex-local-primitives") {
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("route_id", "native_vortex_sample")));
        assert!(stdout.contains(&field("native_vortex_operation_family", "sample")));
        assert!(stdout.contains(&field("resolved_internal_command", "vortex-run")));
        assert!(stdout.contains(&field("vortex_primitive", "sample")));
        assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
        assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
        assert!(stdout.contains(&field("vortex_source_order_limit", "none")));
        assert!(stdout.contains(&field("vortex_sample_fraction", "0.5")));
        assert!(stdout.contains(&field("vortex_sample_seed", "7")));
        assert!(stdout.contains(&field(
            "route_support_status",
            "production_admitted_local_workflow"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("route_id", "blocked")));
        assert!(stdout.contains(&field(
            "blocker_id",
            "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_required_feature_gate",
            "vortex-local-primitives"
        )));
        assert!(stdout.contains(&field("native_vortex_capability_status", "feature_gated")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_ignores_scoped_set_index_metadata_for_native_vortex_shape() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,group_key) -> set_index(id) -> limit(10)",
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
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_project")));
    assert!(stdout.contains(&field(
        "native_vortex_operation_family",
        "filter_project_limit"
    )));
    assert!(stdout.contains(&field("resolved_internal_command", "vortex-project")));
    assert!(stdout.contains(&field("vortex_primitive", "project")));
    assert!(stdout.contains(&field("vortex_columns", "id,group_key")));
    assert!(stdout.contains(&field("vortex_source_order_limit", "10")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_admits_payloadless_native_vortex_metadata_profile_without_smoke_middle() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex)",
        "--request",
        "profile",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_user_profile")));
    assert!(stdout.contains(&field("route_status", "admitted")));
    assert!(stdout.contains(&field(
        "resolved_internal_command",
        "vortex-metadata-summary"
    )));
    assert!(stdout.contains(&field(
        "vortex_middle_status",
        "native_vortex_metadata_profile"
    )));
    assert!(stdout.contains(&field("native_vortex_operation_family", "profile")));
    assert!(stdout.contains(&field("native_vortex_capability_status", "supported")));
    assert!(stdout.contains(&field("native_vortex_required_feature_gate", "default")));
    assert!(stdout.contains(&field(
        "typed_result_contract",
        "metadata_first_native_vortex_profile_summary"
    )));
    assert!(stdout.contains(&field(
        "decode_materialization_boundary",
        "metadata_only_no_decode_materialization"
    )));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_native_vortex_profile_marks_projected_metadata_scope() {
    let (_success, stdout) = run_facade(&[
        "run",
        "dataframe",
        "--input",
        "target/fact.vortex",
        "--input-format",
        "vortex",
        "--plan",
        "read_vortex(target/fact.vortex) -> select(id,label)",
        "--request",
        "profile",
        "--bounded",
        "true",
        "--execution-policy",
        "native_vortex",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_user_profile"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_profile_projection_scope",
        "selected_columns"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_profile_projected_columns",
        "id,label"
    )));
    assert!(stdout.contains(&field(
        "metadata_summary_projection_scope",
        "selected_columns"
    )));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_routes_local_sql_vortex_middle_without_direct_runtime() {
    let workspace = std::path::Path::new("target/public-workflow-run-facade");
    let _ = std::fs::remove_dir_all(workspace);
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

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains(&field(
        "public_workflow_facade_schema_version",
        "shardloom.public_workflow_execution_facade.v1"
    )));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success);
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_project")));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "vortex-project"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_route_id",
            "local_file_prepare_once_first_query"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("project_local_execution_status", "executed")));
        assert!(stdout.contains(&field("project_local_execution_data_decoded", "false")));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_underlying_runtime_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
    assert!(stdout.contains(&field(
        "public_workflow_local_workflow_runtime_profile",
        "not_applicable"
    )));
}

#[test]
fn public_run_blocks_extensionless_local_sql_source_but_preserves_declared_format() {
    let workspace = std::path::Path::new("target/public-workflow-extensionless-source");
    let _ = std::fs::remove_dir_all(workspace);
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

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success);
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_project")));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("project_local_execution_status", "executed")));
        assert!(stdout.contains(&field("project_local_execution_data_decoded", "false")));
        assert!(stdout.contains(&field("public_workflow_local_source_format", "csv")));
        assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
        assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("public_workflow_source_format", "csv")));
        assert!(stdout.contains(&field("fallback_attempted", "false")));
        assert!(stdout.contains(&field("external_engine_invoked", "false")));
    }
}

#[test]
fn public_run_executes_local_write_through_prepared_vortex_row_export() {
    let workspace = std::path::Path::new("target/public-workflow-write-facade");
    let _ = std::fs::remove_dir_all(workspace);
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

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success, "{stdout}");
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "native_vortex_primitive_row_export"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_route_id",
            "local_file_prepare_once_first_query"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_local_source_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field("public_workflow_requested_output", "write_csv")));
        assert!(stdout.contains(&field("native_vortex_result_export_format", "csv")));
        assert!(stdout.contains(&field("native_vortex_result_export_rows_written", "2")));
        assert!(stdout.contains(&field("native_vortex_result_export_target_count", "1")));
        assert!(stdout.contains(&field("data_decoded", "true")));
        assert!(stdout.contains(&field("upstream_vortex_scan_called", "true")));
        assert_eq!(
            std::fs::read_to_string(&output).expect("read csv output"),
            "id,label\n1,alpha\n2,beta\n"
        );
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("public_workflow_blocker_id", expected_blocker)));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("output_io_performed", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_run_executes_local_fanout_through_prepared_vortex_row_export() {
    let workspace = std::path::Path::new("target/public-workflow-fanout-facade");
    let _ = std::fs::remove_dir_all(workspace);
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

    assert!(stdout.contains("\"command\":\"run\""));
    if cfg!(all(
        feature = "vortex-write",
        feature = "vortex-local-primitives"
    )) {
        assert!(success, "{stdout}");
        assert!(stdout.contains("\"status\":\"success\""));
        assert!(stdout.contains(&field(
            "public_workflow_route_id",
            "native_vortex_primitive_row_export"
        )));
        assert!(stdout.contains(&field("public_workflow_requested_output", "write_jsonl")));
        assert!(stdout.contains(&field("public_workflow_fanout_output_count", "1")));
        assert!(stdout.contains(&field("public_workflow_fanout_outputs", &fanout_arg)));
        assert!(stdout.contains(&field("native_vortex_result_export_target_count", "2")));
        assert!(stdout.contains(&field("native_vortex_result_export_fanout_count", "1")));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_fanout_performed",
            "true"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_target_formats",
            "jsonl,csv"
        )));
        assert!(stdout.contains(&field(
            "native_vortex_result_export_target_rows_written",
            "2,2"
        )));
        assert_eq!(
            std::fs::read_to_string(&primary).expect("read jsonl output"),
            "{\"id\":1,\"label\":\"alpha\"}\n{\"id\":2,\"label\":\"beta\"}\n"
        );
        assert_eq!(
            std::fs::read_to_string(&fanout).expect("read csv fanout"),
            "id,label\n1,alpha\n2,beta\n"
        );
    } else {
        assert!(!success);
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        let expected_blocker = if cfg!(feature = "vortex-write") {
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        } else {
            "cg21.route.local_file_vortex_ingest_feature_gated"
        };
        assert!(stdout.contains(&field("public_workflow_blocker_id", expected_blocker)));
        assert!(stdout.contains(&field(
            "public_workflow_underlying_runtime_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("runtime_execution", "false")));
        assert!(stdout.contains(&field("output_io_performed", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[cfg(all(feature = "vortex-write", feature = "vortex-local-primitives"))]
#[test]
fn public_run_executes_local_file_vortex_middle_through_prepared_vortex_primitive() {
    let workspace = std::path::Path::new("target/public-workflow-local-vortex-facade");
    let _ = std::fs::remove_dir_all(workspace);
    std::fs::create_dir_all(workspace).expect("create test workspace");
    let input = workspace.join("fact.csv");
    std::fs::write(&input, "id,value,metric\n1,2,1.5\n2,4,2.5\n3,6,3.5\n").expect("write csv");
    let plan = format!(
        "read_csv({}) -> filter(gte:value:3) -> select(metric,value) -> limit(2)",
        input.display()
    );
    let stdout = run_route(&[
        "run",
        "dataframe",
        "--input",
        input.to_str().expect("utf8 input path"),
        "--input-format",
        "csv",
        "--plan",
        &plan,
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_id",
        "native_vortex_filter_project"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_route_id",
        "local_file_prepare_once_first_query"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_vortex_ingest_performed",
        "true"
    )));
    assert!(stdout.contains(&field(
        "public_workflow_local_source_execution_mode",
        "prepared_vortex_then_native_vortex"
    )));
    assert!(stdout.contains(&field("public_workflow_source_format", "vortex")));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric,value")));
    assert!(stdout.contains(&field("filter_project_local_execution_status", "executed")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_data_decoded",
        "false"
    )));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_data_materialized",
        "false"
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
    assert!(stdout.contains(&field(
        "filter_project_local_execution_fallback_attempted",
        "false"
    )));
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
        "3",
        "--max-parallelism",
        "2",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("route_id", "native_vortex_filter_project")));
    assert!(stdout.contains(&field(
        "native_vortex_plan_route_family",
        "native_vortex_unified_plan"
    )));
    assert!(stdout.contains(&field(
        "native_vortex_plan_payload_kind",
        "primitive_operator"
    )));
    assert!(stdout.contains(&field("resolved_internal_command", "vortex-filter-project")));
    assert!(stdout.contains(&field("start_state", "native_vortex_file")));
    assert!(stdout.contains(&field("execution_mode", "native_vortex")));
    assert!(stdout.contains(&field("vortex_primitive", "filter_project")));
    assert!(stdout.contains(&field("vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("vortex_columns", "metric,value")));
    assert!(stdout.contains(&field("vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("memory_gb", "3")));
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

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_tail_payload_with_attached_route_envelope() {
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
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "tail",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--memory-gb",
        "3",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_tail")));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-run"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "tail")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "tail")));
    assert!(stdout.contains(&field("execution", "local_vortex_tail_primitive_performed")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_requested", "2")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_applied", "true")));
    assert!(stdout.contains(&field("public_workflow_memory_gb", "3")));
    assert!(stdout.contains(&field("local_primitive_resource_memory_gb", "3")));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_sample_payload_with_attached_route_envelope() {
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
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-source-order-limit",
        "2",
        "--vortex-sample-seed",
        "7",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_sample")));
    assert!(stdout.contains(&field(
        "public_workflow_resolved_internal_command",
        "vortex-run"
    )));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_predicate", "gte:value:3")));
    assert!(stdout.contains(&field("public_workflow_vortex_columns", "metric")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "2")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_seed", "7")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_fraction", "none")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "sample")));
    assert!(stdout.contains(&field(
        "execution",
        "local_vortex_sample_primitive_performed"
    )));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_requested", "2")));
    assert!(stdout.contains(&field("local_primitive_source_order_limit_applied", "true")));
    assert!(stdout.contains(&field(
        "local_primitive_native_io_certificate_status",
        "certified"
    )));
    assert!(stdout.contains(&field("local_primitive_native_io_certified", "true")));
    assert!(stdout.contains(&field("data_decoded", "true")));
    assert!(stdout.contains(&field("data_materialized", "true")));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn public_run_executes_native_vortex_sample_fraction_payload() {
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
        "bounded",
        "--evidence-level",
        "runtime_smoke",
        "--bounded",
        "true",
        "--vortex-primitive",
        "sample",
        "--vortex-predicate",
        "gte:value:3",
        "--vortex-columns",
        "metric",
        "--vortex-sample-fraction",
        "0.5",
        "--vortex-sample-seed",
        "7",
        "--memory-gb",
        "1",
        "--max-parallelism",
        "1",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"run\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field("public_workflow_route_id", "native_vortex_sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_primitive", "sample")));
    assert!(stdout.contains(&field("public_workflow_vortex_source_order_limit", "none")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_fraction", "0.5")));
    assert!(stdout.contains(&field("public_workflow_vortex_sample_seed", "7")));
    assert!(stdout.contains(&field("mode", "native_vortex_primitive")));
    assert!(stdout.contains(&field("primitive", "sample")));
    assert!(stdout.contains(&field(
        "execution",
        "local_vortex_sample_primitive_performed"
    )));
    assert!(stdout.contains(&field("output_row_count", "2")));
    assert!(stdout.contains(&field(
        "local_primitive_native_io_certificate_status",
        "certified"
    )));
    assert!(stdout.contains(&field("public_workflow_fallback_attempted", "false")));
    assert!(stdout.contains(&field("public_workflow_external_engine_invoked", "false")));
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
    let _ = std::fs::remove_file(&output);
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
    if cfg!(feature = "vortex-write") {
        assert!(stdout.contains("\"status\":\"success\""));
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
            "vortex-prepare"
        )));
        assert!(stdout.contains(&field("public_workflow_preparation_included", "true")));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_vortex_ingest_performed",
            "true"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_local_workflow_input_row_cap",
            "none_synthetic_row_cap_disabled"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_preparation_local_workflow_synthetic_input_row_cap_enabled",
            "false"
        )));
    } else {
        assert!(stdout.contains("\"status\":\"unsupported\""));
        assert!(stdout.contains(&field(
            "public_workflow_facade_schema_version",
            "shardloom.public_workflow_execution_facade.v1"
        )));
        assert!(stdout.contains(&field("public_workflow_route_attached", "true")));
        assert!(stdout.contains(&field("public_workflow_route_id", "blocked")));
        assert!(stdout.contains(&field(
            "public_workflow_blocker_id",
            "cg21.route.local_file_vortex_ingest_feature_gated"
        )));
        assert!(stdout.contains(&field(
            "public_workflow_resolved_internal_command",
            "not_resolved"
        )));
        assert!(stdout.contains(&field("public_workflow_preparation_included", "false")));
    }
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
