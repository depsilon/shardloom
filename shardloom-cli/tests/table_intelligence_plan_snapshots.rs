use std::process::Command;

fn run_table_intelligence_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["table-intelligence-plan", "--format", "json"])
        .output()
        .expect("table-intelligence-plan command runs");

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
fn table_intelligence_json_exposes_cg9_aggregate_surface() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains("\"command\":\"table-intelligence-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "table_intelligence_plan")));
    assert!(output.contains(&field("schema_version", "shardloom.table_intelligence.v1")));
    assert!(output.contains(&field("report_id", "cg9.table_intelligence.foundation")));
    assert!(output.contains(&field("surface_count", "10")));
    assert!(output.contains(&field("report_only_available_surface_count", "7")));
    assert!(output.contains(&field("required_cg9_surface_count", "10")));
    assert!(output.contains(&field("snapshot_boundary_surface_count", "7")));
    assert!(output.contains(&field(
        "surface_order",
        "schema_evolution,partition_evolution,delete_tombstone,table_compatibility,cdc_incremental,layout_health,compaction,snapshot_manifest,catalog_compatibility,commit_recovery"
    )));
}

#[test]
fn table_intelligence_json_preserves_no_io_no_dependency_no_fallback_defaults() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains(&field(
        "compatibility_profiles",
        "native_vortex,iceberg_compatible,delta_compatible,hudi_like,hive_style_partitions"
    )));
    assert!(output.contains(&field("catalog_io_performed", "false")));
    assert!(output.contains(&field("table_metadata_io_performed", "false")));
    assert!(output.contains(&field("data_io_performed", "false")));
    assert!(output.contains(&field("write_io_performed", "false")));
    assert!(output.contains(&field("external_table_format_dependency_added", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("plan_only", "true")));
}
