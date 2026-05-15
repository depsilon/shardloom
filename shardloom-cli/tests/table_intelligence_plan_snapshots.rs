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

fn run_incremental_cdc_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["incremental-plan", "cdc", "append-only", "--format", "json"])
        .output()
        .expect("incremental-plan cdc command runs");

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
    assert!(output.contains(&field("diagnostic_count", "15")));
    assert!(output.contains(&field("plan_only", "true")));
}

#[test]
fn table_intelligence_json_embeds_gar0004a_cdc_manifest_transaction_gate() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_schema_version",
        "shardloom.cdc_manifest_transaction_gate.v1"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_report_id",
        "gar0004a.cdc_manifest_transaction_gate"
    )));
    assert!(output.contains(&field("cdc_manifest_transaction_gate_surface_count", "8")));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_report_only_surface_count",
        "2"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_unsupported_surface_count",
        "6"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_surface_order",
        "cdc_read_intent,cdc_write_intent,manifest_serialization,manifest_metadata_read,object_store_commit,table_catalog_commit,transaction_execution,unsupported_commit_diagnostic"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_runtime_promotions_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_deterministic_unsupported_diagnostics_ready",
        "true"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_unsupported_diagnostic_count",
        "6"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_external_engine_invoked",
        "false"
    )));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_NOT_IMPLEMENTED\""));
}

#[test]
fn table_intelligence_json_embeds_gar0020a_catalog_metadata_gate() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_schema_version",
        "shardloom.catalog_metadata_integration_gate.v1"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_report_id",
        "cg9.catalog_metadata_integration_gate"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_gar_id",
        "GAR-0020-A"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_surface_count",
        "11"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_unsupported_surface_count",
        "9"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_deterministic_unsupported_diagnostics_ready",
        "true"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_diagnostic_count",
        "9"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_table_metadata_read_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_row_table_metadata_read_status",
        "blocked_until_certified"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_row_table_metadata_read_requires_table_metadata_io",
        "true"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_row_table_metadata_read_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_local_manifest_table_metadata_smoke_supported",
        "true"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_local_manifest_table_metadata_smoke_command",
        "local-table-metadata-read-smoke"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_local_manifest_table_metadata_smoke_report_ref",
        "gar0020c.local_manifest_table_metadata_read_smoke"
    )));
    assert!(output.contains(&field(
        "catalog_metadata_integration_gate_local_manifest_table_metadata_smoke_claim_gate_status",
        "scoped_local_metadata_smoke_only"
    )));
}

#[test]
fn incremental_cdc_json_embeds_gar0004a_gate_without_runtime_promotion() {
    let output = run_incremental_cdc_plan_json();

    assert!(output.contains("\"command\":\"incremental-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cdc_incremental_plan")));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_report_id",
        "gar0004a.cdc_manifest_transaction_gate"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_cdc_read_intent_report_only_available",
        "true"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_cdc_write_intent_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_manifest_serialization_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_manifest_metadata_read_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_transaction_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_commit_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "cdc_manifest_transaction_gate_fallback_execution_allowed",
        "false"
    )));
}
