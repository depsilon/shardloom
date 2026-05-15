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
    assert!(output.contains(&field("diagnostic_count", "23")));
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
fn table_intelligence_json_embeds_gar0020b_table_execution_matrix() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_schema_version",
        "shardloom.table_maintenance_execution_matrix.v1"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_report_id",
        "gar0020b.table_maintenance_execution_matrix"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_gar_id",
        "GAR-0020-B"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_support_status",
        "report_only_with_unsupported_runtime_paths"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_operation_count",
        "12"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_report_only_operation_count",
        "4"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_unsupported_operation_count",
        "8"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_operation_order",
        "file_level_delete_compatibility,segment_tombstone_execution,row_level_delete_execution,position_delete_execution,equality_delete_execution,cdc_append_only_planning,cdc_metadata_only_planning,cdc_update_delete_tombstone_execution,compaction_planning,compaction_execution_write,table_metadata_write,table_maintenance_commit"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_runtime_promotions_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_deterministic_unsupported_diagnostics_ready",
        "true"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_unsupported_diagnostic_count",
        "8"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_diagnostic_count",
        "8"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_table_format_execution_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_file_level_delete_compatibility_status",
        "report_only_available"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_file_level_delete_compatibility_existing_report_ref",
        "shardloom.delete_tombstone_compatibility.v1"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_cdc_update_delete_tombstone_execution_status",
        "unsupported_until_certified"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_cdc_update_delete_tombstone_execution_required_commit_semantics",
        "cdc_transaction_and_delete_semantics"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_table_metadata_write_write_io",
        "false"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_table_metadata_write_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_row_table_maintenance_commit_claim_gate_status",
        "not_claim_grade"
    )));
}

#[test]
fn table_intelligence_json_embeds_gar0020d_local_delete_tombstone_smoke_ref() {
    let output = run_table_intelligence_plan_json();

    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_existing_report_refs",
        "cg9.table_intelligence.foundation,shardloom.delete_tombstone_compatibility.v1,shardloom.cdc_incremental_planning.v1,shardloom.layout_health.v1,shardloom.compaction_planning.v1,gar0020c.local_manifest_table_metadata_read_smoke,gar0020d.local_delete_tombstone_read_smoke,gar0004a.cdc_manifest_transaction_gate,shardloom.object_store_commit_protocol.v1"
    )));
    assert!(output.contains(&field(
        "table_maintenance_execution_matrix_local_delete_tombstone_smoke_present",
        "true"
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
