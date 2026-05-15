use std::process::Command;

fn run_local_delete_tombstone_read_smoke_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["local-delete-tombstone-read-smoke", "--format", "json"])
        .output()
        .expect("local-delete-tombstone-read-smoke command runs");

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
fn local_delete_tombstone_read_smoke_exposes_fixture_result() {
    let output = run_local_delete_tombstone_read_smoke_json();

    assert!(output.contains("\"command\":\"local-delete-tombstone-read-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "local_delete_tombstone_read_smoke")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_delete_tombstone_read_smoke.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "gar0020d.local_delete_tombstone_read_smoke"
    )));
    assert!(output.contains(&field("gar_id", "GAR-0020-D")));
    assert!(output.contains(&field("support_status", "fixture_smoke_only")));
    assert!(output.contains(&field(
        "claim_gate_status",
        "scoped_local_delete_tombstone_smoke_only"
    )));
    assert!(output.contains(&field("fixture_id", "gar0020d-local-delete-tombstone")));
    assert!(output.contains(&field("catalog_kind", "local_manifest")));
    assert!(output.contains(&field("dataset_format", "vortex")));
    assert!(output.contains(&field(
        "admitted_delete_model_order",
        "file_level_delete,segment_level_tombstone"
    )));
    assert!(output.contains(&field(
        "delete_tombstone_admission_rule",
        "local_manifest_file_delete_and_segment_tombstone_admission"
    )));
    assert!(output.contains(&field("row_identity_rule", "stable_fixture_row_id")));
    assert!(output.contains(&field("base_row_count", "6")));
    assert!(output.contains(&field("file_deleted_row_count", "2")));
    assert!(output.contains(&field("segment_tombstoned_row_count", "1")));
    assert!(output.contains(&field("effective_row_count", "3")));
    assert!(output.contains(&field("manifest_file_count", "3")));
    assert!(output.contains(&field("manifest_segment_count", "3")));
    assert!(output.contains(&field("native_vortex_file_count", "3")));
    assert!(output.contains(&field("effective_row_ids", "1001,1002,1003")));
    assert!(output.contains("\"correctness_digest\",\"value\":\"fnv1a64:"));
}

#[test]
fn local_delete_tombstone_read_smoke_preserves_boundaries() {
    let output = run_local_delete_tombstone_read_smoke_json();

    for key in [
        "local_catalog_ref_resolved",
        "local_manifest_metadata_read_performed",
        "in_memory_fixture_rows_read",
        "delete_tombstone_rule_applied",
        "result_row_order_preserved",
        "fixture_smoke_supported",
        "claim_scoped",
        "side_effect_free",
        "deterministic_unsupported_diagnostics_ready",
        "execution",
    ] {
        assert!(
            output.contains(&field(key, "true")) || output.contains(&field(key, "performed")),
            "missing true/performed field {key}"
        );
    }

    for key in [
        "table_metadata_write_performed",
        "data_file_read_performed",
        "object_store_io_performed",
        "write_io_performed",
        "credential_resolution_performed",
        "external_table_format_dependency_invoked",
        "fallback_attempted",
        "fallback_execution_allowed",
        "external_engine_invoked",
        "performance_claim_allowed",
        "table_format_execution_claim_allowed",
        "production_table_catalog_claim_allowed",
        "lakehouse_claim_allowed",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    assert!(output.contains(&field("blocked_model_count", "7")));
    assert!(output.contains(&field(
        "blocked_model_order",
        "row_level_delete,position_delete,equality_delete,external_table_metadata,cdc_update_delete_tombstone,object_store_delete_manifest,table_format_delete_runtime"
    )));
    assert!(output.contains(&field("unsupported_diagnostic_count", "7")));
    assert!(output.contains(&field("diagnostic_count", "7")));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_NOT_IMPLEMENTED\""));
}
