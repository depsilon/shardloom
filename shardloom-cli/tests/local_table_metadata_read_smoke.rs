use std::process::Command;

fn run_local_table_metadata_read_smoke_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["local-table-metadata-read-smoke", "--format", "json"])
        .output()
        .expect("local-table-metadata-read-smoke command runs");

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
fn local_table_metadata_read_smoke_exposes_typed_metadata_summary() {
    let output = run_local_table_metadata_read_smoke_json();

    assert!(output.contains("\"command\":\"local-table-metadata-read-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "local_table_metadata_read_smoke")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_table_metadata_read_smoke.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "gar0020c.local_manifest_table_metadata_read_smoke"
    )));
    assert!(output.contains(&field("gar_id", "GAR-0020-C")));
    assert!(output.contains(&field("support_status", "runtime_supported")));
    assert!(output.contains(&field(
        "claim_gate_status",
        "scoped_local_metadata_smoke_only"
    )));
    assert!(output.contains(&field("catalog_kind", "local_manifest")));
    assert!(output.contains(&field("dataset_format", "vortex")));
    assert!(output.contains(&field("manifest_id", "gar0020c-local-manifest-v1")));
    assert!(output.contains(&field("snapshot_id", "gar0020c-snapshot-0001")));
    assert!(output.contains(&field("schema_id", "gar0020c-orders-schema")));
    assert!(output.contains(&field("schema_version_number", "1")));
    assert!(output.contains(&field("schema_field_count", "4")));
    assert!(output.contains(&field("schema_has_field_ids", "true")));
    assert!(output.contains(&field("partition_field_count", "1")));
    assert!(output.contains(&field("is_partitioned", "true")));
    assert!(output.contains(&field("manifest_file_count", "1")));
    assert!(output.contains(&field("manifest_segment_count", "1")));
    assert!(output.contains(&field("native_vortex_file_count", "1")));
    assert!(output.contains(&field("metadata_capable_segment_count", "1")));
    assert!(output.contains(&field("declared_row_count", "8")));
    assert!(output.contains("\"metadata_summary_digest\",\"value\":\"fnv1a64:"));
}

#[test]
fn local_table_metadata_read_smoke_preserves_no_fallback_boundaries() {
    let output = run_local_table_metadata_read_smoke_json();

    for key in [
        "local_catalog_ref_resolved",
        "local_manifest_metadata_read_performed",
        "table_metadata_summary_emitted",
        "table_metadata_read_performed",
        "runtime_supported",
        "claim_scoped",
        "side_effect_free",
        "deterministic_unsupported_diagnostics_ready",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }

    for key in [
        "catalog_io_performed",
        "table_metadata_file_io_performed",
        "object_store_io_performed",
        "data_file_read_performed",
        "write_io_performed",
        "credential_resolution_performed",
        "external_table_format_dependency_invoked",
        "fallback_attempted",
        "fallback_execution_allowed",
        "external_engine_invoked",
        "performance_claim_allowed",
        "production_table_catalog_claim_allowed",
        "lakehouse_claim_allowed",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    assert!(output.contains(&field("blocked_path_count", "8")));
    assert!(output.contains(&field(
        "blocked_path_order",
        "external_catalog_resolution,object_store_manifest_read,credential_resolution,data_file_read,table_metadata_write,cdc_delete_tombstone_execution,external_table_format_runtime,lakehouse_production_claim"
    )));
    assert!(output.contains(&field("unsupported_diagnostic_count", "8")));
    assert!(output.contains(&field("diagnostic_count", "8")));
    assert!(output.contains(&field("execution", "performed")));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_NOT_IMPLEMENTED\""));
}
