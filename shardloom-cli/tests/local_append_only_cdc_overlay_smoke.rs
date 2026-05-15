use std::process::Command;

fn run_local_append_only_cdc_overlay_smoke_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["local-append-only-cdc-overlay-smoke", "--format", "json"])
        .output()
        .expect("local-append-only-cdc-overlay-smoke command runs");

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
fn local_append_only_cdc_overlay_smoke_exposes_fixture_result() {
    let output = run_local_append_only_cdc_overlay_smoke_json();

    assert!(output.contains("\"command\":\"local-append-only-cdc-overlay-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "local_append_only_cdc_overlay_smoke")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_append_only_cdc_overlay_smoke.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "gar0020e.local_append_only_cdc_overlay_smoke"
    )));
    assert!(output.contains(&field("gar_id", "GAR-0020-E")));
    assert!(output.contains(&field("support_status", "fixture_smoke_only")));
    assert!(output.contains(&field(
        "claim_gate_status",
        "scoped_append_only_cdc_overlay_smoke_only"
    )));
    assert!(output.contains(&field(
        "fixture_id",
        "gar0020e-local-append-only-cdc-overlay"
    )));
    assert!(output.contains(&field("catalog_kind", "local_manifest")));
    assert!(output.contains(&field("dataset_format", "vortex")));
    assert!(output.contains(&field(
        "incremental_plan_report_ref",
        "shardloom.cdc_incremental_planning.v1"
    )));
    assert!(output.contains(&field(
        "incremental_status",
        "execute_changed_segments_only"
    )));
    assert!(output.contains(&field("overlay_rule", "base_snapshot_then_append_delta")));
    assert!(output.contains(&field("cdc_event_order", "insert")));
    assert!(output.contains(&field("base_row_count", "3")));
    assert!(output.contains(&field("append_row_count", "2")));
    assert!(output.contains(&field("effective_row_count", "5")));
    assert!(output.contains(&field("changed_segment_count", "1")));
    assert!(output.contains(&field("insert_count", "2")));
    assert!(output.contains(&field("update_count", "0")));
    assert!(output.contains(&field("delete_count", "0")));
    assert!(output.contains(&field("tombstone_count", "0")));
    assert!(output.contains(&field("unsupported_change_count", "0")));
    assert!(output.contains(&field("base_row_ids", "1001,1002,1003")));
    assert!(output.contains(&field("appended_row_ids", "4001,4002")));
    assert!(output.contains(&field("effective_row_ids", "1001,1002,1003,4001,4002")));
    assert!(output.contains("\"correctness_digest\",\"value\":\"fnv1a64:"));
}

#[test]
fn local_append_only_cdc_overlay_smoke_preserves_boundaries() {
    let output = run_local_append_only_cdc_overlay_smoke_json();

    for key in [
        "local_catalog_ref_resolved",
        "local_base_snapshot_declared",
        "local_append_delta_declared",
        "cdc_incremental_plan_evaluated",
        "append_overlay_rule_applied",
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
        "manifest_write_performed",
        "transaction_execution_performed",
        "commit_execution_performed",
        "data_file_read_performed",
        "object_store_io_performed",
        "write_io_performed",
        "credential_resolution_performed",
        "external_table_format_dependency_invoked",
        "fallback_attempted",
        "fallback_execution_allowed",
        "external_engine_invoked",
        "performance_claim_allowed",
        "production_incremental_claim_allowed",
        "lakehouse_claim_allowed",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    assert!(output.contains(&field("blocked_path_count", "9")));
    assert!(output.contains(&field("unsupported_diagnostic_count", "9")));
    assert!(output.contains(&field("diagnostic_count", "9")));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_NOT_IMPLEMENTED\""));
}
