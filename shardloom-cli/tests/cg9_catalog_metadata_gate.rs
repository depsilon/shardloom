use std::process::Command;

fn run_cg9_catalog_metadata_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg9-catalog-metadata-gate", "--format", "json"])
        .output()
        .expect("cg9-catalog-metadata-gate command runs");

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
fn cg9_catalog_metadata_gate_exposes_surface_order_and_existing_evidence() {
    let output = run_cg9_catalog_metadata_gate_json();

    assert!(output.contains("\"command\":\"cg9-catalog-metadata-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "catalog_metadata_integration_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.catalog_metadata_integration_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg9.catalog_metadata_integration_gate")));
    assert!(output.contains(&field("surface_count", "11")));
    assert!(output.contains(&field("existing_evidence_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "9")));
    assert!(output.contains(&field(
        "surface_order",
        "table_intelligence_foundation,catalog_ref_skeleton,snapshot_manifest_boundary,catalog_table_resolution,table_metadata_read,partition_metadata_read,delete_tombstone_metadata_read,cdc_metadata_read,table_format_dependency_admission,commit_recovery_metadata_binding,metadata_cache_invalidation"
    )));
    assert!(output.contains(&field(
        "existing_table_intelligence_foundation_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_schema_partition_delete_compatibility_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_cdc_layout_compaction_planning_present",
        "true"
    )));
    assert!(output.contains(&field("existing_catalog_ref_skeleton_present", "true")));
}

#[test]
fn cg9_catalog_metadata_gate_blocks_runtime_io_dependencies_and_claims() {
    let output = run_cg9_catalog_metadata_gate_json();

    for key in [
        "snapshot_manifest_metadata_read_allowed",
        "catalog_resolution_allowed",
        "table_metadata_read_allowed",
        "catalog_io_allowed",
        "object_store_io_allowed",
        "data_io_allowed",
        "write_io_allowed",
        "external_table_format_dependency_allowed",
        "credential_resolution_allowed",
        "metadata_cache_runtime_allowed",
        "metadata_integration_claim_allowed",
        "fallback_attempted",
        "fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "table_intelligence_report_required",
        "catalog_ref_required",
        "snapshot_ref_required",
        "schema_digest_required",
        "partition_spec_required",
        "delete_tombstone_policy_required",
        "dependency_license_approval_required",
        "credential_policy_required",
        "effect_policy_required",
        "materialization_boundary_required",
        "execution_certificate_required",
        "native_io_certificate_required",
        "benchmark_evidence_required",
        "runtime_promotions_blocked",
        "claim_blocked",
        "side_effect_free",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
    assert!(output.contains(&field("execution", "not_performed")));
}
