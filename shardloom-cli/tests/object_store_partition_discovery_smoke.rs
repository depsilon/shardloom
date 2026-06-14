use std::{fs, path::PathBuf, process::Command};

fn run_partition_discovery_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("object-store partition discovery command runs");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn temp_partition_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shardloom-object-store-partition-discovery-{name}-{}",
        std::process::id()
    ))
}

#[test]
fn local_emulator_partition_discovery_smoke_lists_key_value_directories() {
    let root = temp_partition_root("success");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("region=us").join("date=2026-06-06"))
        .expect("create first partition");
    fs::create_dir_all(root.join("region=eu").join("date=2026-06-07"))
        .expect("create second partition");
    fs::write(
        root.join("region=us")
            .join("date=2026-06-06")
            .join("part.bin"),
        b"rows",
    )
    .expect("write data file");

    let args = vec![
        "object-store-partition-discovery-smoke".to_string(),
        root.to_string_lossy().into_owned(),
        "--partition-columns".to_string(),
        "region,date".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    let (success, output, stderr) = run_partition_discovery_json(&args);

    let _ = fs::remove_dir_all(&root);
    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-partition-discovery-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_partition_discovery_smoke.v1"
    )));
    assert!(output.contains(&field("provider_profile", "local-emulator")));
    assert!(output.contains(&field(
        "provider_admission_report_id",
        "shardloom.object_store_provider_admission.v1"
    )));
    assert!(output.contains(&field(
        "provider_admission_operation",
        "partition_discovery"
    )));
    assert!(output.contains(&field(
        "provider_admission_status",
        "admitted_local_emulator"
    )));
    assert!(output.contains(&field(
        "provider_admission_classification",
        "use_shardloom_local_emulator_provider"
    )));
    assert!(output.contains(&field("request_signing_allowed", "false")));
    assert!(output.contains(&field("request_signing_performed", "false")));
    assert!(output.contains(&field(
        "request_signing_status",
        "not_required_local_emulator"
    )));
    assert!(output.contains(&field("partition_discovery_status", "succeeded")));
    assert!(output.contains(&field(
        "partition_listing_status",
        "performed_local_emulator"
    )));
    assert!(output.contains(&field("partition_directory_count", "4")));
    assert!(output.contains(&field("partition_key_count", "2")));
    assert!(output.contains(&field("requested_partition_columns", "region,date")));
    assert!(output.contains(&field("discovered_partition_columns", "date,region")));
    assert!(output.contains(&field(
        "discovered_partition_values",
        "date=2026-06-06,date=2026-06-07,region=eu,region=us"
    )));
    assert!(output.contains(&field("object_store_runtime_supported", "true")));
    assert!(output.contains(&field("partition_discovery_runtime_supported", "true")));
    assert!(output.contains(&field(
        "live_provider_partition_discovery_supported",
        "false"
    )));
    assert!(output.contains(&field("catalog_integration_supported", "false")));
    assert!(output.contains(&field("remote_result_delivery_supported", "false")));
    assert!(output.contains(&field("object_store_io", "true")));
    assert!(output.contains(&field("object_store_listing_io", "true")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "fixture_smoke_only")));
}

#[test]
fn partition_discovery_blocks_requested_column_mismatch() {
    let root = temp_partition_root("column-mismatch");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("region=us").join("date=2026-06-06")).expect("create partition");

    let args = vec![
        "object-store-partition-discovery-smoke".to_string(),
        root.to_string_lossy().into_owned(),
        "--partition-columns".to_string(),
        "region".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    let (success, output, stderr) = run_partition_discovery_json(&args);

    let _ = fs::remove_dir_all(&root);
    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "partition_discovery_status",
        "blocked_partition_column_mismatch"
    )));
    assert!(output.contains(&field("requested_partition_columns", "region")));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn remote_provider_partition_discovery_is_blocked_before_credentials_or_network() {
    let args = vec![
        "object-store-partition-discovery-smoke".to_string(),
        "s3://bucket/table/".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ];
    let (success, output, stderr) = run_partition_discovery_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-partition-discovery-smoke\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "partition_discovery_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field(
        "provider_admission_operation",
        "partition_discovery"
    )));
    assert!(output.contains(&field(
        "provider_admission_status",
        "blocked_live_provider_no_probe"
    )));
    assert!(output.contains(&field(
        "credential_policy_status",
        "credential_policy_required_not_admitted"
    )));
    assert!(output.contains(&field("request_signing_allowed", "false")));
    assert!(output.contains(&field("request_signing_performed", "false")));
    assert!(output.contains(&field("request_signing_status", "blocked_not_invoked")));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("object_store_listing_io", "false")));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
}
