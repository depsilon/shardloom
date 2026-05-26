use std::{fs, process::Command};

fn run_object_store_read_smoke_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .expect("object-store-read-smoke command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn local_emulator_range_read_emits_source_state_and_policy_evidence() {
    let fixture = std::env::temp_dir().join(format!(
        "shardloom-object-store-read-smoke-integration-{}.bin",
        std::process::id()
    ));
    fs::write(&fixture, b"abcdef").expect("fixture write");
    let args = vec![
        "object-store-read-smoke".to_string(),
        fixture.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--range".to_string(),
        "1:3".to_string(),
    ];

    let (success, output, stderr) = run_object_store_read_smoke_json(&args);
    fs::remove_file(&fixture).expect("fixture cleanup");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-read-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_read_smoke.v1"
    )));
    assert!(output.contains(&field("provider_profile", "local-emulator")));
    assert!(output.contains(&field("object_store_read_status", "succeeded")));
    assert!(output.contains(&field("byte_range_read_status", "performed_local_emulator")));
    assert!(output.contains(&field("read_range_offset", "1")));
    assert!(output.contains(&field("read_range_length", "3")));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("native_io_certificate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("object_store_io", "true")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains("\"source_state_id\""));
    assert!(output.contains("\"source_state_digest\""));
}

#[test]
fn remote_provider_is_blocked_without_credential_or_network_probe() {
    let args = vec![
        "object-store-read-smoke".to_string(),
        "s3://bucket/object.vortex".to_string(),
    ];

    let (success, output, stderr) = run_object_store_read_smoke_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field(
        "object_store_read_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
}

#[test]
fn public_no_credential_fixture_profile_reads_uri_shape_without_network() {
    let fixture = std::env::temp_dir().join(format!(
        "shardloom-object-store-public-fixture-integration-{}.bin",
        std::process::id()
    ));
    fs::write(&fixture, b"abcdef").expect("fixture write");
    let args = vec![
        "object-store-read-smoke".to_string(),
        "s3://shardloom-public-fixtures/orders.vortex".to_string(),
        "--profile".to_string(),
        "public-no-credential-fixture".to_string(),
        "--public-fixture-path".to_string(),
        fixture.to_string_lossy().into_owned(),
        "--fixture-listing".to_string(),
        "--range".to_string(),
        "2:2".to_string(),
    ];

    let (success, output, stderr) = run_object_store_read_smoke_json(&args);
    fs::remove_file(&fixture).expect("fixture cleanup");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("provider_profile", "public-no-credential-fixture")));
    assert!(output.contains(&field("object_store_provider", "s3")));
    assert!(output.contains(&field("object_store_bucket", "shardloom-public-fixtures")));
    assert!(output.contains(&field("object_store_key", "orders.vortex")));
    assert!(output.contains(&field(
        "object_store_uri_parse_status",
        "parsed_public_no_credential_fixture_uri"
    )));
    assert!(output.contains(&field(
        "byte_range_read_status",
        "performed_public_no_credential_fixture"
    )));
    assert!(output.contains(&field(
        "credential_policy_status",
        "public_no_credential_fixture_admitted"
    )));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field(
        "listing_status",
        "performed_public_fixture_single_object"
    )));
    assert!(output.contains(&field("object_store_io", "true")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field(
        "native_io_certificate_status",
        "public_fixture_smoke_only"
    )));
    assert!(output.contains(&field("claim_gate_status", "public_fixture_smoke_only")));
    assert!(output.contains(&field("public_no_credential_fixture_claim_allowed", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_no_credential_fixture_profile_accepts_adls_container_account_uri() {
    let fixture = std::env::temp_dir().join(format!(
        "shardloom-object-store-public-adls-fixture-integration-{}.bin",
        std::process::id()
    ));
    fs::write(&fixture, b"abcdef").expect("fixture write");
    let args = vec![
        "object-store-read-smoke".to_string(),
        "abfss://public-container@storageacct.dfs.core.windows.net/orders.vortex".to_string(),
        "--profile".to_string(),
        "public-no-credential-fixture".to_string(),
        "--public-fixture-path".to_string(),
        fixture.to_string_lossy().into_owned(),
    ];

    let (success, output, stderr) = run_object_store_read_smoke_json(&args);
    fs::remove_file(&fixture).expect("fixture cleanup");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("object_store_provider", "adls")));
    assert!(output.contains(&field(
        "object_store_bucket",
        "public-container@storageacct.dfs.core.windows.net"
    )));
    assert!(output.contains(&field("object_store_key", "orders.vortex")));
    assert!(output.contains(&field("requested_uri_redaction_status", "not_required")));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_no_credential_fixture_profile_requires_explicit_fixture_path() {
    let args = vec![
        "object-store-read-smoke".to_string(),
        "s3://shardloom-public-fixtures/orders.vortex".to_string(),
        "--profile".to_string(),
        "public-no-credential-fixture".to_string(),
    ];

    let (success, output, stderr) = run_object_store_read_smoke_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "object_store_read_status",
        "blocked_missing_fixture_path"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}
