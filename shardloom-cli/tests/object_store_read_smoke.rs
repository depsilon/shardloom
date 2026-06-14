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

fn assert_json_fields(output: &str, expected_fields: &[(&str, &str)]) {
    for (key, value) in expected_fields {
        assert!(
            output.contains(&field(key, value)),
            "missing field {key}={value} in output: {output}"
        );
    }
}

fn assert_local_emulator_range_read_fields(output: &str) {
    assert_json_fields(
        output,
        &[
            ("schema_version", "shardloom.object_store_read_smoke.v1"),
            ("provider_profile", "local-emulator"),
            (
                "provider_admission_report_id",
                "shardloom.object_store_provider_admission.v1",
            ),
            ("provider_admission_operation", "read"),
            ("provider_profile_status", "admitted_local_emulator_profile"),
            ("provider_admission_status", "admitted_local_emulator"),
            (
                "provider_admission_classification",
                "use_shardloom_local_emulator_provider",
            ),
            (
                "provider_admission_boundary",
                "caller_owned_local_emulator_paths_only",
            ),
            ("object_store_read_status", "succeeded"),
            ("byte_range_read_status", "performed_local_emulator"),
            ("read_range_offset", "1"),
            ("read_range_length", "3"),
            (
                "object_etag_status",
                "derived_local_emulator_metadata_range_fingerprint",
            ),
            ("object_version_status", "derived_local_emulator_mtime"),
            (
                "object_store_checksum_validation_status",
                "validated_requested_bytes_digest",
            ),
            (
                "object_store_checksum_algorithm",
                "fnv64_non_crypto_fixture_digest",
            ),
            ("object_store_checksum_scope", "requested_byte_range"),
            ("object_store_request_count", "1"),
            ("object_store_bytes_requested", "3"),
            ("object_store_bytes_read", "3"),
            (
                "object_store_bounded_read_status",
                "bounded_byte_range_with_fixture_budget",
            ),
            (
                "object_store_request_coalescing_status",
                "not_required_single_byte_range_request",
            ),
            ("object_store_coalesced_request_count", "1"),
            (
                "object_store_prefetch_status",
                "not_required_single_bounded_request",
            ),
            (
                "object_store_retry_policy_status",
                "not_required_single_attempt_local_emulator",
            ),
            ("object_store_retry_attempt_count", "0"),
            (
                "object_store_rate_limit_policy_status",
                "not_required_local_emulator_no_network",
            ),
            ("object_store_cache_hit_count", "0"),
            ("object_store_cache_miss_count", "0"),
            ("credential_resolution_performed", "false"),
            ("request_signing_allowed", "false"),
            ("request_signing_performed", "false"),
            ("request_signing_status", "not_required_local_emulator"),
            (
                "request_signing_boundary",
                "no_remote_request_to_sign_local_emulator",
            ),
            (
                "explain_estimate_doctor_probe_policy",
                "static_no_provider_probe_default",
            ),
            (
                "capability_discovery_probe_policy",
                "static_capability_report_no_provider_probe",
            ),
            ("network_probe_performed", "false"),
            ("native_io_certificate_status", "fixture_smoke_only"),
            ("claim_gate_status", "fixture_smoke_only"),
            ("object_store_io", "true"),
            ("object_store_write_io", "false"),
            ("fallback_attempted", "false"),
            ("external_engine_invoked", "false"),
        ],
    );
    assert!(output.contains("\"source_state_id\""));
    assert!(output.contains("\"source_state_digest\""));
}

fn assert_public_fixture_range_read_fields(output: &str) {
    assert_json_fields(
        output,
        &[
            ("provider_profile", "public-no-credential-fixture"),
            (
                "provider_profile_status",
                "admitted_public_no_credential_fixture_profile",
            ),
            (
                "provider_admission_status",
                "admitted_public_no_credential_fixture",
            ),
            (
                "provider_admission_classification",
                "wrap_public_no_credential_fixture",
            ),
            (
                "provider_admission_boundary",
                "uri_shape_plus_explicit_local_fixture_only_no_live_provider",
            ),
            ("object_store_provider", "s3"),
            ("object_store_bucket", "shardloom-public-fixtures"),
            ("object_store_key", "orders.vortex"),
            (
                "object_store_uri_parse_status",
                "parsed_public_no_credential_fixture_uri",
            ),
            (
                "byte_range_read_status",
                "performed_public_no_credential_fixture",
            ),
            ("object_etag_status", "derived_public_fixture_read_digest"),
            ("object_version_status", "derived_public_fixture_mtime"),
            (
                "object_store_checksum_validation_status",
                "validated_requested_bytes_digest",
            ),
            (
                "object_store_checksum_algorithm",
                "fnv64_non_crypto_fixture_digest",
            ),
            ("object_store_checksum_scope", "requested_byte_range"),
            ("object_store_request_count", "1"),
            ("object_store_bytes_requested", "2"),
            ("object_store_bytes_read", "2"),
            (
                "object_store_bounded_read_status",
                "bounded_byte_range_with_fixture_budget",
            ),
            (
                "object_store_request_coalescing_status",
                "not_required_single_byte_range_request",
            ),
            ("object_store_coalesced_request_count", "1"),
            (
                "object_store_prefetch_status",
                "not_required_single_bounded_request",
            ),
            (
                "object_store_retry_policy_status",
                "not_required_single_attempt_public_fixture",
            ),
            ("object_store_retry_attempt_count", "0"),
            (
                "object_store_rate_limit_policy_status",
                "not_required_public_fixture_no_network",
            ),
            ("object_store_cache_hit_count", "0"),
            ("object_store_cache_miss_count", "0"),
            (
                "credential_policy_status",
                "public_no_credential_fixture_admitted",
            ),
            (
                "request_signing_status",
                "not_required_public_no_credential_fixture",
            ),
            (
                "request_signing_boundary",
                "no_remote_request_to_sign_public_fixture_reads_local_bytes",
            ),
            ("network_probe_performed", "false"),
            ("provider_probe_performed", "false"),
            ("listing_status", "performed_public_fixture_single_object"),
            ("object_store_io", "true"),
            ("object_store_write_io", "false"),
            ("native_io_certificate_status", "public_fixture_smoke_only"),
            ("claim_gate_status", "public_fixture_smoke_only"),
            ("public_no_credential_fixture_claim_allowed", "true"),
            ("fallback_attempted", "false"),
            ("external_engine_invoked", "false"),
        ],
    );
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
    assert_local_emulator_range_read_fields(&output);
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
    assert!(output.contains(&field(
        "provider_admission_status",
        "blocked_live_provider_no_probe"
    )));
    assert!(output.contains(&field(
        "provider_admission_boundary",
        "blocked_before_credentials_signing_provider_or_network_probe"
    )));
    assert!(output.contains(&field(
        "credential_policy_status",
        "credential_policy_required_not_admitted"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("request_signing_allowed", "false")));
    assert!(output.contains(&field("request_signing_performed", "false")));
    assert!(output.contains(&field("request_signing_status", "blocked_not_invoked")));
    assert!(output.contains(&field(
        "request_signing_boundary",
        "blocked_before_request_signing"
    )));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("object_store_request_count", "0")));
    assert!(output.contains(&field("object_store_bytes_requested", "0")));
    assert!(output.contains(&field("object_store_bytes_read", "0")));
    assert!(output.contains(&field(
        "object_store_bounded_read_status",
        "not_performed_blocked"
    )));
    assert!(output.contains(&field(
        "object_store_request_coalescing_status",
        "not_performed_blocked"
    )));
    assert!(output.contains(&field(
        "object_store_retry_policy_status",
        "blocked_before_retry"
    )));
    assert!(output.contains(&field(
        "object_store_rate_limit_policy_status",
        "blocked_before_rate_limit_policy"
    )));
    assert!(output.contains(&field(
        "object_store_checksum_validation_status",
        "not_emitted_no_object_read"
    )));
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
    assert_public_fixture_range_read_fields(&output);
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
    assert!(output.contains(&field(
        "provider_admission_status",
        "blocked_public_fixture_requirements"
    )));
    assert!(output.contains(&field(
        "provider_admission_classification",
        "blocked_until_vortex_or_shardloom_evidence"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field(
        "request_signing_status",
        "not_required_public_no_credential_fixture"
    )));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
}
