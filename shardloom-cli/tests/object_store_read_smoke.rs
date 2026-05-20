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
