use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn run_object_store_write_smoke_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .expect("object-store-write-smoke command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn run_object_store_write_recovery_smoke_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .expect("object-store-write-recovery-smoke command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn sidecar_path(target: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.shardloom-commit.json",
        target.to_string_lossy()
    ))
}

fn temp_case_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shardloom-object-store-write-smoke-{name}-{}",
        std::process::id()
    ))
}

#[test]
fn local_emulator_write_commits_payload_and_manifest_evidence() {
    let temp_dir = temp_case_dir("commit");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"object payload").expect("fixture write");
    let args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "orders-batch-001".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_smoke_json(&args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert_eq!(
        fs::read(&target).expect("target payload"),
        b"object payload"
    );
    assert!(sidecar_path(&target).exists(), "commit manifest exists");
    assert!(output.contains("\"command\":\"object-store-write-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_write_smoke.v1"
    )));
    assert!(output.contains(&field("provider_profile", "local-emulator")));
    assert!(output.contains(&field("object_store_write_status", "committed")));
    assert!(output.contains(&field("write_staging_status", "performed_local_emulator")));
    assert!(output.contains(&field("commit_protocol_status", "committed")));
    assert!(output.contains(&field("commit_status", "committed_local_emulator_object")));
    assert!(output.contains(&field("rollback_status", "not_requested")));
    assert!(output.contains(&field("idempotency_key", "orders-batch-001")));
    assert!(output.contains(&field("idempotency_status", "caller_supplied")));
    assert!(output.contains(&field("object_store_io", "true")));
    assert!(output.contains(&field("object_store_write_io", "true")));
    assert!(output.contains(&field("write_io", "true")));
    assert!(output.contains(&field("table_commit_allowed", "false")));
    assert!(output.contains(&field("native_io_certificate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn local_emulator_write_recovery_replays_commit_manifest_evidence() {
    let temp_dir = temp_case_dir("recovery-success");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"recoverable payload").expect("fixture write");
    let write_args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "recover-001".to_string(),
    ];
    let (write_success, write_output, write_stderr) =
        run_object_store_write_smoke_json(&write_args);
    assert!(write_success, "stdout={write_output} stderr={write_stderr}");

    let recovery_args = vec![
        "object-store-write-recovery-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "recover-001".to_string(),
    ];
    let (success, output, stderr) = run_object_store_write_recovery_smoke_json(&recovery_args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-write-recovery-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_write_recovery_smoke.v1"
    )));
    assert!(output.contains(&field("mode", "object_store_write_recovery_smoke")));
    assert!(output.contains(&field("object_store_write_recovery_status", "recovered")));
    assert!(output.contains(&field(
        "recovery_replay_status",
        "recovered_local_emulator_sidecar"
    )));
    assert!(output.contains(&field(
        "commit_manifest_replay_status",
        "recovered_local_emulator_sidecar"
    )));
    assert!(output.contains(&field("target_digest_matched", "true")));
    assert!(output.contains(&field("payload_digest_matched", "true")));
    assert!(output.contains(&field("payload_bytes_matched", "true")));
    assert!(output.contains(&field("target_path_matched", "true")));
    assert!(output.contains(&field("commit_manifest_shape_matched", "true")));
    assert!(output.contains(&field("expected_idempotency_key", "recover-001")));
    assert!(output.contains(&field("recovered_idempotency_key", "recover-001")));
    assert!(output.contains(&field(
        "idempotency_status",
        "recovered_from_commit_manifest"
    )));
    assert!(output.contains(&field("native_io_certificate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(output.contains(&field("object_store_io", "true")));
    assert!(output.contains(&field("object_store_read_io", "true")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("table_commit_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn write_recovery_blocks_tampered_object_without_fallback() {
    let temp_dir = temp_case_dir("recovery-tamper");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"original payload").expect("fixture write");
    let write_args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "tamper-001".to_string(),
    ];
    let (write_success, write_output, write_stderr) =
        run_object_store_write_smoke_json(&write_args);
    assert!(write_success, "stdout={write_output} stderr={write_stderr}");
    fs::write(&target, b"tampered payload").expect("tamper target object");

    let recovery_args = vec![
        "object-store-write-recovery-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "tamper-001".to_string(),
    ];
    let (success, output, stderr) = run_object_store_write_recovery_smoke_json(&recovery_args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_COMMIT_NOT_ATOMIC\""));
    assert!(output.contains("target_digest_mismatch"));
    assert!(output.contains(&field(
        "object_store_write_recovery_status",
        "blocked_recovery_mismatch"
    )));
    assert!(output.contains(&field("target_digest_matched", "false")));
    assert!(output.contains(&field("payload_digest_matched", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("object_store_read_io", "false")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn remote_recovery_target_is_blocked_without_probe_or_read() {
    let args = vec![
        "object-store-write-recovery-smoke".to_string(),
        "s3://bucket/object.bin".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_recovery_smoke_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field(
        "object_store_write_recovery_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("object_store_read_io", "false")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
}

#[test]
fn rollback_after_commit_cleans_target_and_manifest_with_evidence() {
    let temp_dir = temp_case_dir("rollback");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"rollback payload").expect("fixture write");
    let args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--idempotency-key".to_string(),
        "rollback-001".to_string(),
        "--rollback-after-commit".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_smoke_json(&args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(!target.exists(), "target object was rolled back");
    assert!(!sidecar_path(&target).exists(), "manifest was rolled back");
    assert!(output.contains(&field("object_store_write_status", "rolled_back")));
    assert!(output.contains(&field("commit_protocol_status", "rolled_back")));
    assert!(output.contains(&field("commit_status", "committed_then_rolled_back")));
    assert!(output.contains(&field(
        "rollback_status",
        "performed_local_emulator_cleanup"
    )));
    assert!(output.contains(&field("cleanup_deleted_count", "2")));
    assert!(output.contains(&field("commit_manifest_present", "false")));
    assert!(output.contains(&field("target_exists_after_commit", "false")));
    assert!(output.contains(&field("object_store_write_io", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn no_overwrite_rejects_existing_target_without_clobbering_payload() {
    let temp_dir = temp_case_dir("no-overwrite-existing");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"new payload").expect("fixture write");
    fs::write(&target, b"original payload").expect("target write");
    let args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_smoke_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert_eq!(
        fs::read(&target).expect("target payload"),
        b"original payload"
    );
    assert!(output.contains(&field("object_store_write_status", "blocked_target_exists")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn allow_overwrite_replaces_existing_target_after_staging() {
    let temp_dir = temp_case_dir("overwrite-existing");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    let target = temp_dir.join("object.bin");
    fs::write(&source, b"replacement payload").expect("fixture write");
    fs::write(&target, b"original payload").expect("target write");
    let args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-emulator".to_string(),
        "--allow-overwrite".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_smoke_json(&args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert_eq!(
        fs::read(&target).expect("target payload"),
        b"replacement payload"
    );
    assert!(sidecar_path(&target).exists(), "commit manifest exists");
    assert!(output.contains(&field("object_store_write_status", "committed")));
    assert!(output.contains(&field("allow_overwrite", "true")));
    assert!(output.contains(&field("commit_status", "committed_local_emulator_object")));
    assert!(output.contains(&field("object_store_write_io", "true")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn remote_target_is_blocked_without_write_or_probe() {
    let temp_dir = temp_case_dir("remote");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let source = temp_dir.join("payload.bin");
    fs::write(&source, b"remote payload").expect("fixture write");
    let args = vec![
        "object-store-write-smoke".to_string(),
        source.to_string_lossy().into_owned(),
        "s3://bucket/object.bin".to_string(),
    ];

    let (success, output, stderr) = run_object_store_write_smoke_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field(
        "object_store_write_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("object_store_write_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}
