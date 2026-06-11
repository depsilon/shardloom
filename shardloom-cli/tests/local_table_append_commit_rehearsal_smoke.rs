use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn run_local_table_append_commit_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .expect("local-table-append-commit-rehearsal-smoke command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn run_local_table_commit_recovery_json(args: &[String]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .expect("local-table-commit-recovery-smoke command runs");

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
        "{}.shardloom-table-commit.json",
        target.to_string_lossy()
    ))
}

fn temp_case_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shardloom-local-table-append-commit-{name}-{}",
        std::process::id()
    ))
}

#[test]
fn local_table_append_commit_rehearsal_writes_manifest_and_commit_record() {
    let temp_dir = temp_case_dir("commit");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let target = temp_dir.join("committed-manifest.json");
    let args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-commit-001".to_string(),
    ];

    let (success, output, stderr) = run_local_table_append_commit_json(&args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    let manifest = fs::read_to_string(&target).expect("committed manifest");
    assert!(manifest.contains("\"operation\": \"append_only_commit_rehearsal\""));
    assert!(sidecar_path(&target).exists(), "commit record exists");
    assert!(output.contains("\"command\":\"local-table-append-commit-rehearsal-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_table_append_commit_rehearsal_smoke.v1"
    )));
    assert!(output.contains(&field("mode", "local_table_append_commit_rehearsal_smoke")));
    assert!(output.contains(&field("support_status", "fixture_smoke_only")));
    assert!(output.contains(&field(
        "claim_gate_status",
        "scoped_local_table_append_commit_rehearsal_only"
    )));
    assert!(output.contains(&field("provider_profile", "local-manifest")));
    assert!(output.contains(&field("table_format", "shardloom_local_manifest")));
    assert!(output.contains(&field("base_row_count", "3")));
    assert!(output.contains(&field("append_row_count", "2")));
    assert!(output.contains(&field("effective_row_count", "5")));
    assert!(output.contains(&field("manifest_file_count", "2")));
    assert!(output.contains(&field("manifest_segment_count", "2")));
    assert!(output.contains(&field("write_staging_status", "performed_local_manifest")));
    assert!(output.contains(&field("commit_protocol_status", "committed")));
    assert!(output.contains(&field("commit_status", "committed_local_manifest")));
    assert!(output.contains(&field(
        "table_commit_rehearsal_status",
        "rehearsed_local_manifest_commit"
    )));
    assert!(output.contains(&field("rollback_status", "not_requested")));
    assert!(output.contains(&field("idempotency_key", "orders-table-commit-001")));
    assert!(output.contains(&field("idempotency_status", "caller_supplied")));
    assert!(output.contains(&field("manifest_written", "true")));
    assert!(output.contains(&field("committed_manifest_present", "true")));
    assert!(output.contains(&field("commit_record_present", "true")));
    assert!(output.contains(&field("table_metadata_read_performed", "true")));
    assert!(output.contains(&field("table_metadata_write_performed", "true")));
    assert!(output.contains(&field("manifest_write_performed", "true")));
    assert!(output.contains(&field("commit_rehearsal_performed", "true")));
    assert!(output.contains(&field("commit_execution_performed", "false")));
    assert!(output.contains(&field("table_catalog_commit_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn rollback_after_commit_cleans_manifest_and_record() {
    let temp_dir = temp_case_dir("rollback");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let target = temp_dir.join("committed-manifest.json");
    let args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-rollback-001".to_string(),
        "--rollback-after-commit".to_string(),
    ];

    let (success, output, stderr) = run_local_table_append_commit_json(&args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(!target.exists(), "manifest was rolled back");
    assert!(
        !sidecar_path(&target).exists(),
        "commit record was rolled back"
    );
    assert!(output.contains(&field("table_append_commit_status", "rolled_back")));
    assert!(output.contains(&field("commit_protocol_status", "rolled_back")));
    assert!(output.contains(&field("commit_status", "committed_then_rolled_back")));
    assert!(output.contains(&field(
        "table_commit_rehearsal_status",
        "rehearsed_then_rolled_back"
    )));
    assert!(output.contains(&field(
        "rollback_status",
        "performed_local_manifest_cleanup"
    )));
    assert!(output.contains(&field("cleanup_deleted_count", "2")));
    assert!(output.contains(&field("committed_manifest_present", "false")));
    assert!(output.contains(&field("commit_record_present", "false")));
    assert!(output.contains(&field("table_metadata_write_performed", "true")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn local_table_commit_recovery_replays_manifest_and_commit_record() {
    let temp_dir = temp_case_dir("recovery");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let target = temp_dir.join("committed-manifest.json");
    let commit_args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-001".to_string(),
    ];
    let (commit_success, commit_output, commit_stderr) =
        run_local_table_append_commit_json(&commit_args);
    assert!(
        commit_success,
        "stdout={commit_output} stderr={commit_stderr}"
    );
    assert!(sidecar_path(&target).exists(), "commit sidecar exists");

    let recovery_args = vec![
        "local-table-commit-recovery-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-001".to_string(),
    ];
    let (success, output, stderr) = run_local_table_commit_recovery_json(&recovery_args);

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"local-table-commit-recovery-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.local_table_commit_recovery_smoke.v1"
    )));
    assert!(output.contains(&field("mode", "local_table_commit_recovery_smoke")));
    assert!(output.contains(&field(
        "runtime_enablement",
        "local_table_commit_recovery_replay"
    )));
    assert!(output.contains(&field(
        "claim_gate_status",
        "scoped_local_table_commit_recovery_only"
    )));
    assert!(output.contains(&field("table_commit_recovery_status", "recovered")));
    assert!(output.contains(&field(
        "manifest_replay_status",
        "verified_local_manifest_sidecar"
    )));
    assert!(output.contains(&field(
        "commit_record_replay_status",
        "verified_local_manifest_sidecar"
    )));
    assert!(output.contains(&field("recovery_replay_performed", "true")));
    assert!(output.contains(&field("commit_record_read_performed", "true")));
    assert!(output.contains(&field("manifest_digest_matched", "true")));
    assert!(output.contains(&field("manifest_bytes_matched", "true")));
    assert!(output.contains(&field("correctness_digest_matched", "true")));
    assert!(output.contains(&field(
        "recovered_idempotency_key",
        "orders-table-recovery-001"
    )));
    assert!(output.contains(&field("idempotency_status", "recovered_from_commit_record")));
    assert!(output.contains(&field("table_metadata_read_performed", "true")));
    assert!(output.contains(&field("manifest_write_performed", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("recovery_claim_allowed", "false")));
    assert!(output.contains(&field("exactly_once_claim_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn local_table_commit_recovery_blocks_mismatched_sidecar_without_fallback() {
    let temp_dir = temp_case_dir("recovery-mismatch");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let target = temp_dir.join("committed-manifest.json");
    let commit_args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-mismatch-001".to_string(),
    ];
    let (commit_success, commit_output, commit_stderr) =
        run_local_table_append_commit_json(&commit_args);
    assert!(
        commit_success,
        "stdout={commit_output} stderr={commit_stderr}"
    );
    let sidecar = sidecar_path(&target);
    let mut sidecar_payload = fs::read_to_string(&sidecar).expect("sidecar");
    sidecar_payload = sidecar_payload.replace(
        "orders-table-recovery-mismatch-001",
        "orders-table-recovery-mismatch-other",
    );
    fs::write(&sidecar, sidecar_payload).expect("corrupt sidecar");

    let recovery_args = vec![
        "local-table-commit-recovery-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-mismatch-001".to_string(),
    ];
    let (success, output, stderr) = run_local_table_commit_recovery_json(&recovery_args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_COMMIT_NOT_ATOMIC\""));
    assert!(output.contains(&field(
        "table_commit_recovery_status",
        "blocked_recovery_mismatch"
    )));
    assert!(output.contains(&field("idempotency_status", "recovered_mismatch")));
    assert!(output.contains(&field("manifest_write_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn local_table_commit_recovery_blocks_mismatched_manifest_path_without_fallback() {
    let temp_dir = temp_case_dir("recovery-path-mismatch");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let target = temp_dir.join("committed-manifest.json");
    let commit_args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-path-mismatch-001".to_string(),
    ];
    let (commit_success, commit_output, commit_stderr) =
        run_local_table_append_commit_json(&commit_args);
    assert!(
        commit_success,
        "stdout={commit_output} stderr={commit_stderr}"
    );
    let sidecar = sidecar_path(&target);
    let sidecar_payload = fs::read_to_string(&sidecar).expect("sidecar");
    let recorded_target = target.to_string_lossy().to_string();
    let wrong_target = temp_dir
        .join("other-manifest.json")
        .to_string_lossy()
        .to_string();
    let corrupted_sidecar = sidecar_payload.replace(&recorded_target, &wrong_target);
    fs::write(&sidecar, corrupted_sidecar).expect("corrupt sidecar path");

    let recovery_args = vec![
        "local-table-commit-recovery-smoke".to_string(),
        target.to_string_lossy().into_owned(),
        "--profile".to_string(),
        "local-manifest".to_string(),
        "--idempotency-key".to_string(),
        "orders-table-recovery-path-mismatch-001".to_string(),
    ];
    let (success, output, stderr) = run_local_table_commit_recovery_json(&recovery_args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_COMMIT_NOT_ATOMIC\""));
    assert!(output.contains(&field(
        "table_commit_recovery_status",
        "blocked_recovery_mismatch"
    )));
    assert!(output.contains(&field("commit_record_local_manifest_path_matched", "false")));
    assert!(output.contains("local_manifest_path_mismatch"));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn remote_manifest_target_is_blocked_without_probe_or_write() {
    let args = vec![
        "local-table-append-commit-rehearsal-smoke".to_string(),
        "s3://bucket/table/metadata/v2.json".to_string(),
    ];

    let (success, output, stderr) = run_local_table_append_commit_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field(
        "table_append_commit_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("table_metadata_write_performed", "false")));
    assert!(output.contains(&field("manifest_write_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
}

#[test]
fn remote_commit_recovery_target_is_blocked_without_probe_or_read() {
    let args = vec![
        "local-table-commit-recovery-smoke".to_string(),
        "s3://bucket/table/metadata/v2.json".to_string(),
    ];

    let (success, output, stderr) = run_local_table_commit_recovery_json(&args);

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains(&field(
        "table_commit_recovery_status",
        "blocked_remote_provider"
    )));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("provider_probe_performed", "false")));
    assert!(output.contains(&field("table_metadata_read_performed", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
}
