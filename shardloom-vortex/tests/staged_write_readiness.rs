#![cfg(feature = "vortex-staged-output-fs")]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use shardloom_vortex::{
    VortexCommitMarkerFileRef, VortexStagedManifestFileRef, VortexStagedMarkerRequest,
    VortexStagedWorkspaceId, VortexStagedWorkspacePath,
};

fn unique_workspace_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("shardloom-staged-write-readiness-{nanos}"))
}

fn run_shardloom_json(args: &[&str]) -> String {
    let output = Command::new("cargo")
        .args([
            "run",
            "-q",
            "-p",
            "shardloom-cli",
            "--features",
            "shardloom-vortex/vortex-staged-output-fs",
            "--",
        ])
        .args(args)
        .arg("--format")
        .arg("json")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "command failed: cargo run -q -p shardloom-cli -- {} --format json\nstdout:{}\nstderr:{}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn assert_json_field(output: &str, key: &str, value: &str) {
    let pair_pattern = format!("\"key\":\"{key}\",\"value\":\"{value}\"");
    assert!(
        output.contains(&pair_pattern),
        "expected JSON output to contain key/value pair {key:?}={value:?}; output:
{output}"
    );
}

fn assert_json_field_false(output: &str, key: &str) {
    assert_json_field(output, key, "false");
}

fn assert_json_field_true(output: &str, key: &str) {
    assert_json_field(output, key, "true");
}

fn assert_common_safety_flags(json: &str) {
    assert_json_field_false(json, "fallback_execution_allowed");
    assert_json_field_false(json, "output_data_written");
    assert_json_field_false(json, "object_store_io");
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn staged_write_readiness_local_smoke_test() {
    let workspace_id = "stage-smoke";
    let workspace_path = unique_workspace_path();
    let workspace_path_str = workspace_path.to_str().unwrap();
    let target_uri = "file://tmp/staged-smoke-target.vortex";

    let workspace_path_ref = VortexStagedWorkspacePath::new(workspace_path_str).unwrap();
    let workspace_id_ref = VortexStagedWorkspaceId::new(workspace_id).unwrap();
    let marker_path = workspace_path.join(
        VortexStagedMarkerRequest::new(workspace_id_ref, workspace_path_ref.clone())
            .marker_file_name(),
    );
    let draft_path = workspace_path.join(
        VortexStagedManifestFileRef::default_for_workspace(workspace_path_ref.clone())
            .file_name()
            .as_str(),
    );
    let commit_marker_path = workspace_path.join(
        VortexCommitMarkerFileRef::default_for_workspace(workspace_path_ref)
            .file_name()
            .as_str(),
    );

    let setup_json = run_shardloom_json(&[
        "vortex-staged-workspace-setup",
        workspace_id,
        workspace_path_str,
        "create-if-missing",
    ]);
    assert_common_safety_flags(&setup_json);
    assert_json_field_false(&setup_json, "marker_written");
    assert_json_field_false(&setup_json, "manifest_written");
    assert!(workspace_path.exists());

    let marker_write_json = run_shardloom_json(&[
        "vortex-staged-marker-write",
        workspace_id,
        workspace_path_str,
        "allow-overwrite",
    ]);
    assert_common_safety_flags(&marker_write_json);
    assert_json_field_false(&marker_write_json, "workspace_created");
    assert_json_field_false(&marker_write_json, "manifest_written");
    assert_json_field_false(&marker_write_json, "upstream_vortex_write_called");
    assert!(marker_path.exists());

    let manifest_plan_json = run_shardloom_json(&[
        "vortex-staged-manifest-file-plan",
        workspace_path_str,
        "draft-ready,workspace-known,marker-written,local-workspace",
    ]);
    assert_common_safety_flags(&manifest_plan_json);
    assert!(manifest_plan_json.contains("\"key\":\"manifest_file_written\",\"value\":\"false\""));
    assert!(manifest_plan_json.contains("\"key\":\"commit_performed\",\"value\":\"false\""));
    assert!(
        manifest_plan_json.contains("\"key\":\"upstream_vortex_write_called\",\"value\":\"false\"")
    );

    let manifest_write_json = run_shardloom_json(&[
        "vortex-staged-manifest-file-write",
        workspace_path_str,
        "file-plan-ready,workspace-known,feature-gate-enabled",
        "allow-overwrite",
    ]);
    assert_common_safety_flags(&manifest_write_json);
    assert_json_field_true(&manifest_write_json, "draft_file_written");
    assert!(manifest_write_json.contains("\"key\":\"commit_performed\",\"value\":\"false\""));
    assert!(
        manifest_write_json
            .contains("\"key\":\"upstream_vortex_write_called\",\"value\":\"false\"")
    );
    assert!(draft_path.exists());

    let commit_intent_json = run_shardloom_json(&[
        "vortex-commit-intent-plan",
        target_uri,
        "commit-requested,staged-manifest-draft-written,manifest-finalization-available,commit-protocol-available,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,recovery-ready,retry-gate-open,cancellation-gate-open,feature-gate-enabled",
    ]);
    assert_common_safety_flags(&commit_intent_json);
    assert_json_field_false(&commit_intent_json, "manifest_finalized");
    assert_json_field_false(&commit_intent_json, "manifest_committed");
    assert_json_field_false(&commit_intent_json, "recovery_action_executed");
    assert_json_field_false(&commit_intent_json, "commit_execution_allowed");

    let commit_protocol_json = run_shardloom_json(&[
        "vortex-commit-protocol-plan",
        target_uri,
        "awaiting-commit-marker",
        "mark-commit-ready",
        "commit-intent-ready,draft-manifest-ready,manifest-finalization-available,commit-marker-available,recovery-ready,feature-gate-enabled",
    ]);
    assert_common_safety_flags(&commit_protocol_json);
    assert_json_field_false(&commit_protocol_json, "manifest_finalized");
    assert_json_field_false(&commit_protocol_json, "manifest_committed");
    assert_json_field_false(&commit_protocol_json, "commit_marker_written");
    assert_json_field_false(&commit_protocol_json, "commit_execution_allowed");
    assert_json_field_false(&commit_protocol_json, "recovery_action_executed");

    let commit_marker_plan_json = run_shardloom_json(&[
        "vortex-commit-marker-plan",
        workspace_path_str,
        "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled",
    ]);
    assert_common_safety_flags(&commit_marker_plan_json);
    assert_json_field_false(&commit_marker_plan_json, "manifest_finalized");
    assert_json_field_false(&commit_marker_plan_json, "manifest_committed");
    assert_json_field_false(&commit_marker_plan_json, "commit_marker_written");
    assert_json_field_false(&commit_marker_plan_json, "recovery_action_executed");
    assert!(!commit_marker_path.exists());

    let commit_marker_write_json = run_shardloom_json(&[
        "vortex-commit-marker-write",
        workspace_path_str,
        "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled",
        "allow-overwrite",
    ]);
    assert_common_safety_flags(&commit_marker_write_json);
    assert_json_field_false(&commit_marker_write_json, "manifest_finalized");
    assert_json_field_false(&commit_marker_write_json, "manifest_committed");
    assert_json_field_true(&commit_marker_write_json, "commit_marker_written");
    assert_json_field_false(&commit_marker_write_json, "recovery_action_executed");
    assert_json_field_false(&commit_marker_write_json, "upstream_vortex_write_called");

    assert!(workspace_path.exists());
    assert!(marker_path.exists());
    assert!(draft_path.exists());
    assert!(commit_marker_path.exists());
    assert!(!workspace_path.join(".shardloom-output-data").exists());
    assert!(
        !workspace_path
            .join(".shardloom-committed-manifest")
            .exists()
    );
    assert!(
        !workspace_path
            .join(".shardloom-manifest-finalized")
            .exists()
    );

    remove_if_exists(&commit_marker_path);
    remove_if_exists(&draft_path);
    remove_if_exists(&marker_path);
    fs::remove_dir(&workspace_path).unwrap();
}
