#![cfg(feature = "vortex-staged-output-fs")]

use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

use shardloom_core::DatasetUri;
use shardloom_vortex::{
    VortexCommitIntentRequest, VortexCommitIntentStatus, VortexCommitProtocolRequest,
    VortexCommitProtocolState, VortexCommitProtocolStatus, VortexCommitProtocolTransition,
    VortexStagedManifestDraftContent, VortexStagedManifestFileRef, VortexStagedManifestFileRequest,
    VortexStagedWorkspaceId, VortexStagedWorkspacePath, VortexStagedWorkspaceSetupRequest,
    plan_vortex_commit_intent, plan_vortex_commit_protocol, plan_vortex_staged_manifest_file,
    setup_vortex_staged_workspace, write_vortex_staged_manifest_file, write_vortex_staged_marker,
};

fn unique_workspace_path() -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("shardloom-staged-write-readiness-{nanos}"))
}

#[test]
#[allow(clippy::too_many_lines)]
fn staged_write_readiness_local_smoke_test() {
    let workspace_id = VortexStagedWorkspaceId::new("stage-smoke").unwrap();
    let workspace_path_buf = unique_workspace_path();
    let workspace_path =
        VortexStagedWorkspacePath::new(workspace_path_buf.to_str().unwrap()).unwrap();

    let setup = setup_vortex_staged_workspace(
        VortexStagedWorkspaceSetupRequest::new(workspace_id.clone(), workspace_path.clone())
            .create_if_missing(true),
    )
    .unwrap();
    assert!(
        setup
            .to_human_text()
            .contains("fallback execution: disabled")
    );

    let marker_request =
        shardloom_vortex::VortexStagedMarkerRequest::new(workspace_id, workspace_path.clone())
            .allow_overwrite(true);
    let marker_path = workspace_path_buf.join(marker_request.marker_file_name());
    let marker = write_vortex_staged_marker(marker_request).unwrap();
    assert!(marker.marker_written());

    let file_ref = VortexStagedManifestFileRef::default_for_workspace(workspace_path.clone());
    let draft_content = VortexStagedManifestDraftContent::new(
        "shardloom_staged_manifest_draft=true\noutput_data_written=false\n",
    )
    .unwrap();
    let manifest_plan = plan_vortex_staged_manifest_file(
        VortexStagedManifestFileRequest::new(file_ref.clone(), draft_content.clone())
            .draft_ready(true)
            .workspace_known(true)
            .marker_written(true)
            .local_workspace(true),
    )
    .unwrap();
    assert!(
        manifest_plan
            .to_human_text()
            .contains("output data written: false")
    );

    let draft_path = workspace_path_buf.join(file_ref.file_name().as_str());
    let manifest_write = write_vortex_staged_manifest_file(
        shardloom_vortex::VortexStagedManifestFileWriteRequest::new(file_ref, draft_content)
            .file_plan_ready(true)
            .workspace_known(true)
            .feature_gate_enabled(true)
            .allow_overwrite(true),
    )
    .unwrap();
    assert!(manifest_write.draft_file_written());

    let target_uri = DatasetUri::new("file://tmp/staged-smoke-target.vortex").unwrap();
    let commit_intent = plan_vortex_commit_intent(
        VortexCommitIntentRequest::new(target_uri.clone())
            .commit_requested(true)
            .staged_manifest_draft_written(true)
            .manifest_finalization_available(true)
            .commit_protocol_available(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
            .recovery_ready(true)
            .retry_gate_open(true)
            .cancellation_gate_open(true)
            .feature_gate_enabled(true),
    )
    .unwrap();
    assert_eq!(commit_intent.status, VortexCommitIntentStatus::CommitReady);
    assert!(!commit_intent.has_errors());
    assert!(
        commit_intent
            .to_human_text()
            .contains("manifest committed: false")
    );

    let commit_protocol = plan_vortex_commit_protocol(
        VortexCommitProtocolRequest::new(
            target_uri,
            VortexCommitProtocolState::AwaitingCommitMarker,
            VortexCommitProtocolTransition::MarkCommitReady,
        )
        .commit_intent_ready(true)
        .draft_manifest_ready(true)
        .manifest_finalization_available(true)
        .commit_marker_available(true)
        .recovery_ready(true)
        .feature_gate_enabled(true),
    )
    .unwrap();
    assert_eq!(
        commit_protocol.status,
        VortexCommitProtocolStatus::TransitionAllowed
    );
    assert!(commit_protocol.transition_allowed());
    assert_eq!(
        commit_protocol.next_state(),
        VortexCommitProtocolState::CommitReady
    );
    assert!(!commit_protocol.has_errors());
    assert!(
        commit_protocol
            .to_human_text()
            .contains("commit marker written: false")
    );

    assert!(workspace_path_buf.exists());
    assert!(marker_path.exists());
    assert!(draft_path.exists());
    assert!(!workspace_path_buf.join(".shardloom-output-data").exists());
    assert!(
        !workspace_path_buf
            .join(".shardloom-committed-manifest")
            .exists()
    );
    assert!(!workspace_path_buf.join(".shardloom-commit-marker").exists());

    fs::remove_file(draft_path).unwrap();
    fs::remove_file(marker_path).unwrap();
    fs::remove_dir(workspace_path_buf).unwrap();
}
