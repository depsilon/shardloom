//! Vortex output, commit, and staged artifact CLI handlers.
//!
//! These handlers preserve the existing Vortex-native output/write planning,
//! commit, and staged-artifact command contracts. Report-only paths remain
//! side-effect-free; narrow artifact helpers stay feature-gated and keep
//! fallback execution disabled.

use std::{process::ExitCode, time::Instant};

use shardloom_core::{CommandStatus, DatasetUri, OutputFormat, ShardLoomError};
use shardloom_vortex::{
    VortexCommitIntentReport, VortexCommitIntentRequest, VortexCommitMarkerContent,
    VortexCommitMarkerFileName, VortexCommitMarkerFileRef, VortexCommitMarkerRequest,
    VortexCommitProtocolReport, VortexCommitProtocolRequest, VortexFinalizedManifestFileName,
    VortexFinalizedManifestFileRef, VortexLocalCommitExecutionRequest,
    VortexLocalCommitRecoveryRequest, VortexManifestFinalizationRequest,
    VortexManifestFinalizationSignal, VortexNativeOutputPayloadWriteReport,
    VortexOutputPayloadContentDescriptor, VortexOutputPayloadFileName, VortexOutputPayloadFileRef,
    VortexOutputPayloadReport, VortexOutputPayloadRequest, VortexOutputPayloadSignal,
    VortexStagedManifestFileEffect, VortexStagedManifestFileRef, VortexStagedManifestFileReport,
    VortexStagedManifestFileRequest, VortexStagedManifestFileWriteEffect,
    VortexStagedManifestFileWriteOption, VortexStagedManifestFileWriteRequest,
    VortexStagedMarkerOption, VortexStagedMarkerRequest, VortexStagedWorkspaceId,
    VortexStagedWorkspacePath, VortexStagedWorkspaceSetupOption, VortexStagedWorkspaceSetupRequest,
    VortexWriteIntentReport, VortexWriteIntentRequest, VortexWriteIntentSignal,
    commit_marker_write_request_from_plan, execute_vortex_local_commit,
    execute_vortex_local_commit_rollback, finalized_manifest_artifact_write_request_from_plan,
    native_output_payload_write_request_from_plan, output_payload_artifact_write_request_from_plan,
    plan_vortex_commit_intent, plan_vortex_commit_marker, plan_vortex_commit_protocol,
    plan_vortex_local_commit_recovery, plan_vortex_manifest_finalization,
    plan_vortex_output_payload, plan_vortex_staged_manifest_file, plan_vortex_write_intent,
    setup_vortex_staged_workspace, vortex_local_commit_execution_feature_enabled,
    vortex_native_output_payload_write_feature_enabled, write_vortex_commit_marker,
    write_vortex_finalized_manifest_artifact, write_vortex_native_count_output_payload,
    write_vortex_output_payload_artifact, write_vortex_staged_manifest_file,
    write_vortex_staged_marker,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_time::{duration_micros, micros_to_millis},
    finalized_manifest_cli_content, parse_vortex_commit_intent_signals,
    parse_vortex_commit_marker_signals, parse_vortex_commit_marker_write_options,
    parse_vortex_commit_protocol_signals, parse_vortex_commit_protocol_state,
    parse_vortex_commit_protocol_transition,
    parse_vortex_finalized_manifest_artifact_write_options,
    parse_vortex_local_commit_execution_signals, parse_vortex_local_commit_recovery_signals,
    parse_vortex_manifest_finalization_signals, parse_vortex_output_payload_artifact_write_options,
    parse_vortex_output_payload_signals, parse_vortex_staged_manifest_file_signals,
    parse_vortex_staged_manifest_file_write_options,
    parse_vortex_staged_manifest_file_write_signals, parse_vortex_staged_marker_options,
    parse_vortex_staged_workspace_options, staged_manifest_cli_draft_content,
    vortex_staged_marker_fields,
};

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_write_intent_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-write-intent-plan <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-write-intent-plan <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(target_uri.clone()) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let mut req = VortexWriteIntentRequest::new(uri);
    for token in signals_raw.split(',').filter(|s| !s.trim().is_empty()) {
        match token.trim() {
            "native-vortex-target" => {
                req.add_signal(VortexWriteIntentSignal::TargetIsNativeVortex, true);
            }
            "staged-output-required" => {
                req.add_signal(VortexWriteIntentSignal::StagedOutputRequired, true);
            }
            "schema-known" => req.add_signal(VortexWriteIntentSignal::SchemaKnown, true),
            "schema-compatible" => {
                req.add_signal(VortexWriteIntentSignal::SchemaCompatible, true);
            }
            "delete-semantics-known" => {
                req.add_signal(VortexWriteIntentSignal::DeleteSemanticsKnown, true);
            }
            "tombstone-semantics-known" => {
                req.add_signal(VortexWriteIntentSignal::TombstoneSemanticsKnown, true);
            }
            "commit-protocol-available" => {
                req.add_signal(VortexWriteIntentSignal::CommitProtocolAvailable, true);
            }
            "object-store-target" => {
                req.add_signal(VortexWriteIntentSignal::ObjectStoreTarget, true);
            }
            "upstream-vortex-write-feature-enabled" => req.add_signal(
                VortexWriteIntentSignal::UpstreamVortexWriteFeatureEnabled,
                true,
            ),
            other => {
                eprintln!("unknown signal token: {other}");
                return ExitCode::from(2);
            }
        }
    }
    let report: VortexWriteIntentReport = match plan_vortex_write_intent(req) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("failed to plan write intent: {error}");
            return ExitCode::from(1);
        }
    };
    emit(
        "vortex-write-intent-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex write intent plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_write_intent_plan".to_string()),
            ("target_uri".to_string(), target_uri),
            (
                "target_is_native_vortex".to_string(),
                report.target_is_native_vortex().to_string(),
            ),
            (
                "staged_output_required".to_string(),
                report.staged_output_required().to_string(),
            ),
            (
                "schema_known".to_string(),
                report.schema_known().to_string(),
            ),
            (
                "schema_compatible".to_string(),
                report.schema_compatible().to_string(),
            ),
            (
                "delete_semantics_known".to_string(),
                report.delete_semantics_known().to_string(),
            ),
            (
                "tombstone_semantics_known".to_string(),
                report.tombstone_semantics_known().to_string(),
            ),
            (
                "commit_protocol_available".to_string(),
                report.commit_protocol_available().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            ("output_data_written".to_string(), "false".to_string()),
            ("manifest_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("write_execution_allowed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_commit_intent_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom vortex-commit-intent-plan <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-commit-intent-plan <target_uri> <signals>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(target_uri.clone()) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let signals = match parse_vortex_commit_intent_signals(&signals_raw) {
        Ok(s) => s,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let mut request = VortexCommitIntentRequest::new(uri);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report: VortexCommitIntentReport = match plan_vortex_commit_intent(request) {
        Ok(r) => r,
        Err(error) => {
            eprintln!("failed to plan commit intent: {error}");
            return ExitCode::from(1);
        }
    };
    emit(
        "vortex-commit-intent-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex commit intent plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_commit_intent_plan".to_string()),
            (
                "commit_requested".to_string(),
                report.commit_requested().to_string(),
            ),
            (
                "staged_manifest_draft_written".to_string(),
                report.staged_manifest_draft_written().to_string(),
            ),
            (
                "manifest_finalization_available".to_string(),
                report.manifest_finalization_available().to_string(),
            ),
            (
                "commit_protocol_available".to_string(),
                report.commit_protocol_available().to_string(),
            ),
            (
                "recovery_ready".to_string(),
                report.recovery_ready().to_string(),
            ),
            (
                "retry_gate_open".to_string(),
                report.retry_gate_open().to_string(),
            ),
            (
                "cancellation_gate_open".to_string(),
                report.cancellation_gate_open().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            ("manifest_committed".to_string(), "false".to_string()),
            ("manifest_finalized".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            ("commit_execution_allowed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_manifest_finalization_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-manifest-finalization-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-manifest-finalization-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-manifest-finalization-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-manifest-finalization-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-manifest-finalization-plan",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_manifest_finalization_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-manifest-finalization-plan",
                format,
                "invalid manifest finalization signals",
                &error,
            );
        }
    };
    let _manifest_name = VortexFinalizedManifestFileName::default_finalized();
    let file_ref = VortexFinalizedManifestFileRef::default_for_workspace(workspace_path);
    let content = match finalized_manifest_cli_content(false) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-manifest-finalization-plan",
                format,
                "invalid finalized manifest content",
                &error,
            );
        }
    };
    let mut request = VortexManifestFinalizationRequest::new(target_uri, file_ref, content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report = match plan_vortex_manifest_finalization(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-manifest-finalization-plan",
                format,
                "manifest finalization planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-manifest-finalization-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex manifest finalization plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_manifest_finalization_plan".to_string(),
            ),
            (
                "draft_manifest_written".to_string(),
                report.draft_manifest_written().to_string(),
            ),
            (
                "commit_marker_written".to_string(),
                report.commit_marker_written().to_string(),
            ),
            (
                "commit_protocol_ready".to_string(),
                report.commit_protocol_ready().to_string(),
            ),
            (
                "schema_known".to_string(),
                report.schema_known().to_string(),
            ),
            (
                "schema_compatible".to_string(),
                report.schema_compatible().to_string(),
            ),
            (
                "delete_semantics_known".to_string(),
                report.delete_semantics_known().to_string(),
            ),
            (
                "tombstone_semantics_known".to_string(),
                report.tombstone_semantics_known().to_string(),
            ),
            (
                "local_workspace".to_string(),
                report.local_workspace().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            (
                "feature_gate_enabled".to_string(),
                report
                    .request
                    .has_signal(VortexManifestFinalizationSignal::FeatureGateEnabled)
                    .to_string(),
            ),
            (
                "finalized_manifest_written".to_string(),
                "false".to_string(),
            ),
            ("manifest_committed".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            (
                "finalization_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_output_payload_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-plan",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_output_payload_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-plan",
                format,
                "invalid output payload signals",
                &error,
            );
        }
    };
    let payload_name = VortexOutputPayloadFileName::default_payload();
    let payload_ref = VortexOutputPayloadFileRef::default_for_workspace(workspace_path);
    let payload_content =
        match VortexOutputPayloadContentDescriptor::synthetic_placeholder(payload_name.as_str()) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    "vortex-output-payload-plan",
                    format,
                    "invalid output payload content",
                    &error,
                );
            }
        };
    let mut request = VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report: VortexOutputPayloadReport = match plan_vortex_output_payload(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-plan",
                format,
                "output payload planning failed",
                &error,
            );
        }
    };
    let text = match report.to_human_text() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-plan",
                format,
                "failed to render output payload report",
                &error,
            );
        }
    };
    emit(
        "vortex-output-payload-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex output payload plan".to_string(),
        text,
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_output_payload_plan".to_string()),
            (
                "write_intent_ready".to_string(),
                report.write_intent_ready().to_string(),
            ),
            (
                "staged_output_ready".to_string(),
                report.staged_output_ready().to_string(),
            ),
            (
                "finalized_manifest_ready".to_string(),
                report.finalized_manifest_ready().to_string(),
            ),
            (
                "payload_content_available".to_string(),
                report.payload_content_available().to_string(),
            ),
            (
                "local_workspace".to_string(),
                report.local_workspace().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            (
                "upstream_vortex_write_required".to_string(),
                report.upstream_vortex_write_required().to_string(),
            ),
            (
                "feature_gate_enabled".to_string(),
                report
                    .request
                    .has_signal(VortexOutputPayloadSignal::FeatureGateEnabled)
                    .to_string(),
            ),
            ("output_payload_written".to_string(), "false".to_string()),
            ("vortex_file_written".to_string(), "false".to_string()),
            ("manifest_written".to_string(), "false".to_string()),
            ("manifest_committed".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            ("payload_write_allowed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_finalized_manifest_artifact_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-finalized-manifest-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-finalized-manifest-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-finalized-manifest-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-finalized-manifest-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_manifest_finalization_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "invalid manifest finalization signals",
                &error,
            );
        }
    };
    let options = match parse_vortex_finalized_manifest_artifact_write_options(&options_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "invalid finalized manifest artifact write options",
                &error,
            );
        }
    };
    let file_ref = VortexFinalizedManifestFileRef::default_for_workspace(workspace_path);
    let content = match finalized_manifest_cli_content(true) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "invalid finalized manifest content",
                &error,
            );
        }
    };
    let mut request = VortexManifestFinalizationRequest::new(target_uri, file_ref, content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let plan_report = match plan_vortex_manifest_finalization(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "manifest finalization planning failed",
                &error,
            );
        }
    };
    let mut write_request = match finalized_manifest_artifact_write_request_from_plan(&plan_report)
    {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "failed to build finalized manifest artifact write request",
                &error,
            );
        }
    };
    write_request.options = options;
    let write_report = match write_vortex_finalized_manifest_artifact(write_request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-finalized-manifest-artifact-write",
                format,
                "finalized manifest artifact write failed",
                &error,
            );
        }
    };
    emit(
        "vortex-finalized-manifest-artifact-write",
        format,
        if write_report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex finalized manifest artifact write".to_string(),
        write_report.to_human_text(),
        write_report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_finalized_manifest_artifact_write".to_string(),
            ),
            (
                "finalized_manifest_artifact_written".to_string(),
                write_report
                    .finalized_manifest_artifact_written()
                    .to_string(),
            ),
            (
                "finalized_manifest_written".to_string(),
                write_report.finalized_manifest_written().to_string(),
            ),
            ("manifest_committed".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            (
                "bytes_written".to_string(),
                write_report.bytes_written.to_string(),
            ),
            (
                "checksum".to_string(),
                write_report
                    .checksum
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
            ),
            (
                "execution".to_string(),
                "finalized_manifest_artifact_write_or_not_performed".to_string(),
            ),
        ],
    );
    if write_report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_output_payload_artifact_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-output-payload-artifact-write <target_uri> <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_output_payload_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "invalid output payload signals",
                &error,
            );
        }
    };
    let allow_overwrite = match parse_vortex_output_payload_artifact_write_options(&options_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "invalid output payload artifact write options",
                &error,
            );
        }
    };
    let payload_name = VortexOutputPayloadFileName::default_payload();
    let payload_ref = VortexOutputPayloadFileRef::default_for_workspace(workspace_path);
    let payload_content =
        match VortexOutputPayloadContentDescriptor::synthetic_placeholder(payload_name.as_str()) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    "vortex-output-payload-artifact-write",
                    format,
                    "invalid output payload content",
                    &error,
                );
            }
        };
    let mut request = VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let plan = match plan_vortex_output_payload(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "output payload planning failed",
                &error,
            );
        }
    };
    let mut write_request = match output_payload_artifact_write_request_from_plan(&plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "output payload artifact write request conversion failed",
                &error,
            );
        }
    };
    write_request = write_request.allow_overwrite(allow_overwrite);
    let report = match write_vortex_output_payload_artifact(write_request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "output payload artifact write failed",
                &error,
            );
        }
    };
    let text = match report.to_human_text() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-output-payload-artifact-write",
                format,
                "failed to render output payload artifact write report",
                &error,
            );
        }
    };
    emit(
        "vortex-output-payload-artifact-write",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex output payload artifact write".to_string(),
        text,
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_output_payload_artifact_write".to_string(),
            ),
            (
                "output_payload_artifact_written".to_string(),
                report.output_payload_artifact_written().to_string(),
            ),
            (
                "output_payload_written".to_string(),
                report.output_payload_written().to_string(),
            ),
            ("vortex_file_written".to_string(), "false".to_string()),
            ("manifest_written".to_string(), "false".to_string()),
            ("manifest_committed".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            (
                "bytes_written".to_string(),
                if report.bytes_written == 0 {
                    "none".to_string()
                } else {
                    report.bytes_written.to_string()
                },
            ),
            (
                "checksum".to_string(),
                report
                    .checksum
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
            ),
            (
                "execution".to_string(),
                "output_payload_artifact_write_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_native_count_payload_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-native-count-payload-write <target_uri> <workspace_path> <count_result> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-native-count-payload-write <target_uri> <workspace_path> <count_result> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(count_result_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-native-count-payload-write <target_uri> <workspace_path> <count_result> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-native-count-payload-write <target_uri> <workspace_path> <count_result> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-native-count-payload-write <target_uri> <workspace_path> <count_result> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let count_result = match count_result_raw.parse::<u64>() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "invalid count result",
                &ShardLoomError::InvalidOperation(format!(
                    "count_result must be an unsigned integer: {error}"
                )),
            );
        }
    };
    let signals = match parse_vortex_output_payload_signals(&signals_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "invalid output payload signals",
                &error,
            );
        }
    };
    let allow_overwrite = match parse_vortex_output_payload_artifact_write_options(&options_raw) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "invalid native output payload write options",
                &error,
            );
        }
    };
    let payload_ref = VortexOutputPayloadFileRef::default_for_workspace(workspace_path);
    let payload_content =
        match VortexOutputPayloadContentDescriptor::native_vortex_count_result(count_result) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    "vortex-native-count-payload-write",
                    format,
                    "invalid native output payload content",
                    &error,
                );
            }
        };
    let mut request = VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let plan = match plan_vortex_output_payload(request) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "native output payload planning failed",
                &error,
            );
        }
    };
    let mut write_request = match native_output_payload_write_request_from_plan(&plan, count_result)
    {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "native output payload write request conversion failed",
                &error,
            );
        }
    };
    write_request = write_request.allow_overwrite(allow_overwrite);
    let report: VortexNativeOutputPayloadWriteReport =
        match write_vortex_native_count_output_payload(write_request) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(
                    "vortex-native-count-payload-write",
                    format,
                    "native count output payload write failed",
                    &error,
                );
            }
        };
    let text = match report.to_human_text() {
        Ok(v) => v,
        Err(error) => {
            return emit_error(
                "vortex-native-count-payload-write",
                format,
                "failed to render native output payload write report",
                &error,
            );
        }
    };
    emit(
        "vortex-native-count-payload-write",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex native count output payload write".to_string(),
        text,
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_native_count_payload_write".to_string(),
            ),
            (
                "feature_enabled".to_string(),
                vortex_native_output_payload_write_feature_enabled().to_string(),
            ),
            (
                "native_vortex_payload_written".to_string(),
                report.native_vortex_payload_written().to_string(),
            ),
            (
                "output_payload_written".to_string(),
                report.output_payload_written().to_string(),
            ),
            (
                "vortex_file_written".to_string(),
                report.vortex_file_written().to_string(),
            ),
            (
                "manifest_written".to_string(),
                report.manifest_written().to_string(),
            ),
            (
                "manifest_committed".to_string(),
                report.manifest_committed().to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                report.upstream_vortex_write_called().to_string(),
            ),
            (
                "recovery_action_executed".to_string(),
                report.recovery_action_executed().to_string(),
            ),
            (
                "bytes_written".to_string(),
                report.bytes_written.to_string(),
            ),
            (
                "logical_rows_written".to_string(),
                report.logical_rows_written.to_string(),
            ),
            (
                "count_result_written".to_string(),
                report
                    .count_result_written
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
            ),
            (
                "checksum".to_string(),
                report
                    .checksum
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
            ),
            (
                "execution".to_string(),
                "native_count_payload_write_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_commit_marker_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-commit-marker-plan <workspace_path> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-commit-marker-plan <workspace_path> <signals>");
        return ExitCode::from(2);
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("invalid staged workspace path: {error}");
            return ExitCode::from(2);
        }
    };
    let signals = match parse_vortex_commit_marker_signals(&signals_raw) {
        Ok(s) => s,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let marker_ref = VortexCommitMarkerFileRef::default_for_workspace(workspace_path);
    let _marker_name = VortexCommitMarkerFileName::default_marker();
    let marker_content = match VortexCommitMarkerContent::new(
        "shardloom_commit_marker_draft=true\ncli_plan=true\ncommit_marker_written=false\nmanifest_finalized=false\nmanifest_committed=false\noutput_data_written=false\nfallback_execution_allowed=false\n",
    ) {
        Ok(content) => content,
        Err(error) => {
            eprintln!("failed to construct commit marker content: {error}");
            return ExitCode::from(1);
        }
    };
    let mut request = VortexCommitMarkerRequest::new(marker_ref, marker_content);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report = match plan_vortex_commit_marker(request) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("failed to plan commit marker: {error}");
            return ExitCode::from(1);
        }
    };
    emit(
        "vortex-commit-marker-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex commit marker plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_commit_marker_plan".to_string()),
            (
                "commit_protocol_ready".to_string(),
                report.commit_protocol_ready().to_string(),
            ),
            (
                "manifest_finalization_available".to_string(),
                report.manifest_finalization_available().to_string(),
            ),
            (
                "local_workspace".to_string(),
                report.local_workspace().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            (
                "feature_gate_enabled".to_string(),
                report.feature_gate_enabled().to_string(),
            ),
            ("commit_marker_written".to_string(), "false".to_string()),
            ("manifest_finalized".to_string(), "false".to_string()),
            ("manifest_committed".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            ("marker_write_allowed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_commit_marker_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-marker-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-marker-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-marker-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("invalid staged workspace path: {error}");
            return ExitCode::from(2);
        }
    };
    let signals = match parse_vortex_commit_marker_signals(&signals_raw) {
        Ok(s) => s,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let options = match parse_vortex_commit_marker_write_options(&options_raw) {
        Ok(o) => o,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let marker_ref = VortexCommitMarkerFileRef::default_for_workspace(workspace_path);
    let _marker_name = VortexCommitMarkerFileName::default_marker();
    let marker_content = match VortexCommitMarkerContent::new(
        "shardloom_commit_marker_draft=true\ncli_write=true\ncommit_marker_written=false\nmanifest_finalized=false\nmanifest_committed=false\noutput_data_written=false\nfallback_execution_allowed=false\n",
    ) {
        Ok(content) => content,
        Err(error) => {
            eprintln!("failed to construct commit marker content: {error}");
            return ExitCode::from(1);
        }
    };
    let mut plan_request = VortexCommitMarkerRequest::new(marker_ref, marker_content);
    for signal in signals {
        plan_request.add_signal(signal, true);
    }
    let plan_report = match plan_vortex_commit_marker(plan_request) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("failed to plan commit marker write: {error}");
            return ExitCode::from(1);
        }
    };
    let mut write_request = match commit_marker_write_request_from_plan(&plan_report) {
        Ok(request) => request,
        Err(error) => {
            eprintln!("failed to build commit marker write request: {error}");
            return ExitCode::from(1);
        }
    };
    write_request.options = options;
    let write_report = match write_vortex_commit_marker(write_request) {
        Ok(report) => report,
        Err(error) => {
            eprintln!("failed to write commit marker: {error}");
            return ExitCode::from(1);
        }
    };
    emit(
        "vortex-commit-marker-write",
        format,
        if write_report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex commit marker write".to_string(),
        write_report.to_human_text(),
        write_report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_commit_marker_write".to_string()),
            (
                "commit_marker_written".to_string(),
                write_report.commit_marker_written().to_string(),
            ),
            ("manifest_finalized".to_string(), "false".to_string()),
            ("manifest_committed".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            (
                "bytes_written".to_string(),
                write_report.bytes_written.to_string(),
            ),
            (
                "checksum".to_string(),
                write_report
                    .checksum
                    .map_or_else(|| "none".to_string(), |checksum| checksum.to_string()),
            ),
            (
                "execution".to_string(),
                "commit_marker_write_or_not_performed".to_string(),
            ),
        ],
    );
    if write_report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_commit_protocol_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-protocol-plan <target_uri> <current_state> <transition> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(current_state_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-protocol-plan <target_uri> <current_state> <transition> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(transition_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-protocol-plan <target_uri> <current_state> <transition> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-commit-protocol-plan <target_uri> <current_state> <transition> <signals>"
        );
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(target_uri.clone()) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let current_state = match parse_vortex_commit_protocol_state(&current_state_raw) {
        Ok(s) => s,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let transition = match parse_vortex_commit_protocol_transition(&transition_raw) {
        Ok(t) => t,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let signals = match parse_vortex_commit_protocol_signals(&signals_raw) {
        Ok(s) => s,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let mut request = VortexCommitProtocolRequest::new(uri, current_state, transition);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report: VortexCommitProtocolReport = match plan_vortex_commit_protocol(request) {
        Ok(r) => r,
        Err(error) => {
            eprintln!("failed to plan commit protocol: {error}");
            return ExitCode::from(1);
        }
    };
    emit(
        "vortex-commit-protocol-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex commit protocol plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_commit_protocol_plan".to_string(),
            ),
            (
                "current_state".to_string(),
                report.current_state().as_str().to_string(),
            ),
            (
                "requested_transition".to_string(),
                report.request.transition.as_str().to_string(),
            ),
            (
                "next_state".to_string(),
                report.next_state().as_str().to_string(),
            ),
            (
                "commit_intent_ready".to_string(),
                report.commit_intent_ready().to_string(),
            ),
            (
                "draft_manifest_ready".to_string(),
                report.draft_manifest_ready().to_string(),
            ),
            (
                "manifest_finalization_available".to_string(),
                report.manifest_finalization_available().to_string(),
            ),
            (
                "commit_marker_available".to_string(),
                report.commit_marker_available().to_string(),
            ),
            (
                "recovery_ready".to_string(),
                report.recovery_ready().to_string(),
            ),
            (
                "object_store_target".to_string(),
                report.object_store_target().to_string(),
            ),
            ("manifest_finalized".to_string(), "false".to_string()),
            ("commit_marker_written".to_string(), "false".to_string()),
            ("manifest_committed".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("recovery_action_executed".to_string(), "false".to_string()),
            ("commit_execution_allowed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_local_commit_execute(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-execute",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-execute",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_local_commit_execution_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-execute",
                format,
                "invalid local commit execution signals",
                &error,
            );
        }
    };
    let mut request = VortexLocalCommitExecutionRequest::new(target_uri, workspace_path);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let started = Instant::now();
    let report = match execute_vortex_local_commit(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-execute",
                format,
                "local commit execution failed",
                &error,
            );
        }
    };
    let duration = started.elapsed();
    emit(
        "vortex-local-commit-execute",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local commit execution".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_local_commit_execute".to_string(),
            ),
            (
                "feature_enabled".to_string(),
                vortex_local_commit_execution_feature_enabled().to_string(),
            ),
            (
                "commit_executed".to_string(),
                report.commit_executed().to_string(),
            ),
            (
                "manifest_committed".to_string(),
                report.manifest_committed().to_string(),
            ),
            (
                "manifest_written".to_string(),
                report.manifest_written().to_string(),
            ),
            (
                "output_data_written".to_string(),
                report.output_data_written().to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                report.upstream_vortex_write_called().to_string(),
            ),
            (
                "recovery_action_executed".to_string(),
                report.recovery_action_executed().to_string(),
            ),
            (
                "bytes_written".to_string(),
                report.bytes_written.to_string(),
            ),
            (
                "write_commit_latency_micros".to_string(),
                duration_micros(duration).to_string(),
            ),
            (
                "write_commit_latency_millis".to_string(),
                micros_to_millis(duration_micros(duration)).to_string(),
            ),
            (
                "checksum".to_string(),
                report
                    .checksum
                    .map_or_else(|| "none".to_string(), |checksum| checksum.to_string()),
            ),
            (
                "execution".to_string(),
                "local_commit_execution_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_local_commit_recovery_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-recovery-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-recovery-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-recovery-plan <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-recovery-plan",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-recovery-plan",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_local_commit_recovery_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-recovery-plan",
                format,
                "invalid local commit recovery signals",
                &error,
            );
        }
    };
    let mut request = VortexLocalCommitRecoveryRequest::new(target_uri, workspace_path);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report = match plan_vortex_local_commit_recovery(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-recovery-plan",
                format,
                "local commit recovery planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-local-commit-recovery-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local commit recovery plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_local_commit_recovery_plan".to_string(),
            ),
            (
                "rollback_required".to_string(),
                report.rollback_required().to_string(),
            ),
            (
                "rollback_planned".to_string(),
                report.rollback_planned().to_string(),
            ),
            (
                "ambiguous_commit".to_string(),
                report.ambiguous_commit().to_string(),
            ),
            (
                "cleanup_required".to_string(),
                report.cleanup_required().to_string(),
            ),
            (
                "cleanup_target_count".to_string(),
                report.cleanup_target_count().to_string(),
            ),
            (
                "rollback_executed".to_string(),
                report.rollback_executed().to_string(),
            ),
            (
                "cleanup_performed".to_string(),
                report.cleanup_performed().to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                report.upstream_vortex_write_called().to_string(),
            ),
            (
                "execution".to_string(),
                "local_commit_recovery_planning_only".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_local_commit_rollback_execute(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(target_uri_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-rollback-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-rollback-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-local-commit-rollback-execute <target_uri> <workspace_path> <signals>"
        );
        return ExitCode::from(2);
    };
    let target_uri = match DatasetUri::new(target_uri_raw) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-rollback-execute",
                format,
                "invalid dataset uri",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-rollback-execute",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_local_commit_recovery_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-rollback-execute",
                format,
                "invalid local commit recovery signals",
                &error,
            );
        }
    };
    let mut request = VortexLocalCommitRecoveryRequest::new(target_uri, workspace_path);
    for signal in signals {
        request.add_signal(signal, true);
    }
    let recovery_report = match plan_vortex_local_commit_recovery(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-rollback-execute",
                format,
                "local commit recovery planning failed",
                &error,
            );
        }
    };
    let report = match execute_vortex_local_commit_rollback(recovery_report) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-local-commit-rollback-execute",
                format,
                "local commit rollback execution failed",
                &error,
            );
        }
    };
    emit(
        "vortex-local-commit-rollback-execute",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local commit rollback execute".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_local_commit_rollback_execute".to_string(),
            ),
            (
                "rollback_executed".to_string(),
                report.rollback_executed().to_string(),
            ),
            (
                "cleanup_performed".to_string(),
                report.cleanup_performed().to_string(),
            ),
            (
                "committed_manifest_removed".to_string(),
                report.committed_manifest_removed().to_string(),
            ),
            (
                "bytes_removed".to_string(),
                report.bytes_removed.to_string(),
            ),
            (
                "object_store_io".to_string(),
                report.object_store_io().to_string(),
            ),
            (
                "upstream_vortex_write_called".to_string(),
                report.upstream_vortex_write_called().to_string(),
            ),
            (
                "execution".to_string(),
                "local_commit_rollback_execution_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_staged_workspace_setup(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_id_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-workspace-setup <workspace_id> <workspace_path> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-workspace-setup <workspace_id> <workspace_path> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-workspace-setup <workspace_id> <workspace_path> <options>"
        );
        return ExitCode::from(2);
    };
    let workspace_id = match VortexStagedWorkspaceId::new(workspace_id_raw.clone()) {
        Ok(id) => id,
        Err(error) => {
            return emit_error(
                "vortex-staged-workspace-setup",
                format,
                "invalid workspace id",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-staged-workspace-setup",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let options = match parse_vortex_staged_workspace_options(&options_raw) {
        Ok(options) => options,
        Err(error) => {
            return emit_error(
                "vortex-staged-workspace-setup",
                format,
                "invalid staged workspace options",
                &error,
            );
        }
    };
    let mut request = VortexStagedWorkspaceSetupRequest::new(workspace_id, workspace_path);
    for option in options {
        match option {
            VortexStagedWorkspaceSetupOption::CreateIfMissing => {
                request = request.create_if_missing(true);
            }
            VortexStagedWorkspaceSetupOption::RequireEmpty => {
                request = request.require_empty(true);
            }
        }
    }
    let report = match setup_vortex_staged_workspace(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-staged-workspace-setup",
                format,
                "staged workspace setup failed",
                &error,
            );
        }
    };
    emit(
        "vortex-staged-workspace-setup",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex staged workspace setup".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_staged_workspace_setup".to_string(),
            ),
            ("workspace_id".to_string(), workspace_id_raw),
            ("workspace_path".to_string(), workspace_path_raw),
            (
                "workspace_created".to_string(),
                report.workspace_created().to_string(),
            ),
            ("marker_written".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("manifest_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "workspace_setup_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_staged_marker_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_id_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-marker-write <workspace_id> <workspace_path> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-marker-write <workspace_id> <workspace_path> <options>"
        );
        return ExitCode::from(2);
    };
    let options_raw = args.next().unwrap_or_default();
    let workspace_id = match VortexStagedWorkspaceId::new(workspace_id_raw.clone()) {
        Ok(id) => id,
        Err(error) => {
            return emit_error(
                "vortex-staged-marker-write",
                format,
                "invalid workspace id",
                &error,
            );
        }
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-staged-marker-write",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let options = match parse_vortex_staged_marker_options(&options_raw) {
        Ok(options) => options,
        Err(error) => {
            return emit_error(
                "vortex-staged-marker-write",
                format,
                "invalid staged marker options",
                &error,
            );
        }
    };
    let mut request = VortexStagedMarkerRequest::new(workspace_id, workspace_path);
    if options.contains(&VortexStagedMarkerOption::AllowOverwrite) {
        request = request.allow_overwrite(true);
    }
    let report = match write_vortex_staged_marker(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-staged-marker-write",
                format,
                "staged marker write failed",
                &error,
            );
        }
    };
    emit(
        "vortex-staged-marker-write",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex staged marker write".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vortex_staged_marker_fields(
            workspace_id_raw,
            workspace_path_raw,
            report.marker_written(),
        ),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_staged_manifest_file_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-staged-manifest-file-plan <workspace_path> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom vortex-staged-manifest-file-plan <workspace_path> <signals>");
        return ExitCode::from(2);
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-plan",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_staged_manifest_file_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-plan",
                format,
                "invalid staged manifest file signals",
                &error,
            );
        }
    };
    let draft_content = match staged_manifest_cli_draft_content() {
        Ok(content) => content,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-plan",
                format,
                "invalid staged manifest draft content",
                &error,
            );
        }
    };
    let mut request = VortexStagedManifestFileRequest::new(
        VortexStagedManifestFileRef::default_for_workspace(workspace_path),
        draft_content,
    );
    for signal in signals {
        request.add_signal(signal, true);
    }
    let report: VortexStagedManifestFileReport = match plan_vortex_staged_manifest_file(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-plan",
                format,
                "staged manifest file planning failed",
                &error,
            );
        }
    };
    emit(
        "vortex-staged-manifest-file-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex staged manifest file plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_staged_manifest_file_plan".to_string(),
            ),
            (
                "manifest_file_written".to_string(),
                report
                    .effects_performed
                    .contains(&VortexStagedManifestFileEffect::ManifestFileWritten)
                    .to_string(),
            ),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("commit_performed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn handle_vortex_staged_manifest_file_write(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(workspace_path_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-manifest-file-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-manifest-file-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let Some(options_raw) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-staged-manifest-file-write <workspace_path> <signals> <options>"
        );
        return ExitCode::from(2);
    };
    let workspace_path = match VortexStagedWorkspacePath::new(workspace_path_raw.clone()) {
        Ok(path) => path,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-write",
                format,
                "invalid workspace path",
                &error,
            );
        }
    };
    let signals = match parse_vortex_staged_manifest_file_write_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-write",
                format,
                "invalid staged manifest file write signals",
                &error,
            );
        }
    };
    let options = match parse_vortex_staged_manifest_file_write_options(&options_raw) {
        Ok(options) => options,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-write",
                format,
                "invalid staged manifest file write options",
                &error,
            );
        }
    };
    let draft_content = match staged_manifest_cli_draft_content() {
        Ok(content) => content,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-write",
                format,
                "invalid staged manifest draft content",
                &error,
            );
        }
    };
    let mut request = VortexStagedManifestFileWriteRequest::new(
        VortexStagedManifestFileRef::default_for_workspace(workspace_path),
        draft_content,
    );
    for signal in signals {
        request.add_signal(signal, true);
    }
    if options.contains(&VortexStagedManifestFileWriteOption::AllowOverwrite) {
        request = request.allow_overwrite(true);
    }
    let report = match write_vortex_staged_manifest_file(request) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "vortex-staged-manifest-file-write",
                format,
                "staged manifest file write failed",
                &error,
            );
        }
    };
    emit(
        "vortex-staged-manifest-file-write",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex staged manifest file write".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            (
                "mode".to_string(),
                "vortex_staged_manifest_file_write".to_string(),
            ),
            (
                "draft_file_written".to_string(),
                report
                    .effects_performed
                    .contains(&VortexStagedManifestFileWriteEffect::DraftFileWritten)
                    .to_string(),
            ),
            ("manifest_file_written".to_string(), "false".to_string()),
            ("output_data_written".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            (
                "upstream_vortex_write_called".to_string(),
                "false".to_string(),
            ),
            ("commit_performed".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "draft_file_write_or_not_performed".to_string(),
            ),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
