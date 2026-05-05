//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{
    CatalogKind, CatalogRef, ChangeSet, ColumnRef, CommandStatus, ComparisonOp,
    CorrectnessValidationPlan, DatasetManifest, DatasetRef, DatasetUri, ExtensionId,
    ExtensionInspectionReport, ExtensionLicenseKind, ExtensionManifest, ExtensionProvenance,
    ExtensionRegistrySnapshot, ExtensionVersion, IncrementalPlanSkeleton,
    InputAdapterRegistrySnapshot, KernelRegistrySnapshot, ManifestId, ObservabilityPlan,
    OutputEnvelope, OutputFormat, OutputTarget, PredicateExpr, RedactionPolicy, ReleasePlan,
    RuntimeObservabilityReport, SchemaDefinition, SchemaId, SchemaVersion, SecurityPlan,
    ShardLoomError, SnapshotId, SnapshotRef, StatValue, TableCompatibilityPlan, TableFormatKind,
    TranslationPlan, UdfRuntimeKind, WriteIntent,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, AttemptId, ByteSize, CancellationReason,
    CancellationRequest, CancellationScope, MemoryBudget, MemoryOwner, MemoryPoolPlan,
    OomSafetyPlan, OperatorMemoryClass, ParallelismLimit, ParallelismPlan, RecoveryPlan, RetryPlan,
    RuntimePlanSkeleton, ShardLoomCancellationExecutionGateReport,
    ShardLoomCancellationExecutionGateRequest, ShardLoomCancellationExecutionGateSignal,
    ShardLoomCleanupExecutionRequest, ShardLoomRetryExecutionGateReport,
    ShardLoomRetryExecutionGateRequest, ShardLoomRetryExecutionGateSignal, SizeEstimate,
    SizingInput, SizingPlan, SpillLifecycleRequest, SpillPayloadFsRef, SpillPayloadId,
    SpillPayloadPath, SpillPayloadRef, SpillPayloadRoundTripRequest, SpillPayloadWriteRequest,
    SpillPlan, SpillPolicy, SpillReservationIntegrationRequest, SpillWorkspaceId,
    SpillWorkspacePath, StreamingPlanSkeleton, SyntheticSpillPayload, TaskAttemptRecord,
    plan_cancellation_execution_gate, plan_retry_execution_gate, plan_spill_lifecycle,
    plan_spill_reservation_integration, roundtrip_spill_payload, spill_payload_fs_feature_enabled,
};
use shardloom_plan::{
    EstimateReport, ExplainReport, NativePlanDocument, OptimizerPhase, OptimizerPlanSkeleton,
    PlanExportRequest, PlanId, PlanImportRequest, PlanInteropFormat, ProjectionRequest,
    ScanPlanSkeleton, ScanRequest, plan_universal_input_source,
};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexAdapterReadiness, VortexCommitIntentReport,
    VortexCommitIntentRequest, VortexCommitIntentSignal, VortexCommitMarkerContent,
    VortexCommitMarkerFileName, VortexCommitMarkerFileRef, VortexCommitMarkerRequest,
    VortexCommitMarkerSignal, VortexCommitMarkerWriteOption, VortexCommitProtocolReport,
    VortexCommitProtocolRequest, VortexCommitProtocolSignal, VortexCommitProtocolState,
    VortexCommitProtocolTransition, VortexDTypeMappingReport, VortexEncodedReadReadinessStatus,
    VortexEncodingLayoutMappingReport, VortexExecutionReadinessStatus, VortexFileRef,
    VortexMetadataOpenRequest, VortexMetadataProbeReport, VortexReadPlan,
    VortexStagedManifestDraftContent, VortexStagedManifestFileEffect, VortexStagedManifestFileRef,
    VortexStagedManifestFileReport, VortexStagedManifestFileRequest,
    VortexStagedManifestFileSignal, VortexStagedManifestFileWriteEffect,
    VortexStagedManifestFileWriteOption, VortexStagedManifestFileWriteRequest,
    VortexStagedManifestFileWriteSignal, VortexStagedMarkerOption, VortexStagedMarkerRequest,
    VortexStagedWorkspaceId, VortexStagedWorkspacePath, VortexStagedWorkspaceSetupOption,
    VortexStagedWorkspaceSetupRequest, VortexStatisticsMappingReport, VortexWriteIntentReport,
    VortexWriteIntentRequest, VortexWriteIntentSignal, VortexWriteOptions, VortexWritePlan,
    build_vortex_runtime_task_graph, commit_marker_write_request_from_plan,
    evaluate_vortex_encoded_read_readiness, evaluate_vortex_execution_readiness,
    evaluate_vortex_query_primitive, execute_vortex_bounded_local_query,
    execute_vortex_encoded_read_contract, execute_vortex_encoded_read_spike,
    execute_vortex_local_query_primitive, execute_vortex_metadata_only,
    metadata_planning_is_side_effect_free, metadata_pruning_is_side_effect_free,
    metadata_summary_is_plan_only, open_vortex_metadata_only, parse_vortex_local_engine_primitive,
    plan_from_vortex_metadata_summary, plan_native_vortex_universal_input,
    plan_vortex_commit_intent, plan_vortex_commit_marker, plan_vortex_commit_protocol,
    plan_vortex_encoded_read_probe, plan_vortex_memory_safety, plan_vortex_metadata_pruning,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    plan_vortex_staged_manifest_file, plan_vortex_write_intent, probe_vortex_metadata_only,
    run_vortex_local_engine, setup_vortex_staged_workspace, size_vortex_runtime_task_graph,
    summarize_vortex_metadata_probe, vortex_encoded_read_executor_feature_enabled,
    vortex_encoded_read_public_api_boundary, vortex_encoded_read_spike_feature_enabled,
    vortex_file_io_feature_enabled, vortex_metadata_executor_feature_enabled,
    write_vortex_commit_marker, write_vortex_staged_manifest_file, write_vortex_staged_marker,
};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run(args)
}

const CLI_COMMAND_NAME: &str = "shardloom";

fn cli_command_name() -> &'static str {
    CLI_COMMAND_NAME
}

fn cli_usage_line() -> String {
    format!(
        "usage: {} <status|release-plan|package-plan|api-compat-plan|capabilities|security-plan|agent-safety-plan|redaction-plan|kernel-registry|doctor|manifest-plan|incremental-plan|write-intent|scan-plan|runtime-plan|task-plan|sizing-plan|translation-plan|vortex-plan|vortex-output-plan|vortex-readiness|vortex-api-inventory|vortex-dtype-mapping|vortex-encoding-layout-mapping|vortex-statistics-mapping|vortex-metadata-probe|vortex-file-metadata-open|vortex-metadata-summary|vortex-metadata-plan|vortex-pruning-plan|optimizer-plan|explain|estimate|benchmark-plan|correctness-plan|recovery-plan|cancellation-plan|retry-plan|observability-plan|runtime-report|profile-plan|plan-ir|plan-import|plan-export|table-compat-plan|schema-plan|input-adapters|input-plan|vortex-input-plan|vortex-read-plan|vortex-task-graph|vortex-adaptive-sizing|vortex-memory-plan|vortex-schedule-plan|vortex-execution-readiness|vortex-encoded-read-api|vortex-encoded-read-readiness|vortex-encoded-read-probe|vortex-encoded-read-execute|vortex-encoded-read-spike|vortex-dry-run|vortex-metadata-execute|vortex-count|vortex-count-where|vortex-staged-workspace-setup|vortex-staged-marker-write|vortex-staged-manifest-file-plan|vortex-staged-manifest-file-write|vortex-commit-marker-plan|vortex-commit-marker-write|vortex-commit-intent-plan|vortex-commit-protocol-plan|vortex-project|vortex-filter|vortex-query-trace|vortex-local-exec|vortex-bounded-local-exec|vortex-run|spill-lifecycle|spill-reservation-plan|spill-payload-roundtrip|cleanup-synthetic-payload|retry-gate-plan <signals>|cancellation-gate-plan <signals>> [--format text|json]",
        cli_command_name()
    )
}
fn parse_vortex_commit_marker_write_options(
    options_raw: &str,
) -> Result<Vec<VortexCommitMarkerWriteOption>, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "commit marker write options must not be empty".to_string(),
        ));
    }
    let mut options = Vec::new();
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit marker write options must not contain empty tokens".to_string(),
            ));
        }
        match token {
            "allow-overwrite" => {
                if !options.contains(&VortexCommitMarkerWriteOption::AllowOverwrite) {
                    options.push(VortexCommitMarkerWriteOption::AllowOverwrite);
                }
            }
            "none" => {}
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown commit marker write option token: {token}"
                )));
            }
        }
    }
    Ok(options)
}

fn parse_vortex_staged_workspace_options(
    options_raw: &str,
) -> Result<Vec<VortexStagedWorkspaceSetupOption>, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "staged workspace options must not be empty".to_string(),
        ));
    }
    let mut options = Vec::new();
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "staged workspace options must not contain empty tokens".to_string(),
            ));
        }
        let option = match token {
            "create-if-missing" => VortexStagedWorkspaceSetupOption::CreateIfMissing,
            "require-empty" => VortexStagedWorkspaceSetupOption::RequireEmpty,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown staged workspace option token: {token}"
                )));
            }
        };
        if !options.contains(&option) {
            options.push(option);
        }
    }
    Ok(options)
}

fn parse_vortex_staged_marker_options(
    options_raw: &str,
) -> Result<Vec<VortexStagedMarkerOption>, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut options = Vec::new();
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "staged marker options must not contain empty tokens".to_string(),
            ));
        }
        let option = match token {
            "allow-overwrite" => VortexStagedMarkerOption::AllowOverwrite,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown staged marker option token: {token}"
                )));
            }
        };
        if !options.contains(&option) {
            options.push(option);
        }
    }
    Ok(options)
}

fn staged_manifest_cli_draft_content() -> Result<VortexStagedManifestDraftContent, ShardLoomError> {
    VortexStagedManifestDraftContent::new(
        "shardloom_staged_manifest_draft=true\ncli_plan=true\noutput_data_written=false\ncommit_performed=false\nfallback_execution_allowed=false\n",
    )
}

fn parse_vortex_staged_manifest_file_signals(
    signals_raw: &str,
) -> Result<Vec<VortexStagedManifestFileSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "staged manifest file signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "staged manifest file signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "draft-ready" => VortexStagedManifestFileSignal::DraftReady,
            "draft-blocked" => VortexStagedManifestFileSignal::DraftBlocked,
            "workspace-known" => VortexStagedManifestFileSignal::WorkspaceKnown,
            "workspace-missing" => VortexStagedManifestFileSignal::WorkspaceMissing,
            "marker-written" => VortexStagedManifestFileSignal::MarkerWritten,
            "marker-missing" => VortexStagedManifestFileSignal::MarkerMissing,
            "local-workspace" => VortexStagedManifestFileSignal::LocalWorkspace,
            "object-store-workspace" => VortexStagedManifestFileSignal::ObjectStoreWorkspace,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown staged manifest file signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn vortex_staged_marker_fields(
    workspace_id: String,
    workspace_path: String,
    marker_written: bool,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_staged_marker_write".to_string()),
        ("workspace_id".to_string(), workspace_id),
        ("workspace_path".to_string(), workspace_path),
        ("marker_written".to_string(), marker_written.to_string()),
        ("workspace_created".to_string(), "false".to_string()),
        ("output_data_written".to_string(), "false".to_string()),
        ("manifest_written".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        (
            "upstream_vortex_write_called".to_string(),
            "false".to_string(),
        ),
        (
            "execution".to_string(),
            "marker_write_or_not_performed".to_string(),
        ),
    ]
}

fn parse_vortex_staged_manifest_file_write_signals(
    signals_raw: &str,
) -> Result<Vec<VortexStagedManifestFileWriteSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "staged manifest file write signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "staged manifest file write signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "file-plan-ready" => VortexStagedManifestFileWriteSignal::FilePlanReady,
            "file-plan-blocked" => VortexStagedManifestFileWriteSignal::FilePlanBlocked,
            "workspace-known" => VortexStagedManifestFileWriteSignal::WorkspaceKnown,
            "workspace-missing" => VortexStagedManifestFileWriteSignal::WorkspaceMissing,
            "object-store-target" => VortexStagedManifestFileWriteSignal::ObjectStoreTarget,
            "existing-draft-file" => VortexStagedManifestFileWriteSignal::ExistingDraftFile,
            "feature-gate-enabled" => VortexStagedManifestFileWriteSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown staged manifest file write signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_staged_manifest_file_write_options(
    options_raw: &str,
) -> Result<Vec<VortexStagedManifestFileWriteOption>, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "staged manifest file write options must not be empty".to_string(),
        ));
    }
    if options_raw.trim() == "none" {
        return Ok(Vec::new());
    }
    let mut options = Vec::new();
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "staged manifest file write options must not contain empty tokens".to_string(),
            ));
        }
        let option = match token {
            "allow-overwrite" => VortexStagedManifestFileWriteOption::AllowOverwrite,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown staged manifest file write option token: {token}"
                )));
            }
        };
        if !options.contains(&option) {
            options.push(option);
        }
    }
    Ok(options)
}

fn parse_vortex_commit_intent_signals(
    signals_raw: &str,
) -> Result<Vec<VortexCommitIntentSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "commit intent signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit intent signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "commit-requested" => VortexCommitIntentSignal::CommitRequested,
            "staged-manifest-draft-written" => VortexCommitIntentSignal::StagedManifestDraftWritten,
            "staged-manifest-draft-missing" => VortexCommitIntentSignal::StagedManifestDraftMissing,
            "manifest-finalization-available" => {
                VortexCommitIntentSignal::ManifestFinalizationAvailable
            }
            "manifest-finalization-missing" => {
                VortexCommitIntentSignal::ManifestFinalizationMissing
            }
            "commit-protocol-available" => VortexCommitIntentSignal::CommitProtocolAvailable,
            "schema-known" => VortexCommitIntentSignal::SchemaKnown,
            "schema-compatible" => VortexCommitIntentSignal::SchemaCompatible,
            "delete-semantics-known" => VortexCommitIntentSignal::DeleteSemanticsKnown,
            "tombstone-semantics-known" => VortexCommitIntentSignal::TombstoneSemanticsKnown,
            "recovery-ready" => VortexCommitIntentSignal::RecoveryReady,
            "recovery-blocked" => VortexCommitIntentSignal::RecoveryBlocked,
            "retry-gate-open" => VortexCommitIntentSignal::RetryGateOpen,
            "retry-gate-closed" => VortexCommitIntentSignal::RetryGateClosed,
            "cancellation-gate-open" => VortexCommitIntentSignal::CancellationGateOpen,
            "cancellation-gate-closed" => VortexCommitIntentSignal::CancellationGateClosed,
            "object-store-target" => VortexCommitIntentSignal::ObjectStoreTarget,
            "feature-gate-enabled" => VortexCommitIntentSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown commit intent signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_commit_protocol_state(
    state_raw: &str,
) -> Result<VortexCommitProtocolState, ShardLoomError> {
    match state_raw.trim() {
        "not-started" => Ok(VortexCommitProtocolState::NotStarted),
        "intent-validated" => Ok(VortexCommitProtocolState::IntentValidated),
        "draft-manifest-ready" => Ok(VortexCommitProtocolState::DraftManifestReady),
        "awaiting-manifest-finalization" => {
            Ok(VortexCommitProtocolState::AwaitingManifestFinalization)
        }
        "awaiting-commit-marker" => Ok(VortexCommitProtocolState::AwaitingCommitMarker),
        "commit-ready" => Ok(VortexCommitProtocolState::CommitReady),
        "commit-blocked" => Ok(VortexCommitProtocolState::CommitBlocked),
        "commit-aborted" => Ok(VortexCommitProtocolState::CommitAborted),
        "unsupported" => Ok(VortexCommitProtocolState::Unsupported),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unknown commit protocol current_state token: {state_raw}"
        ))),
    }
}

fn parse_vortex_commit_protocol_transition(
    transition_raw: &str,
) -> Result<VortexCommitProtocolTransition, ShardLoomError> {
    match transition_raw.trim() {
        "validate-intent" => Ok(VortexCommitProtocolTransition::ValidateIntent),
        "prepare-manifest-finalization" => {
            Ok(VortexCommitProtocolTransition::PrepareManifestFinalization)
        }
        "prepare-commit-marker" => Ok(VortexCommitProtocolTransition::PrepareCommitMarker),
        "mark-commit-ready" => Ok(VortexCommitProtocolTransition::MarkCommitReady),
        "abort" => Ok(VortexCommitProtocolTransition::Abort),
        "unsupported" => Ok(VortexCommitProtocolTransition::Unsupported),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unknown commit protocol transition token: {transition_raw}"
        ))),
    }
}

fn parse_vortex_commit_protocol_signals(
    signals_raw: &str,
) -> Result<Vec<VortexCommitProtocolSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "commit protocol signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit protocol signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "commit-intent-ready" => VortexCommitProtocolSignal::CommitIntentReady,
            "commit-intent-blocked" => VortexCommitProtocolSignal::CommitIntentBlocked,
            "draft-manifest-ready" => VortexCommitProtocolSignal::DraftManifestReady,
            "draft-manifest-missing" => VortexCommitProtocolSignal::DraftManifestMissing,
            "manifest-finalization-available" => {
                VortexCommitProtocolSignal::ManifestFinalizationAvailable
            }
            "manifest-finalization-missing" => {
                VortexCommitProtocolSignal::ManifestFinalizationMissing
            }
            "commit-marker-available" => VortexCommitProtocolSignal::CommitMarkerAvailable,
            "commit-marker-missing" => VortexCommitProtocolSignal::CommitMarkerMissing,
            "object-store-target" => VortexCommitProtocolSignal::ObjectStoreTarget,
            "recovery-ready" => VortexCommitProtocolSignal::RecoveryReady,
            "recovery-blocked" => VortexCommitProtocolSignal::RecoveryBlocked,
            "feature-gate-enabled" => VortexCommitProtocolSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown commit protocol signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_commit_marker_signals(
    signals_raw: &str,
) -> Result<Vec<VortexCommitMarkerSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "commit marker signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "commit marker signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "commit-protocol-ready" => VortexCommitMarkerSignal::CommitProtocolReady,
            "commit-protocol-blocked" => VortexCommitMarkerSignal::CommitProtocolBlocked,
            "manifest-finalization-available" => {
                VortexCommitMarkerSignal::ManifestFinalizationAvailable
            }
            "manifest-finalization-missing" => {
                VortexCommitMarkerSignal::ManifestFinalizationMissing
            }
            "local-workspace" => VortexCommitMarkerSignal::LocalWorkspace,
            "object-store-target" => VortexCommitMarkerSignal::ObjectStoreTarget,
            "feature-gate-enabled" => VortexCommitMarkerSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown commit marker signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}
fn parse_output_format(args: Vec<String>) -> Result<(Vec<String>, OutputFormat), String> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut format = OutputFormat::Text;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            let Some(value) = iter.next() else {
                return Err("missing value for --format; expected text or json".to_string());
            };
            format = OutputFormat::parse(&value).map_err(|e| e.to_string())?;
        } else {
            filtered.push(arg);
        }
    }
    Ok((filtered, format))
}

fn detect_requested_output_format(args: &[String]) -> OutputFormat {
    let mut format = OutputFormat::Text;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--format" {
            if let Some(value) = iter.next() {
                if let Ok(parsed) = OutputFormat::parse(value) {
                    format = parsed;
                }
            } else {
                break;
            }
        }
    }
    format
}

fn emit(
    command: &str,
    format: OutputFormat,
    status: CommandStatus,
    summary: String,
    text: String,
    diagnostics: Vec<shardloom_core::Diagnostic>,
    fields: Vec<(String, String)>,
) {
    let mut envelope = OutputEnvelope::new(command, status, summary, text);
    for diagnostic in diagnostics {
        envelope.add_diagnostic(diagnostic);
    }
    for (key, value) in fields {
        envelope = envelope.with_field(key, value);
    }
    println!("{}", envelope.render(format));
}

fn emit_error(
    command: &str,
    format: OutputFormat,
    summary: &str,
    error: &ShardLoomError,
) -> ExitCode {
    let envelope = OutputEnvelope::from_error(command, summary, error);
    match format {
        OutputFormat::Text => eprintln!("{}", envelope.to_text()),
        OutputFormat::Json => println!("{}", envelope.to_json()),
    }
    ExitCode::from(2)
}

fn readiness_is_blocked(status: VortexExecutionReadinessStatus) -> bool {
    matches!(
        status,
        VortexExecutionReadinessStatus::BlockedByUnsupportedInput
            | VortexExecutionReadinessStatus::BlockedByMissingMetadata
            | VortexExecutionReadinessStatus::BlockedByMissingEstimate
            | VortexExecutionReadinessStatus::BlockedByMemoryPolicy
            | VortexExecutionReadinessStatus::BlockedBySpillPolicy
            | VortexExecutionReadinessStatus::BlockedByFeatureGate
    )
}

fn parse_retry_gate_signals(
    value: &str,
) -> Result<ShardLoomRetryExecutionGateRequest, ShardLoomError> {
    let mut signals = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let signal = match token {
            "retry-requested" => ShardLoomRetryExecutionGateSignal::RetryRequested,
            "retry-allowed" => ShardLoomRetryExecutionGateSignal::RetryAllowedByPlan,
            "retry-requires-cleanup" => ShardLoomRetryExecutionGateSignal::RetryRequiresCleanup,
            "cleanup-completed" => ShardLoomRetryExecutionGateSignal::CleanupCompleted,
            "unknown-artifact" => ShardLoomRetryExecutionGateSignal::UnknownArtifactPresent,
            "external-effects" => ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent,
            "object-store-recovery" => {
                ShardLoomRetryExecutionGateSignal::ObjectStoreRecoveryRequired
            }
            "output-recovery" => ShardLoomRetryExecutionGateSignal::OutputRecoveryRequired,
            "cancellation-requested" => ShardLoomRetryExecutionGateSignal::CancellationRequested,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "invalid retry gate signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    if signals.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "retry-gate-plan requires <signals>".to_string(),
        ));
    }

    let mut request = ShardLoomRetryExecutionGateRequest::new();
    for signal in signals {
        request.add_signal(signal);
    }
    Ok(request)
}

fn retry_gate_plan_fields(report: &ShardLoomRetryExecutionGateReport) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "retry_gate_plan".to_string()),
        (
            "retry_requested".to_string(),
            report.retry_requested().to_string(),
        ),
        (
            "retry_allowed_by_plan".to_string(),
            report.retry_allowed_by_plan().to_string(),
        ),
        (
            "retry_gate_open".to_string(),
            report.retry_gate_open().to_string(),
        ),
        (
            "retry_requires_cleanup".to_string(),
            report.retry_requires_cleanup().to_string(),
        ),
        (
            "cleanup_completed".to_string(),
            report.cleanup_completed().to_string(),
        ),
        (
            "unknown_artifact_present".to_string(),
            report.unknown_artifact_present().to_string(),
        ),
        (
            "external_effects_present".to_string(),
            report
                .request
                .has_signal(ShardLoomRetryExecutionGateSignal::ExternalEffectsPresent)
                .to_string(),
        ),
        (
            "object_store_recovery_required".to_string(),
            report
                .request
                .has_signal(ShardLoomRetryExecutionGateSignal::ObjectStoreRecoveryRequired)
                .to_string(),
        ),
        (
            "output_recovery_required".to_string(),
            report
                .request
                .has_signal(ShardLoomRetryExecutionGateSignal::OutputRecoveryRequired)
                .to_string(),
        ),
        (
            "cancellation_requested".to_string(),
            report
                .request
                .has_signal(ShardLoomRetryExecutionGateSignal::CancellationRequested)
                .to_string(),
        ),
        ("retry_executed".to_string(), "false".to_string()),
        ("cleanup_executed_by_gate".to_string(), "false".to_string()),
        ("cancellation_executed".to_string(), "false".to_string()),
        ("external_effects_executed".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("output_dataset_write".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}

fn parse_cancellation_gate_signals(
    value: &str,
) -> Result<ShardLoomCancellationExecutionGateRequest, ShardLoomError> {
    let mut signals = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let signal = match token {
            "cancellation-requested" => {
                ShardLoomCancellationExecutionGateSignal::CancellationRequested
            }
            "cleanup-required" => ShardLoomCancellationExecutionGateSignal::CleanupRequired,
            "cleanup-completed" => ShardLoomCancellationExecutionGateSignal::CleanupCompleted,
            "unknown-artifact" => ShardLoomCancellationExecutionGateSignal::UnknownArtifactPresent,
            "external-effects" => ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent,
            "object-store-recovery" => {
                ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired
            }
            "output-recovery" => ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired,
            "retry-in-progress" => ShardLoomCancellationExecutionGateSignal::RetryInProgress,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "invalid cancellation gate signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    if signals.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "cancellation-gate-plan requires <signals>".to_string(),
        ));
    }
    let mut request = ShardLoomCancellationExecutionGateRequest::new();
    for signal in signals {
        request.add_signal(signal);
    }
    Ok(request)
}

fn cancellation_gate_plan_fields(
    report: &ShardLoomCancellationExecutionGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "cancellation_gate_plan".to_string()),
        (
            "cancellation_requested".to_string(),
            report.cancellation_requested().to_string(),
        ),
        (
            "cancellation_gate_open".to_string(),
            report.cancellation_gate_open().to_string(),
        ),
        (
            "cleanup_required".to_string(),
            report.cleanup_required().to_string(),
        ),
        (
            "cleanup_completed".to_string(),
            report.cleanup_completed().to_string(),
        ),
        (
            "unknown_artifact_present".to_string(),
            report.unknown_artifact_present().to_string(),
        ),
        (
            "external_effects_present".to_string(),
            report
                .request
                .has_signal(ShardLoomCancellationExecutionGateSignal::ExternalEffectsPresent)
                .to_string(),
        ),
        (
            "object_store_recovery_required".to_string(),
            report
                .request
                .has_signal(ShardLoomCancellationExecutionGateSignal::ObjectStoreRecoveryRequired)
                .to_string(),
        ),
        (
            "output_recovery_required".to_string(),
            report
                .request
                .has_signal(ShardLoomCancellationExecutionGateSignal::OutputRecoveryRequired)
                .to_string(),
        ),
        (
            "retry_in_progress".to_string(),
            report
                .request
                .has_signal(ShardLoomCancellationExecutionGateSignal::RetryInProgress)
                .to_string(),
        ),
        ("cancellation_executed".to_string(), "false".to_string()),
        ("retry_executed".to_string(), "false".to_string()),
        ("cleanup_executed_by_gate".to_string(), "false".to_string()),
        ("external_effects_executed".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("output_dataset_write".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}
fn parse_plan_interop_format(value: &str) -> PlanInteropFormat {
    match value {
        "native" => PlanInteropFormat::ShardLoomNative,
        "agent" => PlanInteropFormat::AgentPlanSpec,
        "substrait-like" => PlanInteropFormat::SubstraitLike,
        "json-like" => PlanInteropFormat::JsonLike,
        _ => PlanInteropFormat::Unknown,
    }
}

fn parse_tiny_predicate(value: &str) -> Result<PredicateExpr, ShardLoomError> {
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["is_null", column] => Ok(PredicateExpr::IsNull {
            column: ColumnRef::new(*column)?,
        }),
        ["is_not_null", column] => Ok(PredicateExpr::IsNotNull {
            column: ColumnRef::new(*column)?,
        }),
        [op, column, int_value] => {
            let parsed: i64 = int_value.parse().map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "predicate integer literal must be valid i64".to_string(),
                )
            })?;
            let op = match *op {
                "eq" => ComparisonOp::Eq,
                "gt" => ComparisonOp::Gt,
                "gte" => ComparisonOp::GtEq,
                "lt" => ComparisonOp::Lt,
                "lte" => ComparisonOp::LtEq,
                _ => {
                    return Err(ShardLoomError::InvalidOperation(
                        "unsupported predicate operator".to_string(),
                    ));
                }
            };
            Ok(PredicateExpr::Compare {
                column: ColumnRef::new(*column)?,
                op,
                value: StatValue::Int64(parsed),
            })
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "invalid predicate format; expected is_null:<column>, is_not_null:<column>, or <op>:<column>:<integer>".to_string(),
        )),
    }
}

fn parse_projection_columns(value: &str) -> Result<ProjectionRequest, ShardLoomError> {
    if value == "*" {
        return Ok(ProjectionRequest::all());
    }
    let columns: Result<Vec<_>, _> = value
        .split(',')
        .map(str::trim)
        .map(|name| {
            if name.is_empty() {
                return Err(ShardLoomError::InvalidOperation(
                    "projection contains empty column name".to_string(),
                ));
            }
            shardloom_core::ColumnRef::new(name)
        })
        .collect();
    Ok(ProjectionRequest::columns(columns?))
}

fn parse_vortex_primitive_request(
    uri: DatasetUri,
    primitive_arg: &str,
) -> Result<shardloom_vortex::VortexQueryPrimitiveRequest, ShardLoomError> {
    if primitive_arg == "count" {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::count_all(
            uri,
        ))
    } else if let Some(pred) = primitive_arg.strip_prefix("count-where:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::count_where(
            uri,
            parse_tiny_predicate(pred)?,
        ))
    } else if let Some(cols) = primitive_arg.strip_prefix("project:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::project(
            uri,
            parse_projection_columns(cols)?,
        ))
    } else if let Some(pred) = primitive_arg.strip_prefix("filter:") {
        Ok(shardloom_vortex::VortexQueryPrimitiveRequest::filter(
            uri,
            parse_tiny_predicate(pred)?,
        ))
    } else {
        Err(ShardLoomError::InvalidOperation("invalid primitive; expected count, count-where:<predicate>, project:<columns>, filter:<predicate>".to_string()))
    }
}

#[allow(clippy::too_many_lines)]
fn handle_vortex_encoded_read_probe(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-probe";
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return ExitCode::from(2);
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read probe failed",
                &ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(v) => v,
        Err(_) => {
            return emit_error(
                command,
                format,
                "vortex encoded-read probe failed",
                &ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let input_plan = match plan_native_vortex_universal_input(source) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if input_plan.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            input_plan.to_human_text(),
            input_plan.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let read_report = match plan_vortex_read_from_universal_input(input_plan) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if read_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            read_report.to_human_text(),
            read_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let runtime_report = match build_vortex_runtime_task_graph(read_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if runtime_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            runtime_report.to_human_text(),
            runtime_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let sizing_report = match size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    ) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if sizing_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            sizing_report.to_human_text(),
            sizing_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let budget = match MemoryBudget::from_gib(memory_gb) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if memory_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            memory_report.to_human_text(),
            memory_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if scheduler_report.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            scheduler_report.to_human_text(),
            scheduler_report.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let readiness = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    if readiness.has_errors() {
        emit(
            command,
            format,
            CommandStatus::Unsupported,
            "vortex encoded-read probe failed".to_string(),
            readiness.to_human_text(),
            readiness.diagnostics.clone(),
            vec![],
        );
        return ExitCode::from(1);
    }
    let api = vortex_encoded_read_public_api_boundary();
    let report = match plan_vortex_encoded_read_probe(api, readiness) {
        Ok(v) => v,
        Err(error) => {
            return emit_error(command, format, "vortex encoded-read probe failed", &error);
        }
    };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read probe report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_encoded_read_probe".to_string()),
            ("probe_only".to_string(), "true".to_string()),
            ("data_read".to_string(), "false".to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            ("execution".to_string(), "not_performed".to_string()),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn handle_vortex_encoded_read_spike(
    args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let command = "vortex-encoded-read-spike";
    let parsed = match parse_vortex_spike_args(command, args) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let (memory_gb, max_parallelism, report) =
        match run_vortex_encoded_read_spike(parsed.0, parsed.1, parsed.2) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(command, format, "vortex encoded-read spike failed", &error);
            }
        };
    emit(
        command,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read spike report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        vec![
            (
                "fallback_execution_allowed".to_string(),
                "false".to_string(),
            ),
            ("mode".to_string(), "vortex_encoded_read_spike".to_string()),
            (
                "feature_enabled".to_string(),
                vortex_encoded_read_spike_feature_enabled().to_string(),
            ),
            ("encoded_read_attempted".to_string(), "false".to_string()),
            ("data_read".to_string(), report.data_read.to_string()),
            ("data_decoded".to_string(), "false".to_string()),
            ("data_materialized".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("spill_io_performed".to_string(), "false".to_string()),
            ("external_effects_executed".to_string(), "false".to_string()),
            (
                "execution".to_string(),
                "encoded_read_spike_or_not_performed".to_string(),
            ),
            ("memory_gb".to_string(), memory_gb.to_string()),
            ("max_parallelism".to_string(), max_parallelism.to_string()),
        ],
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_vortex_spike_args(
    command: &str,
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, u64, usize), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(dataset_uri).map_err(|_| ExitCode::from(2))?;
    let memory_gb = memory_gb_text.parse().map_err(|_| ExitCode::from(2))?;
    let max_parallelism = max_parallelism_text
        .parse()
        .map_err(|_| ExitCode::from(2))?;
    Ok((uri, memory_gb, max_parallelism))
}

fn run_vortex_encoded_read_spike(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<(
    u64,
    usize,
    shardloom_vortex::VortexEncodedReadExecutionReport,
)> {
    let source = shardloom_core::UniversalInputSource::from_dataset_uri(uri)?;
    let input_plan = plan_native_vortex_universal_input(source)?;
    let read_report = plan_vortex_read_from_universal_input(input_plan)?;
    let runtime_report = build_vortex_runtime_task_graph(read_report)?;
    let sizing_report = size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    )?;
    let budget = MemoryBudget::from_gib(memory_gb)?;
    let memory_report = plan_vortex_memory_safety(sizing_report, budget)?;
    let scheduler_report = plan_vortex_scheduler_queue(memory_report, max_parallelism)?;
    let readiness_report = evaluate_vortex_encoded_read_readiness(scheduler_report)?;
    let api = vortex_encoded_read_public_api_boundary();
    let probe = plan_vortex_encoded_read_probe(api.clone(), readiness_report.clone())?;
    let report = execute_vortex_encoded_read_spike(readiness_report, api, probe)?;
    Ok((memory_gb, max_parallelism, report))
}

#[allow(clippy::too_many_lines)]
fn run(args: Vec<String>) -> ExitCode {
    let requested_format = detect_requested_output_format(&args);
    let (args, format) = match parse_output_format(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            return emit_error(
                "cli",
                requested_format,
                "cli argument parsing failed",
                &ShardLoomError::InvalidOperation(message),
            );
        }
    };
    let mut args = args.into_iter();

    match args.next().as_deref() {
        Some("spill-lifecycle") => {
            let Some(workspace_id_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>"
                );
                return ExitCode::from(2);
            };
            let Some(workspace_path_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>"
                );
                return ExitCode::from(2);
            };
            let Some(mode_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-lifecycle <workspace_id> <workspace_path> <mode>"
                );
                return ExitCode::from(2);
            };
            let workspace_id = match SpillWorkspaceId::new(workspace_id_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
                }
            };
            let workspace_path = match SpillWorkspacePath::new(workspace_path_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
                }
            };
            let request = match mode_text.as_str() {
                "report-only" => SpillLifecycleRequest::report_only(workspace_id, workspace_path),
                "local-workspace" => {
                    SpillLifecycleRequest::local_workspace(workspace_id, workspace_path)
                }
                "cleanup-only" => SpillLifecycleRequest::cleanup_only(workspace_id, workspace_path),
                _ => {
                    return emit_error(
                        "spill-lifecycle",
                        format,
                        "spill lifecycle failed",
                        &ShardLoomError::InvalidOperation(
                            "mode must be report-only, local-workspace, or cleanup-only"
                                .to_string(),
                        ),
                    );
                }
            };
            let report = match plan_spill_lifecycle(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-lifecycle", format, "spill lifecycle failed", &error);
                }
            };
            emit(
                "spill-lifecycle",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "spill lifecycle report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "spill_lifecycle".to_string()),
                    ("spill_lifecycle_mode".to_string(), mode_text),
                    (
                        "workspace_created".to_string(),
                        report.workspace_created.as_bool().to_string(),
                    ),
                    (
                        "marker_created".to_string(),
                        report.marker_created.as_bool().to_string(),
                    ),
                    (
                        "cleanup_performed".to_string(),
                        report.cleanup_performed.as_bool().to_string(),
                    ),
                    ("spill_data_written".to_string(), "false".to_string()),
                    ("spill_data_read".to_string(), "false".to_string()),
                    (
                        "reservation_integration_status".to_string(),
                        "not_applicable".to_string(),
                    ),
                    ("reservation_granted".to_string(), "false".to_string()),
                    ("estimated_bytes_known".to_string(), "false".to_string()),
                    (
                        "reservation_lifecycle_integration".to_string(),
                        "true".to_string(),
                    ),
                    ("memory_integration".to_string(), "true".to_string()),
                    (
                        "vortex_memory_bridge_integration".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "bounded_execution_integration".to_string(),
                        "true".to_string(),
                    ),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("spill-reservation-plan") => {
            let Some(label) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
                );
                return ExitCode::from(2);
            };
            let Some(policy_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
                );
                return ExitCode::from(2);
            };
            let Some(estimated_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-reservation-plan <reservation_label> <policy> <estimated_bytes>"
                );
                return ExitCode::from(2);
            };
            let policy = match policy_text.as_str() {
                "never" => SpillPolicy::Never,
                "best-effort" => SpillPolicy::BestEffort,
                "required" => SpillPolicy::Required,
                _ => {
                    return emit_error(
                        "spill-reservation-plan",
                        format,
                        "spill reservation plan failed",
                        &ShardLoomError::InvalidOperation(
                            "policy must be never, best-effort, or required".to_string(),
                        ),
                    );
                }
            };
            let mut request = match SpillReservationIntegrationRequest::new(label, policy) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error(
                        "spill-reservation-plan",
                        format,
                        "spill reservation plan failed",
                        &e,
                    );
                }
            };
            if estimated_text != "unknown" {
                let bytes: u64 = match estimated_text.parse() {
                    Ok(v) => v,
                    Err(_) => {
                        return emit_error(
                            "spill-reservation-plan",
                            format,
                            "spill reservation plan failed",
                            &ShardLoomError::InvalidOperation(
                                "estimated_bytes must be unknown or unsigned integer".to_string(),
                            ),
                        );
                    }
                };
                request = request.with_estimated_bytes(ByteSize::from_bytes(bytes));
            }
            let report = match plan_spill_reservation_integration(request) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error(
                        "spill-reservation-plan",
                        format,
                        "spill reservation plan failed",
                        &e,
                    );
                }
            };
            emit(
                "spill-reservation-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "spill reservation integration report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "spill_reservation_plan".to_string()),
                    (
                        "reservation_integration_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                    (
                        "reservation_granted".to_string(),
                        report.reservation_granted.to_string(),
                    ),
                    (
                        "estimated_bytes_known".to_string(),
                        report.estimated_bytes_known.to_string(),
                    ),
                    ("spill_data_written".to_string(), "false".to_string()),
                    ("spill_data_read".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("spill-payload-roundtrip") => {
            let Some(workspace_path_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
                );
                return ExitCode::from(2);
            };
            let Some(payload_id_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
                );
                return ExitCode::from(2);
            };
            let Some(payload_text) = args.next() else {
                eprintln!(
                    "usage: shardloom spill-payload-roundtrip <workspace_path> <payload_id> <payload_text> [--cleanup]"
                );
                return ExitCode::from(2);
            };
            let mut cleanup_after = false;
            if let Some(extra) = args.next() {
                if extra == "--cleanup" {
                    cleanup_after = true;
                } else {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &ShardLoomError::InvalidOperation(
                            "unknown trailing argument; expected optional --cleanup".to_string(),
                        ),
                    );
                }
                if args.next().is_some() {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &ShardLoomError::InvalidOperation(
                            "too many arguments for spill-payload-roundtrip".to_string(),
                        ),
                    );
                }
            }
            let workspace_path = match SpillPayloadPath::new(workspace_path_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &error,
                    );
                }
            };
            let payload_id = match SpillPayloadId::new(payload_id_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &error,
                    );
                }
            };
            let payload = match SyntheticSpillPayload::from_text(payload_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &error,
                    );
                }
            };
            let payload_ref = match SpillPayloadRef::new(payload_id, "shardloom_cli_workspace") {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &error,
                    );
                }
            };
            let fs_ref = SpillPayloadFsRef::new(payload_ref, workspace_path);
            let write_request = SpillPayloadWriteRequest::new(fs_ref, payload);
            let request =
                SpillPayloadRoundTripRequest::new(write_request).cleanup_after(cleanup_after);
            let report = match roundtrip_spill_payload(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-payload-roundtrip",
                        format,
                        "spill payload roundtrip failed",
                        &error,
                    );
                }
            };
            let bytes_read = report.read_report.as_ref().map_or(0, |v| v.bytes_read);
            let verification_passed = report
                .read_report
                .as_ref()
                .is_some_and(|v| v.verification_passed);
            emit(
                "spill-payload-roundtrip",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "spill payload roundtrip report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "spill_payload_roundtrip".to_string()),
                    (
                        "spill_payload_feature_enabled".to_string(),
                        spill_payload_fs_feature_enabled().to_string(),
                    ),
                    (
                        "payload_written".to_string(),
                        report.payload_written().to_string(),
                    ),
                    (
                        "payload_read".to_string(),
                        report.payload_read().to_string(),
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
                        "output_dataset_write".to_string(),
                        report.output_dataset_write().to_string(),
                    ),
                    (
                        "execution".to_string(),
                        "spill_payload_roundtrip_or_not_performed".to_string(),
                    ),
                    (
                        "bytes_written".to_string(),
                        report.write_report.bytes_written.to_string(),
                    ),
                    ("bytes_read".to_string(), bytes_read.to_string()),
                    (
                        "verification_passed".to_string(),
                        verification_passed.to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("cleanup-synthetic-payload") => {
            let Some(workspace_path_text) = args.next() else {
                eprintln!(
                    "usage: shardloom cleanup-synthetic-payload <workspace_path> <payload_id>"
                );
                return ExitCode::from(2);
            };
            let Some(payload_id_text) = args.next() else {
                eprintln!(
                    "usage: shardloom cleanup-synthetic-payload <workspace_path> <payload_id>"
                );
                return ExitCode::from(2);
            };
            if args.next().is_some() {
                return emit_error(
                    "cleanup-synthetic-payload",
                    format,
                    "synthetic spill payload cleanup failed",
                    &ShardLoomError::InvalidOperation(
                        "too many arguments for cleanup-synthetic-payload".to_string(),
                    ),
                );
            }
            let workspace_path = match SpillPayloadPath::new(workspace_path_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "cleanup-synthetic-payload",
                        format,
                        "synthetic spill payload cleanup failed",
                        &error,
                    );
                }
            };
            let payload_id = match SpillPayloadId::new(payload_id_text) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "cleanup-synthetic-payload",
                        format,
                        "synthetic spill payload cleanup failed",
                        &error,
                    );
                }
            };
            let payload_ref =
                match SpillPayloadRef::new(payload_id.clone(), "shardloom_cli_workspace") {
                    Ok(v) => v,
                    Err(error) => {
                        return emit_error(
                            "cleanup-synthetic-payload",
                            format,
                            "synthetic spill payload cleanup failed",
                            &error,
                        );
                    }
                };
            let fs_ref = SpillPayloadFsRef::new(payload_ref, workspace_path);
            let request = ShardLoomCleanupExecutionRequest::synthetic_payload(
                shardloom_exec::recovery::RecoveryArtifactRef::synthetic_spill_payload(&fs_ref),
                fs_ref,
            )
            .allow_synthetic_payload_cleanup(true);
            let report = match shardloom_exec::recovery::execute_cleanup_plan(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "cleanup-synthetic-payload",
                        format,
                        "synthetic spill payload cleanup failed",
                        &error,
                    );
                }
            };
            emit(
                "cleanup-synthetic-payload",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "synthetic spill payload cleanup report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "cleanup_synthetic_payload".to_string()),
                    (
                        "cleanup_executed".to_string(),
                        report.cleanup_executed().to_string(),
                    ),
                    (
                        "cleanup_performed".to_string(),
                        report.cleanup_executed().to_string(),
                    ),
                    (
                        "retry_executed".to_string(),
                        report.retry_executed().to_string(),
                    ),
                    (
                        "cancellation_executed".to_string(),
                        report.cancellation_executed().to_string(),
                    ),
                    (
                        "external_effects_executed".to_string(),
                        report.external_effects_executed().to_string(),
                    ),
                    (
                        "object_store_io".to_string(),
                        report.object_store_io().to_string(),
                    ),
                    (
                        "output_dataset_write".to_string(),
                        report.output_dataset_write().to_string(),
                    ),
                    (
                        "execution".to_string(),
                        "cleanup_or_not_performed".to_string(),
                    ),
                    (
                        "artifact_kind".to_string(),
                        "synthetic_spill_payload".to_string(),
                    ),
                    ("payload_id".to_string(), payload_id.as_str().to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("status") => {
            let status = shardloom_exec::status();
            emit(
                "status",
                format,
                CommandStatus::Success,
                "engine status".to_string(),
                format!("{}\nfallback execution: disabled", status.summary),
                vec![],
                vec![(
                    "fallback_execution_allowed".to_string(),
                    "false".to_string(),
                )],
            );
            ExitCode::SUCCESS
        }
        Some("release-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "release-plan",
                format,
                CommandStatus::Success,
                "release plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "release_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("package-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "package-plan",
                format,
                CommandStatus::Success,
                "package plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "package_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("api-compat-plan") => {
            let plan = ReleasePlan::default_foundation_plan();
            emit(
                "api-compat-plan",
                format,
                CommandStatus::Success,
                "api compatibility plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "api_compat_plan".to_string()),
                    ("publish_allowed".to_string(), "false".to_string()),
                    ("published".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_publish".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }

        Some("input-adapters") => {
            let snapshot = InputAdapterRegistrySnapshot::foundation();
            emit(
                "input-adapters",
                format,
                CommandStatus::Success,
                "input adapters snapshot".to_string(),
                snapshot.to_human_text(),
                snapshot.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "input_adapters".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("input-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom input-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let report = match plan_universal_input_source(source) {
                Ok(v) => v,
                Err(error) => return emit_error("input-plan", format, "input plan failed", &error),
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "input-plan",
                format,
                command_status,
                "input plan report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "input_plan".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-input-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-input-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-input-plan",
                        format,
                        "vortex input plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-input-plan",
                format,
                command_status,
                "vortex universal input plan report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_input_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        report.source.is_native_vortex().to_string(),
                    ),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-read-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-read-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-read-plan",
                        format,
                        "vortex read plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-read-plan",
                format,
                command_status,
                "vortex read planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_read_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-task-graph") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-task-graph <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() {
                let command_status = CommandStatus::Unsupported;
                emit(
                    "vortex-task-graph",
                    format,
                    command_status,
                    "vortex task graph plan failed: unsupported input".to_string(),
                    input_plan.to_human_text(),
                    input_plan.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_task_graph".to_string()),
                        (
                            "native_vortex_input".to_string(),
                            input_plan.source.is_native_vortex().to_string(),
                        ),
                        ("plan_only".to_string(), "true".to_string()),
                        ("tasks_executed".to_string(), "false".to_string()),
                        ("data_executed".to_string(), "false".to_string()),
                        ("data_read".to_string(), "false".to_string()),
                        ("data_materialized".to_string(), "false".to_string()),
                        ("object_store_io".to_string(), "false".to_string()),
                        ("write_io".to_string(), "false".to_string()),
                        ("external_effects_executed".to_string(), "false".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-task-graph",
                        format,
                        "vortex task graph plan failed",
                        &error,
                    );
                }
            };
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-task-graph",
                format,
                command_status,
                "vortex runtime task graph planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_task_graph".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("schema-plan") => {
            let schema = match (SchemaId::new("schema-placeholder"), SchemaVersion::new(1)) {
                (Ok(id), Ok(version)) => SchemaDefinition::new(id, version),
                (Err(error), _) | (_, Err(error)) => {
                    return emit_error("schema-plan", format, "schema plan failed", &error);
                }
            };
            let text = schema.summary();
            emit(
                "schema-plan",
                format,
                CommandStatus::Success,
                "schema plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "schema_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("catalog-plan") => {
            let kind = match args.next().as_deref() {
                Some("local") => CatalogKind::LocalManifest,
                Some("object-store") => CatalogKind::ObjectStoreManifest,
                Some("iceberg") => CatalogKind::IcebergCompatible,
                Some("delta") => CatalogKind::DeltaCompatible,
                Some("hive") => CatalogKind::HiveStylePath,
                Some("foundry") => CatalogKind::FoundryCompatible,
                Some(_) | None => CatalogKind::Unknown,
            };
            let Some(name) = args.next() else {
                return emit_error(
                    "catalog-plan",
                    format,
                    "catalog plan failed",
                    &ShardLoomError::InvalidOperation("missing catalog name".to_string()),
                );
            };
            let catalog = match CatalogRef::new(kind, name) {
                Ok(c) => c,
                Err(error) => {
                    return emit_error("catalog-plan", format, "catalog plan failed", &error);
                }
            };
            emit(
                "catalog-plan",
                format,
                CommandStatus::Success,
                "catalog reference plan skeleton".to_string(),
                catalog.summary(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "catalog_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("table-compat-plan") => {
            let format_kind = match args.next().as_deref() {
                Some("vortex") => TableFormatKind::NativeVortexManifest,
                Some("iceberg") => TableFormatKind::IcebergCompatible,
                Some("delta") => TableFormatKind::DeltaCompatible,
                Some("hive") => TableFormatKind::HiveStyle,
                Some("external") => TableFormatKind::ExternalCatalogOnly,
                Some(_) | None => TableFormatKind::Unknown,
            };
            let plan = if format_kind.is_native_vortex() {
                TableCompatibilityPlan::native_vortex()
            } else if format_kind.is_compatibility_target() {
                TableCompatibilityPlan::compatibility_target(format_kind)
            } else {
                TableCompatibilityPlan::unsupported(
                    format_kind,
                    "table_compat_plan",
                    "Unknown table format is unsupported for compatibility planning.",
                )
            };
            let status = if plan.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "table-compat-plan",
                format,
                status,
                "table compatibility plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "table_compat_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "table_formats_are".to_string(),
                        "compatibility_targets_not_fallback_engines".to_string(),
                    ),
                ],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("capabilities") => {
            let capabilities = shardloom_core::EngineCapabilities::current();
            emit(
                "capabilities",
                format,
                CommandStatus::Success,
                "engine capabilities".to_string(),
                capabilities.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("native_input".to_string(), "vortex".to_string()),
                    ("native_output".to_string(), "vortex".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("extension-registry") => {
            let snapshot = ExtensionRegistrySnapshot::empty();
            emit(
                "extension-registry",
                format,
                CommandStatus::Success,
                "extension registry metadata-only snapshot".to_string(),
                snapshot.to_human_text(),
                snapshot.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "extension_registry".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("extension-inspect") => {
            let Some(extension_id) = args.next() else {
                return emit_error(
                    "extension-inspect",
                    format,
                    "extension inspect failed",
                    &ShardLoomError::InvalidOperation("missing extension_id".to_string()),
                );
            };
            let id = match ExtensionId::new(extension_id.clone()) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error("extension-inspect", format, "extension inspect failed", &e);
                }
            };
            let manifest = match ExtensionManifest::new(
                id,
                extension_id,
                ExtensionVersion::new(0, 1, 0),
                shardloom_core::ExtensionCategory::Unknown,
                ExtensionProvenance::new(ExtensionLicenseKind::Unknown),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return emit_error("extension-inspect", format, "extension inspect failed", &e);
                }
            };
            let report = ExtensionInspectionReport::requires_review(
                manifest,
                "Extension inspection is metadata-only and requires provenance review.",
            );
            let status = if report.has_errors() {
                CommandStatus::Warning
            } else {
                CommandStatus::Success
            };
            emit(
                "extension-inspect",
                format,
                status,
                "extension inspection metadata-only report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "extension_inspect".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("udf-runtime-plan") => {
            let runtime = match args.next().as_deref() {
                Some("rust") => UdfRuntimeKind::RustNative,
                Some("wasm") => UdfRuntimeKind::Wasm,
                Some("python") => UdfRuntimeKind::Python,
                Some("sql") => UdfRuntimeKind::SqlDefined,
                Some("external") => UdfRuntimeKind::ExternalService,
                Some(_) | None => UdfRuntimeKind::Unknown,
            };
            let text = format!(
                "udf runtime={} available_initially={} sandboxing_required={} execution=not_performed fallback_execution=disabled",
                runtime.as_str(),
                runtime.is_available_initially(),
                runtime.requires_sandboxing()
            );
            emit(
                "udf-runtime-plan",
                format,
                CommandStatus::Success,
                "udf runtime availability skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "udf_runtime_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("extension_code_executed".to_string(), "false".to_string()),
                    ("dynamic_loading".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("security-plan") => {
            let plan = SecurityPlan::default_safe();
            let text = plan.to_human_text();
            emit(
                "security-plan",
                format,
                CommandStatus::Success,
                "security plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "security_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("agent-safety-plan") => {
            let mut plan = SecurityPlan::default_safe();
            plan.agent_mode = shardloom_core::AgentSafetyMode::AgentDryRunOnly;
            let text = plan.to_human_text();
            emit(
                "agent-safety-plan",
                format,
                CommandStatus::Success,
                "agent safety plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "agent_safety_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("redaction-plan") => {
            let redaction = RedactionPolicy::strict();
            let text = redaction.summary();
            emit(
                "redaction-plan",
                format,
                CommandStatus::Success,
                "redaction plan skeleton".to_string(),
                text,
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "redaction_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("external_effects".to_string(), "disabled".to_string()),
                    ("credentials_resolved".to_string(), "false".to_string()),
                    ("secrets_loaded".to_string(), "false".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-ir") => {
            let plan_id = match PlanId::new("plan-placeholder") {
                Ok(v) => v,
                Err(error) => return emit_error("plan-ir", format, "invalid plan id", &error),
            };
            let mut document = NativePlanDocument::empty(plan_id);
            document.validate_skeleton();
            emit(
                "plan-ir",
                format,
                CommandStatus::Warning,
                "native plan ir skeleton".to_string(),
                document.to_human_text(),
                document.validation.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_ir".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("interop_format".to_string(), "native".to_string()),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("plan-import") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let Some(source_label) = args.next() else {
                eprintln!("usage: shardloom plan-import <format> <source_label>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = match PlanImportRequest::not_implemented(format_kind, source_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("plan-import", format, "invalid import request", &error);
                }
            };
            emit(
                "plan-import",
                format,
                CommandStatus::Unsupported,
                "plan import skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_import".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "true".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("plan-export") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-export <format>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let request = PlanExportRequest::not_implemented(format_kind);
            emit(
                "plan-export",
                format,
                CommandStatus::Unsupported,
                "plan export skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "plan_export".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "interop_format".to_string(),
                        format_kind.as_str().to_string(),
                    ),
                    ("validation_required".to_string(), "false".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("memory-plan") => {
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom memory-plan <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "memory-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("memory-plan", format, "invalid memory budget", &error);
                }
            };
            let plan = OomSafetyPlan::new(MemoryPoolPlan::new(budget));
            emit(
                "memory-plan",
                format,
                CommandStatus::Success,
                "memory plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("spill-plan") => {
            let Some(operator_label) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb) = args.next() else {
                eprintln!("usage: shardloom spill-plan <operator_label> <memory_gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb.parse::<u64>() {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "spill-plan",
                        format,
                        "invalid memory_gb",
                        &ShardLoomError::InvalidOperation(format!("invalid memory_gb: {error}")),
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid memory budget", &error);
                }
            };
            let pool = MemoryPoolPlan::new(budget);
            let lower = operator_label.to_lowercase();
            let class = if lower.contains("sort") {
                OperatorMemoryClass::Sort
            } else if lower.contains("join") {
                OperatorMemoryClass::Join
            } else if lower.contains("agg") || lower.contains("aggregate") {
                OperatorMemoryClass::Aggregate
            } else {
                OperatorMemoryClass::Unknown
            };
            let owner = match MemoryOwner::new(class, operator_label) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("spill-plan", format, "invalid operator label", &error);
                }
            };
            let spill_plan = SpillPlan::spill_not_implemented(owner, SpillPolicy::BestEffort);
            let mut plan = OomSafetyPlan::new(pool);
            plan.add_spill_plan(spill_plan);
            let status = if plan.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "spill-plan",
                format,
                status,
                "spill plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("correctness-plan") => {
            let plan = CorrectnessValidationPlan::default_foundation_plan();
            emit(
                "correctness-plan",
                format,
                CommandStatus::Success,
                "correctness validation foundation plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "correctness_plan".to_string()),
                    ("status".to_string(), "planned".to_string()),
                    (
                        "external_baselines".to_string(),
                        "test_oracles_only".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("kernel-registry") => {
            let snapshot = KernelRegistrySnapshot::empty();
            emit(
                "kernel-registry",
                format,
                CommandStatus::Success,
                "kernel registry snapshot".to_string(),
                snapshot.summary(),
                vec![],
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "kernel_registry_snapshot".to_string()),
                    ("status".to_string(), "empty".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("recovery-plan") => {
            let plan = RecoveryPlan::recovery_not_implemented(
                "recovery_execution",
                "Recovery planning skeleton exists, but actual recovery execution is not implemented yet.",
            );
            emit(
                "recovery-plan",
                format,
                CommandStatus::Unsupported,
                "recovery plan skeleton".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "recovery_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("cancellation-plan") => {
            let scope = match args.next().as_deref() {
                Some("query") => CancellationScope::Query,
                Some("task") => CancellationScope::Task,
                Some("scan") => CancellationScope::Scan,
                Some("output-write") => CancellationScope::OutputWrite,
                Some("external-effect") => CancellationScope::ExternalEffect,
                Some("spill-cleanup") => CancellationScope::SpillCleanup,
                Some("runtime" | _) | None => CancellationScope::Runtime,
            };
            let request = CancellationRequest::new(scope, CancellationReason::UserRequested);
            emit(
                "cancellation-plan",
                format,
                CommandStatus::Success,
                "cancellation plan skeleton".to_string(),
                request.summary(),
                request.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "cancellation_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("retry-plan") => {
            let Some(task_id) = args.next() else {
                eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
                return ExitCode::from(2);
            };
            let Some(attempt_id) = args.next() else {
                eprintln!("usage: shardloom retry-plan <task_id> <attempt_id>");
                return ExitCode::from(2);
            };
            let task_id = match shardloom_exec::TaskId::new(task_id) {
                Ok(v) => v,
                Err(error) => return emit_error("retry-plan", format, "invalid task id", &error),
            };
            let attempt_id = match AttemptId::new(attempt_id) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("retry-plan", format, "invalid attempt id", &error);
                }
            };
            let attempt = TaskAttemptRecord::new(task_id, attempt_id);
            let plan = RetryPlan::from_attempt(
                shardloom_exec::RetryPolicy::default_read_retries(),
                attempt,
            );
            emit(
                "retry-plan",
                format,
                CommandStatus::Success,
                "retry plan skeleton".to_string(),
                plan.summary(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "retry_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("retry-gate-plan") => {
            let Some(raw) = args.next() else {
                return emit_error(
                    "retry-gate-plan",
                    format,
                    "invalid retry gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "retry-gate-plan requires <signals>".to_string(),
                    ),
                );
            };
            if raw.trim().is_empty() {
                return emit_error(
                    "retry-gate-plan",
                    format,
                    "invalid retry gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "retry-gate-plan requires <signals>".to_string(),
                    ),
                );
            }
            if args.next().is_some() {
                return emit_error(
                    "retry-gate-plan",
                    format,
                    "invalid retry gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "too many arguments for retry-gate-plan".to_string(),
                    ),
                );
            }
            let request = match parse_retry_gate_signals(&raw) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "retry-gate-plan",
                        format,
                        "invalid retry gate signal list",
                        &error,
                    );
                }
            };
            let report = match plan_retry_execution_gate(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "retry-gate-plan",
                        format,
                        "retry gate planning failed",
                        &error,
                    );
                }
            };
            emit(
                "retry-gate-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "retry execution gate plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                retry_gate_plan_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("cancellation-gate-plan") => {
            let Some(raw) = args.next() else {
                return emit_error(
                    "cancellation-gate-plan",
                    format,
                    "invalid cancellation gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "cancellation-gate-plan requires <signals>".to_string(),
                    ),
                );
            };
            if raw.trim().is_empty() {
                return emit_error(
                    "cancellation-gate-plan",
                    format,
                    "invalid cancellation gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "cancellation-gate-plan requires <signals>".to_string(),
                    ),
                );
            }
            if args.next().is_some() {
                return emit_error(
                    "cancellation-gate-plan",
                    format,
                    "invalid cancellation gate signal list",
                    &ShardLoomError::InvalidOperation(
                        "too many arguments for cancellation-gate-plan".to_string(),
                    ),
                );
            }
            let request = match parse_cancellation_gate_signals(&raw) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "cancellation-gate-plan",
                        format,
                        "invalid cancellation gate signal list",
                        &error,
                    );
                }
            };
            let report = match plan_cancellation_execution_gate(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "cancellation-gate-plan",
                        format,
                        "cancellation gate planning failed",
                        &error,
                    );
                }
            };
            emit(
                "cancellation-gate-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "cancellation execution gate plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                cancellation_gate_plan_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("observability-plan") => {
            let plan = ObservabilityPlan::default_foundation_plan();
            emit(
                "observability-plan",
                format,
                CommandStatus::Success,
                "observability plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "observability_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("runtime-report") => {
            let report = RuntimeObservabilityReport::not_run();
            emit(
                "runtime-report",
                format,
                CommandStatus::Success,
                "runtime observability report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "runtime_report".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("profile-plan") => {
            let plan = ObservabilityPlan::collection_not_implemented(
                "profiling",
                "Profiling domain types exist, but runtime profiling collection is not implemented yet.",
            );
            emit(
                "profile-plan",
                format,
                CommandStatus::Unsupported,
                "profiling collection not implemented".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "profile_plan".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metrics_collection".to_string(),
                        "not_performed".to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("doctor") => {
            emit("doctor", format, CommandStatus::Success, "doctor checks".to_string(), "ShardLoom doctor\nfallback execution: disabled\nnative input target: vortex\nnative output target: vortex\nstatus: early implementation skeleton".to_string(), vec![], vec![("native_input".to_string(), "vortex".to_string()), ("native_output".to_string(), "vortex".to_string())]);
            ExitCode::SUCCESS
        }
        Some("explain") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = ExplainReport::unsupported(
                operation,
                "planning",
                "Real planning is not implemented yet.",
            );
            emit(
                "explain",
                format,
                CommandStatus::Unsupported,
                "explain plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("benchmark-plan") => {
            let plan = shardloom_core::BenchmarkPlan::default_foundation_plan();
            emit(
                "benchmark-plan",
                format,
                CommandStatus::Success,
                "benchmark plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("manifest-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom manifest-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "manifest-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let snapshot =
                SnapshotRef::new(SnapshotId::new("snapshot-placeholder").expect("valid"));
            let manifest = DatasetManifest::new(
                ManifestId::new("manifest-placeholder").expect("valid"),
                dataset,
                snapshot,
            );
            emit(
                "manifest-plan",
                format,
                CommandStatus::Success,
                "manifest plan".to_string(),
                manifest.summary(),
                vec![],
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("incremental-plan") => {
            let Some(snapshot_id) = args.next() else {
                eprintln!("usage: shardloom incremental-plan <snapshot_id>");
                return ExitCode::from(2);
            };
            let snapshot_id = match SnapshotId::new(snapshot_id) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    eprintln!("invalid snapshot id: {error}");
                    return ExitCode::from(2);
                }
            };
            let change_set = ChangeSet::new(snapshot_id);
            let plan = IncrementalPlanSkeleton::from_change_set(change_set);
            emit(
                "incremental-plan",
                format,
                CommandStatus::Success,
                "incremental plan".to_string(),
                plan.to_human_text(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-write-intent-plan") => {
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
        Some("vortex-commit-intent-plan") => {
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
        Some("vortex-commit-marker-plan") => {
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
        Some("vortex-commit-marker-write") => {
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
                            .map(|checksum| checksum.to_string())
                            .unwrap_or_else(|| "none".to_string()),
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
        Some("vortex-commit-protocol-plan") => {
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
        Some("vortex-staged-workspace-setup") => {
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

        Some("vortex-staged-marker-write") => {
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
        Some("vortex-staged-manifest-file-plan") => {
            let Some(workspace_path_raw) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-staged-manifest-file-plan <workspace_path> <signals>"
                );
                return ExitCode::from(2);
            };
            let Some(signals_raw) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-staged-manifest-file-plan <workspace_path> <signals>"
                );
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
            let report: VortexStagedManifestFileReport =
                match plan_vortex_staged_manifest_file(request) {
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
        Some("vortex-staged-manifest-file-write") => {
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
        Some("write-intent") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom write-intent <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let intent = WriteIntent::write_not_implemented(OutputTarget::from_uri(uri));
            emit(
                "write-intent",
                format,
                CommandStatus::Unsupported,
                "write intent".to_string(),
                intent.summary(),
                intent.diagnostics.clone(),
                vec![],
            );
            if intent.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("scan-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom scan-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let request = ScanRequest::new(dataset);
            let skeleton = ScanPlanSkeleton::plan_only(request);
            emit(
                "scan-plan",
                format,
                if skeleton.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "scan plan".to_string(),
                skeleton.to_human_text(),
                skeleton.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("streaming-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
                return ExitCode::from(2);
            };
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
                return ExitCode::from(2);
            };
            let dataset_uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
                Ok(dataset_ref) => dataset_ref,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let target_uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid target uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let output_target = OutputTarget::from_uri(target_uri);
            let plan = StreamingPlanSkeleton::for_vortex_to_target(dataset_ref, output_target);
            emit(
                "streaming-plan",
                format,
                if plan.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "streaming plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("runtime-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom runtime-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
                Ok(plan) => plan,
                Err(error) => {
                    eprintln!("failed to build runtime plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "runtime-plan",
                format,
                CommandStatus::Success,
                "runtime plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("sizing-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            let Some(memory_flag) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            if memory_flag != "--memory-gb" {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            }
            let Some(memory_gb_raw) = args.next() else {
                eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
                return ExitCode::from(2);
            };
            let memory_gb = match memory_gb_raw.parse::<u64>() {
                Ok(value) if value > 0 => value,
                _ => {
                    return emit_error(
                        "sizing-plan",
                        format,
                        "invalid memory setting",
                        &ShardLoomError::InvalidOperation(
                            "memory-gb must be a positive integer".to_string(),
                        ),
                    );
                }
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "sizing-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let sizer = AdaptiveSizer::new(policy.clone());
            let input = SizingInput::new(
                shardloom_core::SegmentId::new("placeholder-segment").expect("valid segment id"),
                SizeEstimate::unknown(),
            );
            let decision = sizer.decide_for_segment(&input);
            let parallelism =
                ParallelismPlan::new(ParallelismLimit::auto(), 1, 1, "planning skeleton");
            let mut plan = SizingPlan::new(policy, parallelism);
            plan.add_decision(input.segment_id.clone(), decision);
            emit(
                "sizing-plan",
                format,
                CommandStatus::Success,
                "sizing plan".to_string(),
                format!("dataset: {}\n{}", dataset.summary(), plan.to_human_text()),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }
        Some("task-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom task-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let dataset = match DatasetRef::from_uri(uri) {
                Ok(dataset) => dataset,
                Err(error) => {
                    eprintln!("failed to create dataset reference: {error}");
                    return ExitCode::from(2);
                }
            };
            let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
                Ok(plan) => plan,
                Err(error) => {
                    eprintln!("failed to build task plan: {error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "task-plan",
                format,
                CommandStatus::Success,
                "task plan".to_string(),
                plan.graph.summary(),
                vec![],
                vec![],
            );
            ExitCode::SUCCESS
        }

        Some("vortex-adaptive-sizing") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom vortex-adaptive-sizing <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                emit(
                    "vortex-adaptive-sizing",
                    format,
                    CommandStatus::Unsupported,
                    "vortex adaptive sizing report".to_string(),
                    input_plan.to_human_text(),
                    input_plan.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if read_report.has_errors() {
                emit(
                    "vortex-adaptive-sizing",
                    format,
                    CommandStatus::Unsupported,
                    "vortex adaptive sizing report".to_string(),
                    read_report.to_human_text(),
                    read_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            if runtime_report.has_errors() {
                emit(
                    "vortex-adaptive-sizing",
                    format,
                    CommandStatus::Unsupported,
                    "vortex adaptive sizing report".to_string(),
                    runtime_report.to_human_text(),
                    runtime_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let report = match size_vortex_runtime_task_graph(runtime_report, policy) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-adaptive-sizing",
                        format,
                        "vortex adaptive sizing failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-adaptive-sizing",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex adaptive sizing report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_adaptive_sizing".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    (
                        "reservation_lifecycle_integration".to_string(),
                        "true".to_string(),
                    ),
                    ("memory_integration".to_string(), "true".to_string()),
                    (
                        "vortex_memory_bridge_integration".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "bounded_execution_integration".to_string(),
                        "true".to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-memory-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom vortex-memory-plan <dataset_uri> <memory_gb>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            if !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let sizing_policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let sizing_report = match size_vortex_runtime_task_graph(runtime_report, sizing_policy)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            if sizing_report.has_errors() {
                emit(
                    "vortex-memory-plan",
                    format,
                    CommandStatus::Unsupported,
                    "vortex memory planning report".to_string(),
                    sizing_report.to_human_text(),
                    sizing_report.diagnostics.clone(),
                    vec![
                        (
                            "fallback_execution_allowed".to_string(),
                            "false".to_string(),
                        ),
                        ("mode".to_string(), "vortex_memory_plan".to_string()),
                        ("execution".to_string(), "not_performed".to_string()),
                    ],
                );
                return ExitCode::from(1);
            }
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-memory-plan",
                        format,
                        "vortex memory plan failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-memory-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex memory planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_memory_plan".to_string()),
                    (
                        "native_vortex_input".to_string(),
                        input_plan.source.is_native_vortex().to_string(),
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    (
                        "reservation_lifecycle_integration".to_string(),
                        "true".to_string(),
                    ),
                    ("memory_integration".to_string(), "true".to_string()),
                    (
                        "vortex_memory_bridge_integration".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "bounded_execution_integration".to_string(),
                        "true".to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-schedule-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-schedule-plan <dataset_uri> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan.clone()) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            if read_report.has_errors() {
                return ExitCode::from(1);
            }
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            if runtime_report.has_errors() {
                return ExitCode::from(1);
            }
            let sizing_policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
            let sizing_report = match size_vortex_runtime_task_graph(runtime_report, sizing_policy)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            if sizing_report.has_errors() {
                return ExitCode::from(1);
            }
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            if memory_report.has_errors() {
                return ExitCode::from(1);
            }
            let report = match plan_vortex_scheduler_queue(memory_report, max_parallelism) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-schedule-plan",
                        format,
                        "vortex schedule plan failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-schedule-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex scheduler queue planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_schedule_plan".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-execution-readiness") => {
            let is_dry_run = false;
            let command = "vortex-execution-readiness";
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex readiness planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex readiness planning failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let sizing_report = match size_vortex_runtime_task_graph(
                runtime_report,
                AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let readiness_report = match evaluate_vortex_execution_readiness(scheduler_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let text = if is_dry_run {
                readiness_report.dry_run_contract.to_human_text()
            } else {
                readiness_report.to_human_text()
            };
            emit(
                command,
                format,
                if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                if is_dry_run {
                    "vortex dry-run contract".to_string()
                } else {
                    "vortex execution readiness report".to_string()
                },
                text,
                readiness_report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        if is_dry_run {
                            "vortex_dry_run".to_string()
                        } else {
                            "vortex_execution_readiness".to_string()
                        },
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("dry_run_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-encoded-read-api") => {
            let command = "vortex-encoded-read-api";
            let report = vortex_encoded_read_public_api_boundary();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex encoded-read API boundary report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_encoded_read_api".to_string()),
                    ("contract_only".to_string(), "true".to_string()),
                    ("execution_usable".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-encoded-read-readiness") => {
            let command = "vortex-encoded-read-readiness";
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let sizing_report = match size_vortex_runtime_task_graph(
                runtime_report,
                AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            let report = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read readiness failed",
                        &error,
                    );
                }
            };
            emit(
                command,
                format,
                if report.has_errors()
                    || !matches!(
                        report.status,
                        VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
                            | VortexEncodedReadReadinessStatus::ReadyForContract
                            | VortexEncodedReadReadinessStatus::NoEncodedReadCandidates
                    )
                {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex encoded-read readiness report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        "vortex_encoded_read_readiness".to_string(),
                    ),
                    ("readiness_only".to_string(), "true".to_string()),
                    ("encoded_read_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if report.has_errors()
                || !matches!(
                    report.status,
                    VortexEncodedReadReadinessStatus::ReadyForFutureEncodedRead
                        | VortexEncodedReadReadinessStatus::ReadyForContract
                        | VortexEncodedReadReadinessStatus::NoEncodedReadCandidates
                )
            {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-encoded-read-probe") => handle_vortex_encoded_read_probe(args, format),
        Some("vortex-encoded-read-spike") => handle_vortex_encoded_read_spike(args, format),

        Some("vortex-encoded-read-execute") => {
            let command = "vortex-encoded-read-execute";
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let read_report = match plan_vortex_read_from_universal_input(input_plan) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let sizing_report = match size_vortex_runtime_task_graph(
                runtime_report,
                AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let readiness_report = match evaluate_vortex_encoded_read_readiness(scheduler_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            let report = match execute_vortex_encoded_read_contract(readiness_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read execute failed",
                        &error,
                    );
                }
            };
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex encoded-read executor skeleton report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        "vortex_encoded_read_execute".to_string(),
                    ),
                    (
                        "executor_feature_enabled".to_string(),
                        vortex_encoded_read_executor_feature_enabled().to_string(),
                    ),
                    ("encoded_read_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-metadata-execute") => {
            let command = "vortex-metadata-execute";
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let sizing_report = match size_vortex_runtime_task_graph(
                runtime_report,
                AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let readiness_report = match evaluate_vortex_execution_readiness(scheduler_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            let exec_report = match execute_vortex_metadata_only(readiness_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex metadata execute planning failed",
                        &error,
                    );
                }
            };
            emit(
                command,
                format,
                if exec_report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata-only executor report".to_string(),
                exec_report.to_human_text(),
                exec_report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_execute".to_string()),
                    (
                        "executor_feature_enabled".to_string(),
                        vortex_metadata_executor_feature_enabled().to_string(),
                    ),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if exec_report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-dry-run") => {
            let is_dry_run = true;
            let command = "vortex-dry-run";
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!("usage: shardloom {command} <dataset_uri> <memory_gb> <max_parallelism>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex readiness planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        command,
                        format,
                        "vortex readiness planning failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let source = match shardloom_core::UniversalInputSource::from_dataset_uri(uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let input_plan = match plan_native_vortex_universal_input(source) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            if input_plan.has_errors() || !input_plan.source.is_native_vortex() {
                return ExitCode::from(1);
            }
            let read_report = match plan_vortex_read_from_universal_input(input_plan) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let runtime_report = match build_vortex_runtime_task_graph(read_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let sizing_report = match size_vortex_runtime_task_graph(
                runtime_report,
                AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let budget = match MemoryBudget::from_gib(memory_gb) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let memory_report = match plan_vortex_memory_safety(sizing_report, budget) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let scheduler_report = match plan_vortex_scheduler_queue(memory_report, max_parallelism)
            {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let readiness_report = match evaluate_vortex_execution_readiness(scheduler_report) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(command, format, "vortex readiness planning failed", &error);
                }
            };
            let text = if is_dry_run {
                readiness_report.dry_run_contract.to_human_text()
            } else {
                readiness_report.to_human_text()
            };
            emit(
                command,
                format,
                if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                if is_dry_run {
                    "vortex dry-run contract".to_string()
                } else {
                    "vortex execution readiness report".to_string()
                },
                text,
                readiness_report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        if is_dry_run {
                            "vortex_dry_run".to_string()
                        } else {
                            "vortex_execution_readiness".to_string()
                        },
                    ),
                    ("plan_only".to_string(), "true".to_string()),
                    ("dry_run_only".to_string(), "true".to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                ],
            );
            if readiness_report.has_errors() || readiness_is_blocked(readiness_report.status) {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-plan <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-plan",
                format,
                CommandStatus::Success,
                "vortex read plan".to_string(),
                VortexReadPlan::metadata_only(file_ref).to_human_text(),
                vec![],
                vec![("mode".to_string(), "metadata_only".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("translation-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom translation-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let target = OutputTarget::from_uri(uri);
            let plan = TranslationPlan::for_target(target);
            emit(
                "translation-plan",
                format,
                if plan.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "translation plan".to_string(),
                plan.to_human_text(),
                plan.diagnostics.clone(),
                vec![],
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-output-plan") => {
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom vortex-output-plan <target_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    eprintln!("invalid dataset uri: {error}");
                    return ExitCode::from(2);
                }
            };
            let file_ref = match VortexFileRef::from_uri(uri) {
                Ok(file_ref) => file_ref,
                Err(error) => {
                    eprintln!("{error}");
                    return ExitCode::from(2);
                }
            };
            emit(
                "vortex-output-plan",
                format,
                CommandStatus::Success,
                "vortex output plan".to_string(),
                VortexWritePlan::planned(file_ref, VortexWriteOptions::native_defaults())
                    .to_human_text(),
                vec![],
                vec![("target_format".to_string(), "vortex".to_string())],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-readiness") => {
            let readiness = VortexAdapterReadiness::dependency_added_compile_only();
            emit(
                "vortex-readiness",
                format,
                CommandStatus::Success,
                "vortex dependency readiness".to_string(),
                readiness.to_human_text(),
                readiness.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_readiness".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        readiness.dependency_status.as_str().to_string(),
                    ),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("io".to_string(), "not_performed".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-dtype-mapping") => {
            let report = if shardloom_vortex::typed_vortex_dtype_mapping_available() {
                VortexDTypeMappingReport::implemented("vortex::DType")
            } else {
                VortexDTypeMappingReport::deferred_api_unclear()
            };
            emit(
                "vortex-dtype-mapping",
                format,
                CommandStatus::Success,
                "vortex dtype mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_dtype_mapping".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "name_based_mapping_available".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "typed_mapping_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-encoding-layout-mapping") => {
            let report = VortexEncodingLayoutMappingReport::deferred_api_unclear();
            emit(
                "vortex-encoding-layout-mapping",
                format,
                CommandStatus::Success,
                "vortex encoding/layout mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "mode".to_string(),
                        "vortex_encoding_layout_mapping".to_string(),
                    ),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "name_based_mapping_available".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoding_mapping_status".to_string(),
                        report.encoding_status.as_str().to_string(),
                    ),
                    (
                        "layout_mapping_status".to_string(),
                        report.layout_status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }

        Some("vortex-statistics-mapping") => {
            let report = if shardloom_vortex::typed_vortex_statistics_mapping_available() {
                VortexStatisticsMappingReport::implemented("vortex::statistics::<public_api>")
            } else {
                VortexStatisticsMappingReport::deferred_api_unclear()
            };
            emit(
                "vortex-statistics-mapping",
                format,
                CommandStatus::Success,
                "vortex statistics mapping".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_statistics_mapping".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("segment_stats_available".to_string(), "true".to_string()),
                    (
                        "statistics_mapping_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("vortex-file-metadata-open") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-file-metadata-open <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(err) => {
                    return emit_error(
                        "vortex-file-metadata-open",
                        format,
                        "vortex file metadata open failed",
                        &err,
                    );
                }
            };
            let request = VortexMetadataOpenRequest::metadata_only(uri);
            let report = match open_vortex_metadata_only(request) {
                Ok(report) => report,
                Err(err) => {
                    return emit_error(
                        "vortex-file-metadata-open",
                        format,
                        "vortex file metadata open failed",
                        &err,
                    );
                }
            };
            let status = if report.has_errors() {
                CommandStatus::Error
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-file-metadata-open",
                format,
                status,
                "vortex file metadata-only open".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    ("mode".to_string(), "vortex_file_metadata_open".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    (
                        "file_io_feature_enabled".to_string(),
                        vortex_file_io_feature_enabled().to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "file_io_performed".to_string(),
                        report.file_io_performed.to_string(),
                    ),
                    (
                        "data_io_performed".to_string(),
                        report.data_io_performed.to_string(),
                    ),
                    (
                        "object_store_io_performed".to_string(),
                        report.object_store_io_performed.to_string(),
                    ),
                    (
                        "write_io_performed".to_string(),
                        report.write_io_performed.to_string(),
                    ),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if matches!(status, CommandStatus::Error) {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-metadata-summary") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-summary",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-summary",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let probe = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            let report = summarize_vortex_metadata_probe(&probe);
            emit(
                "vortex-metadata-summary",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata summary".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_summary".to_string()),
                    (
                        "metadata_summary_plan_only".to_string(),
                        metadata_summary_is_plan_only(&report).to_string(),
                    ),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-count") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-count <dataset_uri>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error("vortex-count", format, "vortex count failed", &error);
                }
            };
            let request = shardloom_vortex::VortexQueryPrimitiveRequest::count_all(uri.clone());
            let open = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri));
            let summary = if let Ok(report) = open {
                if let Some(summary) = report.metadata_summary {
                    summary
                } else if report.has_errors() {
                    let mut degraded = summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    );
                    degraded.diagnostics.extend(report.diagnostics.clone());
                    degraded
                } else {
                    summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    )
                }
            } else {
                summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
            };
            let result = match evaluate_vortex_query_primitive(request, &summary) {
                Ok(result) => result,
                Err(error) => {
                    return emit_error("vortex-count", format, "vortex count failed", &error);
                }
            };
            let count = match result.value {
                shardloom_vortex::VortexQueryPrimitiveValue::Count(v) => Some(v),
                _ => None,
            };
            let status = if result.has_errors() || count.is_none() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-count",
                format,
                status,
                "vortex count primitive".to_string(),
                result.to_human_text(),
                result.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_count".to_string()),
                    ("primitive".to_string(), "count_all".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                    ("result_known".to_string(), count.is_some().to_string()),
                    (
                        "count".to_string(),
                        count.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
                    ),
                ],
            );
            if result.has_errors() || count.is_none() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-count-where") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-count-where <dataset_uri> <predicate>");
                return ExitCode::from(2);
            };
            let Some(predicate_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-count-where <dataset_uri> <predicate>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-count-where",
                        format,
                        "vortex count where failed",
                        &error,
                    );
                }
            };
            let predicate = match parse_tiny_predicate(&predicate_arg) {
                Ok(predicate) => predicate,
                Err(error) => {
                    return emit_error(
                        "vortex-count-where",
                        format,
                        "vortex count where failed",
                        &error,
                    );
                }
            };
            let request =
                shardloom_vortex::VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate);
            let open = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri));
            let summary = if let Ok(report) = open {
                report.metadata_summary.unwrap_or_else(|| {
                    summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    )
                })
            } else {
                summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
            };
            let result = match evaluate_vortex_query_primitive(request, &summary) {
                Ok(result) => result,
                Err(error) => {
                    return emit_error(
                        "vortex-count-where",
                        format,
                        "vortex count where failed",
                        &error,
                    );
                }
            };
            let status = if result.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            let count = match result.value {
                shardloom_vortex::VortexQueryPrimitiveValue::Count(v) => Some(v),
                _ => None,
            };
            emit(
                "vortex-count-where",
                format,
                status,
                "vortex count where primitive".to_string(),
                result.to_human_text(),
                result.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_count_where".to_string()),
                    ("primitive".to_string(), "count_where".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                    ("result_known".to_string(), count.is_some().to_string()),
                    (
                        "count".to_string(),
                        count.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
                    ),
                    ("predicate".to_string(), predicate_arg),
                ],
            );
            if result.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-project") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-project <dataset_uri> <columns>");
                return ExitCode::from(2);
            };
            let Some(columns_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-project <dataset_uri> <columns>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error("vortex-project", format, "vortex project failed", &error);
                }
            };
            let projection = match parse_projection_columns(&columns_arg) {
                Ok(projection) => projection,
                Err(error) => {
                    return emit_error("vortex-project", format, "vortex project failed", &error);
                }
            };
            let request =
                shardloom_vortex::VortexQueryPrimitiveRequest::project(uri.clone(), projection);
            let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
                .ok()
                .and_then(|report| report.metadata_summary)
                .unwrap_or_else(|| {
                    summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    )
                });
            let result = match evaluate_vortex_query_primitive(request, &summary) {
                Ok(result) => result,
                Err(error) => {
                    return emit_error("vortex-project", format, "vortex project failed", &error);
                }
            };
            emit(
                "vortex-project",
                format,
                if result.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex project primitive".to_string(),
                result.to_human_text(),
                result.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_project".to_string()),
                    ("primitive".to_string(), "project_columns".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    (
                        "result_known".to_string(),
                        result.value.is_known().to_string(),
                    ),
                ],
            );
            if result.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-filter") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-filter <dataset_uri> <predicate>");
                return ExitCode::from(2);
            };
            let Some(predicate_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-filter <dataset_uri> <predicate>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error("vortex-filter", format, "vortex filter failed", &error);
                }
            };
            let predicate = match parse_tiny_predicate(&predicate_arg) {
                Ok(predicate) => predicate,
                Err(error) => {
                    return emit_error("vortex-filter", format, "vortex filter failed", &error);
                }
            };
            let request =
                shardloom_vortex::VortexQueryPrimitiveRequest::filter(uri.clone(), predicate);
            let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
                .ok()
                .and_then(|report| report.metadata_summary)
                .unwrap_or_else(|| {
                    summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    )
                });
            let result = match evaluate_vortex_query_primitive(request, &summary) {
                Ok(result) => result,
                Err(error) => {
                    return emit_error("vortex-filter", format, "vortex filter failed", &error);
                }
            };
            emit(
                "vortex-filter",
                format,
                if result.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex filter primitive".to_string(),
                result.to_human_text(),
                result.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_filter".to_string()),
                    ("primitive".to_string(), "filter_predicate".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    (
                        "result_known".to_string(),
                        result.value.is_known().to_string(),
                    ),
                ],
            );
            if result.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-local-exec") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-local-exec <dataset_uri> <primitive>");
                return ExitCode::from(2);
            };
            let Some(primitive_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-local-exec <dataset_uri> <primitive>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-local-exec",
                        format,
                        "vortex local exec failed",
                        &error,
                    );
                }
            };
            let request = match parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-local-exec",
                        format,
                        "vortex local exec failed",
                        &error,
                    );
                }
            };
            let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
                .ok()
                .and_then(|report| report.metadata_summary);
            let report = match execute_vortex_local_query_primitive(request, summary) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-local-exec",
                        format,
                        "vortex local exec failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-local-exec",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex local execution loop skeleton".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_local_exec".to_string()),
                    ("primitive".to_string(), primitive_arg),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                    (
                        "result_known".to_string(),
                        report.value.is_known().to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-bounded-local-exec") => {
            let Some(uri_arg) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(primitive_arg) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-bounded-local-exec <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &error,
                    );
                }
            };
            let request = match parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &error,
                    );
                }
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
                .ok()
                .and_then(|report| report.metadata_summary);
            let local = match execute_vortex_local_query_primitive(request, summary) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &error,
                    );
                }
            };
            let policy = match shardloom_vortex::VortexBoundedExecutionPolicy::memory_limited(
                memory_gb,
                max_parallelism,
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &error,
                    );
                }
            };
            let report = match execute_vortex_bounded_local_query(local, policy) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        "vortex-bounded-local-exec",
                        format,
                        "vortex bounded local exec failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-bounded-local-exec",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex bounded local execution".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_bounded_local_exec".to_string()),
                    ("primitive".to_string(), primitive_arg),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                    (
                        "result_known".to_string(),
                        report.local_execution_report.value.is_known().to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-run") => {
            let Some(uri_arg) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(primitive_arg) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!(
                    "usage: shardloom vortex-run <dataset_uri> <primitive> <memory_gb> <max_parallelism>"
                );
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(v) => v,
                Err(error) => return emit_error("vortex-run", format, "vortex run failed", &error),
            };
            let primitive = match parse_vortex_local_engine_primitive(&primitive_arg) {
                Ok(v) => v,
                Err(error) => return emit_error("vortex-run", format, "vortex run failed", &error),
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-run",
                        format,
                        "vortex run failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(v) => v,
                Err(_) => {
                    return emit_error(
                        "vortex-run",
                        format,
                        "vortex run failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let request = match shardloom_vortex::VortexLocalEngineRequest::new(
                uri,
                primitive,
                memory_gb,
                max_parallelism,
            ) {
                Ok(v) => v,
                Err(error) => return emit_error("vortex-run", format, "vortex run failed", &error),
            };
            let report = match run_vortex_local_engine(request) {
                Ok(v) => v,
                Err(error) => return emit_error("vortex-run", format, "vortex run failed", &error),
            };
            emit(
                "vortex-run",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex local engine surface".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_run".to_string()),
                    ("primitive".to_string(), primitive_arg),
                    ("memory_gb".to_string(), memory_gb.to_string()),
                    ("max_parallelism".to_string(), max_parallelism.to_string()),
                    (
                        "metadata_open_report_present".to_string(),
                        report.metadata_open_report.is_some().to_string(),
                    ),
                    (
                        "metadata_open_status".to_string(),
                        report.metadata_open_report.as_ref().map_or_else(
                            || "none".to_string(),
                            |open| open.open_status.as_str().to_string(),
                        ),
                    ),
                    (
                        "metadata_open_feature_enabled".to_string(),
                        report.metadata_open_report.as_ref().map_or_else(
                            || "false".to_string(),
                            |open| open.feature_status.is_enabled().to_string(),
                        ),
                    ),
                    (
                        "file_io_performed".to_string(),
                        report.metadata_open_report.as_ref().map_or_else(
                            || "false".to_string(),
                            |open| open.file_io_performed.to_string(),
                        ),
                    ),
                    ("data_io_performed".to_string(), "false".to_string()),
                    ("object_store_io_performed".to_string(), "false".to_string()),
                    ("write_io_performed".to_string(), "false".to_string()),
                    ("result_known".to_string(), report.result_known.to_string()),
                    ("tasks_executed".to_string(), "false".to_string()),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("external_effects_executed".to_string(), "false".to_string()),
                    (
                        "execution".to_string(),
                        "metadata_only_or_not_performed".to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-query-trace") => {
            let Some(uri_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-query-trace <dataset_uri> <primitive>");
                return ExitCode::from(2);
            };
            let Some(primitive_arg) = args.next() else {
                eprintln!("usage: shardloom vortex-query-trace <dataset_uri> <primitive>");
                return ExitCode::from(2);
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error("vortex-query-trace", format, "query trace failed", &error);
                }
            };
            let request = match parse_vortex_primitive_request(uri.clone(), &primitive_arg) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("vortex-query-trace", format, "query trace failed", &error);
                }
            };
            let summary = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri))
                .ok()
                .and_then(|report| report.metadata_summary)
                .unwrap_or_else(|| {
                    summarize_vortex_metadata_probe(
                        &VortexMetadataProbeReport::deferred_api_unclear(),
                    )
                });
            let analysis = match shardloom_vortex::evaluate_vortex_query_primitive_with_analysis(
                request, &summary,
            ) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error("vortex-query-trace", format, "query trace failed", &error);
                }
            };
            emit(
                "vortex-query-trace",
                format,
                if analysis.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex query trace primitive analysis".to_string(),
                analysis.to_human_text(),
                analysis.result.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_query_trace".to_string()),
                    ("primitive".to_string(), primitive_arg),
                    ("data_read".to_string(), "false".to_string()),
                    ("data_decoded".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("spill_io_performed".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    (
                        "decision_trace_entries".to_string(),
                        analysis.decision_trace.entry_count().to_string(),
                    ),
                    (
                        "work_avoided_metrics".to_string(),
                        analysis.work_avoided.metric_count().to_string(),
                    ),
                    (
                        "result_known".to_string(),
                        analysis.result.value.is_known().to_string(),
                    ),
                ],
            );
            if analysis.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-metadata-plan") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let probe = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            let summary = summarize_vortex_metadata_probe(&probe);
            let report = match plan_from_vortex_metadata_summary(summary) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-plan",
                        format,
                        "vortex metadata plan failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-metadata-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata planning".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_plan".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), report.is_plan_only().to_string()),
                    ("data_executed".to_string(), "false".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "side_effect_free".to_string(),
                        metadata_planning_is_side_effect_free(&report).to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-pruning-plan") => {
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-pruning-plan",
                    format,
                    "vortex pruning plan failed",
                    &ShardLoomError::InvalidOperation("missing <dataset_uri> argument".to_string()),
                );
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let probe = match probe_vortex_metadata_only(uri) {
                Ok(p) => p,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let summary = summarize_vortex_metadata_probe(&probe);
            let planning = match plan_from_vortex_metadata_summary(summary) {
                Ok(p) => p,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let report = match plan_vortex_metadata_pruning(planning, None) {
                Ok(r) => r,
                Err(error) => {
                    return emit_error(
                        "vortex-pruning-plan",
                        format,
                        "vortex pruning plan failed",
                        &error,
                    );
                }
            };
            let text = report.to_human_text();
            let status = if report.has_errors() {
                CommandStatus::Error
            } else {
                CommandStatus::Success
            };
            emit(
                "vortex-pruning-plan",
                format,
                status,
                "vortex metadata pruning plan".to_string(),
                text,
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_pruning_plan".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("plan_only".to_string(), report.is_plan_only().to_string()),
                    (
                        "data_executed".to_string(),
                        report.data_executed.to_string(),
                    ),
                    (
                        "data_materialized".to_string(),
                        report.data_materialized.to_string(),
                    ),
                    (
                        "object_store_io".to_string(),
                        report.object_store_io.to_string(),
                    ),
                    ("write_io".to_string(), report.write_io.to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "side_effect_free".to_string(),
                        metadata_pruning_is_side_effect_free(&report).to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-metadata-probe") => {
            let Some(uri_text) = args.next() else {
                return emit_error(
                    "vortex-metadata-probe",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let uri = match DatasetUri::new(uri_text) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-metadata-probe",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let report = probe_vortex_metadata_only(uri)
                .unwrap_or_else(|_| VortexMetadataProbeReport::deferred_api_unclear());
            emit(
                "vortex-metadata-probe",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex metadata-only probe".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_metadata_probe".to_string()),
                    ("metadata_only".to_string(), "true".to_string()),
                    ("data_materialized".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    (
                        "metadata_io_status".to_string(),
                        report.status.as_str().to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-api-inventory") => {
            let report = VortexAdapterCapabilityReport::foundation();
            emit(
                "vortex-api-inventory",
                format,
                CommandStatus::Success,
                "vortex API inventory".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "vortex_api_inventory".to_string()),
                    (
                        "upstream_vortex_dependency".to_string(),
                        "linked".to_string(),
                    ),
                    ("actual_io".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                ],
            );
            ExitCode::SUCCESS
        }
        Some("optimizer-plan") => {
            let report = OptimizerPlanSkeleton::not_implemented(
                OptimizerPhase::VortexPhysical,
                "optimizer_execution",
                "ShardLoom optimizer planning skeleton exists, but real optimizer execution is not implemented yet.",
            );
            emit(
                "optimizer-plan",
                format,
                CommandStatus::Unsupported,
                "optimizer plan skeleton".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "optimizer_plan".to_string()),
                    ("status".to_string(), "not_implemented".to_string()),
                    ("write_io".to_string(), "false".to_string()),
                    ("execution".to_string(), "not_performed".to_string()),
                    ("plan_only".to_string(), "true".to_string()),
                    ("optimizer_phase".to_string(), "vortex_physical".to_string()),
                ],
            );
            ExitCode::from(1)
        }
        Some("estimate") => {
            let operation = args
                .next()
                .unwrap_or_else(|| "<unspecified operation>".to_string());
            let report = EstimateReport::unsupported(
                operation,
                "estimation",
                "Real estimation is not implemented yet.",
            );
            emit(
                "estimate",
                format,
                CommandStatus::Unsupported,
                "estimate plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![("mode".to_string(), "plan_only".to_string())],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        _ => {
            eprintln!("{}", cli_usage_line());
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run_test_with_larger_stack(test_name: &str, test_fn: impl FnOnce() + Send + 'static) {
        let handle = std::thread::Builder::new()
            .name(test_name.to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(test_fn)
            .expect("spawn test thread");
        handle.join().expect("join test thread");
    }

    fn run_with_larger_stack(test_name: &str, args: Vec<String>) -> ExitCode {
        let (sender, receiver) = std::sync::mpsc::channel();
        run_test_with_larger_stack(test_name, move || {
            let _ = sender.send(run(args));
        });
        receiver.recv().expect("receive test exit code")
    }

    #[test]
    fn explain_unsupported_returns_non_zero() {
        let code = run(vec!["explain".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn estimate_unsupported_returns_non_zero() {
        let code = run(vec!["estimate".to_string(), "demo-op".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn optimizer_plan_returns_non_zero() {
        let code = run(vec!["optimizer-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn manifest_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "manifest-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn incremental_plan_with_snapshot_id_returns_success() {
        let code = run(vec!["incremental-plan".to_string(), "snap-1".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_write_intent_plan_missing_target_returns_non_zero() {
        let code = run(vec!["vortex-write-intent-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_write_intent_plan_missing_signals_returns_non_zero() {
        let code = run(vec![
            "vortex-write-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_write_intent_plan_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-write-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "native-vortex-target,unknown-signal".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_write_intent_plan_native_vortex_staged_returns_success_plan_only() {
        let code = run(vec![
            "vortex-write-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "native-vortex-target,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,commit-protocol-available,staged-output-required".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_write_intent_plan_missing_commit_protocol_returns_non_zero() {
        let code = run(vec![
            "vortex-write-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "native-vortex-target,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_write_intent_plan_object_store_target_returns_non_zero() {
        let code = run(vec![
            "vortex-write-intent-plan".to_string(),
            "s3://bucket/out.vortex".to_string(),
            "native-vortex-target,schema-known,schema-compatible,object-store-target".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_intent_plan_valid_full_ready_returns_success() {
        let code = run(vec!["vortex-commit-intent-plan".to_string(),"file://tmp/out.vortex".to_string(),"commit-requested,staged-manifest-draft-written,manifest-finalization-available,commit-protocol-available,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,recovery-ready,retry-gate-open,cancellation-gate-open,feature-gate-enabled".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_intent_plan_missing_signals_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_intent_plan_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-intent-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "commit-requested,unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_intent_plan_object_store_target_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-intent-plan".to_string(),
            "s3://bucket/out.vortex".to_string(),
            "commit-requested,object-store-target".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_protocol_plan_validate_intent_ready_returns_success() {
        let code = run(vec!["vortex-commit-protocol-plan".to_string(),"file://tmp/out.vortex".to_string(),"not-started".to_string(),"validate-intent".to_string(),"commit-intent-ready,draft-manifest-ready,manifest-finalization-available,commit-marker-available,recovery-ready,feature-gate-enabled".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_protocol_plan_mark_commit_ready_missing_marker_blocks() {
        let code = run(vec!["vortex-commit-protocol-plan".to_string(),"file://tmp/out.vortex".to_string(),"awaiting-commit-marker".to_string(),"mark-commit-ready".to_string(),"commit-intent-ready,draft-manifest-ready,manifest-finalization-available,commit-marker-missing,recovery-ready".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_protocol_plan_missing_args_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-protocol-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "not-started".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_protocol_plan_unknown_transition_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-protocol-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "not-started".to_string(),
            "unknown".to_string(),
            "commit-intent-ready".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_ready_returns_success() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_object_store_target_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "s3://bucket/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,object-store-target,feature-gate-enabled".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_missing_feature_gate_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_missing_workspace_returns_non_zero() {
        let code = run(vec!["vortex-commit-marker-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_missing_signals_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
            "feature-gate-enabled,unknown-token".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_blank_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
            "   ".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_plan_json_format_returns_success() {
        let code = run(vec![
            "vortex-commit-marker-plan".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_ready_returns_success_by_default_feature_disabled() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_missing_workspace_returns_non_zero() {
        let code = run(vec!["vortex-commit-marker-write".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_missing_signals_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_missing_options_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled"
                .to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "feature-gate-enabled,unknown-token".to_string(),
            "none".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_unknown_option_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled"
                .to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_blank_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "   ".to_string(),
            "none".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_blank_options_returns_non_zero() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled"
                .to_string(),
            "   ".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_missing_feature_gate_returns_success_when_feature_disabled() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace".to_string(),
            "none".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_commit_marker_write_json_format_returns_success() {
        let code = run(vec![
            "vortex-commit-marker-write".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,manifest-finalization-available,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_staged_workspace_setup_missing_workspace_id_returns_non_zero() {
        let code = run(vec!["vortex-staged-workspace-setup".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_workspace_setup_missing_workspace_path_returns_non_zero() {
        let code = run(vec![
            "vortex-staged-workspace-setup".to_string(),
            "stage1".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_workspace_setup_missing_options_returns_non_zero() {
        let code = run(vec![
            "vortex-staged-workspace-setup".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_workspace_setup_unknown_option_returns_non_zero() {
        let code = run(vec![
            "vortex-staged-workspace-setup".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_workspace_setup_valid_args_returns_success() {
        let code = run(vec![
            "vortex-staged-workspace-setup".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
            "create-if-missing".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn parse_staged_workspace_options_deduplicates_and_trims() {
        let options = parse_vortex_staged_workspace_options(
            "create-if-missing, require-empty,create-if-missing",
        )
        .unwrap();
        assert_eq!(options.len(), 2);
        assert!(options.contains(&VortexStagedWorkspaceSetupOption::CreateIfMissing));
        assert!(options.contains(&VortexStagedWorkspaceSetupOption::RequireEmpty));
    }

    #[test]
    fn vortex_staged_marker_write_missing_workspace_id_returns_non_zero() {
        let code = run(vec!["vortex-staged-marker-write".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_marker_write_missing_workspace_path_returns_non_zero() {
        let code = run(vec![
            "vortex-staged-marker-write".to_string(),
            "stage1".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_marker_write_missing_options_uses_no_overwrite_default() {
        let code = run(vec![
            "vortex-staged-marker-write".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_marker_write_unknown_option_returns_non_zero() {
        let code = run(vec![
            "vortex-staged-marker-write".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_marker_write_valid_args_default_build_returns_success() {
        let code = run(vec![
            "vortex-staged-marker-write".to_string(),
            "stage1".to_string(),
            "/tmp/shardloom-stage".to_string(),
            "allow-overwrite".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn parse_staged_marker_options_deduplicates_and_trims() {
        let options =
            parse_vortex_staged_marker_options("allow-overwrite, allow-overwrite").unwrap();
        assert_eq!(options.len(), 1);
        assert!(options.contains(&VortexStagedMarkerOption::AllowOverwrite));
    }

    #[test]
    fn parse_staged_marker_options_whitespace_only_means_no_overwrite() {
        let options = parse_vortex_staged_marker_options("   ").unwrap();
        assert!(options.is_empty());
    }

    #[test]
    fn vortex_staged_marker_fields_include_required_defaults() {
        let fields = vortex_staged_marker_fields(
            "stage1".to_string(),
            "file:///tmp/shardloom-stage".to_string(),
            false,
        );
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&("marker_written".to_string(), "false".to_string())));
        assert!(fields.contains(&("output_data_written".to_string(), "false".to_string())));
    }

    #[test]
    fn vortex_staged_manifest_file_plan_valid_args_returns_success() {
        let code = run(vec![
            "vortex-staged-manifest-file-plan".to_string(),
            "/tmp/stage".to_string(),
            "draft-ready,workspace-known,marker-written,local-workspace".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_staged_manifest_file_write_valid_args_returns_success_report_only_when_feature_disabled()
     {
        let code = run(vec![
            "vortex-staged-manifest-file-write".to_string(),
            "/tmp/stage".to_string(),
            "file-plan-ready,workspace-known,feature-gate-enabled".to_string(),
            "allow-overwrite".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn parse_staged_manifest_file_signals_rejects_unknown_and_empty() {
        assert!(parse_vortex_staged_manifest_file_signals("draft-ready,unknown").is_err());
        assert!(parse_vortex_staged_manifest_file_signals(" ").is_err());
    }

    #[test]
    fn parse_staged_manifest_file_write_signals_and_options_validate_tokens() {
        assert!(
            parse_vortex_staged_manifest_file_write_signals("file-plan-ready,unknown").is_err()
        );
        assert!(parse_vortex_staged_manifest_file_write_options(" ").is_err());
        assert!(parse_vortex_staged_manifest_file_write_options("unknown-option").is_err());
        assert_eq!(
            parse_vortex_staged_manifest_file_write_options("none")
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn write_intent_with_target_uri_returns_non_zero() {
        let code = run(vec![
            "write-intent".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_missing_dataset_uri_returns_non_zero() {
        let code = run(vec!["scan-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn sizing_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "sizing-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
            "--memory-gb".to_string(),
            "8".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn scan_plan_with_dataset_uri_returns_success() {
        let code = run(vec![
            "scan-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn release_plan_returns_success() {
        let code = run(vec!["release-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn security_plan_returns_success() {
        let code = run(vec!["security-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn observability_plan_returns_success() {
        let code = run(vec!["observability-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_with_iceberg_returns_success() {
        let code = run(vec!["table-compat-plan".to_string(), "iceberg".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_with_unknown_returns_non_zero() {
        let code = run(vec!["table-compat-plan".to_string(), "unknown".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn translation_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn translation_plan_with_unknown_uri_returns_non_zero() {
        let code = run(vec![
            "translation-plan".to_string(),
            "file://tmp/out.unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_output_plan_with_vortex_uri_returns_success() {
        let code = run(vec![
            "vortex-output-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_plan_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-plan".to_string(),
            "file://tmp/test.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_readiness_returns_success() {
        let code = run(vec!["vortex-readiness".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_api_inventory_returns_success() {
        let code = run(vec!["vortex-api-inventory".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_dtype_mapping_returns_success() {
        let code = run(vec!["vortex-dtype-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_encoding_layout_mapping_returns_success() {
        let code = run(vec!["vortex-encoding-layout-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_statistics_mapping_command_returns_success() {
        let code = run(vec!["vortex-statistics-mapping".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_missing_uri_returns_non_zero() {
        let code = run(vec!["vortex-metadata-probe".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_invalid_uri_returns_non_zero() {
        let code = run(vec!["vortex-metadata-probe".to_string(), "   ".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_summary_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-summary".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_probe_with_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-probe".to_string(),
            "file://tmp/data.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn input_plan_with_vortex_uri_returns_success() {
        run_test_with_larger_stack("input-plan-vortex-uri", || {
            let code = run(vec![
                "input-plan".to_string(),
                "file://tmp/data.vortex".to_string(),
            ]);
            assert_eq!(code, ExitCode::SUCCESS);
        });
    }

    #[test]
    fn vortex_input_plan_with_vortex_uri_returns_success() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-input-plan".to_string(),
                    "file://tmp/data.vortex".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_input_plan_with_parquet_uri_returns_non_zero() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-input-plan".to_string(),
                    "file://tmp/data.parquet".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_vortex_uri_returns_success() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-task-graph".to_string(),
                    "file://tmp/data.vortex".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_parquet_uri_returns_non_zero() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-task-graph".to_string(),
                    "file://tmp/data.parquet".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn input_plan_with_unknown_uri_returns_non_zero() {
        run_test_with_larger_stack("input-plan-unknown-uri", || {
            let code = run(vec![
                "input-plan".to_string(),
                "file://tmp/data.unknown".to_string(),
            ]);
            assert_ne!(code, ExitCode::SUCCESS);
        });
    }

    #[test]
    fn correctness_plan_returns_success() {
        let code = run(vec!["correctness-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn detect_requested_output_format_preserves_json_for_trailing_format_flag() {
        let args = vec![
            "status".to_string(),
            "--format".to_string(),
            "json".to_string(),
            "--format".to_string(),
        ];
        assert_eq!(detect_requested_output_format(&args), OutputFormat::Json);
    }

    #[test]
    fn plan_ir_returns_success() {
        let code = run(vec!["plan-ir".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_import_returns_non_zero_for_not_implemented() {
        let code = run(vec![
            "plan-import".to_string(),
            "substrait-like".to_string(),
            "fixture".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn recovery_plan_returns_non_zero_for_not_implemented() {
        let code = run(vec!["recovery-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_export_returns_non_zero_for_not_implemented() {
        let code = run(vec!["plan-export".to_string(), "native".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_schedule_plan_with_vortex_uri_returns_success() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-schedule-plan".to_string(),
                    "file://tmp/data.vortex".to_string(),
                    "8".to_string(),
                    "2".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_execution_readiness_with_vortex_uri_returns_non_zero_when_blocked() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-execution-readiness".to_string(),
                    "file://tmp/data.vortex".to_string(),
                    "8".to_string(),
                    "2".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_dry_run_with_vortex_uri_returns_non_zero_when_readiness_blocked() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-dry-run".to_string(),
                    "file://tmp/data.vortex".to_string(),
                    "8".to_string(),
                    "2".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_execution_readiness_with_non_vortex_uri_returns_non_zero() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-execution-readiness".to_string(),
                    "file://tmp/data.parquet".to_string(),
                    "8".to_string(),
                    "2".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_schedule_plan_with_non_vortex_uri_returns_non_zero() {
        let code = std::thread::Builder::new()
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "vortex-schedule-plan".to_string(),
                    "file://tmp/data.parquet".to_string(),
                    "8".to_string(),
                    "2".to_string(),
                ])
            })
            .expect("thread spawn should succeed")
            .join()
            .expect("thread join should succeed");
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cli_contract_name_is_shardloom() {
        assert_eq!(cli_command_name(), "shardloom");
    }

    #[test]
    fn cli_contract_core_commands_dispatch_without_unknown_command_usage() {
        for command in [
            "status",
            "capabilities",
            "doctor",
            "release-plan",
            "optimizer-plan",
            "vortex-readiness",
        ] {
            let code = run(vec![command.to_string()]);
            assert_ne!(
                code,
                ExitCode::from(2),
                "command `{command}` should be recognized by dispatcher"
            );
        }
    }

    #[test]
    fn vortex_file_metadata_open_non_vortex_uri_returns_non_zero() {
        let code = run(vec![
            "vortex-file-metadata-open".to_string(),
            "file://tmp/not-vortex.parquet".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn spill_payload_roundtrip_valid_args_default_build_returns_success() {
        let code = std::thread::Builder::new()
            .name("spill-payload-roundtrip-test".to_string())
            .stack_size(8 * 1024 * 1024)
            .spawn(|| {
                run(vec![
                    "spill-payload-roundtrip".to_string(),
                    "/tmp/shardloom_spill_payload".to_string(),
                    "payload-1".to_string(),
                    "hello".to_string(),
                ])
            })
            .expect("spawn spill payload roundtrip test thread")
            .join()
            .expect("join spill payload roundtrip test thread");
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn spill_payload_roundtrip_invalid_payload_id_returns_non_zero() {
        let code = run(vec![
            "spill-payload-roundtrip".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "../bad".to_string(),
            "hello".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn spill_payload_roundtrip_empty_payload_text_returns_non_zero() {
        let code = run(vec![
            "spill-payload-roundtrip".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "payload-1".to_string(),
            String::new(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn spill_payload_roundtrip_missing_args_returns_non_zero() {
        let code = run(vec!["spill-payload-roundtrip".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_valid_args_default_build_reports_without_execution() {
        let code = run(vec![
            "cleanup-synthetic-payload".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "payload-1".to_string(),
        ]);
        assert_ne!(code, ExitCode::from(2));
    }

    #[test]
    fn cleanup_synthetic_payload_invalid_payload_id_returns_non_zero() {
        let code = run(vec![
            "cleanup-synthetic-payload".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "../bad".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_missing_args_returns_non_zero() {
        let code = run(vec!["cleanup-synthetic-payload".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_too_many_args_returns_non_zero() {
        let code = run(vec![
            "cleanup-synthetic-payload".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "payload-1".to_string(),
            "extra".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_json_format_dispatches() {
        let code = run(vec![
            "cleanup-synthetic-payload".to_string(),
            "/tmp/shardloom_spill_payload".to_string(),
            "payload-1".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_ne!(code, ExitCode::from(2));
    }

    #[test]
    fn retry_gate_plan_requested_and_allowed_returns_success() {
        let code = run(vec![
            "retry-gate-plan".to_string(),
            "retry-requested,retry-allowed".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_plan_missing_signals_returns_non_zero() {
        let code = run(vec!["retry-gate-plan".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_plan_whitespace_only_signals_returns_non_zero() {
        let code = run(vec!["retry-gate-plan".to_string(), "   ".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_plan_empty_signal_list_returns_non_zero() {
        let code = run(vec!["retry-gate-plan".to_string(), ",,,".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_plan_retry_not_allowed_returns_non_zero() {
        let code = run(vec![
            "retry-gate-plan".to_string(),
            "retry-requested".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_plan_unknown_signal_returns_non_zero() {
        let code = run(vec!["retry-gate-plan".to_string(), "unknown".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn retry_gate_signal_parsing_and_fields_cover_required_states() {
        let blocked = plan_retry_execution_gate(
            parse_retry_gate_signals(
                "retry-requested,retry-allowed,retry-requires-cleanup,unknown-artifact,external-effects,cancellation-requested",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!blocked.retry_gate_open());
        let fields = retry_gate_plan_fields(&blocked);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&("retry_executed".to_string(), "false".to_string())));

        let open = plan_retry_execution_gate(
            parse_retry_gate_signals(
                "retry-requested,retry-allowed,retry-requires-cleanup,cleanup-completed",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(open.retry_gate_open());
    }

    #[test]
    fn cancellation_gate_plan_missing_signals_returns_non_zero() {
        let code = run_with_larger_stack(
            "cancellation-gate-plan-missing-signals",
            vec!["cancellation-gate-plan".to_string()],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cancellation_gate_plan_whitespace_only_signals_returns_non_zero() {
        let code = run_with_larger_stack(
            "cancellation-gate-plan-whitespace-only-signals",
            vec!["cancellation-gate-plan".to_string(), "   ".to_string()],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cancellation_gate_plan_unknown_signal_returns_non_zero() {
        let code = run_with_larger_stack(
            "cancellation-gate-plan-unknown-signal",
            vec!["cancellation-gate-plan".to_string(), "unknown".to_string()],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cancellation_gate_plan_requested_returns_success() {
        let code = run_with_larger_stack(
            "cancellation-gate-plan-requested",
            vec![
                "cancellation-gate-plan".to_string(),
                "cancellation-requested".to_string(),
            ],
        );
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cancellation_gate_signal_parsing_and_fields_cover_required_states() {
        let cleanup_required = plan_cancellation_execution_gate(
            parse_cancellation_gate_signals("cancellation-requested,cleanup-required")
                .expect("request"),
        )
        .expect("report");
        assert!(!cleanup_required.cancellation_gate_open());

        let open = plan_cancellation_execution_gate(
            parse_cancellation_gate_signals(
                "cancellation-requested,cleanup-required,cleanup-completed",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(open.cancellation_gate_open());

        let unknown_closed = plan_cancellation_execution_gate(
            parse_cancellation_gate_signals("cancellation-requested,unknown-artifact")
                .expect("request"),
        )
        .expect("report");
        assert!(!unknown_closed.cancellation_gate_open());

        let external_closed = plan_cancellation_execution_gate(
            parse_cancellation_gate_signals("cancellation-requested,external-effects")
                .expect("request"),
        )
        .expect("report");
        assert!(!external_closed.cancellation_gate_open());

        let retry_closed = plan_cancellation_execution_gate(
            parse_cancellation_gate_signals("cancellation-requested,retry-in-progress")
                .expect("request"),
        )
        .expect("report");
        assert!(!retry_closed.cancellation_gate_open());

        let fields = cancellation_gate_plan_fields(&retry_closed);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&("cancellation_executed".to_string(), "false".to_string())));
    }
}
