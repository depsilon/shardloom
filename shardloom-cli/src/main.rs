//! Command-line entry point for `ShardLoom`.
//!
//! The `CLI` remains intentionally small in setup phase and exposes basic
//! introspection commands for workspace bring-up.

use std::process::ExitCode;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, BenchmarkPlan, ByteRange,
    CapabilityCertificationReport, CapabilityCertificationStatus, CatalogKind, CatalogRef,
    CdcEventKind, CdcEventSummary, CdcIncrementalPlanningReport, ChangeSet,
    CliApiJsonProtocolReport, ColumnRef, CommandStatus, CompactionPlanningPolicy,
    CompactionPlanningReport, ComparisonOp, CorrectnessFixture, CorrectnessValidationPlan,
    CpuOperatorSpecializationReport, DatasetFormat, DatasetManifest, DatasetRef, DatasetUri,
    DeleteModel, DeleteTombstoneCompatibilityReport, Diagnostic, EncodedSegment, EncodingKind,
    ExecutionCertificate, ExecutionCertificateEvidenceSurfaceReport, ExecutionEvidenceArtifactKind,
    ExtensionId, ExtensionInspectionReport, ExtensionLicenseKind, ExtensionManifest,
    ExtensionProvenance, ExtensionRegistrySnapshot, ExtensionVersion, FieldId, FieldName,
    FieldPath, FileDescriptor, FileRole, IncrementalPlanSkeleton, InputAdapterRegistrySnapshot,
    KernelRegistrySnapshot, LayoutHealthPolicy, LayoutHealthReport, LayoutKind, LogicalDType,
    ManifestId, ManifestSegment, NativeIoEnvelopeReport, Nullability, ObservabilityPlan,
    OperatorMemoryCertification, OutputEnvelope, OutputFormat, OutputTarget,
    PartitionEvolutionCompatibilityReport, PartitionField, PartitionSpec, PartitionTransform,
    PhysicalKernelRegistryPlan, PhysicalOperatorExecutionLevel,
    PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorKind, PhysicalOperatorPlan,
    PredicateExpr, PythonWrapperFoundationReport, RedactionPolicy, ReleasePlan,
    RuntimeObservabilityReport, SchemaDefinition, SchemaEvolutionCompatibilityReport,
    SchemaEvolutionPolicy, SchemaField, SchemaId, SchemaVersion, SecurityPlan, SegmentChange,
    SegmentChangeKind, SegmentId, SegmentLayout, SegmentStats, ShardLoomError, SnapshotId,
    SnapshotRef, StatValue, StatefulReuseReport, TableCompatibilityPlan, TableCompatibilityReport,
    TableFormatKind, TranslationPlan, UdfRuntimeKind, UniversalHarnessReport,
    WorldClassSufficiencyDimensionKind, WorldClassSufficiencyReport, WriteIntent,
    evaluate_cdc_incremental_planning, evaluate_compaction_planning,
    evaluate_delete_tombstone_compatibility, evaluate_layout_health,
    evaluate_partition_evolution_compatibility, evaluate_schema_evolution_compatibility,
    plan_cpu_operator_specialization, plan_execution_certificate_evidence_surface,
    plan_native_io_envelope, plan_stateful_reuse, plan_universal_harness,
    plan_world_class_sufficiency,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, AttemptId, BackpressurePlanInput, BackpressurePlanReport,
    BoundedMemoryPolicy, ByteSize, CancellationReason, CancellationRequest, CancellationScope,
    DynamicSizingFeedbackInput, DynamicSizingFeedbackReport, EncodedStreamingBatchPlanInput,
    EncodedStreamingBatchPlanReport, MemoryBudget, MemoryOwner, MemoryPoolPlan, OomSafetyPlan,
    OperatorMemoryClass, ParallelismLimit, ParallelismPlan, RecoveryPlan, RetryPlan,
    RuntimePlanSkeleton, ShardLoomCancellationExecutionGateReport,
    ShardLoomCancellationExecutionGateRequest, ShardLoomCancellationExecutionGateSignal,
    ShardLoomCleanupExecutionRequest, ShardLoomRetryExecutionGateReport,
    ShardLoomRetryExecutionGateRequest, ShardLoomRetryExecutionGateSignal, SizeEstimate,
    SizingFeedbackSignal, SizingFeedbackSignalKind, SizingInput, SizingPlan, SpillLifecycleRequest,
    SpillPayloadFsRef, SpillPayloadId, SpillPayloadPath, SpillPayloadRef,
    SpillPayloadRoundTripRequest, SpillPayloadWriteRequest, SpillPlan, SpillPolicy,
    SpillReservationIntegrationRequest, SpillWorkspaceId, SpillWorkspacePath,
    StreamingPlanSkeleton, StreamingSink, StreamingSource, SyntheticSpillPayload,
    TaskAttemptRecord, plan_backpressure, plan_cancellation_execution_gate,
    plan_dynamic_sizing_feedback, plan_encoded_streaming_batches, plan_retry_execution_gate,
    plan_spill_lifecycle, plan_spill_reservation_integration, roundtrip_spill_payload,
    spill_payload_fs_feature_enabled,
};
use shardloom_plan::{
    AdaptiveOptimizerMemoryReport, EstimateReport, ExplainReport, NativePlanDocument,
    NativePlanNode, NativePlanNodeKind, ObjectStoreCheckpointRetryInput,
    ObjectStoreCheckpointRetryReport, ObjectStoreCommitProtocolInput,
    ObjectStoreCommitProtocolReport, ObjectStoreDistributedSchedulingPolicy,
    ObjectStoreDistributedSchedulingReport, ObjectStoreRangePlanningPolicy,
    ObjectStoreRangePlanningReport, ObjectStoreRequestCoalescingReport, OptimizerPhase,
    OptimizerPlanSkeleton, PlanBoundaryKind, PlanCapabilityKind, PlanCapabilityRequirement,
    PlanExportRequest, PlanId, PlanImportRequest, PlanInteropFormat, PlanLayer, PlanNodeId,
    PlanPortabilityReport, ProjectionRequest, ScanPlanSkeleton, ScanRequest,
    plan_adaptive_optimizer_memory, plan_object_store_checkpoint_retry,
    plan_object_store_commit_protocol, plan_object_store_distributed_scheduling,
    plan_object_store_ranges, plan_object_store_request_coalescing, plan_universal_input_source,
};
use shardloom_vortex::{
    VortexAdapterCapabilityReport, VortexAdapterReadiness, VortexAdaptiveSizingReport,
    VortexBoundedExecutionReport, VortexCommitIntentReport, VortexCommitIntentRequest,
    VortexCommitIntentSignal, VortexCommitMarkerContent, VortexCommitMarkerFileName,
    VortexCommitMarkerFileRef, VortexCommitMarkerRequest, VortexCommitMarkerSignal,
    VortexCommitMarkerWriteOption, VortexCommitProtocolReport, VortexCommitProtocolRequest,
    VortexCommitProtocolSignal, VortexCommitProtocolState, VortexCommitProtocolTransition,
    VortexCountCandidateSource, VortexCountReadinessRequest, VortexCountReadinessSignal,
    VortexDTypeMappingReport, VortexEncodedCountKernelAdmissionReport,
    VortexEncodedCountPhysicalKernelReport, VortexEncodedExecutionPathSelectionReport,
    VortexEncodedReadBoundaryReport, VortexEncodedReadBoundaryRequest,
    VortexEncodedReadBoundarySignal, VortexEncodedReadFixtureRef,
    VortexEncodedReadMetadataProbeReport, VortexEncodedReadMetadataProbeRequest,
    VortexEncodedReadMetadataProbeSignal, VortexEncodedReadReadinessStatus,
    VortexEncodingLayoutMappingReport, VortexExecutionReadinessStatus, VortexFileRef,
    VortexFilteredCountCandidateSource, VortexFilteredCountReadinessSignal,
    VortexFinalizedManifestArtifactWriteOption, VortexFinalizedManifestContent,
    VortexFinalizedManifestFileName, VortexFinalizedManifestFileRef,
    VortexGeneralizedEncodedPrimitiveGateReport, VortexLayoutReaderDriverApprovalInput,
    VortexLayoutReaderDriverApprovalSignal, VortexLocalCommitExecutionRequest,
    VortexLocalCommitExecutionSignal, VortexLocalCommitRecoveryRequest,
    VortexLocalCommitRecoverySignal, VortexLocalExecutionReport, VortexManifestFinalizationRequest,
    VortexManifestFinalizationSignal, VortexMemoryBridgeReport,
    VortexMetadataCountKernelAdmissionReport, VortexMetadataFilterKernelAdmissionReport,
    VortexMetadataOpenRequest, VortexMetadataProbeReport, VortexNativeOutputPayloadWriteReport,
    VortexOutputPayloadContentDescriptor, VortexOutputPayloadFileName, VortexOutputPayloadFileRef,
    VortexOutputPayloadReport, VortexOutputPayloadRequest, VortexOutputPayloadSignal,
    VortexProjectionCandidateSource, VortexProjectionReadinessSignal, VortexQueryPrimitiveRequest,
    VortexQueryPrimitiveResult, VortexQueryPrimitiveSignal, VortexQueryPrimitiveValue,
    VortexReadPlan, VortexSchedulerBridgeReport, VortexStagedManifestDraftContent,
    VortexStagedManifestFileEffect, VortexStagedManifestFileRef, VortexStagedManifestFileReport,
    VortexStagedManifestFileRequest, VortexStagedManifestFileSignal,
    VortexStagedManifestFileWriteEffect, VortexStagedManifestFileWriteOption,
    VortexStagedManifestFileWriteRequest, VortexStagedManifestFileWriteSignal,
    VortexStagedMarkerOption, VortexStagedMarkerRequest, VortexStagedWorkspaceId,
    VortexStagedWorkspacePath, VortexStagedWorkspaceSetupOption, VortexStagedWorkspaceSetupRequest,
    VortexStatisticsMappingReport, VortexStreamingBatchRuntimeReport, VortexTaskSchedulingDecision,
    VortexWriteIntentReport, VortexWriteIntentRequest, VortexWriteIntentSignal, VortexWriteOptions,
    VortexWritePlan, admit_vortex_encoded_count_kernel, admit_vortex_metadata_count_kernel,
    admit_vortex_metadata_filter_kernel, build_vortex_runtime_task_graph,
    commit_marker_write_request_from_plan, evaluate_vortex_encoded_read_readiness,
    evaluate_vortex_execution_readiness, evaluate_vortex_local_encoded_count_physical_kernel,
    evaluate_vortex_metadata_physical_kernels, evaluate_vortex_query_primitive,
    execute_vortex_bounded_local_query, execute_vortex_count_all_from_approved_local_scan,
    execute_vortex_count_all_from_approved_local_scan_result,
    execute_vortex_count_all_from_encoded_count_data_path_approval,
    execute_vortex_encoded_read_contract, execute_vortex_encoded_read_spike,
    execute_vortex_local_commit, execute_vortex_local_commit_rollback,
    execute_vortex_local_query_primitive, execute_vortex_metadata_only,
    execute_vortex_streaming_batches_from_local_encoded_count,
    finalized_manifest_artifact_write_request_from_plan, local_encoded_count_execution_certificate,
    metadata_planning_is_side_effect_free, metadata_pruning_is_side_effect_free,
    metadata_summary_is_plan_only, native_output_payload_write_request_from_plan,
    open_vortex_metadata_only, output_payload_artifact_write_request_from_plan,
    parse_vortex_local_engine_primitive, plan_from_vortex_metadata_summary,
    plan_native_vortex_universal_input, plan_vortex_commit_intent, plan_vortex_commit_marker,
    plan_vortex_commit_protocol, plan_vortex_count_readiness,
    plan_vortex_encoded_count_data_path_approval,
    plan_vortex_encoded_count_data_path_approval_with_layout_driver,
    plan_vortex_encoded_execution_path_selection, plan_vortex_encoded_read_boundary,
    plan_vortex_encoded_read_probe, plan_vortex_filtered_count_readiness,
    plan_vortex_generalized_encoded_primitive_gate, plan_vortex_layout_reader_driver_approval,
    plan_vortex_local_commit_recovery, plan_vortex_manifest_finalization,
    plan_vortex_memory_safety, plan_vortex_metadata_pruning, plan_vortex_output_payload,
    plan_vortex_projection_readiness, plan_vortex_query_primitive,
    plan_vortex_query_primitive_result_physical_operators_with_evidence,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    plan_vortex_staged_manifest_file, plan_vortex_write_intent, probe_vortex_encoded_read_metadata,
    probe_vortex_metadata_only, run_vortex_local_engine, setup_vortex_staged_workspace,
    size_vortex_runtime_task_graph, summarize_vortex_metadata_probe,
    vortex_encoded_count_local_guard_discovery_report,
    vortex_encoded_count_physical_kernel_discovery_report,
    vortex_encoded_predicate_evaluation_discovery_report,
    vortex_encoded_read_executor_feature_enabled,
    vortex_encoded_read_local_scan_count_api_boundary, vortex_encoded_read_public_api_boundary,
    vortex_encoded_read_spike_feature_enabled, vortex_file_io_feature_enabled,
    vortex_local_commit_execution_feature_enabled, vortex_metadata_executor_feature_enabled,
    vortex_native_output_payload_write_feature_enabled,
    vortex_selection_vector_filter_kernel_discovery_report, write_vortex_commit_marker,
    write_vortex_finalized_manifest_artifact, write_vortex_native_count_output_payload,
    write_vortex_output_payload_artifact, write_vortex_staged_manifest_file,
    write_vortex_staged_marker,
};

fn main() -> ExitCode {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    run_with_cli_stack(args)
}

const CLI_COMMAND_NAME: &str = "shardloom";
const CLI_STACK_SIZE: usize = 16 * 1024 * 1024;

fn run_with_cli_stack(args: Vec<String>) -> ExitCode {
    let handle = match std::thread::Builder::new()
        .name("shardloom-cli".to_string())
        .stack_size(CLI_STACK_SIZE)
        .spawn(move || run(args))
    {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("failed to start shardloom CLI worker thread: {error}");
            return ExitCode::from(1);
        }
    };

    if let Ok(code) = handle.join() {
        code
    } else {
        eprintln!("shardloom CLI worker thread panicked");
        ExitCode::from(1)
    }
}

fn cli_command_name() -> &'static str {
    CLI_COMMAND_NAME
}

fn cli_usage_line() -> String {
    format!(
        "usage: {} <status|release-plan|package-plan|api-compat-plan|python-wrapper-plan|capabilities [sql|functions|operators|adapters|semantic-profiles|migration|certification|data-etl|python|dataframe|notebook|udfs|universal-adapters|event-api-saas-adapters|unstructured-media|api-surfaces|observability|deployment|extensions|security-governance]|security-plan|agent-safety-plan|redaction-plan|kernel-registry|doctor|manifest-plan|incremental-plan|stateful-reuse-plan|universal-harness-plan|native-io-envelope-plan|world-class-sufficiency-plan|layout-health-plan|compaction-plan|object-store-range-plan|object-store-coalesce-plan|object-store-schedule-plan|object-store-checkpoint-retry-plan|object-store-commit-plan|write-intent|scan-plan|streaming-plan|streaming-batch-plan|backpressure-plan|runtime-plan|task-plan|sizing-plan|sizing-feedback-plan|translation-plan|vortex-plan|vortex-output-plan|vortex-readiness|vortex-api-inventory|vortex-dtype-mapping|vortex-encoding-layout-mapping|vortex-statistics-mapping|vortex-metadata-probe|vortex-file-metadata-open|vortex-metadata-summary|vortex-metadata-plan|vortex-pruning-plan|optimizer-plan|optimizer-adaptive-memory-plan|cpu-specialization-plan|explain|estimate|benchmark-plan|correctness-plan|execution-certificate-plan|recovery-plan|cancellation-plan|retry-plan|observability-plan|runtime-report|profile-plan|plan-ir|plan-import|plan-export|table-compat-plan [aggregate|partition-evolution|delete-semantics]|schema-plan|input-adapters|input-plan|vortex-input-plan|vortex-read-plan|vortex-task-graph|vortex-adaptive-sizing|vortex-memory-plan|vortex-schedule-plan|vortex-execution-readiness|vortex-encoded-path-selection-plan|vortex-generalized-encoded-primitive-gate|vortex-encoded-read-api|vortex-encoded-read-boundary|vortex-encoded-read-metadata-probe|vortex-encoded-read-readiness|vortex-encoded-read-probe|vortex-encoded-read-execute|vortex-encoded-read-spike|vortex-dry-run|vortex-metadata-execute|vortex-query-primitive-plan|vortex-metadata-physical-kernel-plan|vortex-count-readiness-plan|vortex-encoded-count-approval-plan|vortex-layout-driver-approval-plan|vortex-filtered-count-readiness-plan|vortex-projection-readiness-plan|vortex-count|vortex-count-where|vortex-staged-workspace-setup|vortex-staged-marker-write|vortex-staged-manifest-file-plan|vortex-staged-manifest-file-write|vortex-output-payload-plan|vortex-output-payload-artifact-write|vortex-native-count-payload-write|vortex-manifest-finalization-plan|vortex-finalized-manifest-artifact-write|vortex-commit-marker-plan|vortex-commit-marker-write|vortex-commit-intent-plan|vortex-commit-protocol-plan|vortex-local-commit-execute|vortex-local-commit-recovery-plan|vortex-local-commit-rollback-execute|vortex-project|vortex-filter|vortex-query-trace|vortex-local-exec|vortex-bounded-local-exec|vortex-run|spill-lifecycle|spill-reservation-plan|spill-payload-roundtrip|cleanup-synthetic-payload|retry-gate-plan <signals>|cancellation-gate-plan <signals>> [--format text|json]",
        cli_command_name()
    )
}
fn parse_vortex_output_payload_signals(
    signals_raw: &str,
) -> Result<Vec<VortexOutputPayloadSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "output payload signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "output payload signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "write-intent-ready" => VortexOutputPayloadSignal::WriteIntentReady,
            "write-intent-blocked" => VortexOutputPayloadSignal::WriteIntentBlocked,
            "staged-output-ready" => VortexOutputPayloadSignal::StagedOutputReady,
            "staged-output-blocked" => VortexOutputPayloadSignal::StagedOutputBlocked,
            "finalized-manifest-ready" => VortexOutputPayloadSignal::FinalizedManifestReady,
            "finalized-manifest-missing" => VortexOutputPayloadSignal::FinalizedManifestMissing,
            "payload-content-available" => VortexOutputPayloadSignal::PayloadContentAvailable,
            "payload-content-missing" => VortexOutputPayloadSignal::PayloadContentMissing,
            "local-workspace" => VortexOutputPayloadSignal::LocalWorkspace,
            "object-store-target" => VortexOutputPayloadSignal::ObjectStoreTarget,
            "upstream-vortex-write-required" => {
                VortexOutputPayloadSignal::UpstreamVortexWriteRequired
            }
            "feature-gate-enabled" => VortexOutputPayloadSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown output payload signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_encoded_read_boundary_signals(
    signals_raw: &str,
) -> Result<Vec<VortexEncodedReadBoundarySignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "encoded read boundary signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "encoded read boundary signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "upstream-open-options-available" => {
                VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable
            }
            "upstream-footer-available" => VortexEncodedReadBoundarySignal::UpstreamFooterAvailable,
            "upstream-metadata-surface-available" => {
                VortexEncodedReadBoundarySignal::UpstreamMetadataSurfaceAvailable
            }
            "upstream-scan-surface-deferred" => {
                VortexEncodedReadBoundarySignal::UpstreamScanSurfaceDeferred
            }
            "local-path-only" => VortexEncodedReadBoundarySignal::LocalPathOnly,
            "object-store-target" => VortexEncodedReadBoundarySignal::ObjectStoreTarget,
            "decode-risk" => VortexEncodedReadBoundarySignal::DecodeRisk,
            "materialization-risk" => VortexEncodedReadBoundarySignal::MaterializationRisk,
            "arrow-default-risk" => VortexEncodedReadBoundarySignal::ArrowDefaultRisk,
            "write-risk" => VortexEncodedReadBoundarySignal::WriteRisk,
            "feature-gate-enabled" => VortexEncodedReadBoundarySignal::FeatureGateEnabled,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-encoded-read-boundary",
                    "encoded-read-boundary",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_encoded_read_metadata_probe_signals(
    signals_raw: &str,
) -> Result<Vec<VortexEncodedReadMetadataProbeSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "encoded read metadata probe signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "encoded read metadata probe signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "fixture-ready" => VortexEncodedReadMetadataProbeSignal::FixtureReady,
            "fixture-blocked" => VortexEncodedReadMetadataProbeSignal::FixtureBlocked,
            "fixture-ref-provided" => VortexEncodedReadMetadataProbeSignal::FixtureRefProvided,
            "local-path-only" => VortexEncodedReadMetadataProbeSignal::LocalPathOnly,
            "object-store-target" => VortexEncodedReadMetadataProbeSignal::ObjectStoreTarget,
            "scan-execution-risk" => VortexEncodedReadMetadataProbeSignal::ScanExecutionRisk,
            "decode-risk" => VortexEncodedReadMetadataProbeSignal::DecodeRisk,
            "materialization-risk" => VortexEncodedReadMetadataProbeSignal::MaterializationRisk,
            "arrow-default-risk" => VortexEncodedReadMetadataProbeSignal::ArrowDefaultRisk,
            "write-risk" => VortexEncodedReadMetadataProbeSignal::WriteRisk,
            "feature-gate-enabled" => VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-encoded-read-metadata-probe",
                    "encoded-read-metadata-probe",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_layout_driver_approval_signals(
    signals_raw: &str,
) -> Result<Vec<VortexLayoutReaderDriverApprovalSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "layout driver approval signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "layout driver approval signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "local-fixture-only" => VortexLayoutReaderDriverApprovalSignal::LocalFixtureOnly,
            "caller-session-allowed" => {
                VortexLayoutReaderDriverApprovalSignal::CallerSessionAllowed
            }
            "runtime-driver-start-allowed" => {
                VortexLayoutReaderDriverApprovalSignal::RuntimeDriverStartAllowed
            }
            "layout-row-count-only-intent" => {
                VortexLayoutReaderDriverApprovalSignal::LayoutRowCountOnlyIntent
            }
            "scan-forbidden" => VortexLayoutReaderDriverApprovalSignal::ScanForbidden,
            "evaluation-forbidden" => VortexLayoutReaderDriverApprovalSignal::EvaluationForbidden,
            "data-read-forbidden" => VortexLayoutReaderDriverApprovalSignal::DataReadForbidden,
            "decode-forbidden" => VortexLayoutReaderDriverApprovalSignal::DecodeForbidden,
            "materialization-forbidden" => {
                VortexLayoutReaderDriverApprovalSignal::MaterializationForbidden
            }
            "arrow-forbidden" => VortexLayoutReaderDriverApprovalSignal::ArrowForbidden,
            "object-store-forbidden" => {
                VortexLayoutReaderDriverApprovalSignal::ObjectStoreForbidden
            }
            "write-forbidden" => VortexLayoutReaderDriverApprovalSignal::WriteForbidden,
            "fallback-forbidden" => VortexLayoutReaderDriverApprovalSignal::FallbackForbidden,
            _ => {
                return Err(cli_unknown_signal_error(
                    "vortex-layout-driver-approval-plan",
                    "layout-driver-approval",
                    token,
                ));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn vortex_encoded_read_boundary_fields(
    report: &VortexEncodedReadBoundaryReport,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "vortex_encoded_read_boundary".to_string(),
        ),
        (
            "upstream_open_options_available".to_string(),
            report.upstream_open_options_available().to_string(),
        ),
        (
            "upstream_footer_available".to_string(),
            report.upstream_footer_available().to_string(),
        ),
        (
            "upstream_metadata_surface_available".to_string(),
            report.upstream_metadata_surface_available().to_string(),
        ),
        (
            "upstream_scan_surface_deferred".to_string(),
            report.upstream_scan_surface_deferred().to_string(),
        ),
        (
            "local_path_only".to_string(),
            report.local_path_only().to_string(),
        ),
        (
            "object_store_target".to_string(),
            report.object_store_target().to_string(),
        ),
        ("decode_risk".to_string(), report.decode_risk().to_string()),
        (
            "materialization_risk".to_string(),
            report.materialization_risk().to_string(),
        ),
        (
            "arrow_default_risk".to_string(),
            report.arrow_default_risk().to_string(),
        ),
        ("write_risk".to_string(), report.write_risk().to_string()),
        ("data_read".to_string(), "false".to_string()),
        ("array_decoded".to_string(), "false".to_string()),
        ("values_materialized".to_string(), "false".to_string()),
        ("arrow_converted".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("data_written".to_string(), "false".to_string()),
        ("upstream_scan_called".to_string(), "false".to_string()),
        ("read_execution_allowed".to_string(), "false".to_string()),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}

fn vortex_encoded_read_metadata_probe_fields(
    report: &VortexEncodedReadMetadataProbeReport,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "vortex_encoded_read_metadata_probe".to_string(),
        ),
        (
            "fixture_ready".to_string(),
            report.fixture_ready().to_string(),
        ),
        (
            "fixture_ref_provided".to_string(),
            report.fixture_ref_provided().to_string(),
        ),
        (
            "local_path_only".to_string(),
            report.local_path_only().to_string(),
        ),
        (
            "object_store_target".to_string(),
            report.object_store_target().to_string(),
        ),
        (
            "scan_execution_risk".to_string(),
            report.scan_execution_risk().to_string(),
        ),
        ("decode_risk".to_string(), report.decode_risk().to_string()),
        (
            "materialization_risk".to_string(),
            report.materialization_risk().to_string(),
        ),
        (
            "arrow_default_risk".to_string(),
            report.arrow_default_risk().to_string(),
        ),
        ("write_risk".to_string(), report.write_risk().to_string()),
        (
            "metadata_opened".to_string(),
            report.metadata_opened().to_string(),
        ),
        (
            "footer_inspected".to_string(),
            report.footer_inspected().to_string(),
        ),
        (
            "encoded_data_read".to_string(),
            report.encoded_data_read().to_string(),
        ),
        ("row_read".to_string(), report.row_read().to_string()),
        (
            "array_decoded".to_string(),
            report.array_decoded().to_string(),
        ),
        (
            "values_materialized".to_string(),
            report.values_materialized().to_string(),
        ),
        (
            "arrow_converted".to_string(),
            report.arrow_converted().to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io().to_string(),
        ),
        (
            "data_written".to_string(),
            report.data_written().to_string(),
        ),
        (
            "upstream_scan_called".to_string(),
            report.upstream_scan_called().to_string(),
        ),
        (
            "metadata_probe_completed".to_string(),
            report.metadata_probe_completed().to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
    ]
}

fn vortex_encoded_path_selection_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    let mut fields = vortex_encoded_path_selection_identity_fields(report);
    fields.extend(vortex_encoded_path_selection_candidate_fields(report));
    fields.extend(vortex_encoded_path_selection_discovery_fields(report));
    fields.extend(vortex_encoded_path_selection_side_effect_fields(report));
    fields
}

fn vortex_encoded_path_selection_identity_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "mode".to_string(),
            "vortex_encoded_path_selection_plan".to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("report_id".to_string(), report.report_id.clone()),
        (
            "profile_matrix_id".to_string(),
            report.profile_matrix_id.clone(),
        ),
        (
            "selection_status".to_string(),
            report.status.as_str().to_string(),
        ),
        ("entry_count".to_string(), report.entry_count().to_string()),
        (
            "operator_order".to_string(),
            report.operator_order().join(","),
        ),
        (
            "selected_execution_levels".to_string(),
            report.selected_execution_levels().join(","),
        ),
        (
            "evidence_sources".to_string(),
            report.evidence_sources().join(","),
        ),
    ]
}

fn vortex_encoded_path_selection_candidate_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "direct_count_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::CountAggregate)
                .to_string(),
        ),
        (
            "direct_filter_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::Filter)
                .to_string(),
        ),
        (
            "direct_project_candidate_present".to_string(),
            report
                .has_operator(PhysicalOperatorKind::Project)
                .to_string(),
        ),
        (
            "metadata_only_candidate_count".to_string(),
            report.metadata_only_candidate_count().to_string(),
        ),
        (
            "encoded_native_candidate_count".to_string(),
            report.encoded_native_candidate_count().to_string(),
        ),
        (
            "hybrid_native_candidate_count".to_string(),
            report.hybrid_native_candidate_count().to_string(),
        ),
        (
            "native_decoded_candidate_count".to_string(),
            report.native_decoded_candidate_count().to_string(),
        ),
        (
            "decode_avoided_candidate_count".to_string(),
            report.decode_avoided_candidate_count().to_string(),
        ),
        (
            "materialization_avoided_candidate_count".to_string(),
            report.materialization_avoided_candidate_count().to_string(),
        ),
        (
            "selection_vector_preserved_count".to_string(),
            report.selection_vector_preserved_count().to_string(),
        ),
    ]
}

fn vortex_encoded_path_selection_discovery_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        (
            "encoded_count_discovery_present".to_string(),
            report.encoded_count_discovery_present.to_string(),
        ),
        (
            "encoded_predicate_discovery_present".to_string(),
            report.encoded_predicate_discovery_present.to_string(),
        ),
        (
            "selection_vector_filter_discovery_present".to_string(),
            report.selection_vector_filter_discovery_present.to_string(),
        ),
        (
            "encoded_projection_evidence_present".to_string(),
            report.encoded_projection_evidence_present.to_string(),
        ),
    ]
}

fn vortex_encoded_path_selection_side_effect_fields(
    report: &VortexEncodedExecutionPathSelectionReport,
) -> Vec<(String, String)> {
    vec![
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        ("row_read".to_string(), report.row_read.to_string()),
        (
            "arrow_converted".to_string(),
            report.arrow_converted.to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("write_io".to_string(), report.write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            report.spill_io_performed.to_string(),
        ),
        (
            "runtime_execution_allowed".to_string(),
            report.runtime_execution_allowed.to_string(),
        ),
        (
            "external_engine_execution".to_string(),
            report.external_engine_execution.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    let mut fields = vortex_generalized_encoded_primitive_gate_identity_fields(report);
    fields.extend(vortex_generalized_encoded_primitive_gate_evidence_fields(
        report,
    ));
    fields.extend(vortex_generalized_encoded_primitive_gate_requirement_fields(report));
    fields.extend(vortex_generalized_encoded_primitive_gate_side_effect_fields(report));
    fields
}

fn vortex_generalized_encoded_primitive_gate_identity_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "mode".to_string(),
            "vortex_generalized_encoded_primitive_gate".to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        ("report_id".to_string(), report.report_id.clone()),
        (
            "gate_status".to_string(),
            report.status.as_str().to_string(),
        ),
        ("entry_count".to_string(), report.entry_count().to_string()),
        (
            "primitive_order".to_string(),
            report.primitive_order().join(","),
        ),
        (
            "primitive_statuses".to_string(),
            report.primitive_statuses().join(","),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_evidence_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "local_count_all_only".to_string(),
            report.local_count_all_only.to_string(),
        ),
        (
            "entries_with_local_count_support".to_string(),
            report.entries_with_local_count_support().to_string(),
        ),
        (
            "entries_with_metadata_proof".to_string(),
            report.entries_with_metadata_proof().to_string(),
        ),
        (
            "entries_with_readiness_contract".to_string(),
            report.entries_with_readiness_contract().to_string(),
        ),
        (
            "implementation_blocker_count".to_string(),
            report.implementation_blocker_count().to_string(),
        ),
        (
            "required_next_evidence_count".to_string(),
            report.required_next_evidence_count().to_string(),
        ),
        (
            "generalized_count_ready".to_string(),
            report.generalized_count_ready.to_string(),
        ),
        (
            "filtered_count_execution_ready".to_string(),
            report.filtered_count_execution_ready.to_string(),
        ),
        (
            "projection_execution_ready".to_string(),
            report.projection_execution_ready.to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_requirement_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        (
            "requires_public_scan_or_read_start_path".to_string(),
            report.requires_public_scan_or_read_start_path.to_string(),
        ),
        (
            "requires_encoded_predicate_path".to_string(),
            report.requires_encoded_predicate_path.to_string(),
        ),
        (
            "requires_encoded_projection_path".to_string(),
            report.requires_encoded_projection_path.to_string(),
        ),
        (
            "requires_selection_vector_pipeline".to_string(),
            report.requires_selection_vector_pipeline.to_string(),
        ),
        (
            "requires_native_io_certificate".to_string(),
            report.requires_native_io_certificate.to_string(),
        ),
        (
            "requires_execution_certificate".to_string(),
            report.requires_execution_certificate.to_string(),
        ),
        (
            "requires_correctness_evidence".to_string(),
            report.requires_correctness_evidence.to_string(),
        ),
        (
            "requires_benchmark_evidence".to_string(),
            report.requires_benchmark_evidence.to_string(),
        ),
    ]
}

fn vortex_generalized_encoded_primitive_gate_side_effect_fields(
    report: &VortexGeneralizedEncodedPrimitiveGateReport,
) -> Vec<(String, String)> {
    vec![
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        ("row_read".to_string(), report.row_read.to_string()),
        (
            "arrow_converted".to_string(),
            report.arrow_converted.to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("write_io".to_string(), report.write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            report.spill_io_performed.to_string(),
        ),
        (
            "runtime_execution_allowed".to_string(),
            report.runtime_execution_allowed.to_string(),
        ),
        (
            "external_engine_execution".to_string(),
            report.external_engine_execution.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "diagnostic_count".to_string(),
            report.diagnostics.len().to_string(),
        ),
    ]
}

fn correctness_plan_fields(plan: &CorrectnessValidationPlan) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), "correctness_plan".to_string()),
        ("status".to_string(), plan.status.as_str().to_string()),
        (
            "fallback_execution_allowed".to_string(),
            plan.fallback_execution_allowed().to_string(),
        ),
        (
            "external_baselines".to_string(),
            "test_oracles_only".to_string(),
        ),
        (
            "fixture_count".to_string(),
            plan.fixture_count().to_string(),
        ),
        (
            "fixture_id_order".to_string(),
            plan.fixture_id_order().join(","),
        ),
        (
            "semantic_area_order".to_string(),
            plan.semantic_area_order().join(","),
        ),
        (
            "edge_case_order".to_string(),
            plan.edge_case_order().join(","),
        ),
        (
            "reference_role_order".to_string(),
            plan.reference_role_order().join(","),
        ),
        (
            "fixtures_with_source_ref_count".to_string(),
            plan.fixtures_with_source_ref_count().to_string(),
        ),
        (
            "golden_fixture_count".to_string(),
            plan.golden_fixture_count().to_string(),
        ),
        (
            "executable_expected_output_count".to_string(),
            plan.executable_expected_output_count().to_string(),
        ),
        (
            "not_yet_defined_fixture_count".to_string(),
            plan.not_yet_defined_fixture_count().to_string(),
        ),
        (
            "diagnostic_expected_output_count".to_string(),
            plan.diagnostic_expected_output_count().to_string(),
        ),
        (
            "unsupported_expected_output_count".to_string(),
            plan.unsupported_expected_output_count().to_string(),
        ),
        (
            "baseline_count".to_string(),
            plan.baseline_count().to_string(),
        ),
        (
            "covered_required_foundation_edge_case_count".to_string(),
            plan.covered_required_foundation_edge_case_count()
                .to_string(),
        ),
        (
            "required_foundation_edge_case_count".to_string(),
            CorrectnessValidationPlan::required_foundation_edge_cases()
                .len()
                .to_string(),
        ),
        (
            "missing_required_foundation_edge_cases".to_string(),
            plan.missing_required_foundation_edge_cases().join(","),
        ),
        (
            "required_foundation_edge_cases_covered".to_string(),
            plan.required_foundation_edge_cases_covered().to_string(),
        ),
        (
            "reference_roles_test_only".to_string(),
            plan.reference_roles_are_test_only().to_string(),
        ),
        (
            "baselines_fallback_free".to_string(),
            plan.baselines_are_fallback_free().to_string(),
        ),
    ]
}

fn benchmark_plan_fields(plan: &BenchmarkPlan) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    append_benchmark_plan_overview_fields(&mut fields, plan);
    append_benchmark_plan_scenario_fields(&mut fields, plan);
    append_benchmark_plan_metric_fields(&mut fields, plan);
    append_benchmark_plan_claim_fields(&mut fields, plan);
    fields
}

fn append_benchmark_plan_overview_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    let claim_gate = plan.claim_gate();
    push_field(fields, "mode", "benchmark_plan");
    push_field(fields, "status", "planned");
    push_bool_field(
        fields,
        "benchmark_execution_implemented",
        plan.benchmark_execution_implemented(),
    );
    push_bool_field(
        fields,
        "performance_claim_allowed",
        claim_gate.can_publish_performance_claim(),
    );
    push_bool_field(fields, "fallback_execution_allowed", false);
    push_field(fields, "external_baselines", "comparison_only");
}

fn append_benchmark_plan_scenario_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    push_count_field(fields, "scenario_count", plan.scenario_count());
    push_field(
        fields,
        "scenario_name_order",
        &plan.scenario_name_order().join(","),
    );
    push_field(
        fields,
        "workload_class_order",
        &plan.workload_class_order().join(","),
    );
    push_field(
        fields,
        "correctness_validation_order",
        &plan.correctness_validation_order().join(","),
    );
    push_count_field(
        fields,
        "scenario_with_correctness_validation_count",
        plan.scenario_with_correctness_validation_count(),
    );
    push_count_field(
        fields,
        "scenario_with_required_metrics_count",
        plan.scenario_with_required_metrics_count(),
    );
    push_count_field(
        fields,
        "scenario_with_baselines_count",
        plan.scenario_with_baselines_count(),
    );
}

fn append_benchmark_plan_metric_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    push_count_field(
        fields,
        "required_metric_count",
        plan.required_metrics().len(),
    );
    push_field(
        fields,
        "required_metric_order",
        &plan.required_metric_order().join(","),
    );
    push_count_field(
        fields,
        "required_foundation_metric_count",
        BenchmarkPlan::required_foundation_metrics().len(),
    );
    push_count_field(
        fields,
        "covered_required_foundation_metric_count",
        plan.covered_required_foundation_metric_count(),
    );
    push_field(
        fields,
        "missing_required_foundation_metrics",
        &plan.missing_required_foundation_metrics().join(","),
    );
    push_bool_field(
        fields,
        "required_foundation_metrics_covered",
        plan.required_foundation_metrics_covered(),
    );
    push_bool_field(
        fields,
        "runtime_metrics_covered",
        plan.runtime_metrics_covered(),
    );
    push_bool_field(
        fields,
        "peak_memory_metric_covered",
        plan.peak_memory_metric_covered(),
    );
    push_bool_field(
        fields,
        "bytes_read_written_metrics_covered",
        plan.bytes_read_written_metrics_covered(),
    );
    push_bool_field(
        fields,
        "startup_latency_metric_covered",
        plan.startup_latency_metric_covered(),
    );
    push_bool_field(
        fields,
        "query_runtime_metric_covered",
        plan.query_runtime_metric_covered(),
    );
    push_bool_field(
        fields,
        "write_commit_latency_metric_covered",
        plan.write_commit_latency_metric_covered(),
    );
    push_bool_field(
        fields,
        "spill_metrics_covered",
        plan.spill_metrics_covered(),
    );
    push_bool_field(
        fields,
        "object_store_request_metric_covered",
        plan.object_store_request_metric_covered(),
    );
    push_bool_field(
        fields,
        "materialization_metrics_covered",
        plan.materialization_metrics_covered(),
    );
}

fn append_benchmark_plan_claim_fields(fields: &mut Vec<(String, String)>, plan: &BenchmarkPlan) {
    let claim_gate = plan.claim_gate();
    push_field(
        fields,
        "baseline_engine_order",
        &plan.baseline_engine_order().join(","),
    );
    push_field(
        fields,
        "external_baseline_engine_order",
        &plan.external_baseline_engine_order().join(","),
    );
    push_count_field(
        fields,
        "external_baseline_count",
        plan.external_baseline_count(),
    );
    push_count_field(
        fields,
        "expected_result_count",
        plan.expected_result_count(),
    );
    push_field(fields, "claim_gate_status", claim_gate.status.as_str());
    push_field(
        fields,
        "claim_gate_correctness_evidence",
        claim_gate.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "claim_gate_benchmark_evidence",
        claim_gate.benchmark_evidence.as_str(),
    );
    push_field(
        fields,
        "claim_gate_required_metrics",
        claim_gate.required_metrics.as_str(),
    );
    push_field(
        fields,
        "claim_gate_comparison_report",
        claim_gate.comparison_report.as_str(),
    );
    push_field(
        fields,
        "claim_gate_reproducibility_evidence",
        claim_gate.reproducibility_evidence.as_str(),
    );
    push_field(fields, "claim_gate_fallback", claim_gate.fallback.as_str());
    push_bool_field(
        fields,
        "baselines_fallback_free",
        plan.baselines_are_fallback_free(),
    );
}

fn streaming_plan_fields(plan: &StreamingPlanSkeleton) -> Vec<(String, String)> {
    vec![
        ("mode".to_string(), plan.mode.as_str().to_string()),
        ("status".to_string(), plan.status.as_str().to_string()),
        (
            "source_kind".to_string(),
            plan.source.kind.as_str().to_string(),
        ),
        (
            "source_capability".to_string(),
            plan.source.capability.as_str().to_string(),
        ),
        (
            "source_zero_decode".to_string(),
            plan.source.zero_decode.as_str().to_string(),
        ),
        ("sink_kind".to_string(), plan.sink.kind.as_str().to_string()),
        (
            "sink_capability".to_string(),
            plan.sink.capability.as_str().to_string(),
        ),
        (
            "sink_accepts_encoded".to_string(),
            plan.sink.requirement.accepts_encoded.to_string(),
        ),
        (
            "sink_requires_materialization".to_string(),
            plan.sink.requirement.requires_materialization.to_string(),
        ),
        (
            "sink_preserves_metadata".to_string(),
            plan.sink.requirement.preserves_metadata.to_string(),
        ),
        (
            "backpressure_enabled".to_string(),
            plan.backpressure.enabled.to_string(),
        ),
        (
            "backpressure_bounded".to_string(),
            plan.backpressure.is_bounded().to_string(),
        ),
        (
            "memory_policy_required".to_string(),
            plan.memory.required.to_string(),
        ),
        (
            "memory_policy_allow_spill".to_string(),
            plan.memory.allow_spill.to_string(),
        ),
        (
            "materialization_required".to_string(),
            plan.requires_materialization().to_string(),
        ),
        (
            "best_data_work_level".to_string(),
            plan.best_data_work_level().as_str().to_string(),
        ),
        ("stage_count".to_string(), plan.stages.len().to_string()),
        (
            "operator_count".to_string(),
            plan.operators.len().to_string(),
        ),
        ("runtime_execution".to_string(), "false".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
    ]
}

fn encoded_streaming_batch_plan_fields(
    report: &EncodedStreamingBatchPlanReport,
    memory_gb: u64,
    estimated_batch_mib: Option<u64>,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "streaming_batch_plan");
    push_field(
        &mut fields,
        "encoded_streaming_batch_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "streaming_mode", report.mode.as_str());
    push_field(
        &mut fields,
        "source_kind",
        report.input.source.kind.as_str(),
    );
    push_field(
        &mut fields,
        "source_capability",
        report.input.source.capability.as_str(),
    );
    push_field(&mut fields, "sink_kind", report.input.sink.kind.as_str());
    push_field(
        &mut fields,
        "sink_capability",
        report.input.sink.capability.as_str(),
    );
    push_field(
        &mut fields,
        "representation",
        report.representation.as_str(),
    );
    push_field(&mut fields, "zero_decode", report.zero_decode.as_str());
    push_bool_field(
        &mut fields,
        "encoded_representation_preserved",
        report.encoded_representation_preserved,
    );
    push_bool_field(
        &mut fields,
        "selection_vector_preserved",
        report.selection_vector_preserved,
    );
    push_bool_field(
        &mut fields,
        "bounded_parallelism",
        report.bounded_parallelism,
    );
    push_count_field(&mut fields, "max_parallelism", report.input.max_parallelism);
    push_bool_field(&mut fields, "bounded_memory", report.bounded_memory);
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_bool_field(
        &mut fields,
        "backpressure_bounded",
        report.backpressure_bounded,
    );
    push_bool_field(
        &mut fields,
        "materialization_required",
        report.materialization_boundary.required,
    );
    push_field(
        &mut fields,
        "materialization_boundary",
        report.materialization_boundary.canonical_label(),
    );
    push_field(
        &mut fields,
        "estimated_batch_count",
        &report
            .estimated_batch_count
            .map_or("unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "estimated_batch_mib",
        &estimated_batch_mib.map_or("unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "estimated_batch_bytes",
        &report
            .estimated_batch_bytes
            .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
    );
    push_bool_field(&mut fields, "streams_executed", report.streams_executed);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "row_read", report.row_read);
    push_bool_field(&mut fields, "arrow_converted", report.arrow_converted);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_field(&mut fields, "execution", "not_performed");
    fields
}

fn parse_vortex_output_payload_artifact_write_options(
    options_raw: &str,
) -> Result<bool, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "output payload artifact write options must not be empty".to_string(),
        ));
    }
    let mut allow_overwrite = false;
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "output payload artifact write options must not contain empty tokens".to_string(),
            ));
        }
        match token {
            "allow-overwrite" => allow_overwrite = true,
            "none" => {}
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown output payload artifact write option token: {token}"
                )));
            }
        }
    }
    Ok(allow_overwrite)
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

fn parse_vortex_local_commit_execution_signals(
    signals_raw: &str,
) -> Result<Vec<VortexLocalCommitExecutionSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local commit execution signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local commit execution signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "commit-protocol-ready" => VortexLocalCommitExecutionSignal::CommitProtocolReady,
            "commit-protocol-blocked" => VortexLocalCommitExecutionSignal::CommitProtocolBlocked,
            "finalized-manifest-written" => {
                VortexLocalCommitExecutionSignal::FinalizedManifestWritten
            }
            "finalized-manifest-missing" => {
                VortexLocalCommitExecutionSignal::FinalizedManifestMissing
            }
            "commit-marker-written" => VortexLocalCommitExecutionSignal::CommitMarkerWritten,
            "commit-marker-missing" => VortexLocalCommitExecutionSignal::CommitMarkerMissing,
            "output-payload-written" => VortexLocalCommitExecutionSignal::OutputPayloadWritten,
            "output-payload-missing" => VortexLocalCommitExecutionSignal::OutputPayloadMissing,
            "local-workspace" => VortexLocalCommitExecutionSignal::LocalWorkspace,
            "object-store-target" => VortexLocalCommitExecutionSignal::ObjectStoreTarget,
            "feature-gate-enabled" => VortexLocalCommitExecutionSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown local commit execution signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_local_commit_recovery_signals(
    signals_raw: &str,
) -> Result<Vec<VortexLocalCommitRecoverySignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local commit recovery signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local commit recovery signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "rollback-requested" => VortexLocalCommitRecoverySignal::RollbackRequested,
            "committed-manifest-written" => {
                VortexLocalCommitRecoverySignal::CommittedManifestWritten
            }
            "committed-manifest-missing" => {
                VortexLocalCommitRecoverySignal::CommittedManifestMissing
            }
            "already-committed" => VortexLocalCommitRecoverySignal::AlreadyCommitted,
            "ambiguous-commit" => VortexLocalCommitRecoverySignal::AmbiguousCommittedManifest,
            "local-workspace" => VortexLocalCommitRecoverySignal::LocalWorkspace,
            "object-store-target" => VortexLocalCommitRecoverySignal::ObjectStoreTarget,
            "cleanup-allowed" => VortexLocalCommitRecoverySignal::CleanupAllowed,
            "cleanup-blocked" => VortexLocalCommitRecoverySignal::CleanupBlocked,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown local commit recovery signal token: {token}"
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

fn finalized_manifest_cli_content(
    cli_write: bool,
) -> Result<VortexFinalizedManifestContent, ShardLoomError> {
    let mode = if cli_write { "cli_write" } else { "cli_plan" };
    VortexFinalizedManifestContent::new(format!(
        "shardloom_finalized_manifest_candidate=true\n{mode}=true\nfinalized_manifest_written=false\nmanifest_committed=false\noutput_data_written=false\nfallback_execution_allowed=false\n"
    ))
}

fn parse_vortex_manifest_finalization_signals(
    signals_raw: &str,
) -> Result<Vec<VortexManifestFinalizationSignal>, ShardLoomError> {
    if signals_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "manifest finalization signals must not be empty".to_string(),
        ));
    }
    let mut signals = Vec::new();
    for token in signals_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "manifest finalization signals must not contain empty tokens".to_string(),
            ));
        }
        let signal = match token {
            "draft-manifest-written" => VortexManifestFinalizationSignal::DraftManifestWritten,
            "draft-manifest-missing" => VortexManifestFinalizationSignal::DraftManifestMissing,
            "commit-marker-written" => VortexManifestFinalizationSignal::CommitMarkerWritten,
            "commit-marker-missing" => VortexManifestFinalizationSignal::CommitMarkerMissing,
            "commit-protocol-ready" => VortexManifestFinalizationSignal::CommitProtocolReady,
            "commit-protocol-blocked" => VortexManifestFinalizationSignal::CommitProtocolBlocked,
            "schema-known" => VortexManifestFinalizationSignal::SchemaKnown,
            "schema-compatible" => VortexManifestFinalizationSignal::SchemaCompatible,
            "delete-semantics-known" => VortexManifestFinalizationSignal::DeleteSemanticsKnown,
            "tombstone-semantics-known" => {
                VortexManifestFinalizationSignal::TombstoneSemanticsKnown
            }
            "local-workspace" => VortexManifestFinalizationSignal::LocalWorkspace,
            "object-store-target" => VortexManifestFinalizationSignal::ObjectStoreTarget,
            "feature-gate-enabled" => VortexManifestFinalizationSignal::FeatureGateEnabled,
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown manifest finalization signal token: {token}"
                )));
            }
        };
        if !signals.contains(&signal) {
            signals.push(signal);
        }
    }
    Ok(signals)
}

fn parse_vortex_finalized_manifest_artifact_write_options(
    options_raw: &str,
) -> Result<Vec<VortexFinalizedManifestArtifactWriteOption>, ShardLoomError> {
    if options_raw.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "finalized manifest artifact write options must not be empty".to_string(),
        ));
    }
    let mut options = Vec::new();
    for token in options_raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "finalized manifest artifact write options must not contain empty tokens"
                    .to_string(),
            ));
        }
        match token {
            "allow-overwrite" => {
                if !options.contains(&VortexFinalizedManifestArtifactWriteOption::AllowOverwrite) {
                    options.push(VortexFinalizedManifestArtifactWriteOption::AllowOverwrite);
                }
            }
            "none" => {}
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "unknown finalized manifest artifact write option token: {token}"
                )));
            }
        }
    }
    Ok(options)
}

fn cli_missing_arg_error(command: &str, arg: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{command} missing required argument: <{arg}>"))
}

fn cli_unknown_arg_error(command: &str, value: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{command} unknown argument/value: {value}"))
}

fn cli_unknown_signal_error(command: &str, signal_family: &str, token: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{command} unknown {signal_family} signal: {token}"))
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

fn handle_schema_plan(mut args: std::vec::IntoIter<String>, format: OutputFormat) -> ExitCode {
    match args.next().as_deref() {
        None => emit_schema_plan_skeleton(format),
        Some("evolution") => {
            let scenario = args.next().unwrap_or_else(|| "add-nullable".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "schema-plan",
                    format,
                    "schema evolution plan failed",
                    &cli_unknown_arg_error("schema-plan evolution", &extra),
                );
            }
            emit_schema_evolution_plan(format, &scenario)
        }
        Some(value) => emit_error(
            "schema-plan",
            format,
            "schema plan failed",
            &cli_unknown_arg_error("schema-plan", value),
        ),
    }
}

fn emit_schema_plan_skeleton(format: OutputFormat) -> ExitCode {
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
            (
                "schema_evolution_report_emitted".to_string(),
                "false".to_string(),
            ),
            ("data_read".to_string(), "false".to_string()),
            ("write_io".to_string(), "false".to_string()),
            ("catalog_io".to_string(), "false".to_string()),
            ("object_store_io".to_string(), "false".to_string()),
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

fn emit_schema_evolution_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (from, to, policy) = match schema_evolution_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "schema-plan",
                format,
                "schema evolution plan failed",
                &error,
            );
        }
    };
    let report = evaluate_schema_evolution_compatibility(&from, &to, &policy);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "schema-plan",
        format,
        status,
        "schema evolution compatibility report".to_string(),
        report.to_human_text(),
        report.compatibility.diagnostics.clone(),
        schema_evolution_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn schema_evolution_output_fields(
    report: &SchemaEvolutionCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "schema_evolution_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "schema_evolution_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.compatibility.level.as_str().to_string(),
        ),
        (
            "change_count".to_string(),
            report.compatibility.changes.len().to_string(),
        ),
        (
            "safe_change_count".to_string(),
            report.safe_change_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "field_id_required_count".to_string(),
            report.field_id_required_count.to_string(),
        ),
        (
            "missing_field_id_count".to_string(),
            report.missing_field_id_count.to_string(),
        ),
        (
            "requires_projection".to_string(),
            report.requires_projection.to_string(),
        ),
        (
            "requires_cast".to_string(),
            report.requires_cast.to_string(),
        ),
        (
            "requires_default_values".to_string(),
            report.requires_default_values.to_string(),
        ),
        (
            "metadata_loss_reported".to_string(),
            report.metadata_loss_reported.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

fn schema_evolution_fixture(
    scenario: &str,
) -> Result<(SchemaDefinition, SchemaDefinition, SchemaEvolutionPolicy), ShardLoomError> {
    let policy = SchemaEvolutionPolicy::default_conservative();
    match scenario {
        "exact" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_v1(true, LogicalDType::Int64)?,
            policy,
        )),
        "add-nullable" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_with_extra_region()?,
            policy,
        )),
        "rename-with-id" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_renamed_status(true)?,
            policy,
        )),
        "rename-without-id" => Ok((
            orders_schema_v1(false, LogicalDType::Int64)?,
            orders_schema_renamed_status(false)?,
            policy,
        )),
        "drop-field" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_without_status()?,
            policy,
        )),
        "widen" => Ok((
            orders_schema_v1(true, LogicalDType::Int64)?,
            orders_schema_v1(true, LogicalDType::Float64)?,
            policy,
        )),
        "narrow" => Ok((
            orders_schema_v1(true, LogicalDType::Float64)?,
            orders_schema_v1(true, LogicalDType::Int64)?,
            policy,
        )),
        value => Err(cli_unknown_arg_error("schema-plan evolution", value)),
    }
}

fn table_compatibility_aggregation_fixture(
    scenario: &str,
) -> Result<(&'static str, &'static str, &'static str), ShardLoomError> {
    match scenario {
        "compatible" => Ok(("exact", "same", "none-to-file-level")),
        "schema-blocked" => Ok(("rename-without-id", "same", "none")),
        "partition-blocked" => Ok(("exact", "unknown-transform", "none")),
        "delete-blocked" => Ok(("exact", "same", "equality-delete")),
        value => Err(cli_unknown_arg_error("table-compat-plan aggregate", value)),
    }
}

fn orders_schema_v1(
    with_ids: bool,
    amount_dtype: LogicalDType,
) -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(1)?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f2",
        "status",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f3",
        "amount",
        amount_dtype,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_with_extra_region() -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = orders_schema_v1(true, LogicalDType::Int64)?;
    schema.version = SchemaVersion::new(2)?;
    schema.add_field(schema_fixture_field(
        true,
        "f4",
        "region",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_renamed_status(with_ids: bool) -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(2)?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f2",
        "order_status",
        LogicalDType::Utf8,
        Nullability::Nullable,
    )?);
    schema.add_field(schema_fixture_field(
        with_ids,
        "f3",
        "amount",
        LogicalDType::Int64,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn orders_schema_without_status() -> Result<SchemaDefinition, ShardLoomError> {
    let mut schema = SchemaDefinition::new(SchemaId::new("orders")?, SchemaVersion::new(2)?);
    schema.add_field(schema_fixture_field(
        true,
        "f1",
        "order_id",
        LogicalDType::Int64,
        Nullability::NonNullable,
    )?);
    schema.add_field(schema_fixture_field(
        true,
        "f3",
        "amount",
        LogicalDType::Int64,
        Nullability::Nullable,
    )?);
    Ok(schema)
}

fn schema_fixture_field(
    with_id: bool,
    id: &str,
    name: &str,
    dtype: LogicalDType,
    nullability: Nullability,
) -> Result<SchemaField, ShardLoomError> {
    let field = SchemaField::new(FieldName::new(name)?, dtype, nullability);
    if with_id {
        Ok(field.with_id(FieldId::new(id)?))
    } else {
        Ok(field)
    }
}

fn handle_table_compat_plan(
    mut args: std::vec::IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    match args.next().as_deref() {
        Some("aggregate") => {
            let scenario = args.next().unwrap_or_else(|| "compatible".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "table compatibility aggregation failed",
                    &cli_unknown_arg_error("table-compat-plan aggregate", &extra),
                );
            }
            emit_table_compatibility_aggregation(format, &scenario)
        }
        Some("partition-evolution") => {
            let scenario = args.next().unwrap_or_else(|| "add-field".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "partition evolution plan failed",
                    &cli_unknown_arg_error("table-compat-plan partition-evolution", &extra),
                );
            }
            emit_partition_evolution_plan(format, &scenario)
        }
        Some("delete-semantics") => {
            let scenario = args.next().unwrap_or_else(|| "file-level".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "delete/tombstone plan failed",
                    &cli_unknown_arg_error("table-compat-plan delete-semantics", &extra),
                );
            }
            emit_delete_tombstone_plan(format, &scenario)
        }
        maybe_format => emit_table_compat_plan(format, maybe_format),
    }
}

fn emit_table_compat_plan(format: OutputFormat, format_token: Option<&str>) -> ExitCode {
    let format_kind = match format_token {
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

fn emit_table_compatibility_aggregation(format: OutputFormat, scenario: &str) -> ExitCode {
    let (schema_scenario, partition_scenario, delete_scenario) =
        match table_compatibility_aggregation_fixture(scenario) {
            Ok(parts) => parts,
            Err(error) => {
                return emit_error(
                    "table-compat-plan",
                    format,
                    "table compatibility aggregation failed",
                    &error,
                );
            }
        };
    let (from_schema, to_schema, policy) = match schema_evolution_fixture(schema_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };
    let (from_spec, to_spec) = match partition_evolution_fixture(partition_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };
    let (source_model, target_model) = match delete_tombstone_fixture(delete_scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "table compatibility aggregation failed",
                &error,
            );
        }
    };

    let schema_report = evaluate_schema_evolution_compatibility(&from_schema, &to_schema, &policy);
    let partition_report = evaluate_partition_evolution_compatibility(&from_spec, &to_spec);
    let delete_report = evaluate_delete_tombstone_compatibility(source_model, target_model);
    let plan = TableCompatibilityPlan::native_vortex().with_delete_model(target_model);
    let report = TableCompatibilityReport::from_plan(plan)
        .with_schema_evolution_report(schema_report)
        .with_partition_evolution_report(partition_report)
        .with_delete_tombstone_report(delete_report);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    let diagnostics = table_compatibility_aggregation_diagnostics(&report);

    emit(
        "table-compat-plan",
        format,
        status,
        "table compatibility aggregation report".to_string(),
        report.to_human_text(),
        diagnostics,
        table_compatibility_aggregation_output_fields(
            &report,
            scenario,
            schema_scenario,
            partition_scenario,
            delete_scenario,
        ),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn table_compatibility_aggregation_output_fields(
    report: &TableCompatibilityReport,
    scenario: &str,
    schema_scenario: &str,
    partition_scenario: &str,
    delete_scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "mode".to_string(),
            "table_compatibility_aggregation".to_string(),
        ),
        ("scenario".to_string(), scenario.to_string()),
        ("schema_scenario".to_string(), schema_scenario.to_string()),
        (
            "partition_scenario".to_string(),
            partition_scenario.to_string(),
        ),
        ("delete_scenario".to_string(), delete_scenario.to_string()),
        (
            "table_compatibility_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "evidence_report_count".to_string(),
            report.evidence_report_count().to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported().to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported().to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.side_effect_free().to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ];
    if let Some(schema_report) = &report.schema_evolution_report {
        fields.push((
            "schema_evolution_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "schema_compatibility_level".to_string(),
            schema_report.compatibility.level.as_str().to_string(),
        ));
        fields.push((
            "schema_unsafe_change_count".to_string(),
            schema_report.unsafe_change_count.to_string(),
        ));
    }
    if let Some(partition_report) = &report.partition_evolution_report {
        fields.push((
            "partition_evolution_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "partition_compatibility_level".to_string(),
            partition_report.level.as_str().to_string(),
        ));
        fields.push((
            "partition_unsafe_change_count".to_string(),
            partition_report.unsafe_change_count.to_string(),
        ));
    }
    if let Some(delete_report) = &report.delete_tombstone_report {
        fields.push((
            "delete_tombstone_report_emitted".to_string(),
            "true".to_string(),
        ));
        fields.push((
            "delete_compatibility_level".to_string(),
            delete_report.level.as_str().to_string(),
        ));
        fields.push((
            "delete_unsafe_change_count".to_string(),
            delete_report.unsafe_change_count.to_string(),
        ));
    }
    fields
}

fn table_compatibility_aggregation_diagnostics(
    report: &TableCompatibilityReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = report.plan.diagnostics.clone();
    if let Some(schema_report) = &report.schema_report {
        diagnostics.extend(schema_report.diagnostics.clone());
    }
    if let Some(schema_evolution_report) = &report.schema_evolution_report {
        diagnostics.extend(schema_evolution_report.compatibility.diagnostics.clone());
    }
    if let Some(partition_evolution_report) = &report.partition_evolution_report {
        diagnostics.extend(partition_evolution_report.diagnostics.clone());
    }
    if let Some(delete_tombstone_report) = &report.delete_tombstone_report {
        diagnostics.extend(delete_tombstone_report.diagnostics.clone());
    }
    diagnostics.extend(report.diagnostics.clone());
    diagnostics
}

fn emit_partition_evolution_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (from_spec, to_spec) = match partition_evolution_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "partition evolution plan failed",
                &error,
            );
        }
    };
    let report = evaluate_partition_evolution_compatibility(&from_spec, &to_spec);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "table-compat-plan",
        format,
        status,
        "partition evolution compatibility report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        partition_evolution_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn partition_evolution_output_fields(
    report: &PartitionEvolutionCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "partition_evolution_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "partition_evolution_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.level.as_str().to_string(),
        ),
        ("change_count".to_string(), report.changes.len().to_string()),
        (
            "preserved_field_count".to_string(),
            report.preserved_field_count.to_string(),
        ),
        (
            "added_field_count".to_string(),
            report.added_field_count.to_string(),
        ),
        (
            "dropped_field_count".to_string(),
            report.dropped_field_count.to_string(),
        ),
        (
            "transform_change_count".to_string(),
            report.transform_change_count.to_string(),
        ),
        (
            "reorder_count".to_string(),
            report.reorder_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "requires_partition_router".to_string(),
            report.requires_partition_router.to_string(),
        ),
        (
            "requires_metadata_rewrite".to_string(),
            report.requires_metadata_rewrite.to_string(),
        ),
        (
            "requires_repartition".to_string(),
            report.requires_repartition.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

fn emit_delete_tombstone_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (source_model, target_model) = match delete_tombstone_fixture(scenario) {
        Ok(models) => models,
        Err(error) => {
            return emit_error(
                "table-compat-plan",
                format,
                "delete/tombstone plan failed",
                &error,
            );
        }
    };
    let report = evaluate_delete_tombstone_compatibility(source_model, target_model);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "table-compat-plan",
        format,
        status,
        "delete/tombstone compatibility report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        delete_tombstone_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn delete_tombstone_output_fields(
    report: &DeleteTombstoneCompatibilityReport,
    scenario: &str,
) -> Vec<(String, String)> {
    vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "delete_tombstone_plan".to_string()),
        ("scenario".to_string(), scenario.to_string()),
        (
            "delete_tombstone_report_emitted".to_string(),
            "true".to_string(),
        ),
        (
            "compatibility_level".to_string(),
            report.level.as_str().to_string(),
        ),
        (
            "source_delete_model".to_string(),
            report.source_model.as_str().to_string(),
        ),
        (
            "target_delete_model".to_string(),
            report.target_model.as_str().to_string(),
        ),
        (
            "delete_semantics_preserved".to_string(),
            report.delete_semantics_preserved.to_string(),
        ),
        (
            "tombstone_semantics_preserved".to_string(),
            report.tombstone_semantics_preserved.to_string(),
        ),
        (
            "requires_explicit_delete_handling".to_string(),
            report.requires_explicit_delete_handling.to_string(),
        ),
        (
            "requires_file_delete_filter".to_string(),
            report.requires_file_delete_filter.to_string(),
        ),
        (
            "requires_tombstone_filter".to_string(),
            report.requires_tombstone_filter.to_string(),
        ),
        (
            "requires_row_identity".to_string(),
            report.requires_row_identity.to_string(),
        ),
        (
            "requires_position_identity".to_string(),
            report.requires_position_identity.to_string(),
        ),
        (
            "requires_equality_predicate".to_string(),
            report.requires_equality_predicate.to_string(),
        ),
        (
            "requires_external_table_metadata".to_string(),
            report.requires_external_table_metadata.to_string(),
        ),
        (
            "metadata_loss_reported".to_string(),
            report.metadata_loss_reported.to_string(),
        ),
        (
            "unsupported_model_count".to_string(),
            report.unsupported_model_count.to_string(),
        ),
        (
            "unsafe_change_count".to_string(),
            report.unsafe_change_count.to_string(),
        ),
        (
            "read_supported".to_string(),
            report.read_supported.to_string(),
        ),
        (
            "write_supported".to_string(),
            report.write_supported.to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("write_io".to_string(), report.write_io.to_string()),
        ("catalog_io".to_string(), report.catalog_io.to_string()),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("execution".to_string(), "not_performed".to_string()),
        ("plan_only".to_string(), "true".to_string()),
        (
            "table_formats_are".to_string(),
            "compatibility_targets_not_fallback_engines".to_string(),
        ),
    ]
}

fn delete_tombstone_fixture(scenario: &str) -> Result<(DeleteModel, DeleteModel), ShardLoomError> {
    match scenario {
        "none" => Ok((DeleteModel::None, DeleteModel::None)),
        "file-level" => Ok((DeleteModel::FileLevelDelete, DeleteModel::FileLevelDelete)),
        "none-to-file-level" => Ok((DeleteModel::None, DeleteModel::FileLevelDelete)),
        "file-to-none" => Ok((DeleteModel::FileLevelDelete, DeleteModel::None)),
        "segment-tombstone" => Ok((
            DeleteModel::SegmentLevelTombstone,
            DeleteModel::SegmentLevelTombstone,
        )),
        "row-level" => Ok((DeleteModel::RowLevelDelete, DeleteModel::RowLevelDelete)),
        "position-delete" => Ok((DeleteModel::PositionDelete, DeleteModel::PositionDelete)),
        "equality-delete" => Ok((DeleteModel::EqualityDelete, DeleteModel::EqualityDelete)),
        "external-table-metadata" => Ok((
            DeleteModel::ExternalTableMetadata,
            DeleteModel::ExternalTableMetadata,
        )),
        "unknown" => Ok((DeleteModel::Unknown, DeleteModel::Unknown)),
        value => Err(cli_unknown_arg_error(
            "table-compat-plan delete-semantics",
            value,
        )),
    }
}

fn partition_evolution_fixture(
    scenario: &str,
) -> Result<(PartitionSpec, PartitionSpec), ShardLoomError> {
    match scenario {
        "same" => {
            let spec = base_partition_spec()?;
            Ok((spec.clone(), spec))
        }
        "add-field" => Ok((base_partition_spec()?, added_partition_field_spec()?)),
        "change-transform" => Ok((base_partition_spec()?, changed_partition_transform_spec()?)),
        "drop-field" => Ok((added_partition_field_spec()?, base_partition_spec()?)),
        "reorder" => Ok((added_partition_field_spec()?, reordered_partition_spec()?)),
        "unknown-transform" => Ok((base_partition_spec()?, unknown_partition_transform_spec()?)),
        value => Err(cli_unknown_arg_error(
            "table-compat-plan partition-evolution",
            value,
        )),
    }
}

fn base_partition_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Day,
    )?]))
}

fn added_partition_field_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![
        partition_fixture_field("created_at", PartitionTransform::Day)?,
        partition_fixture_field("customer_id", PartitionTransform::Bucket { buckets: 16 })?,
    ]))
}

fn changed_partition_transform_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Month,
    )?]))
}

fn reordered_partition_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![
        partition_fixture_field("customer_id", PartitionTransform::Bucket { buckets: 16 })?,
        partition_fixture_field("created_at", PartitionTransform::Day)?,
    ]))
}

fn unknown_partition_transform_spec() -> Result<PartitionSpec, ShardLoomError> {
    Ok(partition_spec_from_fields(vec![partition_fixture_field(
        "created_at",
        PartitionTransform::Unknown("vendor_specific".to_string()),
    )?]))
}

fn partition_spec_from_fields(fields: Vec<PartitionField>) -> PartitionSpec {
    let mut spec = PartitionSpec::empty();
    for field in fields {
        spec.add_field(field);
    }
    spec
}

fn partition_fixture_field(
    source: &str,
    transform: PartitionTransform,
) -> Result<PartitionField, ShardLoomError> {
    Ok(PartitionField::new(
        FieldPath::from_dot_separated(source)?,
        transform,
    ))
}

#[must_use]
fn safe_metadata_kernel_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[must_use]
fn metadata_kernel_memory_safe(memory: OperatorMemoryCertification) -> bool {
    memory.oom_safe
        && !memory.requires_full_materialization
        && (memory.streaming || memory.bounded_memory || memory.spillable)
}

#[allow(clippy::too_many_lines)]
fn run_vortex_metadata_physical_kernel_plan(format: OutputFormat, args: Vec<String>) -> ExitCode {
    let command = "vortex-metadata-physical-kernel-plan";
    let mut args = args.into_iter();
    let Some(primitive_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing primitive",
            &cli_missing_arg_error(command, "primitive"),
        );
    };
    let Some(uri_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing dataset uri",
            &cli_missing_arg_error(command, "dataset_uri"),
        );
    };
    let Some(value_arg) = args.next() else {
        return emit_error(
            command,
            format,
            "missing metadata value",
            &cli_missing_arg_error(command, "metadata_value"),
        );
    };
    let uri = match DatasetUri::new(uri_arg) {
        Ok(uri) => uri,
        Err(error) => return emit_error(command, format, "invalid dataset uri", &error),
    };
    let (request, value) = match primitive_arg.as_str() {
        "count" | "count-all" | "count_all" => {
            let Ok(count) = value_arg.parse::<u64>() else {
                return emit_error(
                    command,
                    format,
                    "invalid count metadata value",
                    &ShardLoomError::InvalidOperation(format!(
                        "count metadata value must be u64: {value_arg}"
                    )),
                );
            };
            (
                VortexQueryPrimitiveRequest::count_all(uri),
                VortexQueryPrimitiveValue::Count(count),
            )
        }
        "filtered-count" | "filtered_count" => {
            let Ok(count) = value_arg.parse::<u64>() else {
                return emit_error(
                    command,
                    format,
                    "invalid filtered count metadata value",
                    &ShardLoomError::InvalidOperation(format!(
                        "filtered count metadata value must be u64: {value_arg}"
                    )),
                );
            };
            (
                VortexQueryPrimitiveRequest::count_where(uri, PredicateExpr::AlwaysTrue),
                VortexQueryPrimitiveValue::Count(count),
            )
        }
        "filter" | "predicate-filter" | "predicate_filter" => {
            let value = match value_arg.as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    return emit_error(
                        command,
                        format,
                        "invalid filter metadata value",
                        &ShardLoomError::InvalidOperation(format!(
                            "filter metadata value must be true or false: {value_arg}"
                        )),
                    );
                }
            };
            (
                VortexQueryPrimitiveRequest::filter(uri, PredicateExpr::AlwaysFalse),
                VortexQueryPrimitiveValue::Boolean(value),
            )
        }
        _ => {
            return emit_error(
                command,
                format,
                "invalid primitive",
                &ShardLoomError::InvalidOperation(format!("invalid primitive: {primitive_arg}")),
            );
        }
    };
    let mut correctness_evidence = BenchmarkEvidenceState::Missing;
    let mut benchmark_evidence = BenchmarkEvidenceState::Missing;
    let mut memory = OperatorMemoryCertification::unsupported();
    let mut fallback = BenchmarkFallbackState::NotAttempted;
    for token in args {
        match token.as_str() {
            "--correctness-evidence" | "--correctness-passed" => {
                correctness_evidence = BenchmarkEvidenceState::Present;
            }
            "--benchmark-evidence" | "--benchmark-passed" => {
                benchmark_evidence = BenchmarkEvidenceState::Present;
            }
            "--memory-safe" => {
                memory = safe_metadata_kernel_memory();
            }
            "--fallback-attempted" => {
                fallback = BenchmarkFallbackState::Attempted;
            }
            _ => {
                return emit_error(
                    command,
                    format,
                    "unknown option",
                    &cli_unknown_arg_error(command, &token),
                );
            }
        }
    }
    let result = VortexQueryPrimitiveResult::metadata_answered(request, value);
    let bridge = match plan_vortex_query_primitive_result_physical_operators_with_evidence(
        &result,
        correctness_evidence,
        benchmark_evidence,
        memory,
        fallback,
    ) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(command, format, "physical bridge planning failed", &error);
        }
    };
    let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);
    let count_admission = if matches!(
        report.primitive_kind,
        shardloom_vortex::VortexQueryPrimitiveKind::CountAll
            | shardloom_vortex::VortexQueryPrimitiveKind::CountWhere
    ) {
        match admit_vortex_metadata_count_kernel(&report) {
            Ok(admission) => Some(admission),
            Err(error) => {
                return emit_error(command, format, "count kernel admission failed", &error);
            }
        }
    } else {
        None
    };
    let filter_admission =
        if report.primitive_kind == shardloom_vortex::VortexQueryPrimitiveKind::FilterPredicate {
            match admit_vortex_metadata_filter_kernel(&report) {
                Ok(admission) => Some(admission),
                Err(error) => {
                    return emit_error(command, format, "filter kernel admission failed", &error);
                }
            }
        } else {
            None
        };
    let report_has_errors = report.has_errors()
        || count_admission
            .as_ref()
            .is_some_and(VortexMetadataCountKernelAdmissionReport::has_errors)
        || filter_admission
            .as_ref()
            .is_some_and(VortexMetadataFilterKernelAdmissionReport::has_errors);
    let mut diagnostics = report.diagnostics.clone();
    if let Some(count_admission) = &count_admission {
        diagnostics.extend(count_admission.diagnostics.clone());
    }
    if let Some(filter_admission) = &filter_admission {
        diagnostics.extend(filter_admission.diagnostics.clone());
    }
    let mut fields = vec![
        (
            "primitive".to_string(),
            report.primitive_kind.as_str().to_string(),
        ),
        ("status".to_string(), report.status.as_str().to_string()),
        (
            "certificate_status".to_string(),
            report.certificate_status.as_str().to_string(),
        ),
        (
            "metadata_kernel_count".to_string(),
            report.metadata_kernel_count.to_string(),
        ),
        (
            "kernel_kind".to_string(),
            report.kernel_kind.as_str().to_string(),
        ),
        ("value".to_string(), report.value.as_str()),
        (
            "correctness_evidence".to_string(),
            correctness_evidence.as_str().to_string(),
        ),
        (
            "benchmark_evidence".to_string(),
            benchmark_evidence.as_str().to_string(),
        ),
        (
            "memory_safe".to_string(),
            metadata_kernel_memory_safe(memory).to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            fallback.attempted().to_string(),
        ),
        ("data_read".to_string(), report.data_read.to_string()),
        ("data_decoded".to_string(), report.data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            report.data_materialized.to_string(),
        ),
        (
            "object_store_io".to_string(),
            report.object_store_io.to_string(),
        ),
        ("write_io".to_string(), report.write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            report.spill_io_performed.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
    ];
    if let Some(count_admission) = &count_admission {
        append_metadata_count_kernel_admission_fields(&mut fields, count_admission);
    }
    if let Some(filter_admission) = &filter_admission {
        append_metadata_filter_kernel_admission_fields(&mut fields, filter_admission);
    }
    emit(
        command,
        format,
        if report_has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex metadata physical kernel report".to_string(),
        report.to_human_text(),
        diagnostics,
        fields,
    );
    if report_has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapabilityDiscoveryScope {
    Engine,
    Sql,
    Functions,
    Operators,
    Adapters,
    SemanticProfiles,
    Migration,
    Certification,
    DataEtl,
    Python,
    DataFrame,
    Notebook,
    Udfs,
    UniversalAdapters,
    EventApiSaasAdapters,
    UnstructuredMedia,
    ApiSurfaces,
    Observability,
    Deployment,
    Extensions,
    SecurityGovernance,
}

impl CapabilityDiscoveryScope {
    fn parse(value: Option<&str>) -> Result<Self, ShardLoomError> {
        match value {
            None => Ok(Self::Engine),
            Some("sql") => Ok(Self::Sql),
            Some("functions") => Ok(Self::Functions),
            Some("operators") => Ok(Self::Operators),
            Some("adapters") => Ok(Self::Adapters),
            Some("semantic-profiles") => Ok(Self::SemanticProfiles),
            Some("migration") => Ok(Self::Migration),
            Some("certification") => Ok(Self::Certification),
            Some("data-etl") => Ok(Self::DataEtl),
            Some("python") => Ok(Self::Python),
            Some("dataframe") => Ok(Self::DataFrame),
            Some("notebook") => Ok(Self::Notebook),
            Some("udfs") => Ok(Self::Udfs),
            Some("universal-adapters") => Ok(Self::UniversalAdapters),
            Some("event-api-saas-adapters") => Ok(Self::EventApiSaasAdapters),
            Some("unstructured-media") => Ok(Self::UnstructuredMedia),
            Some("api-surfaces") => Ok(Self::ApiSurfaces),
            Some("observability") => Ok(Self::Observability),
            Some("deployment") => Ok(Self::Deployment),
            Some("extensions") => Ok(Self::Extensions),
            Some("security-governance") => Ok(Self::SecurityGovernance),
            Some(value) => Err(cli_unknown_arg_error("capabilities", value)),
        }
    }

    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::Engine => "engine",
            Self::Sql => "sql",
            Self::Functions => "functions",
            Self::Operators => "operators",
            Self::Adapters => "adapters",
            Self::SemanticProfiles => "semantic_profiles",
            Self::Migration => "migration",
            Self::Certification => "certification",
            Self::DataEtl => "data_etl",
            Self::Python => "python",
            Self::DataFrame => "dataframe",
            Self::Notebook => "notebook",
            Self::Udfs => "udfs",
            Self::UniversalAdapters => "universal_adapters",
            Self::EventApiSaasAdapters => "event_api_saas_adapters",
            Self::UnstructuredMedia => "unstructured_media",
            Self::ApiSurfaces => "api_surfaces",
            Self::Observability => "observability",
            Self::Deployment => "deployment",
            Self::Extensions => "extensions",
            Self::SecurityGovernance => "security_governance",
        }
    }

    #[must_use]
    const fn world_class_dimension(self) -> Option<WorldClassSufficiencyDimensionKind> {
        match self {
            Self::DataEtl => Some(WorldClassSufficiencyDimensionKind::DataEtlSurface),
            Self::Python => Some(WorldClassSufficiencyDimensionKind::PythonSurface),
            Self::DataFrame => Some(WorldClassSufficiencyDimensionKind::DataFrameQueryBuilder),
            Self::Notebook => Some(WorldClassSufficiencyDimensionKind::NotebookExperience),
            Self::Udfs => Some(WorldClassSufficiencyDimensionKind::UdfPlugin),
            Self::UniversalAdapters => {
                Some(WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog)
            }
            Self::EventApiSaasAdapters => {
                Some(WorldClassSufficiencyDimensionKind::EventApiSaasAdapters)
            }
            Self::UnstructuredMedia => Some(WorldClassSufficiencyDimensionKind::UnstructuredMedia),
            Self::ApiSurfaces => Some(WorldClassSufficiencyDimensionKind::ApiSurface),
            Self::Observability => Some(WorldClassSufficiencyDimensionKind::ObservabilitySurface),
            Self::Deployment => Some(WorldClassSufficiencyDimensionKind::DeploymentSurface),
            Self::Extensions => Some(WorldClassSufficiencyDimensionKind::ExtensionSurface),
            Self::SecurityGovernance => {
                Some(WorldClassSufficiencyDimensionKind::SecurityGovernance)
            }
            _ => None,
        }
    }
}

fn count_certification_status<I>(statuses: I, status: CapabilityCertificationStatus) -> usize
where
    I: Iterator<Item = CapabilityCertificationStatus>,
{
    statuses
        .filter(|entry_status| *entry_status == status)
        .count()
}

fn certification_common_fields(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> Vec<(String, String)> {
    vec![
        ("scope".to_string(), scope.as_str().to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted().to_string(),
        ),
        ("side_effect_free".to_string(), "true".to_string()),
        ("filesystem_probe".to_string(), "false".to_string()),
        ("network_probe".to_string(), "false".to_string()),
        ("catalog_probe".to_string(), "false".to_string()),
        ("adapter_probe".to_string(), "false".to_string()),
        ("parser_executed".to_string(), "false".to_string()),
        ("runtime_execution".to_string(), "false".to_string()),
    ]
}

fn certification_fields(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> Vec<(String, String)> {
    let mut fields = certification_common_fields(report, scope);
    match scope {
        CapabilityDiscoveryScope::Engine
        | CapabilityDiscoveryScope::DataEtl
        | CapabilityDiscoveryScope::Python
        | CapabilityDiscoveryScope::DataFrame
        | CapabilityDiscoveryScope::Notebook
        | CapabilityDiscoveryScope::Udfs
        | CapabilityDiscoveryScope::UniversalAdapters
        | CapabilityDiscoveryScope::EventApiSaasAdapters
        | CapabilityDiscoveryScope::UnstructuredMedia
        | CapabilityDiscoveryScope::ApiSurfaces
        | CapabilityDiscoveryScope::Observability
        | CapabilityDiscoveryScope::Deployment
        | CapabilityDiscoveryScope::Extensions
        | CapabilityDiscoveryScope::SecurityGovernance => {}
        CapabilityDiscoveryScope::Sql => append_sql_certification_fields(report, &mut fields),
        CapabilityDiscoveryScope::Functions => {
            append_function_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Operators => {
            append_operator_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Adapters => {
            append_adapter_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::SemanticProfiles => {
            append_semantic_profile_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Migration => {
            append_migration_certification_fields(report, &mut fields);
        }
        CapabilityDiscoveryScope::Certification => {
            append_full_certification_fields(report, &mut fields);
        }
    }
    fields
}

fn api_protocol_fields(report: &CliApiJsonProtocolReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "api_compat_plan");
    push_field(&mut fields, "publish_allowed", "false");
    push_field(&mut fields, "published", "false");
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "protocol_id", report.protocol_id);
    push_field(&mut fields, "protocol_stability", report.protocol_stability);
    push_field(
        &mut fields,
        "output_envelope_schema_version",
        report.output_envelope_schema_version,
    );
    push_field(
        &mut fields,
        "required_envelope_fields",
        &report.required_envelope_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_fallback_fields",
        &report.required_fallback_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_diagnostic_fields",
        &report.required_diagnostic_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_field_entry_fields",
        &report.required_field_entry_fields.join(","),
    );
    push_field(
        &mut fields,
        "command_status_values",
        &report.command_status_values.join(","),
    );
    push_field(
        &mut fields,
        "output_formats",
        &report.output_formats.join(","),
    );
    push_field(
        &mut fields,
        "thin_python_wrapper_boundary",
        report.thin_python_wrapper_boundary,
    );
    push_bool_field(
        &mut fields,
        "pyo3_maturin_allowed",
        report.pyo3_maturin_allowed,
    );
    push_bool_field(&mut fields, "foundry_required", report.foundry_required);
    push_bool_field(
        &mut fields,
        "dataframe_api_implemented",
        report.dataframe_api_implemented,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free);
    push_bool_field(&mut fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(&mut fields, "network_probe", report.network_probe);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(&mut fields, "adapter_probe", report.adapter_probe);
    push_bool_field(&mut fields, "parser_executed", report.parser_executed);
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "external_publish", "not_performed");
    push_bool_field(
        &mut fields,
        "external_publish_performed",
        report.external_publish,
    );
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn python_wrapper_fields(report: &PythonWrapperFoundationReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "python_wrapper_plan");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "wrapper_id", report.wrapper_id);
    push_field(&mut fields, "wrapper_status", report.wrapper_status);
    push_field(
        &mut fields,
        "transport_protocol_id",
        report.transport_protocol_id,
    );
    push_field(
        &mut fields,
        "output_envelope_schema_version",
        report.output_envelope_schema_version,
    );
    push_field(&mut fields, "invocation_model", report.invocation_model);
    push_field(
        &mut fields,
        "initial_command_scope",
        &report.initial_command_scope.join(","),
    );
    push_field(
        &mut fields,
        "required_client_behaviors",
        &report.required_client_behaviors.join(","),
    );
    push_field(&mut fields, "package_status", report.package_status);
    push_field(
        &mut fields,
        "native_binding_status",
        report.native_binding_status,
    );
    push_bool_field(
        &mut fields,
        "pyo3_maturin_allowed",
        report.pyo3_maturin_allowed,
    );
    push_bool_field(
        &mut fields,
        "python_package_created",
        report.python_package_created,
    );
    push_bool_field(
        &mut fields,
        "native_extension_required",
        report.native_extension_required,
    );
    push_bool_field(
        &mut fields,
        "dataframe_api_implemented",
        report.dataframe_api_implemented,
    );
    push_bool_field(
        &mut fields,
        "notebook_api_implemented",
        report.notebook_api_implemented,
    );
    push_bool_field(
        &mut fields,
        "python_udf_runtime_implemented",
        report.python_udf_runtime_implemented,
    );
    push_bool_field(
        &mut fields,
        "materialization_boundary_reporting_required",
        report.materialization_boundary_reporting_required,
    );
    push_bool_field(
        &mut fields,
        "diagnostics_passthrough_required",
        report.diagnostics_passthrough_required,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free);
    push_bool_field(&mut fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(&mut fields, "network_probe", report.network_probe);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(&mut fields, "adapter_probe", report.adapter_probe);
    push_bool_field(&mut fields, "parser_executed", report.parser_executed);
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "external_publish", "not_performed");
    push_bool_field(
        &mut fields,
        "external_publish_performed",
        report.external_publish,
    );
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn plan_portability_fields(report: &PlanPortabilityReport, mode: &str) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", mode);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", &report.report_id);
    push_field(&mut fields, "direction", report.direction.as_str());
    push_field(&mut fields, "portability_status", report.status.as_str());
    push_field(
        &mut fields,
        "interop_format",
        report.interop_format.as_str(),
    );
    push_field(
        &mut fields,
        "native_plan_schema_version",
        &report.native_plan_schema_version.summary(),
    );
    push_bool_field(&mut fields, "native_first", report.native_first);
    push_bool_field(&mut fields, "validation_only", report.validation_only);
    push_bool_field(
        &mut fields,
        "validation_required",
        report.validation_required,
    );
    push_bool_field(
        &mut fields,
        "capability_check_required",
        report.capability_check_required,
    );
    push_field(
        &mut fields,
        "supported_constructs",
        &report.supported_constructs.join(","),
    );
    push_field(
        &mut fields,
        "native_only_nodes",
        &report.native_only_nodes.join(","),
    );
    push_field(
        &mut fields,
        "substrait_like_representable_nodes",
        &report.substrait_like_representable_nodes.join(","),
    );
    push_field(&mut fields, "lossy_nodes", &report.lossy_nodes.join(","));
    push_field(
        &mut fields,
        "unsupported_nodes",
        &report.unsupported_nodes.join(","),
    );
    push_field(
        &mut fields,
        "residual_unsupported_constructs",
        &report.residual_unsupported_constructs.join(","),
    );
    push_field(
        &mut fields,
        "metadata_loss_boundaries",
        &report.metadata_loss_boundaries.join(","),
    );
    push_bool_field(
        &mut fields,
        "encoded_semantics_loss",
        report.encoded_semantics_loss,
    );
    push_bool_field(&mut fields, "redaction_required", report.redaction_required);
    push_bool_field(&mut fields, "parser_executed", report.parser_executed);
    push_bool_field(
        &mut fields,
        "import_export_serialization_performed",
        report.import_export_serialization_performed,
    );
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(
        &mut fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(&mut fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(&mut fields, "network_probe", report.network_probe);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(&mut fields, "adapter_probe", report.adapter_probe);
    push_bool_field(&mut fields, "read_io", report.read_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn native_plan_export_document() -> Result<NativePlanDocument, ShardLoomError> {
    let mut document = NativePlanDocument::new(
        PlanId::new("plan-export-native-skeleton")?,
        PlanLayer::Logical,
    );
    let mut scan = NativePlanNode::new(
        PlanNodeId::new("scan_0")?,
        PlanLayer::Logical,
        NativePlanNodeKind::Scan,
        "native Vortex scan placeholder",
    );
    scan.add_capability(PlanCapabilityRequirement::required(
        PlanCapabilityKind::VortexNativeInput,
        "native serialization preserves ShardLoom plan capability requirements",
    ));
    scan.add_boundary(PlanBoundaryKind::NativeVortexInput);
    document.add_node(scan);
    document.validate_skeleton();
    Ok(document)
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}

fn adaptive_optimizer_memory_fields(
    report: &AdaptiveOptimizerMemoryReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_adaptive_optimizer_memory_identity_fields(&mut fields, report);
    append_adaptive_optimizer_memory_gate_fields(&mut fields, report);
    append_adaptive_optimizer_memory_side_effect_fields(&mut fields, report);
    fields
}

fn append_adaptive_optimizer_memory_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_field(fields, "mode", "optimizer_adaptive_memory_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "adaptive_optimizer_status", report.status.as_str());
    push_field(fields, "optimizer_phase", report.optimizer_phase.as_str());
    push_count_field(fields, "rule_decision_count", report.rule_decision_count());
    push_count_field(fields, "deferred_rule_count", report.deferred_rule_count());
    push_count_field(
        fields,
        "runtime_filter_count",
        report.runtime_filter_count(),
    );
    push_count_field(
        fields,
        "conservative_runtime_filter_count",
        report.conservative_runtime_filter_count(),
    );
    push_count_field(
        fields,
        "adaptive_decision_count",
        report.adaptive_decision_count(),
    );
    push_count_field(fields, "skew_signal_count", report.skew_signal_count());
    push_field(
        fields,
        "dynamic_pruning_decision",
        report.dynamic_pruning_decision.summary().as_str(),
    );
}

fn append_adaptive_optimizer_memory_gate_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_bool_field(
        fields,
        "conservative_runtime_filter_required",
        report.conservative_runtime_filter_required,
    );
    push_bool_field(
        fields,
        "dynamic_pruning_requires_proof",
        report.dynamic_pruning_requires_proof,
    );
    push_bool_field(
        fields,
        "memory_budget_required",
        report.memory_budget_required,
    );
    push_bool_field(
        fields,
        "bounded_memory_required",
        report.bounded_memory_required,
    );
    push_bool_field(
        fields,
        "spill_policy_required",
        report.spill_policy_required,
    );
    push_bool_field(
        fields,
        "deterministic_oom_boundary",
        report.deterministic_oom_boundary,
    );
    push_bool_field(
        fields,
        "sink_requirement_boundary_required",
        report.sink_requirement_boundary_required,
    );
    push_bool_field(
        fields,
        "runtime_fact_required_before_adaptation",
        report.runtime_fact_required_before_adaptation,
    );
}

fn append_adaptive_optimizer_memory_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &AdaptiveOptimizerMemoryReport,
) {
    push_bool_field(fields, "optimizer_execution", report.optimizer_execution);
    push_bool_field(
        fields,
        "runtime_adaptation_applied",
        report.runtime_adaptation_applied,
    );
    push_bool_field(fields, "runtime_filter_built", report.runtime_filter_built);
    push_bool_field(
        fields,
        "runtime_filter_applied",
        report.runtime_filter_applied,
    );
    push_bool_field(fields, "plan_rewritten", report.plan_rewritten);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn cpu_operator_specialization_fields(
    report: &CpuOperatorSpecializationReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_cpu_specialization_identity_fields(&mut fields, report);
    append_cpu_specialization_evidence_fields(&mut fields, report);
    append_cpu_specialization_accelerator_fields(&mut fields, report);
    append_cpu_specialization_side_effect_fields(&mut fields, report);
    fields
}

fn append_cpu_specialization_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_field(fields, "mode", "cpu_operator_specialization_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "cpu_specialization_status", report.status.as_str());
    push_count_field(fields, "entry_count", report.entry_count());
    push_count_field(
        fields,
        "specialization_candidate_count",
        report.specialization_candidate_count(),
    );
    push_count_field(
        fields,
        "simd_candidate_count",
        report.simd_candidate_count(),
    );
    push_count_field(
        fields,
        "cache_aware_candidate_count",
        report.cache_aware_candidate_count(),
    );
    push_count_field(
        fields,
        "encoded_layout_aware_candidate_count",
        report.encoded_layout_aware_candidate_count(),
    );
    push_field(fields, "operator_order", &report.operator_order());
    push_field(fields, "kernel_kind_order", &report.kernel_kind_order());
}

fn append_cpu_specialization_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(
        fields,
        "cpu_feature_guard_required",
        report.cpu_feature_guard_required,
    );
    push_bool_field(
        fields,
        "portable_native_baseline_required",
        report.portable_native_baseline_required,
    );
    push_bool_field(
        fields,
        "deterministic_dispatch_required",
        report.deterministic_dispatch_required,
    );
}

fn append_cpu_specialization_accelerator_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(fields, "host_cpu_probe", report.host_cpu_probe);
    push_bool_field(
        fields,
        "runtime_dispatch_implemented",
        report.runtime_dispatch_implemented,
    );
    push_bool_field(fields, "unsafe_code_required", report.unsafe_code_required);
    push_bool_field(fields, "gpu_required", report.gpu_required);
    push_bool_field(fields, "fpga_required", report.fpga_required);
}

fn append_cpu_specialization_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CpuOperatorSpecializationReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn execution_certificate_surface_fields(
    report: &ExecutionCertificateEvidenceSurfaceReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_execution_certificate_surface_identity_fields(&mut fields, report);
    append_execution_certificate_surface_artifact_fields(&mut fields, report);
    append_execution_certificate_surface_requirement_fields(&mut fields, report);
    append_execution_certificate_surface_side_effect_fields(&mut fields, report);
    fields
}

fn append_execution_certificate_surface_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_field(fields, "mode", "execution_certificate_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "certificate_surface_status", report.status.as_str());
    push_field(
        fields,
        "certificate_schema_version",
        report.certificate_schema_version,
    );
}

fn append_execution_certificate_surface_artifact_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_count_field(fields, "artifact_count", report.artifact_count());
    push_count_field(
        fields,
        "required_artifact_count",
        report.required_artifact_count(),
    );
    push_count_field(fields, "hash_required_count", report.hash_required_count());
    push_count_field(
        fields,
        "machine_readable_required_count",
        report.machine_readable_required_count(),
    );
    push_count_field(
        fields,
        "plan_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::Plan),
    );
    push_count_field(
        fields,
        "input_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::InputSnapshot),
    );
    push_count_field(
        fields,
        "output_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::OutputPayload),
    );
    push_count_field(
        fields,
        "segment_trace_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::SegmentTrace),
    );
    push_count_field(
        fields,
        "side_effect_manifest_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::SideEffectManifest),
    );
    push_count_field(
        fields,
        "reproducibility_metadata_artifact_count",
        report.artifact_kind_count(ExecutionEvidenceArtifactKind::ReproducibilityMetadata),
    );
    push_field(fields, "artifact_order", &report.artifact_order());
}

fn append_execution_certificate_surface_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_bool_field(fields, "plan_hash_required", report.plan_hash_required);
    push_bool_field(
        fields,
        "input_snapshot_hash_required",
        report.input_snapshot_hash_required,
    );
    push_bool_field(fields, "output_hash_required", report.output_hash_required);
    push_bool_field(
        fields,
        "selected_segment_trace_required",
        report.selected_segment_trace_required,
    );
    push_bool_field(
        fields,
        "skipped_segment_trace_required",
        report.skipped_segment_trace_required,
    );
    push_bool_field(
        fields,
        "side_effect_manifest_required",
        report.side_effect_manifest_required,
    );
    push_bool_field(
        fields,
        "reproducibility_metadata_required",
        report.reproducibility_metadata_required,
    );
    push_bool_field(
        fields,
        "correctness_fixture_required",
        report.correctness_fixture_required,
    );
    push_bool_field(
        fields,
        "machine_readable_certificate_surface",
        report.machine_readable_certificate_surface,
    );
    push_bool_field(
        fields,
        "deterministic_field_order_required",
        report.deterministic_field_order_required,
    );
}

fn append_execution_certificate_surface_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ExecutionCertificateEvidenceSurfaceReport,
) {
    push_bool_field(
        fields,
        "certificate_evaluation_performed",
        report.certificate_evaluation_performed,
    );
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn stateful_reuse_fields(report: &StatefulReuseReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_stateful_reuse_identity_fields(&mut fields, report);
    append_stateful_reuse_requirement_fields(&mut fields, report);
    append_stateful_reuse_side_effect_fields(&mut fields, report);
    fields
}

fn append_stateful_reuse_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_field(fields, "mode", "stateful_reuse_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "stateful_reuse_status", report.status.as_str());
    push_count_field(fields, "boundary_count", report.boundary_count());
    push_count_field(
        fields,
        "invalidation_requirement_count",
        report.invalidation_requirement_count(),
    );
    push_count_field(
        fields,
        "correctness_proof_required_count",
        report.correctness_proof_required_count(),
    );
    push_count_field(
        fields,
        "invalidation_proof_required_count",
        report.invalidation_proof_required_count(),
    );
    push_count_field(
        fields,
        "execution_certificate_required_count",
        report.execution_certificate_required_count(),
    );
    push_field(fields, "cache_kind_order", &report.cache_kind_order());
    push_field(
        fields,
        "invalidation_signal_order",
        &report.invalidation_signal_order(),
    );
}

fn append_stateful_reuse_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_bool_field(
        fields,
        "typed_cache_boundaries_required",
        report.typed_cache_boundaries_required,
    );
    push_bool_field(
        fields,
        "deterministic_keys_required",
        report.deterministic_keys_required,
    );
    push_bool_field(
        fields,
        "invalidation_proofs_required",
        report.invalidation_proofs_required,
    );
    push_bool_field(
        fields,
        "correctness_proofs_required",
        report.correctness_proofs_required,
    );
    push_bool_field(
        fields,
        "execution_certificates_required",
        report.execution_certificates_required,
    );
    push_bool_field(
        fields,
        "manifest_diff_required",
        report.manifest_diff_required,
    );
}

fn append_stateful_reuse_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &StatefulReuseReport,
) {
    push_bool_field(fields, "cache_read", report.cache_read);
    push_bool_field(fields, "cache_write", report.cache_write);
    push_bool_field(fields, "cache_replay", report.cache_replay);
    push_bool_field(
        fields,
        "incremental_execution",
        report.incremental_execution,
    );
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn universal_harness_fields(report: &UniversalHarnessReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_universal_harness_identity_fields(&mut fields, report);
    append_universal_harness_requirement_fields(&mut fields, report);
    append_universal_harness_side_effect_fields(&mut fields, report);
    fields
}

fn append_universal_harness_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_field(fields, "mode", "universal_harness_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "universal_harness_status", report.status.as_str());
    push_count_field(fields, "surface_count", report.surface_count());
    push_count_field(
        fields,
        "external_baseline_count",
        report.external_baseline_count(),
    );
    push_field(
        fields,
        "runner_contract_field_order",
        &report.runner_contract_field_order(),
    );
    push_field(fields, "surface_kind_order", &report.surface_kind_order());
    push_field(
        fields,
        "baseline_engine_order",
        &report.baseline_engine_order(),
    );
}

fn append_universal_harness_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_bool_field(
        fields,
        "output_envelope_required",
        report.output_envelope_required,
    );
    push_bool_field(
        fields,
        "stable_command_schema_required",
        report.stable_command_schema_required,
    );
    push_bool_field(fields, "exit_code_required", report.exit_code_required);
    push_bool_field(fields, "diagnostics_required", report.diagnostics_required);
    push_bool_field(
        fields,
        "side_effect_manifest_required",
        report.side_effect_manifest_required,
    );
    push_bool_field(
        fields,
        "output_artifacts_required",
        report.output_artifacts_required,
    );
    push_bool_field(fields, "metrics_required", report.metrics_required);
    push_bool_field(
        fields,
        "comparison_dataset_required",
        report.comparison_dataset_required,
    );
    push_bool_field(
        fields,
        "correctness_evidence_required",
        report.correctness_evidence_required,
    );
    push_bool_field(
        fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(fields, "foundry_required", report.foundry_required);
    push_bool_field(
        fields,
        "foundry_optional_example",
        report.foundry_optional_example,
    );
}

fn append_universal_harness_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &UniversalHarnessReport,
) {
    push_bool_field(
        fields,
        "package_import_performed",
        report.package_import_performed,
    );
    push_bool_field(fields, "deployment_performed", report.deployment_performed);
    push_bool_field(
        fields,
        "external_baseline_execution",
        report.external_baseline_execution,
    );
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(fields, "network_probe", report.network_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "read_io", report.read_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "external_publish", report.external_publish);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn native_io_envelope_fields(report: &NativeIoEnvelopeReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_native_io_envelope_identity_fields(&mut fields, report);
    append_native_io_envelope_requirement_fields(&mut fields, report);
    append_native_io_envelope_side_effect_fields(&mut fields, report);
    fields
}

fn world_class_sufficiency_fields(report: &WorldClassSufficiencyReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_world_class_sufficiency_identity_fields(&mut fields, report);
    append_world_class_sufficiency_surface_status_fields(&mut fields, report);
    append_world_class_sufficiency_evidence_status_fields(&mut fields, report);
    append_world_class_sufficiency_metric_fields(&mut fields, report);
    append_world_class_sufficiency_side_effect_fields(&mut fields, report);
    fields
}

fn append_world_class_sufficiency_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_field(fields, "mode", "world_class_sufficiency_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(
        fields,
        "workload_constitution_ref",
        &report.workload_constitution_ref,
    );
    push_field(fields, "claim_level", report.claim_level.as_str());
    push_field(
        fields,
        "publication_decision",
        report.publication_decision.as_str(),
    );
    push_count_field(fields, "dimension_count", report.dimension_count());
    push_count_field(
        fields,
        "required_dimension_count",
        report.required_dimension_count(),
    );
    push_count_field(
        fields,
        "evidence_insufficient_dimension_count",
        report.evidence_insufficient_dimension_count(),
    );
    push_field(
        fields,
        "dimension_kind_order",
        &report.dimension_kind_order(),
    );
}

fn append_world_class_sufficiency_surface_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_world_class_sufficiency_status_field(
        fields,
        "sql_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::SqlSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "operator_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::OperatorSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "function_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::FunctionSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "adapter_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::AdapterSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "python_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::PythonSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "data_etl_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::DataEtlSurface,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "unstructured_media_surface_status",
        report,
        WorldClassSufficiencyDimensionKind::UnstructuredMedia,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "universal_adapter_catalog_status",
        report,
        WorldClassSufficiencyDimensionKind::UniversalAdapterCatalog,
    );
}

fn append_world_class_sufficiency_evidence_status_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_world_class_sufficiency_status_field(
        fields,
        "correctness_evidence_status",
        report,
        WorldClassSufficiencyDimensionKind::CorrectnessEvidence,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "benchmark_evidence_status",
        report,
        WorldClassSufficiencyDimensionKind::BenchmarkEvidence,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "native_io_certificate_coverage",
        report,
        WorldClassSufficiencyDimensionKind::NativeIoCertificateCoverage,
    );
    push_world_class_sufficiency_status_field(
        fields,
        "execution_certificate_coverage",
        report,
        WorldClassSufficiencyDimensionKind::ExecutionCertificateCoverage,
    );
    push_field(
        fields,
        "performance_regression_budget_status",
        report.performance_regression_budget_status.as_str(),
    );
}

fn append_world_class_sufficiency_metric_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_field(
        fields,
        "unsupported_rate",
        &report
            .unsupported_rate
            .clone()
            .unwrap_or_else(|| "not_measured".to_string()),
    );
    push_field(
        fields,
        "materialization_rate",
        &report
            .materialization_rate
            .clone()
            .unwrap_or_else(|| "not_measured".to_string()),
    );
    push_count_field(fields, "known_limit_count", report.known_limits.len());
    push_count_field(fields, "blocking_gap_count", report.blocking_gaps.len());
    push_count_field(
        fields,
        "capability_snapshot_ref_count",
        report.capability_snapshot_refs.len(),
    );
    push_count_field(
        fields,
        "external_baseline_ref_count",
        report.external_baseline_refs.len(),
    );
    push_bool_field(
        fields,
        "best_default_claim_allowed",
        report.can_publish_best_default_claim(),
    );
    push_field(fields, "scorecard_ref", &report.scorecard_ref);
    push_field(
        fields,
        "best_default_dossier_ref",
        &report.best_default_dossier_ref,
    );
}

fn push_world_class_sufficiency_status_field(
    fields: &mut Vec<(String, String)>,
    key: &str,
    report: &WorldClassSufficiencyReport,
    kind: WorldClassSufficiencyDimensionKind,
) {
    push_field(fields, key, report.status_for(kind).as_str());
}

fn append_world_class_sufficiency_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &WorldClassSufficiencyReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "parser_executed", report.parser_executed);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(fields, "network_probe", report.network_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "external_engine_execution",
        report.external_engine_execution,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn append_native_io_envelope_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_field(fields, "mode", "native_io_envelope_plan");
    push_field(fields, "execution", "not_performed");
    push_field(fields, "plan_only", "true");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", &report.report_id);
    push_field(fields, "native_io_envelope_status", report.status.as_str());
    push_count_field(fields, "contract_count", report.contract_count());
    push_count_field(
        fields,
        "representation_state_count",
        report.representation_state_count(),
    );
    push_count_field(
        fields,
        "transition_example_count",
        report.transition_example_count(),
    );
    push_count_field(
        fields,
        "certificate_path_requirement_count",
        report.certificate_path_requirement_count(),
    );
    push_field(fields, "contract_kind_order", &report.contract_kind_order());
    push_field(
        fields,
        "representation_state_order",
        &report.representation_state_order(),
    );
    push_field(
        fields,
        "transition_example_order",
        &report.transition_example_order(),
    );
    push_field(
        fields,
        "certificate_path_order",
        &report.certificate_path_order(),
    );
}

fn append_native_io_envelope_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_bool_field(
        fields,
        "per_path_certificate_required",
        report.per_path_certificate_required,
    );
    push_bool_field(
        fields,
        "aggregate_certificate_not_sufficient",
        report.aggregate_certificate_not_sufficient,
    );
    push_bool_field(
        fields,
        "preserve_encoded_or_foreign_encoded_when_possible",
        report.preserve_encoded_or_foreign_encoded_when_possible,
    );
    push_bool_field(
        fields,
        "decoded_arrow_normalization_allowed",
        report.decoded_arrow_normalization_allowed,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required_for_decoded_columnar",
        report.materialization_boundary_required_for_decoded_columnar,
    );
    push_bool_field(
        fields,
        "materialization_boundary_required_for_rows",
        report.materialization_boundary_required_for_rows,
    );
    push_bool_field(
        fields,
        "source_pushdown_proof_required",
        report.source_pushdown_proof_required,
    );
    push_bool_field(
        fields,
        "sink_requirement_propagation_required",
        report.sink_requirement_propagation_required,
    );
    push_bool_field(
        fields,
        "adapter_fidelity_report_required",
        report.adapter_fidelity_report_required,
    );
}

fn append_native_io_envelope_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &NativeIoEnvelopeReport,
) {
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "adapter_probe", report.adapter_probe);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "side_effect_free", report.is_side_effect_free());
    push_count_field(fields, "diagnostic_count", report.diagnostics.len());
}

fn backpressure_plan_fields(report: &BackpressurePlanReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "backpressure_plan");
    push_field(&mut fields, "backpressure_status", report.status.as_str());
    push_field(&mut fields, "backpressure_mode", report.mode.as_str());
    push_bool_field(&mut fields, "bounded", report.bounded);
    push_bool_field(&mut fields, "memory_required", report.memory_required);
    push_bool_field(&mut fields, "spill_allowed", report.spill_allowed);
    push_count_field(&mut fields, "max_parallelism", report.input.max_parallelism);
    push_field(
        &mut fields,
        "max_in_flight_chunks",
        &report
            .max_in_flight_chunks
            .map_or("none".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "max_buffered_bytes",
        &report
            .max_buffered_bytes
            .map_or("none".to_string(), |value| value.as_bytes().to_string()),
    );
    push_field(
        &mut fields,
        "estimated_chunk_bytes",
        &report
            .estimated_chunk_bytes
            .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
    );
    push_bool_field(&mut fields, "streams_executed", report.streams_executed);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_field(&mut fields, "execution", "not_performed");
    fields
}

fn parse_sizing_feedback_signals(value: &str) -> Result<Vec<SizingFeedbackSignal>, ShardLoomError> {
    let mut signals = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let kind = match token {
            "stable" => SizingFeedbackSignalKind::Stable,
            "task-too-large" | "task_too_large" => SizingFeedbackSignalKind::TaskTooLarge,
            "task-too-small" | "task_too_small" => SizingFeedbackSignalKind::TaskTooSmall,
            "memory-pressure-high" | "memory_pressure_high" => {
                SizingFeedbackSignalKind::MemoryPressureHigh
            }
            "object-store-throttled" | "object_store_throttled" => {
                SizingFeedbackSignalKind::ObjectStoreThrottled
            }
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "invalid sizing feedback signal token: {token}"
                )));
            }
        };
        if !signals
            .iter()
            .any(|signal: &SizingFeedbackSignal| signal.kind == kind)
        {
            signals.push(SizingFeedbackSignal::new(
                kind,
                format!("observed sizing feedback signal: {}", kind.as_str()),
            ));
        }
    }
    if signals.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "sizing-feedback-plan requires <signals>".to_string(),
        ));
    }
    Ok(signals)
}

fn dynamic_sizing_feedback_fields(
    report: &DynamicSizingFeedbackReport,
    memory_gb: u64,
    signals_raw: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "sizing_feedback_plan");
    push_field(
        &mut fields,
        "dynamic_sizing_feedback_status",
        report.status.as_str(),
    );
    push_field(
        &mut fields,
        "dynamic_sizing_feedback_mode",
        report.mode.as_str(),
    );
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_field(&mut fields, "signals", signals_raw);
    push_count_field(&mut fields, "signal_count", report.signal_count);
    push_count_field(
        &mut fields,
        "reduce_signal_count",
        report.reduce_signal_count,
    );
    push_count_field(
        &mut fields,
        "increase_signal_count",
        report.increase_signal_count,
    );
    push_count_field(
        &mut fields,
        "stable_signal_count",
        report.stable_signal_count,
    );
    push_field(
        &mut fields,
        "current_target_task_bytes",
        &report.current_target_task_bytes.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "recommended_target_task_bytes",
        &report.recommended_target_task_bytes.as_bytes().to_string(),
    );
    push_bool_field(
        &mut fields,
        "target_task_bytes_changed",
        report.current_target_task_bytes != report.recommended_target_task_bytes,
    );
    push_bool_field(
        &mut fields,
        "adaptive_splitting_allowed",
        report.recommended_policy.allow_splitting,
    );
    push_bool_field(
        &mut fields,
        "adaptive_coalescing_allowed",
        report.recommended_policy.allow_coalescing,
    );
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(&mut fields, "feedback_applied", report.feedback_applied);
    push_field(&mut fields, "execution", "not_performed");
    fields
}

fn adaptive_sizing_report_fields(
    report: &VortexAdaptiveSizingReport,
    memory_gb: u64,
    native_vortex_input: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "vortex_adaptive_sizing");
    push_field(
        &mut fields,
        "adaptive_sizing_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "adaptive_sizing_mode", report.mode.as_str());
    push_bool_field(&mut fields, "native_vortex_input", native_vortex_input);
    push_bool_field(&mut fields, "plan_only", true);
    push_bool_field(&mut fields, "tasks_executed", false);
    push_bool_field(&mut fields, "data_executed", report.data_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_count_field(
        &mut fields,
        "segment_input_count",
        report.segment_inputs.len(),
    );
    push_count_field(&mut fields, "planned_task_count", report.planned_task_count);
    push_count_field(
        &mut fields,
        "split_decision_count",
        report.split_decision_count,
    );
    push_count_field(
        &mut fields,
        "coalesce_candidate_count",
        report.coalesce_candidate_count,
    );
    push_count_field(
        &mut fields,
        "needs_estimate_count",
        report.needs_estimate_count,
    );
    push_count_field(&mut fields, "keep_single_count", report.keep_single_count);
    push_count_field(
        &mut fields,
        "metadata_only_count",
        report.metadata_only_count,
    );
    push_bool_field(
        &mut fields,
        "adaptive_splitting_allowed",
        report.input.policy.allow_splitting,
    );
    push_bool_field(
        &mut fields,
        "adaptive_coalescing_allowed",
        report.input.policy.allow_coalescing,
    );
    push_field(
        &mut fields,
        "target_task_bytes",
        &report.input.policy.target_task_bytes.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "min_task_bytes",
        &report.input.policy.min_task_bytes.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "max_task_bytes",
        &report.input.policy.max_task_bytes.as_bytes().to_string(),
    );
    push_field(&mut fields, "reservation_lifecycle_integration", "true");
    push_field(&mut fields, "memory_integration", "true");
    push_field(&mut fields, "vortex_memory_bridge_integration", "true");
    push_field(&mut fields, "bounded_execution_integration", "true");
    fields
}

fn memory_bridge_report_fields(
    report: &VortexMemoryBridgeReport,
    memory_gb: u64,
    native_vortex_input: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "vortex_memory_plan");
    push_field(&mut fields, "memory_bridge_status", report.status.as_str());
    push_field(&mut fields, "memory_bridge_mode", report.mode.as_str());
    push_bool_field(&mut fields, "native_vortex_input", native_vortex_input);
    push_bool_field(&mut fields, "plan_only", true);
    push_bool_field(&mut fields, "tasks_executed", false);
    push_bool_field(&mut fields, "data_executed", report.io_flags.data_executed);
    push_bool_field(&mut fields, "data_read", report.io_flags.data_read);
    push_bool_field(
        &mut fields,
        "data_materialized",
        report.io_flags.data_materialized,
    );
    push_bool_field(
        &mut fields,
        "object_store_io",
        report.effect_flags.object_store_io,
    );
    push_bool_field(&mut fields, "write_io", report.effect_flags.write_io);
    push_bool_field(
        &mut fields,
        "spill_io_performed",
        report.effect_flags.spill_io_performed,
    );
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.execution_policy_flags.external_effects_executed,
    );
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_field(
        &mut fields,
        "memory_budget_total_bytes",
        &report.input.memory_budget.total.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "memory_budget_soft_limit_bytes",
        &report.input.memory_budget.soft_limit.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "memory_budget_hard_limit_bytes",
        &report.input.memory_budget.hard_limit.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "spill_policy",
        report.input.spill_policy.as_str(),
    );
    push_count_field(&mut fields, "tasks_considered", report.tasks_considered);
    push_count_field(
        &mut fields,
        "tasks_needing_estimate",
        report.tasks_needing_estimate,
    );
    push_count_field(&mut fields, "tasks_memory_safe", report.tasks_memory_safe);
    push_count_field(
        &mut fields,
        "tasks_spill_may_be_required",
        report.tasks_spill_may_be_required,
    );
    push_count_field(
        &mut fields,
        "tasks_spill_required_not_implemented",
        report.tasks_spill_required_not_implemented,
    );
    push_count_field(&mut fields, "spill_plan_count", report.spill_plans.len());
    push_field(&mut fields, "reservation_lifecycle_integration", "true");
    push_field(&mut fields, "memory_integration", "true");
    push_field(&mut fields, "vortex_memory_bridge_integration", "true");
    push_field(&mut fields, "bounded_execution_integration", "true");
    fields
}

fn scheduler_bridge_report_fields(
    report: &VortexSchedulerBridgeReport,
    memory_gb: u64,
    max_parallelism: usize,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    let max_batch_decisions = report
        .batches
        .iter()
        .map(|batch| batch.decisions.len())
        .max()
        .unwrap_or(0);
    let bounded_parallelism_enforced = report
        .batches
        .iter()
        .all(|batch| batch.decisions.len() <= batch.max_parallelism);
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "vortex_schedule_plan");
    push_field(
        &mut fields,
        "scheduler_bridge_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "scheduler_bridge_mode", report.mode.as_str());
    push_bool_field(&mut fields, "plan_only", true);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_executed", report.data_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_count_field(&mut fields, "batch_count", report.batches.len());
    push_count_field(&mut fields, "max_batch_decision_count", max_batch_decisions);
    push_bool_field(
        &mut fields,
        "bounded_parallelism_enforced",
        bounded_parallelism_enforced,
    );
    push_count_field(
        &mut fields,
        "scheduled_task_count",
        report.scheduled_task_count,
    );
    push_count_field(
        &mut fields,
        "metadata_only_task_count",
        report.metadata_only_task_count,
    );
    push_count_field(&mut fields, "blocked_task_count", report.blocked_task_count);
    push_count_field(
        &mut fields,
        "unsupported_task_count",
        report.unsupported_task_count,
    );
    push_bool_field(
        &mut fields,
        "scheduler_requires_future_action",
        report.status.requires_future_action(),
    );
    fields
}

fn bounded_local_execution_fields(
    report: &VortexBoundedExecutionReport,
    primitive: &str,
    memory_gb: u64,
    max_parallelism: usize,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "vortex_bounded_local_exec");
    push_field(
        &mut fields,
        "bounded_execution_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "bounded_execution_mode", report.mode.as_str());
    push_field(&mut fields, "primitive", primitive);
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_count_field(
        &mut fields,
        "metadata_tasks_completed",
        report.metadata_tasks_completed,
    );
    push_count_field(
        &mut fields,
        "noop_tasks_completed",
        report.noop_tasks_completed,
    );
    push_count_field(
        &mut fields,
        "encoded_read_tasks_deferred",
        report.encoded_read_tasks_deferred,
    );
    push_count_field(&mut fields, "blocked_task_count", report.blocked_task_count);
    push_count_field(
        &mut fields,
        "bounded_decision_count",
        report.decisions.len(),
    );
    push_field(
        &mut fields,
        "local_execution_status",
        report.local_execution_report.status.as_str(),
    );
    push_field(
        &mut fields,
        "local_execution_mode",
        report.local_execution_report.mode.as_str(),
    );
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", "metadata_only_or_not_performed");
    push_bool_field(
        &mut fields,
        "result_known",
        report.local_execution_report.value.is_known(),
    );
    fields
}

fn append_sql_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "sql_feature_count",
        report.sql_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "planned_count",
        count_certification_status(
            report.sql_coverage.entries.iter().map(|entry| entry.status),
            CapabilityCertificationStatus::Planned,
        ),
    );
    push_count_field(
        fields,
        "certified_count",
        count_certification_status(
            report.sql_coverage.entries.iter().map(|entry| entry.status),
            CapabilityCertificationStatus::Certified,
        ),
    );
}

fn append_function_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "function_group_count",
        report.function_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "planned_count",
        count_certification_status(
            report
                .function_coverage
                .entries
                .iter()
                .map(|entry| entry.status),
            CapabilityCertificationStatus::Planned,
        ),
    );
}

fn append_operator_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    let physical_plan = PhysicalOperatorPlan::cg7_foundation();
    let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
    push_count_field(
        fields,
        "operator_family_count",
        report.operator_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "production_certified_count",
        report
            .operator_coverage
            .entries
            .iter()
            .filter(|entry| entry.status.can_satisfy_production_claim())
            .count(),
    );
    push_field(
        fields,
        "physical_operator_schema_version",
        physical_plan.schema_version,
    );
    push_field(fields, "physical_operator_plan_id", &physical_plan.plan_id);
    push_count_field(
        fields,
        "physical_operator_count",
        physical_plan.operators.len(),
    );
    push_count_field(
        fields,
        "physical_operator_ready_count",
        physical_plan.ready_for_native_planning_count(),
    );
    push_count_field(
        fields,
        "physical_operator_missing_kernel_count",
        physical_plan.missing_kernel_count(),
    );
    push_count_field(
        fields,
        "physical_operator_unsupported_count",
        physical_plan.unsupported_count(),
    );
    push_field(
        fields,
        "physical_operator_fallback_execution_allowed",
        if physical_plan.fallback_execution_allowed() {
            "true"
        } else {
            "false"
        },
    );
    push_field(fields, "physical_operator_runtime_execution", "false");
    push_field(
        fields,
        "physical_operator_execution_profile_schema_version",
        execution_profiles.schema_version,
    );
    push_count_field(
        fields,
        "physical_operator_execution_profile_count",
        execution_profiles.profile_count(),
    );
    append_physical_operator_execution_level_fields(fields, &execution_profiles);
    push_count_field(
        fields,
        "physical_operator_reference_only_level_count",
        execution_profiles.reference_only_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_row_materialization_level_count",
        execution_profiles.row_materialization_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_arrow_conversion_level_count",
        execution_profiles.arrow_conversion_allowed_count(),
    );
    push_count_field(
        fields,
        "physical_operator_fallback_level_count",
        execution_profiles.fallback_allowed_count(),
    );
    append_metadata_physical_kernel_discovery_fields(fields);
    append_metadata_count_kernel_admission_discovery_fields(fields);
    append_metadata_filter_kernel_admission_discovery_fields(fields);
    append_metadata_projection_kernel_admission_discovery_fields(fields);
    append_encoded_projection_kernel_admission_discovery_fields(fields);
    append_encoded_count_physical_kernel_discovery_fields(fields);
    append_encoded_count_kernel_admission_discovery_fields(fields);
    append_encoded_predicate_evaluation_discovery_fields(fields);
    append_selection_vector_filter_kernel_discovery_fields(fields);
    append_selection_vector_filter_kernel_admission_discovery_fields(fields);
    append_encoded_count_local_guard_discovery_fields(fields);
}

fn append_physical_operator_execution_level_fields(
    fields: &mut Vec<(String, String)>,
    execution_profiles: &PhysicalOperatorExecutionProfileMatrix,
) {
    push_count_field(
        fields,
        "physical_operator_native_execution_level_count",
        execution_profiles.native_execution_level_count(),
    );
    push_count_field(
        fields,
        "physical_operator_metadata_only_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::MetadataOnly),
    );
    push_count_field(
        fields,
        "physical_operator_encoded_native_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::EncodedNative),
    );
    push_count_field(
        fields,
        "physical_operator_hybrid_native_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::HybridNative),
    );
    push_count_field(
        fields,
        "physical_operator_native_decoded_level_count",
        execution_profiles.allowed_level_count(PhysicalOperatorExecutionLevel::NativeDecoded),
    );
}

fn append_metadata_physical_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_physical_kernel_schema_version",
        "shardloom.vortex_metadata_physical_kernel.v1",
    );
    push_field(
        fields,
        "metadata_physical_kernel_supported_primitives",
        "count_all,count_where,filter_predicate",
    );
    push_field(fields, "metadata_physical_kernel_contextual_only", "true");
    push_field(
        fields,
        "metadata_physical_kernel_requires_correctness_evidence",
        "true",
    );
    push_field(
        fields,
        "metadata_physical_kernel_requires_memory_safety_evidence",
        "true",
    );
    push_field(
        fields,
        "metadata_physical_kernel_requires_benchmark_for_production",
        "true",
    );
    push_field(fields, "metadata_physical_kernel_data_read", "false");
    push_field(fields, "metadata_physical_kernel_data_decoded", "false");
    push_field(
        fields,
        "metadata_physical_kernel_data_materialized",
        "false",
    );
    push_field(fields, "metadata_physical_kernel_object_store_io", "false");
    push_field(fields, "metadata_physical_kernel_write_io", "false");
    push_field(fields, "metadata_physical_kernel_spill_io", "false");
    push_field(
        fields,
        "metadata_physical_kernel_runtime_execution",
        "false",
    );
    push_field(
        fields,
        "metadata_physical_kernel_fallback_execution_allowed",
        "false",
    );
}

fn append_metadata_count_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_count_kernel_admission_schema_version",
        "shardloom.vortex_metadata_count_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_operator_kind",
        "count_aggregate",
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_metadata_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_metadata_filter_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "metadata_filter_kernel_admission_schema_version",
        "shardloom.vortex_metadata_filter_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_operator_kind",
        "filter",
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_metadata_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_metadata_projection_kernel_admission_discovery_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_field(
        fields,
        "metadata_projection_kernel_admission_schema_version",
        "shardloom.vortex_metadata_projection_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "metadata_projection_kernel_admission_operator_kind",
        "project",
    );
    push_field(
        fields,
        "metadata_projection_kernel_admission_required_kernel_kind",
        "metadata",
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_projection_readiness",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "metadata_projection_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_projection_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "encoded_projection_kernel_admission_schema_version",
        "shardloom.vortex_encoded_projection_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "encoded_projection_kernel_admission_operator_kind",
        "project",
    );
    push_field(
        fields,
        "encoded_projection_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_projection_readiness",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_encoded_column_path",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "encoded_projection_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_count_physical_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_count_physical_kernel_discovery_report();
    push_field(
        fields,
        "encoded_count_physical_kernel_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_id",
        report.kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_supported_primitive",
        report.supported_primitive.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_execution_certificate",
        report.requires_execution_certificate,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_correctness_evidence",
        report.requires_correctness_evidence,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_memory_safety_evidence",
        report.requires_memory_safety_evidence,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_requires_benchmark_for_production",
        report.requires_benchmark_for_production,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_evaluated_path_reads_data",
        report.evaluated_path_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_encoded_count_kernel_admission_discovery_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "encoded_count_kernel_admission_schema_version",
        "shardloom.vortex_encoded_count_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_operator_kind",
        "count_aggregate",
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_physical_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_predicate_evaluation_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_predicate_evaluation_discovery_report();
    push_field(
        fields,
        "encoded_predicate_evaluation_schema_version",
        report.schema_version,
    );
    push_field(fields, "encoded_predicate_evaluation_id", report.report_id);
    push_field(
        fields,
        "encoded_predicate_evaluation_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_predicate_evaluation_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_predicate_evaluation_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_emits_selection_vectors",
        report.emits_selection_vectors,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_supports_metadata_proven_all",
        report.supports_metadata_proven_all,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_supports_metadata_proven_none",
        report.supports_metadata_proven_none,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_defers_inconclusive_to_encoded_values",
        report.defers_inconclusive_predicates_to_encoded_values,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "encoded_predicate_evaluation_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_selection_vector_filter_kernel_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_selection_vector_filter_kernel_discovery_report();
    push_field(
        fields,
        "selection_vector_filter_kernel_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_id",
        report.kernel_report_id,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_operator_kind",
        report.operator_kind.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_kernel_kind",
        report.kernel_kind.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_execution_level",
        report.execution_level.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_contextual_only",
        report.contextual_only,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_encoded_predicate_evaluation",
        report.requires_encoded_predicate_evaluation,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_selection_vectors",
        report.requires_selection_vectors,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_correctness_evidence",
        report.requires_correctness_evidence,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_memory_safety_evidence",
        report.requires_memory_safety_evidence,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_requires_benchmark_for_production",
        report.requires_benchmark_for_production,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_discovery_reads_data",
        report.discovery_reads_data,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_runtime_execution",
        report.runtime_execution_allowed_by_discovery,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_selection_vector_filter_kernel_admission_discovery_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_schema_version",
        "shardloom.vortex_selection_vector_filter_kernel_admission.v1",
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_contextual_only",
        true,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_operator_kind",
        "filter",
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_admission_required_kernel_kind",
        "encoded",
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_filter_kernel_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_correctness_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_memory_safety_evidence",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_requires_benchmark_for_production",
        true,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_runtime_execution",
        false,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_admission_fallback_execution_allowed",
        false,
    );
}

fn append_encoded_count_local_guard_discovery_fields(fields: &mut Vec<(String, String)>) {
    let report = vortex_encoded_count_local_guard_discovery_report();
    push_field(
        fields,
        "encoded_count_local_guard_schema_version",
        report.schema_version,
    );
    push_field(fields, "encoded_count_local_guard_id", report.guard_id);
    push_field(
        fields,
        "encoded_count_local_guard_accepted_approval_sources",
        &report.accepted_approval_sources_text(),
    );
    push_field(
        fields,
        "encoded_count_local_guard_local_execution_status",
        report.local_execution_status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_local_guard_mode",
        report.mode.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_layout_row_count_path_accepted",
        report.layout_row_count_path_accepted,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_approved_local_scan_result_bridge_available",
        report.approved_local_scan_result_bridge_available,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_approved_local_scan_result_bridge_requires_executed_report",
        report.approved_local_scan_result_bridge_requires_executed_report,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_returns_count_result",
        report.returns_count_result,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_side_effect_free",
        report.is_side_effect_free(),
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_data_materialized",
        report.data_materialized,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_runtime_execution",
        report.tasks_executed,
    );
    push_bool_field(
        fields,
        "encoded_count_local_guard_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_adapter_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "adapter_entry_count",
        report.adapter_certification.entries.len(),
    );
    push_count_field(
        fields,
        "read_supported_count",
        report
            .adapter_certification
            .entries
            .iter()
            .filter(|entry| entry.read_supported)
            .count(),
    );
}

fn append_semantic_profile_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "semantic_profile_count",
        report.semantic_profiles.len(),
    );
    push_count_field(
        fields,
        "dimensions_declared_count",
        report
            .semantic_profiles
            .iter()
            .filter(|entry| entry.dimensions_declared)
            .count(),
    );
}

fn append_migration_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "migration_report_count",
        report.migration_reports.len(),
    );
    push_count_field(
        fields,
        "supported_construct_count",
        report
            .migration_reports
            .iter()
            .map(|entry| entry.supported_constructs.len())
            .sum::<usize>(),
    );
}

fn append_full_certification_fields(
    report: &CapabilityCertificationReport,
    fields: &mut Vec<(String, String)>,
) {
    push_count_field(
        fields,
        "sql_feature_count",
        report.sql_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "operator_family_count",
        report.operator_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "function_group_count",
        report.function_coverage.entries.len(),
    );
    push_count_field(
        fields,
        "adapter_entry_count",
        report.adapter_certification.entries.len(),
    );
    push_field(
        fields,
        "best_choice_claim",
        if report.can_publish_best_choice_claim() {
            "certified"
        } else {
            "not_certified"
        },
    );
}

fn certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    match scope {
        CapabilityDiscoveryScope::Engine => unreachable!("engine scope uses EngineCapabilities"),
        CapabilityDiscoveryScope::Sql => sql_certification_text(report, scope),
        CapabilityDiscoveryScope::Functions => function_certification_text(report, scope),
        CapabilityDiscoveryScope::Operators => operator_certification_text(report, scope),
        CapabilityDiscoveryScope::Adapters => adapter_certification_text(report, scope),
        CapabilityDiscoveryScope::SemanticProfiles => {
            semantic_profile_certification_text(report, scope)
        }
        CapabilityDiscoveryScope::Migration => migration_certification_text(report, scope),
        CapabilityDiscoveryScope::Certification => report.to_human_text(),
        CapabilityDiscoveryScope::DataEtl
        | CapabilityDiscoveryScope::Python
        | CapabilityDiscoveryScope::DataFrame
        | CapabilityDiscoveryScope::Notebook
        | CapabilityDiscoveryScope::Udfs
        | CapabilityDiscoveryScope::UniversalAdapters
        | CapabilityDiscoveryScope::EventApiSaasAdapters
        | CapabilityDiscoveryScope::UnstructuredMedia
        | CapabilityDiscoveryScope::ApiSurfaces
        | CapabilityDiscoveryScope::Observability
        | CapabilityDiscoveryScope::Deployment
        | CapabilityDiscoveryScope::Extensions
        | CapabilityDiscoveryScope::SecurityGovernance => {
            unreachable!("world-class user-surface scopes use WorldClassSufficiencyReport")
        }
    }
}

fn sql_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nsql coverage entries:\n{}",
        certification_summary_header(report, scope),
        report
            .sql_coverage
            .entries
            .iter()
            .map(|entry| format!(
                "  - {} [{} / {}]",
                entry.feature.as_str(),
                entry.status.as_str(),
                entry.tier.as_str()
            ))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn function_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nfunction coverage groups:\n{}",
        certification_summary_header(report, scope),
        report
            .function_coverage
            .entries
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.group.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn operator_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    let physical_plan = PhysicalOperatorPlan::cg7_foundation();
    let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
    let encoded_count_local_guard = vortex_encoded_count_local_guard_discovery_report();
    format!(
        "{}\noperator coverage families:\n{}\n{}\n{}\n{}",
        certification_summary_header(report, scope),
        report
            .operator_coverage
            .entries
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.family.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n"),
        physical_plan.to_human_text(),
        execution_profiles.to_human_text(),
        encoded_count_local_guard.to_human_text()
    )
}

fn adapter_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nadapter certification entries:\n{}",
        certification_summary_header(report, scope),
        report
            .adapter_certification
            .entries
            .iter()
            .map(|entry| {
                format!(
                    "  - {} [{} / {}]",
                    entry.adapter_id,
                    entry.status.as_str(),
                    entry.maturity.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn semantic_profile_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nsemantic profiles:\n{}",
        certification_summary_header(report, scope),
        report
            .semantic_profiles
            .iter()
            .map(|entry| format!("  - {} [{}]", entry.profile.as_str(), entry.status.as_str()))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn migration_certification_text(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "{}\nmigration reports:\n{}",
        certification_summary_header(report, scope),
        report
            .migration_reports
            .iter()
            .map(|entry| {
                format!(
                    "  - {} [{}]",
                    entry.report_kind.as_str(),
                    entry.status.as_str()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    )
}

fn certification_summary_header(
    report: &CapabilityCertificationReport,
    scope: CapabilityDiscoveryScope,
) -> String {
    format!(
        "capability discovery: {}\nschema_version: {}\nfallback execution: disabled\nfallback_attempted: {}\nside effects: none\nstatus: planned/report-only",
        scope.as_str(),
        report.schema_version,
        report.fallback_attempted()
    )
}

fn emit_capability_certification(
    scope: CapabilityDiscoveryScope,
    format: OutputFormat,
    report: &CapabilityCertificationReport,
) {
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        format!("capability discovery: {}", scope.as_str()),
        certification_text(report, scope),
        report.diagnostics.clone(),
        certification_fields(report, scope),
    );
}

fn world_class_surface_components(scope: CapabilityDiscoveryScope) -> &'static str {
    match scope {
        CapabilityDiscoveryScope::DataEtl => {
            "ingestion,schema_contracts,data_quality,cleaning,transformation,enrichment,incremental_state,writes_exports,lineage_observability,governance"
        }
        CapabilityDiscoveryScope::Python => {
            "thin_cli_json_wrapper,python_api,diagnostics,materialization_boundaries,python_udf_boundaries,packaging"
        }
        CapabilityDiscoveryScope::DataFrame => {
            "dataframe_query_builder,expressions,lazy_plans,explain,materialization_boundaries"
        }
        CapabilityDiscoveryScope::Notebook => {
            "notebook_helpers,rich_diagnostics,explain_estimate_profile,display_materialization_boundaries"
        }
        CapabilityDiscoveryScope::Udfs => {
            "sql_udf,rust_udf,wasm_udf,python_udf,external_service_udf,sandboxing,effects"
        }
        CapabilityDiscoveryScope::UniversalAdapters => {
            "tabular_files,lakehouse_tables,object_stores,catalogs,relational_warehouses,events_apis_saas,python_notebook,unstructured_media"
        }
        CapabilityDiscoveryScope::EventApiSaasAdapters => {
            "event_streams,rest_apis,saas_exports,webhooks,rate_limits,credentials,effect_boundaries"
        }
        CapabilityDiscoveryScope::UnstructuredMedia => {
            "document_refs,media_refs,text_extraction,chunk_manifests,provenance,redaction,effect_permissions"
        }
        CapabilityDiscoveryScope::ApiSurfaces => {
            "cli_json,rust_api,python_api,query_builder,http_grpc,flightsql_like,jdbc_odbc"
        }
        CapabilityDiscoveryScope::Observability => {
            "explain,estimate,profile,diagnostics,certificates,lineage,metrics"
        }
        CapabilityDiscoveryScope::Deployment => {
            "cli_local,server,container,foundry,cloud_storage,catalog_config,release_packaging"
        }
        CapabilityDiscoveryScope::Extensions => {
            "plugin_manifest,udf_registry,wasm_runtime,python_boundary,permissions,sandboxing"
        }
        CapabilityDiscoveryScope::SecurityGovernance => {
            "credential_boundaries,redaction,audit,tenant_isolation,policy,provenance"
        }
        _ => unreachable!("non-world-class capability scope has no user-surface components"),
    }
}

fn world_class_surface_fields(
    scope: CapabilityDiscoveryScope,
    report: &WorldClassSufficiencyReport,
) -> Vec<(String, String)> {
    let kind = scope
        .world_class_dimension()
        .expect("world-class surface scope has dimension");
    let dimension = report
        .dimensions
        .iter()
        .find(|dimension| dimension.kind == kind)
        .expect("world-class sufficiency report includes all dimensions");
    vec![
        ("scope".to_string(), scope.as_str().to_string()),
        (
            "schema_version".to_string(),
            report.schema_version.to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            report.fallback_execution_allowed.to_string(),
        ),
        (
            "fallback_attempted".to_string(),
            report.fallback_attempted.to_string(),
        ),
        (
            "side_effect_free".to_string(),
            report.is_side_effect_free().to_string(),
        ),
        (
            "filesystem_probe".to_string(),
            report.filesystem_probe.to_string(),
        ),
        (
            "network_probe".to_string(),
            report.network_probe.to_string(),
        ),
        (
            "catalog_probe".to_string(),
            report.catalog_probe.to_string(),
        ),
        (
            "adapter_probe".to_string(),
            report.adapter_probe.to_string(),
        ),
        (
            "parser_executed".to_string(),
            report.parser_executed.to_string(),
        ),
        (
            "runtime_execution".to_string(),
            report.runtime_execution.to_string(),
        ),
        ("dimension".to_string(), dimension.kind.as_str().to_string()),
        (
            "dimension_status".to_string(),
            dimension.status.as_str().to_string(),
        ),
        ("required".to_string(), dimension.required.to_string()),
        (
            "correctness_evidence_required".to_string(),
            dimension.correctness_evidence_required.to_string(),
        ),
        (
            "semantic_conformance_required".to_string(),
            dimension.semantic_conformance_required.to_string(),
        ),
        (
            "benchmark_evidence_required".to_string(),
            dimension.benchmark_evidence_required.to_string(),
        ),
        (
            "adapter_certification_required".to_string(),
            dimension.adapter_certification_required.to_string(),
        ),
        (
            "native_io_certificate_required".to_string(),
            dimension.native_io_certificate_required.to_string(),
        ),
        (
            "execution_certificate_required".to_string(),
            dimension.execution_certificate_required.to_string(),
        ),
        (
            "capability_snapshot_required".to_string(),
            dimension.capability_snapshot_required.to_string(),
        ),
        (
            "surface_components".to_string(),
            world_class_surface_components(scope).to_string(),
        ),
        (
            "production_claim_allowed".to_string(),
            report.production_claim_allowed.to_string(),
        ),
        (
            "best_default_publication_allowed".to_string(),
            report.can_publish_best_default_claim().to_string(),
        ),
    ]
}

fn world_class_surface_text(
    scope: CapabilityDiscoveryScope,
    report: &WorldClassSufficiencyReport,
) -> String {
    let kind = scope
        .world_class_dimension()
        .expect("world-class surface scope has dimension");
    let dimension_status = report.status_for(kind).as_str();
    format!(
        "capability discovery: {}\nschema_version: {}\nfallback execution: disabled\nfallback_attempted: {}\nside effects: none\ndimension: {}\ndimension_status: {}\nsurface_components: {}\nstatus: planned/report-only",
        scope.as_str(),
        report.schema_version,
        report.fallback_attempted,
        kind.as_str(),
        dimension_status,
        world_class_surface_components(scope)
    )
}

fn emit_world_class_surface_capability(
    scope: CapabilityDiscoveryScope,
    format: OutputFormat,
    report: &WorldClassSufficiencyReport,
) {
    emit(
        "capabilities",
        format,
        CommandStatus::Success,
        format!("capability discovery: {}", scope.as_str()),
        world_class_surface_text(scope, report),
        report.diagnostics.clone(),
        world_class_surface_fields(scope, report),
    );
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

fn vortex_projection_readiness_fields(
    report: &shardloom_vortex::VortexProjectionReadinessReport,
) -> Vec<(String, String)> {
    vec![
        (
            "candidate_source".to_string(),
            report.request.candidate_source.as_str().to_string(),
        ),
        ("status".to_string(), report.status.as_str().to_string()),
        ("mode".to_string(), report.mode.as_str().to_string()),
        (
            "projection_ready".to_string(),
            report.projection_ready().to_string(),
        ),
        (
            "projection_executed".to_string(),
            report.projection_executed().to_string(),
        ),
        (
            "projection_applied".to_string(),
            report.projection_applied().to_string(),
        ),
        (
            "feature_gate_enabled".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::FeatureGateEnabled)
                .to_string(),
        ),
        (
            "query_primitive_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady)
                .to_string(),
        ),
        (
            "metadata_footer_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::MetadataFooterReady)
                .to_string(),
        ),
        (
            "encoded_data_path_ready".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::EncodedDataPathReady)
                .to_string(),
        ),
        (
            "projection_primitive".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionPrimitive)
                .to_string(),
        ),
        (
            "projection_provided".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionProvided)
                .to_string(),
        ),
        (
            "projection_supported".to_string(),
            report
                .request
                .has_signal(VortexProjectionReadinessSignal::ProjectionSupported)
                .to_string(),
        ),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("metadata_read".to_string(), "false".to_string()),
        ("encoded_data_read".to_string(), "false".to_string()),
        ("row_read".to_string(), "false".to_string()),
        ("array_decoded".to_string(), "false".to_string()),
        ("values_materialized".to_string(), "false".to_string()),
        ("arrow_converted".to_string(), "false".to_string()),
        ("object_store_io".to_string(), "false".to_string()),
        ("data_written".to_string(), "false".to_string()),
        ("upstream_scan_called".to_string(), "false".to_string()),
    ]
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
    let (memory_gb, max_parallelism, execute_local_count, report, local_execution_report) =
        match run_vortex_encoded_read_spike(parsed.0, parsed.1, parsed.2, parsed.3) {
            Ok(v) => v,
            Err(error) => {
                return emit_error(command, format, "vortex encoded-read spike failed", &error);
            }
        };
    let local_execution_failed = local_execution_report
        .as_ref()
        .is_some_and(VortexLocalExecutionReport::has_errors);
    let mut diagnostics = report.diagnostics.clone();
    if let Some(local) = &local_execution_report {
        diagnostics.extend(local.diagnostics.clone());
    }
    let human_text = local_execution_report.as_ref().map_or_else(
        || report.to_human_text(),
        |local| format!("{}\n\n{}", report.to_human_text(), local.to_human_text()),
    );
    let fields = vortex_encoded_read_spike_fields(
        memory_gb,
        max_parallelism,
        execute_local_count,
        &report,
        local_execution_report.as_ref(),
    );
    emit(
        command,
        format,
        if report.has_errors() || local_execution_failed {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex encoded-read spike report".to_string(),
        human_text,
        diagnostics,
        fields,
    );
    if report.has_errors() || local_execution_failed {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn vortex_encoded_read_spike_fields(
    memory_gb: u64,
    max_parallelism: usize,
    execute_local_count: bool,
    report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_execution_report: Option<&VortexLocalExecutionReport>,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_field(&mut fields, "mode", "vortex_encoded_read_spike");
    push_bool_field(
        &mut fields,
        "feature_enabled",
        vortex_encoded_read_spike_feature_enabled(),
    );
    push_bool_field(
        &mut fields,
        "execute_local_count_requested",
        execute_local_count,
    );
    push_bool_field(
        &mut fields,
        "encoded_read_attempted",
        report.upstream_scan_called,
    );
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        report.external_effects_executed,
    );
    push_field(&mut fields, "execution", report.status.as_str());
    fields.push(("memory_gb".to_string(), memory_gb.to_string()));
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_count_field(&mut fields, "arrays_read_count", report.arrays_read_count);
    fields.push(("rows_counted".to_string(), report.rows_counted.to_string()));
    fields.push((
        "count_result".to_string(),
        report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    fields.push((
        "local_scan_target_uri".to_string(),
        report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    fields.push((
        "local_scan_readiness_source_uri".to_string(),
        report
            .local_scan_readiness_source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    push_bool_field(
        &mut fields,
        "local_scan_source_uri_matches_target",
        report.local_scan_source_uri_matches_target,
    );
    if let Some(local) = local_execution_report {
        append_vortex_encoded_read_spike_local_execution_fields(&mut fields, local);
    }
    fields
}

fn append_vortex_encoded_read_spike_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalExecutionReport,
) {
    push_field(fields, "local_execution_status", local.status.as_str());
    push_field(fields, "local_execution_mode", local.mode.as_str());
    push_bool_field(
        fields,
        "local_execution_result_known",
        local.value.is_known(),
    );
    fields.push(("local_execution_value".to_string(), local.value.summary()));
    push_bool_field(
        fields,
        "local_execution_tasks_executed",
        local.tasks_executed,
    );
    push_bool_field(fields, "local_execution_data_read", local.data_read);
    push_bool_field(fields, "local_execution_data_decoded", local.data_decoded);
    push_bool_field(
        fields,
        "local_execution_data_materialized",
        local.data_materialized,
    );
    push_bool_field(
        fields,
        "local_execution_object_store_io",
        local.object_store_io,
    );
    push_bool_field(fields, "local_execution_write_io", local.write_io);
    push_bool_field(
        fields,
        "local_execution_spill_io_performed",
        local.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_execution_external_effects_executed",
        local.external_effects_executed,
    );
    push_bool_field(
        fields,
        "local_execution_fallback_execution_allowed",
        local.fallback_execution_allowed,
    );
}

fn parse_vortex_spike_args(
    command: &str,
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, u64, usize, bool), ExitCode> {
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
    let mut execute_local_count = false;
    for token in args {
        if token == "--execute-local-count" {
            execute_local_count = true;
        } else {
            eprintln!("unknown option for shardloom {command}: {token}");
            return Err(ExitCode::from(2));
        }
    }
    Ok((uri, memory_gb, max_parallelism, execute_local_count))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VortexCountExecutionRequest {
    MetadataOnly,
    LocalEncodedCount {
        memory_gb: u64,
        max_parallelism: usize,
    },
}

fn parse_vortex_count_args(
    mut args: std::vec::IntoIter<String>,
) -> std::result::Result<(DatasetUri, VortexCountExecutionRequest), ExitCode> {
    let Some(dataset_uri) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> [--execute-local-encoded-count <memory_gb> <max_parallelism>]"
        );
        return Err(ExitCode::from(2));
    };
    let uri = DatasetUri::new(dataset_uri).map_err(|_| ExitCode::from(2))?;
    let Some(option) = args.next() else {
        return Ok((uri, VortexCountExecutionRequest::MetadataOnly));
    };
    if option != "--execute-local-encoded-count" {
        eprintln!("unknown option for shardloom vortex-count: {option}");
        return Err(ExitCode::from(2));
    }
    let Some(memory_gb_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!(
            "usage: shardloom vortex-count <dataset_uri> --execute-local-encoded-count <memory_gb> <max_parallelism>"
        );
        return Err(ExitCode::from(2));
    };
    if let Some(extra) = args.next() {
        eprintln!("unknown extra argument for shardloom vortex-count: {extra}");
        return Err(ExitCode::from(2));
    }
    let memory_gb = memory_gb_text.parse().map_err(|_| ExitCode::from(2))?;
    let max_parallelism = max_parallelism_text
        .parse()
        .map_err(|_| ExitCode::from(2))?;
    Ok((
        uri,
        VortexCountExecutionRequest::LocalEncodedCount {
            memory_gb,
            max_parallelism,
        },
    ))
}

fn run_vortex_encoded_read_spike(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
    execute_local_count: bool,
) -> shardloom_core::Result<(
    u64,
    usize,
    bool,
    shardloom_vortex::VortexEncodedReadExecutionReport,
    Option<VortexLocalExecutionReport>,
)> {
    let source = shardloom_core::UniversalInputSource::from_dataset_uri(uri.clone())?;
    let input_plan = plan_native_vortex_universal_input(source)?;
    let read_report = plan_vortex_read_from_universal_input(input_plan)?;
    let runtime_report = build_vortex_runtime_task_graph(read_report)?;
    let sizing_report = size_vortex_runtime_task_graph(
        runtime_report,
        AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb)),
    )?;
    let budget = MemoryBudget::from_gib(memory_gb)?;
    let memory_report = plan_vortex_memory_safety(sizing_report, budget)?;
    let mut scheduler_report = plan_vortex_scheduler_queue(memory_report, max_parallelism)?;
    if execute_local_count && scheduler_report.scheduled_task_count == 0 {
        scheduler_report
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "approved local encoded count execution",
            ));
        scheduler_report.recompute_counts();
    }
    let readiness_report = evaluate_vortex_encoded_read_readiness(scheduler_report)?;
    let (report, local_execution_report) = if execute_local_count {
        let (report, local_execution_report) =
            run_vortex_approved_local_encoded_count_from_readiness(uri, &readiness_report)?;
        (report, Some(local_execution_report))
    } else {
        let api = vortex_encoded_read_public_api_boundary();
        let probe = plan_vortex_encoded_read_probe(api.clone(), readiness_report.clone())?;
        (
            execute_vortex_encoded_read_spike(readiness_report, api, probe)?,
            None,
        )
    };
    Ok((
        memory_gb,
        max_parallelism,
        execute_local_count,
        report,
        local_execution_report,
    ))
}

fn build_vortex_encoded_count_readiness(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<shardloom_vortex::VortexEncodedReadReadinessReport> {
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
    let mut scheduler_report = plan_vortex_scheduler_queue(memory_report, max_parallelism)?;
    if scheduler_report.scheduled_task_count == 0 {
        scheduler_report
            .decisions
            .push(VortexTaskSchedulingDecision::schedule_now(
                None,
                "approved local encoded count execution",
            ));
        scheduler_report.recompute_counts();
    }
    evaluate_vortex_encoded_read_readiness(scheduler_report)
}

fn run_vortex_approved_local_encoded_count_from_readiness(
    uri: DatasetUri,
    readiness_report: &shardloom_vortex::VortexEncodedReadReadinessReport,
) -> shardloom_core::Result<(
    shardloom_vortex::VortexEncodedReadExecutionReport,
    VortexLocalExecutionReport,
)> {
    let count_report = plan_vortex_count_readiness(
        VortexCountReadinessRequest::new(uri, VortexCountCandidateSource::EncodedDataPath)
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .count_primitive(true)
            .encoded_data_path_ready(true),
    )?;
    let approval = plan_vortex_encoded_count_data_path_approval(
        count_report,
        vortex_encoded_read_local_scan_count_api_boundary(),
    )?;
    let report = execute_vortex_count_all_from_approved_local_scan(&approval, readiness_report)?;
    let local_execution_report =
        execute_vortex_count_all_from_approved_local_scan_result(&approval, &report)?;
    Ok((report, local_execution_report))
}

fn run_vortex_approved_local_encoded_count(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<(
    shardloom_vortex::VortexEncodedReadExecutionReport,
    VortexLocalExecutionReport,
)> {
    let readiness_report =
        build_vortex_encoded_count_readiness(uri.clone(), memory_gb, max_parallelism)?;
    run_vortex_approved_local_encoded_count_from_readiness(uri, &readiness_report)
}

fn handle_vortex_count(args: std::vec::IntoIter<String>, format: OutputFormat) -> ExitCode {
    let (uri, execution_request) = match parse_vortex_count_args(args) {
        Ok(parsed) => parsed,
        Err(code) => return code,
    };
    match execution_request {
        VortexCountExecutionRequest::MetadataOnly => handle_vortex_count_metadata(uri, format),
        VortexCountExecutionRequest::LocalEncodedCount {
            memory_gb,
            max_parallelism,
        } => handle_vortex_count_local_encoded(uri, memory_gb, max_parallelism, format),
    }
}

fn handle_vortex_count_metadata(uri: DatasetUri, format: OutputFormat) -> ExitCode {
    let request = shardloom_vortex::VortexQueryPrimitiveRequest::count_all(uri.clone());
    let open = open_vortex_metadata_only(VortexMetadataOpenRequest::metadata_only(uri));
    let summary = if let Ok(report) = open {
        if let Some(summary) = report.metadata_summary {
            summary
        } else if report.has_errors() {
            let mut degraded =
                summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear());
            degraded.diagnostics.extend(report.diagnostics.clone());
            degraded
        } else {
            summarize_vortex_metadata_probe(&VortexMetadataProbeReport::deferred_api_unclear())
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
            (
                "explicit_local_encoded_count_requested".to_string(),
                "false".to_string(),
            ),
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

fn handle_vortex_count_local_encoded(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
    format: OutputFormat,
) -> ExitCode {
    let (encoded_report, local_report) =
        match run_vortex_approved_local_encoded_count(uri.clone(), memory_gb, max_parallelism) {
            Ok(reports) => reports,
            Err(error) => {
                return emit_error("vortex-count", format, "vortex count failed", &error);
            }
        };
    let streaming_plan =
        match build_vortex_count_local_streaming_batch_plan(uri, memory_gb, max_parallelism) {
            Ok(report) => report,
            Err(error) => {
                return emit_error(
                    "vortex-count",
                    format,
                    "vortex streaming-batch runtime evidence failed",
                    &error,
                );
            }
        };
    let streaming_report = execute_vortex_streaming_batches_from_local_encoded_count(
        streaming_plan,
        encoded_report.clone(),
    );
    let local_execution_failed = local_report.has_errors();
    let evidence = match vortex_count_local_encoded_evidence(&encoded_report, &local_report) {
        Ok(evidence) => evidence,
        Err(error) => {
            return emit_error(
                "vortex-count",
                format,
                "vortex count evidence failed",
                &error,
            );
        }
    };
    let mut diagnostics = encoded_report.diagnostics.clone();
    diagnostics.extend(local_report.diagnostics.clone());
    diagnostics.extend(streaming_report.diagnostics.clone());
    diagnostics.extend(evidence.diagnostics());
    let mut human_sections = vec![encoded_report.to_human_text(), local_report.to_human_text()];
    human_sections.push(streaming_report.to_human_text());
    human_sections.extend(evidence.human_sections());
    let human_text = human_sections.join("\n\n");
    let fields = vortex_count_local_encoded_fields(
        memory_gb,
        max_parallelism,
        &encoded_report,
        &local_report,
        &streaming_report,
        &evidence,
    );
    emit(
        "vortex-count",
        format,
        if encoded_report.has_errors()
            || local_execution_failed
            || streaming_report.has_errors()
            || evidence.has_errors()
        {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "vortex local encoded count execution".to_string(),
        human_text,
        diagnostics,
        fields,
    );
    if encoded_report.has_errors()
        || local_execution_failed
        || streaming_report.has_errors()
        || evidence.has_errors()
    {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn build_vortex_count_local_streaming_batch_plan(
    uri: DatasetUri,
    memory_gb: u64,
    max_parallelism: usize,
) -> shardloom_core::Result<EncodedStreamingBatchPlanReport> {
    let dataset = DatasetRef::from_uri(uri)?;
    let input = EncodedStreamingBatchPlanInput::new(
        StreamingSource::vortex_dataset(dataset),
        StreamingSink::null_benchmark(),
        BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb)),
        max_parallelism,
    )?;
    plan_encoded_streaming_batches(input)
}

struct VortexCountLocalEncodedEvidence {
    fixture_id: Option<String>,
    fixture_source_ref: Option<String>,
    certificate: Option<ExecutionCertificate>,
    physical_kernel: Option<VortexEncodedCountPhysicalKernelReport>,
    kernel_admission: Option<VortexEncodedCountKernelAdmissionReport>,
}

impl VortexCountLocalEncodedEvidence {
    fn unavailable() -> Self {
        Self {
            fixture_id: None,
            fixture_source_ref: None,
            certificate: None,
            physical_kernel: None,
            kernel_admission: None,
        }
    }

    fn from_fixture(
        fixture: &CorrectnessFixture,
        certificate: ExecutionCertificate,
        physical_kernel: VortexEncodedCountPhysicalKernelReport,
        kernel_admission: VortexEncodedCountKernelAdmissionReport,
    ) -> Self {
        Self {
            fixture_id: Some(fixture.id.as_str().to_string()),
            fixture_source_ref: fixture.source_ref.clone(),
            certificate: Some(certificate),
            physical_kernel: Some(physical_kernel),
            kernel_admission: Some(kernel_admission),
        }
    }

    fn has_errors(&self) -> bool {
        self.certificate
            .as_ref()
            .is_some_and(|certificate| !certificate.is_certified())
            || self
                .physical_kernel
                .as_ref()
                .is_some_and(VortexEncodedCountPhysicalKernelReport::has_errors)
            || self
                .kernel_admission
                .as_ref()
                .is_some_and(VortexEncodedCountKernelAdmissionReport::has_errors)
    }

    fn diagnostics(&self) -> Vec<shardloom_core::Diagnostic> {
        let mut diagnostics = Vec::new();
        if let Some(certificate) = &self.certificate {
            diagnostics.extend(certificate.diagnostics.clone());
        }
        if let Some(physical_kernel) = &self.physical_kernel {
            diagnostics.extend(physical_kernel.diagnostics.clone());
        }
        if let Some(kernel_admission) = &self.kernel_admission {
            diagnostics.extend(kernel_admission.diagnostics.clone());
        }
        diagnostics
    }

    fn human_sections(&self) -> Vec<String> {
        let mut sections = Vec::new();
        if let Some(certificate) = &self.certificate {
            sections.push(certificate.to_human_text());
        }
        if let Some(physical_kernel) = &self.physical_kernel {
            sections.push(physical_kernel.to_human_text());
        }
        if let Some(kernel_admission) = &self.kernel_admission {
            sections.push(kernel_admission.to_human_text());
        }
        sections
    }
}

fn vortex_count_local_encoded_evidence(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
) -> shardloom_core::Result<VortexCountLocalEncodedEvidence> {
    let Some(fixture) = local_encoded_count_correctness_fixture_for_report(encoded_report) else {
        return Ok(VortexCountLocalEncodedEvidence::unavailable());
    };
    let certificate =
        local_encoded_count_execution_certificate(&fixture, encoded_report, local_report)?;
    let physical_kernel = evaluate_vortex_local_encoded_count_physical_kernel(
        encoded_report,
        local_report,
        &certificate,
    );
    let kernel_admission = admit_vortex_encoded_count_kernel(&physical_kernel)?;
    Ok(VortexCountLocalEncodedEvidence::from_fixture(
        &fixture,
        certificate,
        physical_kernel,
        kernel_admission,
    ))
}

fn local_encoded_count_correctness_fixture_for_report(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
) -> Option<CorrectnessFixture> {
    if !encoded_report.local_scan_source_uri_matches_target {
        return None;
    }
    let target_uri = encoded_report.local_scan_target_uri.as_ref()?;
    local_encoded_count_correctness_fixture_for_target(target_uri)
}

fn local_encoded_count_correctness_fixture_for_target(
    target_uri: &DatasetUri,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            fixture.id.as_str() == "vortex-local-encoded-count-u64-20000"
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

fn local_fixture_ref_matches(target_uri: &DatasetUri, source_ref: &str) -> bool {
    let Some(target_ref) = canonical_local_fixture_ref(target_uri.as_str()) else {
        return false;
    };
    let Some(workspace_source_ref) = canonical_workspace_fixture_ref(source_ref) else {
        return false;
    };
    target_ref == workspace_source_ref
}

fn canonical_workspace_fixture_ref(source_ref: &str) -> Option<String> {
    let source_ref = normalized_local_fixture_ref(source_ref);
    let source_path = std::path::Path::new(&source_ref);
    let absolute = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        workspace_root().join(source_path)
    };
    canonical_path_string(&absolute)
}

fn canonical_local_fixture_ref(value: &str) -> Option<String> {
    let target_ref = normalized_local_fixture_ref(value);
    let target_path = std::path::Path::new(&target_ref);
    let absolute = if target_path.is_absolute() {
        target_path.to_path_buf()
    } else {
        workspace_root().join(target_path)
    };
    canonical_path_string(&absolute)
}

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn canonical_path_string(path: &std::path::Path) -> Option<String> {
    path.canonicalize()
        .ok()
        .map(|path| path.to_string_lossy().replace('\\', "/"))
}

fn normalized_local_fixture_ref(value: &str) -> String {
    let without_fragment = value
        .split_once(['?', '#'])
        .map_or(value, |(prefix, _)| prefix);
    let without_scheme = without_fragment
        .strip_prefix("file:///")
        .or_else(|| without_fragment.strip_prefix("file://"))
        .unwrap_or(without_fragment);
    without_scheme
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string()
}

fn vortex_count_local_encoded_fields(
    memory_gb: u64,
    max_parallelism: usize,
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
    streaming_report: &VortexStreamingBatchRuntimeReport,
    evidence: &VortexCountLocalEncodedEvidence,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_bool_field(&mut fields, "fallback_execution_allowed", false);
    push_field(&mut fields, "mode", "vortex_count");
    push_field(&mut fields, "primitive", "count_all");
    push_bool_field(&mut fields, "explicit_local_encoded_count_requested", true);
    push_field(&mut fields, "feature_gate", "vortex-encoded-read-spike");
    push_bool_field(
        &mut fields,
        "feature_enabled",
        vortex_encoded_read_spike_feature_enabled(),
    );
    push_bool_field(
        &mut fields,
        "encoded_read_attempted",
        encoded_report.upstream_scan_called,
    );
    push_bool_field(&mut fields, "data_read", encoded_report.data_read);
    push_bool_field(&mut fields, "data_decoded", encoded_report.data_decoded);
    push_bool_field(
        &mut fields,
        "data_materialized",
        encoded_report.data_materialized,
    );
    push_bool_field(
        &mut fields,
        "object_store_io",
        encoded_report.object_store_io,
    );
    push_bool_field(&mut fields, "write_io", encoded_report.write_io);
    push_bool_field(
        &mut fields,
        "spill_io_performed",
        encoded_report.spill_io_performed,
    );
    push_bool_field(
        &mut fields,
        "external_effects_executed",
        encoded_report.external_effects_executed,
    );
    push_field(&mut fields, "execution", encoded_report.status.as_str());
    fields.push(("memory_gb".to_string(), memory_gb.to_string()));
    push_count_field(&mut fields, "max_parallelism", max_parallelism);
    push_count_field(
        &mut fields,
        "arrays_read_count",
        encoded_report.arrays_read_count,
    );
    fields.push((
        "rows_counted".to_string(),
        encoded_report.rows_counted.to_string(),
    ));
    fields.push((
        "result_known".to_string(),
        encoded_report.count_result.is_some().to_string(),
    ));
    fields.push((
        "count".to_string(),
        encoded_report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    fields.push((
        "local_scan_target_uri".to_string(),
        encoded_report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    fields.push((
        "local_scan_readiness_source_uri".to_string(),
        encoded_report
            .local_scan_readiness_source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    ));
    push_bool_field(
        &mut fields,
        "local_scan_source_uri_matches_target",
        encoded_report.local_scan_source_uri_matches_target,
    );
    append_vortex_encoded_read_spike_local_execution_fields(&mut fields, local_report);
    append_vortex_streaming_batch_runtime_fields(&mut fields, streaming_report);
    append_vortex_count_local_encoded_evidence_fields(&mut fields, evidence);
    fields
}

fn append_vortex_streaming_batch_runtime_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    append_vortex_streaming_batch_runtime_contract_fields(fields, report);
    append_vortex_streaming_batch_runtime_source_fields(fields, report);
    append_vortex_streaming_batch_runtime_execution_fields(fields, report);
}

fn append_vortex_streaming_batch_runtime_contract_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_bool_field(fields, "streaming_batch_runtime_report_emitted", true);
    push_field(
        fields,
        "streaming_batch_runtime_schema_version",
        report.schema_version,
    );
    push_field(
        fields,
        "streaming_batch_runtime_status",
        report.status.as_str(),
    );
    push_field(fields, "streaming_batch_runtime_mode", report.mode.as_str());
    push_field(
        fields,
        "streaming_batch_runtime_representation",
        report.representation.as_str(),
    );
    push_field(
        fields,
        "streaming_batch_runtime_zero_decode",
        report.zero_decode.as_str(),
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_encoded_representation_preserved",
        report.encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_bounded_parallelism",
        report.bounded_parallelism,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_bounded_memory",
        report.bounded_memory,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_backpressure_bounded",
        report.backpressure_bounded,
    );
}

fn append_vortex_streaming_batch_runtime_source_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_field(
        fields,
        "streaming_batch_runtime_source_uri",
        &report
            .source_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    );
    push_field(
        fields,
        "streaming_batch_runtime_local_scan_target_uri",
        &report
            .local_scan_target_uri
            .as_ref()
            .map_or_else(|| "none".to_string(), |uri| uri.as_str().to_string()),
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_source_uri_matches_local_scan",
        report.source_uri_matches_local_scan,
    );
}

fn append_vortex_streaming_batch_runtime_execution_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexStreamingBatchRuntimeReport,
) {
    push_count_field(
        fields,
        "streaming_batch_runtime_batches_executed",
        report.batches_executed,
    );
    fields.push((
        "streaming_batch_runtime_rows_processed".to_string(),
        report.rows_processed.to_string(),
    ));
    fields.push((
        "streaming_batch_runtime_count_result".to_string(),
        report
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    ));
    push_bool_field(
        fields,
        "streaming_batch_runtime_streams_executed",
        report.streams_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_tasks_executed",
        report.tasks_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_data_materialized",
        report.data_materialized,
    );
    push_bool_field(fields, "streaming_batch_runtime_row_read", report.row_read);
    push_bool_field(
        fields,
        "streaming_batch_runtime_arrow_converted",
        report.arrow_converted,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_object_store_io",
        report.object_store_io,
    );
    push_bool_field(fields, "streaming_batch_runtime_write_io", report.write_io);
    push_bool_field(
        fields,
        "streaming_batch_runtime_spill_io_performed",
        report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_external_effects_executed",
        report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "streaming_batch_runtime_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_vortex_count_local_encoded_evidence_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    push_bool_field(
        fields,
        "correctness_fixture_matched",
        evidence.fixture_id.is_some(),
    );
    push_field(
        fields,
        "correctness_fixture_id",
        evidence.fixture_id.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "correctness_fixture_source_ref",
        evidence.fixture_source_ref.as_deref().unwrap_or("none"),
    );
    if let Some(certificate) = &evidence.certificate {
        append_execution_certificate_fields(fields, certificate);
    } else {
        push_bool_field(fields, "execution_certificate_emitted", false);
        push_field(
            fields,
            "execution_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "execution_certificate_correctness_passed", false);
        push_bool_field(
            fields,
            "execution_certificate_unsafe_effect_detected",
            false,
        );
        push_bool_field(fields, "execution_certificate_fallback_attempted", false);
        push_bool_field(
            fields,
            "execution_certificate_fallback_execution_allowed",
            false,
        );
    }
    if let Some(physical_kernel) = &evidence.physical_kernel {
        append_encoded_count_physical_kernel_fields(fields, physical_kernel);
    } else {
        push_bool_field(fields, "encoded_count_physical_kernel_emitted", false);
        push_field(
            fields,
            "encoded_count_physical_kernel_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "encoded_count_physical_kernel_safe_evidence", false);
        push_bool_field(
            fields,
            "encoded_count_physical_kernel_production_claim_allowed",
            false,
        );
    }
    if let Some(kernel_admission) = &evidence.kernel_admission {
        append_encoded_count_kernel_admission_fields(fields, kernel_admission);
    } else {
        push_bool_field(fields, "encoded_count_kernel_admission_emitted", false);
        push_field(
            fields,
            "encoded_count_kernel_admission_status",
            "evidence_unavailable",
        );
        push_bool_field(
            fields,
            "encoded_count_kernel_admission_slot_marked_present",
            false,
        );
        push_bool_field(
            fields,
            "encoded_count_kernel_admission_production_claim_allowed",
            false,
        );
    }
}

fn append_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(fields, "execution_certificate_emitted", true);
    push_field(
        fields,
        "execution_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "execution_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "execution_certificate_execution_kind",
        &certificate.execution_kind,
    );
    push_field(
        fields,
        "execution_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "execution_certificate_input_ref",
        certificate.input_ref.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_output_ref",
        certificate.output_ref.as_deref().unwrap_or("none"),
    );
    push_bool_field(
        fields,
        "execution_certificate_correctness_passed",
        certificate.correctness_passed,
    );
    push_bool_field(
        fields,
        "execution_certificate_data_read",
        certificate.data_read,
    );
    push_bool_field(
        fields,
        "execution_certificate_data_decoded",
        certificate.data_decoded,
    );
    push_bool_field(
        fields,
        "execution_certificate_data_materialized",
        certificate.data_materialized,
    );
    push_bool_field(
        fields,
        "execution_certificate_row_read",
        certificate.row_read,
    );
    push_bool_field(
        fields,
        "execution_certificate_arrow_converted",
        certificate.arrow_converted,
    );
    push_bool_field(
        fields,
        "execution_certificate_object_store_io",
        certificate.object_store_io,
    );
    push_bool_field(
        fields,
        "execution_certificate_write_io",
        certificate.write_io,
    );
    push_bool_field(
        fields,
        "execution_certificate_spill_io_performed",
        certificate.spill_io_performed,
    );
    push_bool_field(
        fields,
        "execution_certificate_external_effects_executed",
        certificate.external_effects_executed,
    );
    push_bool_field(
        fields,
        "execution_certificate_unsafe_effect_detected",
        certificate.unsafe_effect_detected,
    );
    push_bool_field(
        fields,
        "execution_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "execution_certificate_fallback_execution_allowed",
        certificate.fallback_execution_allowed,
    );
}

fn append_encoded_count_physical_kernel_fields(
    fields: &mut Vec<(String, String)>,
    physical_kernel: &VortexEncodedCountPhysicalKernelReport,
) {
    push_bool_field(fields, "encoded_count_physical_kernel_emitted", true);
    push_field(
        fields,
        "encoded_count_physical_kernel_schema_version",
        physical_kernel.schema_version,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_id",
        &physical_kernel.kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_status",
        physical_kernel.status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_execution_certificate_id",
        physical_kernel
            .execution_certificate_id
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "encoded_count_physical_kernel_count",
        &physical_kernel
            .count_result
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_safe_evidence",
        physical_kernel.is_safe_native_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_production_claim_allowed",
        physical_kernel.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_read",
        physical_kernel.data_read,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_decoded",
        physical_kernel.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_data_materialized",
        physical_kernel.data_materialized,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_row_read",
        physical_kernel.row_read,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_arrow_converted",
        physical_kernel.arrow_converted,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_object_store_io",
        physical_kernel.object_store_io,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_write_io",
        physical_kernel.write_io,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_spill_io_performed",
        physical_kernel.spill_io_performed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_external_effects_executed",
        physical_kernel.external_effects_executed,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_attempted",
        physical_kernel.fallback_attempted,
    );
    push_bool_field(
        fields,
        "encoded_count_physical_kernel_fallback_execution_allowed",
        physical_kernel.fallback_execution_allowed,
    );
}

fn append_metadata_filter_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    filter_admission: &VortexMetadataFilterKernelAdmissionReport,
) {
    push_bool_field(fields, "metadata_filter_kernel_admission_emitted", true);
    push_field(
        fields,
        "metadata_filter_kernel_admission_schema_version",
        filter_admission.schema_version,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_id",
        &filter_admission.admission_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_kernel_report_id",
        &filter_admission.metadata_kernel_report_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_slot_id",
        &filter_admission.slot_id,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_operator_kind",
        filter_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_required_kernel_kind",
        filter_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_candidate_kernel_kind",
        filter_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_status",
        filter_admission.status.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_correctness_evidence",
        filter_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_benchmark_evidence",
        filter_admission.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_streaming",
        filter_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_bounded",
        filter_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_memory_oom_safe",
        filter_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_requires_full_materialization",
        filter_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "metadata_filter_kernel_admission_fallback_state",
        filter_admission.fallback.as_str(),
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_slot_marked_present",
        filter_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_production_claim_allowed",
        filter_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_runtime_execution",
        filter_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "metadata_filter_kernel_admission_fallback_execution_allowed",
        filter_admission.fallback_execution_allowed,
    );
}

fn append_metadata_count_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(fields, "metadata_count_kernel_admission_emitted", true);
    push_field(
        fields,
        "metadata_count_kernel_admission_schema_version",
        count_admission.schema_version,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_id",
        &count_admission.admission_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_kernel_report_id",
        &count_admission.metadata_kernel_report_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_primitive_kind",
        count_admission.primitive_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_slot_id",
        &count_admission.slot_id,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_operator_kind",
        count_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_required_kernel_kind",
        count_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_candidate_kernel_kind",
        count_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_status",
        count_admission.status.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_correctness_evidence",
        count_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_benchmark_evidence",
        count_admission.benchmark_evidence.as_str(),
    );
    append_metadata_count_kernel_admission_resource_fields(fields, count_admission);
    append_metadata_count_kernel_admission_outcome_fields(fields, count_admission);
}

fn append_metadata_count_kernel_admission_resource_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_streaming",
        count_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_bounded",
        count_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_memory_oom_safe",
        count_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_requires_full_materialization",
        count_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "metadata_count_kernel_admission_fallback_state",
        count_admission.fallback.as_str(),
    );
}

fn append_metadata_count_kernel_admission_outcome_fields(
    fields: &mut Vec<(String, String)>,
    count_admission: &VortexMetadataCountKernelAdmissionReport,
) {
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_slot_marked_present",
        count_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_production_claim_allowed",
        count_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_runtime_execution",
        count_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "metadata_count_kernel_admission_fallback_execution_allowed",
        count_admission.fallback_execution_allowed,
    );
}

fn append_encoded_count_kernel_admission_fields(
    fields: &mut Vec<(String, String)>,
    kernel_admission: &VortexEncodedCountKernelAdmissionReport,
) {
    push_bool_field(fields, "encoded_count_kernel_admission_emitted", true);
    push_field(
        fields,
        "encoded_count_kernel_admission_schema_version",
        kernel_admission.schema_version,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_id",
        &kernel_admission.admission_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_physical_kernel_report_id",
        &kernel_admission.physical_kernel_report_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_slot_id",
        &kernel_admission.slot_id,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_operator_kind",
        kernel_admission.operator_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_required_kernel_kind",
        kernel_admission.required_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_candidate_kernel_kind",
        kernel_admission.candidate_kernel_kind.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_status",
        kernel_admission.status.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_correctness_evidence",
        kernel_admission.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_benchmark_evidence",
        kernel_admission.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_streaming",
        kernel_admission.memory.streaming,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_bounded",
        kernel_admission.memory.bounded_memory,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_memory_oom_safe",
        kernel_admission.memory.oom_safe,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_requires_full_materialization",
        kernel_admission.memory.requires_full_materialization,
    );
    push_field(
        fields,
        "encoded_count_kernel_admission_fallback_state",
        kernel_admission.fallback.as_str(),
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_slot_marked_present",
        kernel_admission.slot_marked_present,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_production_claim_allowed",
        kernel_admission.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_runtime_execution",
        kernel_admission.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "encoded_count_kernel_admission_fallback_execution_allowed",
        kernel_admission.fallback_execution_allowed,
    );
}

fn emit_layout_health_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match layout_health_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "layout-health-plan",
                format,
                "layout health planning failed",
                &error,
            );
        }
    };
    let report = evaluate_layout_health(manifest, LayoutHealthPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "layout-health-plan",
        format,
        status,
        "layout health planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        layout_health_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn layout_health_output_fields(
    report: &LayoutHealthReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "layout_health_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "layout_health_status", report.status.as_str());
    append_layout_health_count_fields(&mut fields, report);
    append_layout_health_requirement_fields(&mut fields, report);
    append_layout_health_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_layout_health_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(
        fields,
        "native_vortex_file_count",
        report.native_vortex_file_count,
    );
    push_count_field(
        fields,
        "non_native_data_file_count",
        report.non_native_data_file_count,
    );
    push_count_field(fields, "small_file_count", report.small_file_count);
    push_count_field(fields, "small_segment_count", report.small_segment_count);
    push_count_field(
        fields,
        "missing_statistics_segment_count",
        report.missing_statistics_segment_count,
    );
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(fields, "unique_format_count", report.unique_format_count);
    push_count_field(
        fields,
        "unique_encoding_count",
        report.unique_encoding_count,
    );
    push_count_field(fields, "unique_layout_count", report.unique_layout_count);
    push_count_field(
        fields,
        "compaction_candidate_count",
        report.compaction_candidate_count,
    );
}

fn append_layout_health_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_bool_field(
        fields,
        "requires_statistics_refresh",
        report.requires_statistics_refresh,
    );
    push_bool_field(
        fields,
        "requires_byte_range_index",
        report.requires_byte_range_index,
    );
    push_bool_field(
        fields,
        "requires_layout_review",
        report.requires_layout_review,
    );
    push_bool_field(
        fields,
        "recommends_compaction",
        report.recommends_compaction,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_layout_health_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &LayoutHealthReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(
        fields,
        "compaction_execution_allowed",
        report.compaction_execution_allowed,
    );
}

fn emit_compaction_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match layout_health_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "compaction-plan",
                format,
                "compaction planning failed",
                &error,
            );
        }
    };
    let report = evaluate_compaction_planning(
        manifest,
        LayoutHealthPolicy::default(),
        CompactionPlanningPolicy::default(),
    );
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "compaction-plan",
        format,
        status,
        "compaction planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        compaction_plan_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn compaction_plan_output_fields(
    report: &CompactionPlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "compaction_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "compaction_status", report.status.as_str());
    append_compaction_count_fields(&mut fields, report);
    append_compaction_requirement_fields(&mut fields, report);
    append_compaction_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_compaction_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(fields, "candidate_file_count", report.candidate_file_count);
    push_count_field(
        fields,
        "candidate_segment_count",
        report.candidate_segment_count,
    );
    push_count_field(fields, "candidate_count", report.candidate_count);
    push_count_field(
        fields,
        "blocked_candidate_count",
        report.blocked_candidate_count,
    );
    push_count_field(
        fields,
        "estimated_compaction_group_count",
        report.estimated_compaction_group_count,
    );
    push_count_field(
        fields,
        "missing_statistics_segment_count",
        report.missing_statistics_segment_count,
    );
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(
        fields,
        "non_native_data_file_count",
        report.non_native_data_file_count,
    );
    push_count_field(fields, "action_count", report.actions.len());
}

fn append_compaction_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_bool_field(
        fields,
        "requires_statistics_refresh",
        report.requires_statistics_refresh,
    );
    push_bool_field(
        fields,
        "requires_byte_range_index",
        report.requires_byte_range_index,
    );
    push_bool_field(
        fields,
        "requires_layout_review",
        report.requires_layout_review,
    );
    push_bool_field(
        fields,
        "requires_native_input_review",
        report.requires_native_input_review,
    );
    push_bool_field(
        fields,
        "compaction_recommended",
        report.compaction_recommended,
    );
    push_bool_field(
        fields,
        "recommendation_emitted",
        report.recommendation_emitted,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_compaction_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CompactionPlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(
        fields,
        "compaction_execution_allowed",
        report.compaction_execution_allowed,
    );
}

fn emit_object_store_range_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest = match object_store_range_fixture(scenario) {
        Ok(manifest) => manifest,
        Err(error) => {
            return emit_error(
                "object-store-range-plan",
                format,
                "object-store range planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_ranges(manifest, ObjectStoreRangePlanningPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-range-plan",
        format,
        status,
        "object-store range planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_range_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_range_output_fields(
    report: &ObjectStoreRangePlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_range_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_range_status",
        report.status.as_str(),
    );
    append_object_store_range_count_fields(&mut fields, report);
    append_object_store_range_requirement_fields(&mut fields, report);
    append_object_store_range_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_object_store_range_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_count_field(fields, "file_count", report.file_count);
    push_count_field(fields, "segment_count", report.segment_count);
    push_count_field(
        fields,
        "object_store_file_count",
        report.object_store_file_count,
    );
    push_count_field(
        fields,
        "non_object_store_file_count",
        report.non_object_store_file_count,
    );
    push_count_field(fields, "ranged_segment_count", report.ranged_segment_count);
    push_count_field(
        fields,
        "missing_byte_range_segment_count",
        report.missing_byte_range_segment_count,
    );
    push_count_field(fields, "invalid_range_count", report.invalid_range_count);
    push_count_field(
        fields,
        "oversized_range_count",
        report.oversized_range_count,
    );
    push_count_field(
        fields,
        "planned_request_count",
        report.planned_request_count,
    );
    push_count_field(fields, "planned_range_count", report.planned_range_count);
    push_count_field(
        fields,
        "coalesced_range_count",
        report.coalesced_range_count,
    );
    push_u64_field(
        fields,
        "estimated_request_bytes",
        report.estimated_request_bytes,
    );
}

fn append_object_store_range_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_bool_field(fields, "requires_byte_ranges", report.requires_byte_ranges);
    push_bool_field(
        fields,
        "requires_request_budget_review",
        report.requires_request_budget_review,
    );
    push_bool_field(
        fields,
        "full_file_read_required",
        report.full_file_read_required,
    );
    push_bool_field(
        fields,
        "full_file_read_allowed",
        report.full_file_read_allowed,
    );
    push_bool_field(fields, "can_plan_without_io", report.can_plan_without_io);
}

fn append_object_store_range_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &ObjectStoreRangePlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
}

fn emit_object_store_coalesce_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let manifest =
        match object_store_range_fixture_for_command("object-store-coalesce-plan", scenario) {
            Ok(manifest) => manifest,
            Err(error) => {
                return emit_error(
                    "object-store-coalesce-plan",
                    format,
                    "object-store request coalescing failed",
                    &error,
                );
            }
        };
    let report =
        plan_object_store_request_coalescing(manifest, ObjectStoreRangePlanningPolicy::default());
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-coalesce-plan",
        format,
        status,
        "object-store request coalescing report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_coalesce_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_coalesce_output_fields(
    report: &ObjectStoreRequestCoalescingReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_coalesce_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_coalescing_status",
        report.status.as_str(),
    );
    push_count_field(
        &mut fields,
        "input_request_count",
        report.input_request_count,
    );
    push_count_field(
        &mut fields,
        "output_request_count",
        report.output_request_count,
    );
    push_count_field(
        &mut fields,
        "request_reduction_count",
        report.request_reduction_count,
    );
    push_count_field(&mut fields, "input_range_count", report.input_range_count);
    push_count_field(
        &mut fields,
        "coalesced_range_count",
        report.coalesced_range_count,
    );
    push_count_field(&mut fields, "decision_count", report.decisions.len());
    push_u64_field(
        &mut fields,
        "estimated_request_bytes_before",
        report.estimated_request_bytes_before,
    );
    push_u64_field(
        &mut fields,
        "estimated_request_bytes_after",
        report.estimated_request_bytes_after,
    );
    push_bool_field(&mut fields, "coalescing_applied", report.coalescing_applied);
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn emit_object_store_schedule_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (manifest, range_policy, scheduling_policy) = match object_store_schedule_fixture(scenario)
    {
        Ok(fixture) => fixture,
        Err(error) => {
            return emit_error(
                "object-store-schedule-plan",
                format,
                "object-store scheduling planning failed",
                &error,
            );
        }
    };
    let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
    let report = plan_object_store_distributed_scheduling(coalescing_report, scheduling_policy);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-schedule-plan",
        format,
        status,
        "object-store distributed scheduling report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_schedule_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_schedule_output_fields(
    report: &ObjectStoreDistributedSchedulingReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_schedule_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_schedule_status",
        report.status.as_str(),
    );
    push_count_field(
        &mut fields,
        "max_requests_per_task",
        report.policy.max_requests_per_task,
    );
    push_count_field(&mut fields, "max_task_count", report.policy.max_task_count);
    push_count_field(
        &mut fields,
        "input_request_count",
        report.input_request_count,
    );
    push_count_field(&mut fields, "planned_task_count", report.planned_task_count);
    push_u64_field(
        &mut fields,
        "estimated_request_bytes",
        report.estimated_request_bytes,
    );
    push_bool_field(
        &mut fields,
        "requires_checkpoint_plan",
        report.requires_checkpoint_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_retry_policy",
        report.requires_retry_policy,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_keys",
        report.requires_idempotency_keys,
    );
    push_bool_field(
        &mut fields,
        "scheduler_execution_allowed",
        report.scheduler_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "coordinator_started",
        report.coordinator_started,
    );
    push_bool_field(&mut fields, "worker_started", report.worker_started);
    push_bool_field(
        &mut fields,
        "task_execution_allowed",
        report.task_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_schedule_fixture(
    scenario: &str,
) -> Result<
    (
        DatasetManifest,
        ObjectStoreRangePlanningPolicy,
        ObjectStoreDistributedSchedulingPolicy,
    ),
    ShardLoomError,
> {
    let default_range_policy = ObjectStoreRangePlanningPolicy::default();
    let default_scheduling_policy = ObjectStoreDistributedSchedulingPolicy::default();
    let spaced_range_policy = ObjectStoreRangePlanningPolicy {
        max_coalesce_gap_bytes: 0,
        ..ObjectStoreRangePlanningPolicy::default()
    };
    let spaced_manifest = || {
        object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![
                ByteRange::new(0, 1024),
                ByteRange::new(8192, 1024),
                ByteRange::new(16_384, 1024),
            ],
        )
    };

    match scenario {
        "s3-ranges" => Ok((
            object_store_range_fixture("s3-ranges")?,
            default_range_policy,
            default_scheduling_policy,
        )),
        "multi-task" => Ok((
            spaced_manifest()?,
            spaced_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 4,
            },
        )),
        "missing-ranges" => Ok((
            object_store_range_fixture("missing-ranges")?,
            default_range_policy,
            default_scheduling_policy,
        )),
        "task-budget" => Ok((
            spaced_manifest()?,
            spaced_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 1,
                max_task_count: 2,
            },
        )),
        "invalid-policy" => Ok((
            object_store_range_fixture("s3-ranges")?,
            default_range_policy,
            ObjectStoreDistributedSchedulingPolicy {
                max_requests_per_task: 0,
                max_task_count: 1,
            },
        )),
        value => Err(cli_unknown_arg_error("object-store-schedule-plan", value)),
    }
}

fn emit_object_store_checkpoint_retry_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let input = match object_store_checkpoint_retry_fixture(scenario) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "object-store-checkpoint-retry-plan",
                format,
                "object-store checkpoint/retry planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_checkpoint_retry(input);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-checkpoint-retry-plan",
        format,
        status,
        "object-store checkpoint/retry report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_checkpoint_retry_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_checkpoint_retry_output_fields(
    report: &ObjectStoreCheckpointRetryReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_checkpoint_retry_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_checkpoint_retry_status",
        report.status.as_str(),
    );
    push_count_field(&mut fields, "task_count", report.task_count);
    push_count_field(
        &mut fields,
        "retryable_task_count",
        report.retryable_task_count,
    );
    push_count_field(
        &mut fields,
        "planned_checkpoint_record_count",
        report.planned_checkpoint_record_count,
    );
    push_count_field(
        &mut fields,
        "planned_attempt_record_count",
        report.planned_attempt_record_count,
    );
    push_bool_field(
        &mut fields,
        "requires_retry_policy",
        report.requires_retry_policy,
    );
    push_bool_field(
        &mut fields,
        "requires_checkpoint_plan",
        report.requires_checkpoint_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_keys",
        report.requires_idempotency_keys,
    );
    push_bool_field(
        &mut fields,
        "requires_attempt_records",
        report.requires_attempt_records,
    );
    push_bool_field(
        &mut fields,
        "requires_cleanup_policy",
        report.requires_cleanup_policy,
    );
    push_bool_field(
        &mut fields,
        "retry_execution_allowed",
        report.retry_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "checkpoint_write_allowed",
        report.checkpoint_write_allowed,
    );
    push_bool_field(
        &mut fields,
        "cleanup_execution_allowed",
        report.cleanup_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "coordinator_started",
        report.coordinator_started,
    );
    push_bool_field(&mut fields, "worker_started", report.worker_started);
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_checkpoint_retry_fixture(
    scenario: &str,
) -> Result<ObjectStoreCheckpointRetryInput, ShardLoomError> {
    let scheduling_report = |schedule_scenario: &str| -> Result<
        ObjectStoreDistributedSchedulingReport,
        ShardLoomError,
    > {
        let (manifest, range_policy, scheduling_policy) =
            object_store_schedule_fixture(schedule_scenario)?;
        let coalescing_report = plan_object_store_request_coalescing(manifest, range_policy);
        Ok(plan_object_store_distributed_scheduling(
            coalescing_report,
            scheduling_policy,
        ))
    };
    let ready = || -> Result<ObjectStoreCheckpointRetryInput, ShardLoomError> {
        Ok(
            ObjectStoreCheckpointRetryInput::new(scheduling_report("multi-task")?)
                .with_retry_policy(true)
                .with_checkpoint_plan(true)
                .with_idempotency_keys(true)
                .with_attempt_record(true)
                .with_cleanup_policy(true),
        )
    };

    match scenario {
        "ready" => ready(),
        "missing-retry" => Ok(ready()?.with_retry_policy(false)),
        "missing-checkpoint" => Ok(ready()?.with_checkpoint_plan(false)),
        "missing-idempotency" => Ok(ready()?.with_idempotency_keys(false)),
        "missing-attempt" => Ok(ready()?.with_attempt_record(false)),
        "missing-cleanup" => Ok(ready()?.with_cleanup_policy(false)),
        "blocked-scheduling" => Ok(ObjectStoreCheckpointRetryInput::new(scheduling_report(
            "task-budget",
        )?)
        .with_retry_policy(true)
        .with_checkpoint_plan(true)
        .with_idempotency_keys(true)
        .with_attempt_record(true)
        .with_cleanup_policy(true)),
        value => Err(cli_unknown_arg_error(
            "object-store-checkpoint-retry-plan",
            value,
        )),
    }
}

fn emit_object_store_commit_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let input = match object_store_commit_fixture(scenario) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "object-store-commit-plan",
                format,
                "object-store commit planning failed",
                &error,
            );
        }
    };
    let report = plan_object_store_commit_protocol(input);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "object-store-commit-plan",
        format,
        status,
        "object-store commit protocol report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        object_store_commit_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn object_store_commit_output_fields(
    report: &ObjectStoreCommitProtocolReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "object_store_commit_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(
        &mut fields,
        "object_store_commit_status",
        report.status.as_str(),
    );
    push_bool_field(
        &mut fields,
        "object_store_target",
        report.object_store_target,
    );
    push_bool_field(
        &mut fields,
        "requires_staging_prefix",
        report.requires_staging_prefix,
    );
    push_bool_field(
        &mut fields,
        "requires_manifest_pointer_update",
        report.requires_manifest_pointer_update,
    );
    push_bool_field(
        &mut fields,
        "requires_commit_record",
        report.requires_commit_record,
    );
    push_bool_field(
        &mut fields,
        "requires_idempotency_key",
        report.requires_idempotency_key,
    );
    push_bool_field(
        &mut fields,
        "requires_cleanup_plan",
        report.requires_cleanup_plan,
    );
    push_bool_field(
        &mut fields,
        "requires_atomic_commit_evidence",
        report.requires_atomic_commit_evidence,
    );
    push_bool_field(
        &mut fields,
        "commit_execution_allowed",
        report.commit_execution_allowed,
    );
    push_bool_field(
        &mut fields,
        "can_plan_without_io",
        report.can_plan_without_io,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn object_store_commit_fixture(
    scenario: &str,
) -> Result<ObjectStoreCommitProtocolInput, ShardLoomError> {
    let ready = || -> Result<ObjectStoreCommitProtocolInput, ShardLoomError> {
        Ok(
            ObjectStoreCommitProtocolInput::new(DatasetUri::new("s3://bucket/table/_commit")?)
                .with_staging_prefix(true)
                .with_manifest_pointer_update(true)
                .with_commit_record(true)
                .with_idempotency_key(true)
                .with_cleanup_plan(true)
                .with_provider_atomic_commit(true),
        )
    };
    match scenario {
        "ready" => ready(),
        "missing-staging" => Ok(ready()?.with_staging_prefix(false)),
        "missing-idempotency" => Ok(ready()?.with_idempotency_key(false)),
        "missing-atomicity" => Ok(ready()?.with_provider_atomic_commit(false)),
        "local-file" => Ok(ObjectStoreCommitProtocolInput::new(DatasetUri::new(
            "file://tmp/table/_commit",
        )?)
        .with_staging_prefix(true)
        .with_manifest_pointer_update(true)
        .with_commit_record(true)
        .with_idempotency_key(true)
        .with_cleanup_plan(true)
        .with_provider_atomic_commit(true)),
        value => Err(cli_unknown_arg_error("object-store-commit-plan", value)),
    }
}

fn object_store_range_fixture(scenario: &str) -> Result<DatasetManifest, ShardLoomError> {
    object_store_range_fixture_for_command("object-store-range-plan", scenario)
}

fn object_store_range_fixture_for_command(
    command: &str,
    scenario: &str,
) -> Result<DatasetManifest, ShardLoomError> {
    match scenario {
        "s3-ranges" => object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 1024), ByteRange::new(2048, 1024)],
        ),
        "missing-ranges" => object_store_range_manifest("s3://bucket/table.vortex", vec![]),
        "local-file" => object_store_range_manifest(
            "file://object-store-range/table.vortex",
            vec![ByteRange::new(0, 1024)],
        ),
        "invalid-range" => {
            object_store_range_manifest("s3://bucket/table.vortex", vec![ByteRange::new(0, 0)])
        }
        "oversized-range" => object_store_range_manifest(
            "s3://bucket/table.vortex",
            vec![ByteRange::new(0, 32 * 1024 * 1024)],
        ),
        "empty" => Ok(object_store_range_base_manifest()?),
        value => Err(cli_unknown_arg_error(command, value)),
    }
}

fn object_store_range_base_manifest() -> Result<DatasetManifest, ShardLoomError> {
    Ok(DatasetManifest::new(
        ManifestId::new("object-store-range-manifest")?,
        DatasetRef::from_uri(DatasetUri::new("s3://bucket/table.vortex")?)?,
        SnapshotRef::new(SnapshotId::new("object-store-range-snapshot")?),
    ))
}

fn object_store_range_manifest(
    uri: &str,
    ranges: Vec<ByteRange>,
) -> Result<DatasetManifest, ShardLoomError> {
    let dataset_uri = DatasetUri::new(uri)?;
    let mut manifest = DatasetManifest::new(
        ManifestId::new("object-store-range-manifest")?,
        DatasetRef::from_uri(dataset_uri.clone())?,
        SnapshotRef::new(SnapshotId::new("object-store-range-snapshot")?),
    );
    let file = FileDescriptor::new(
        dataset_uri,
        DatasetFormat::Vortex,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(128 * 1024 * 1024);
    let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
    layout.byte_ranges = ranges;
    layout.physical_size_bytes = Some(8 * 1024 * 1024);
    let segment = EncodedSegment::new(
        SegmentId::new("object-store-range-segment")?,
        ColumnRef::new("value")?,
        LogicalDType::Int64,
        Nullability::Nullable,
        layout,
        SegmentStats::with_row_count(64_000),
    );
    manifest.add_file(file.clone());
    manifest.add_segment(ManifestSegment::new(segment, file));
    Ok(manifest)
}

fn layout_health_fixture(scenario: &str) -> Result<DatasetManifest, ShardLoomError> {
    let mut manifest = layout_health_base_manifest()?;
    match scenario {
        "healthy" => {
            layout_health_add_segment(
                &mut manifest,
                "healthy",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "small-files" => {
            layout_health_add_segment(
                &mut manifest,
                "small",
                1024,
                Some(10),
                Some(512),
                true,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "missing-stats" => {
            layout_health_add_segment(
                &mut manifest,
                "missing-stats",
                64 * 1024 * 1024,
                None,
                Some(8 * 1024 * 1024),
                false,
                DatasetFormat::Vortex,
            )?;
            Ok(manifest)
        }
        "mixed-layout" => {
            layout_health_add_segment(
                &mut manifest,
                "vortex",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Vortex,
            )?;
            layout_health_add_segment(
                &mut manifest,
                "parquet",
                64 * 1024 * 1024,
                Some(64_000),
                Some(8 * 1024 * 1024),
                true,
                DatasetFormat::Parquet,
            )?;
            Ok(manifest)
        }
        "empty" => Ok(manifest),
        value => Err(cli_unknown_arg_error("layout-health-plan", value)),
    }
}

fn layout_health_base_manifest() -> Result<DatasetManifest, ShardLoomError> {
    Ok(DatasetManifest::new(
        ManifestId::new("layout-health-manifest")?,
        DatasetRef::from_uri(DatasetUri::new("file://layout-health/table.vortex")?)?,
        SnapshotRef::new(SnapshotId::new("layout-health-snapshot")?),
    ))
}

#[allow(clippy::too_many_arguments)]
fn layout_health_add_segment(
    manifest: &mut DatasetManifest,
    name: &str,
    file_size_bytes: u64,
    row_count: Option<u64>,
    physical_size_bytes: Option<u64>,
    has_byte_ranges: bool,
    format: DatasetFormat,
) -> Result<(), ShardLoomError> {
    let extension = if format.is_native_vortex() {
        "vortex"
    } else {
        "parquet"
    };
    let file = FileDescriptor::new(
        DatasetUri::new(format!("file://layout-health/{name}.{extension}"))?,
        format,
        FileRole::NativeVortexData,
    )
    .with_size_bytes(file_size_bytes);
    let segment = layout_health_segment(name, row_count, physical_size_bytes, has_byte_ranges)?;
    manifest.add_file(file.clone());
    manifest.add_segment(ManifestSegment::new(segment, file));
    Ok(())
}

fn layout_health_segment(
    name: &str,
    row_count: Option<u64>,
    physical_size_bytes: Option<u64>,
    has_byte_ranges: bool,
) -> Result<EncodedSegment, ShardLoomError> {
    let mut layout = SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat);
    layout.physical_size_bytes = physical_size_bytes;
    if has_byte_ranges {
        layout = layout.with_byte_ranges(vec![ByteRange::new(0, 1024)]);
    }
    let stats = row_count.map_or_else(SegmentStats::unknown, SegmentStats::with_row_count);
    Ok(EncodedSegment::new(
        SegmentId::new(format!("segment-{name}"))?,
        ColumnRef::new("value")?,
        LogicalDType::Int64,
        Nullability::Nullable,
        layout,
        stats,
    ))
}

fn emit_cdc_incremental_plan(format: OutputFormat, scenario: &str) -> ExitCode {
    let (change_set, cdc_events) = match cdc_incremental_fixture(scenario) {
        Ok(parts) => parts,
        Err(error) => {
            return emit_error(
                "incremental-plan",
                format,
                "CDC incremental plan failed",
                &error,
            );
        }
    };
    let report = evaluate_cdc_incremental_planning(change_set, cdc_events);
    let status = if report.has_errors() {
        CommandStatus::Unsupported
    } else {
        CommandStatus::Success
    };
    emit(
        "incremental-plan",
        format,
        status,
        "CDC incremental planning report".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        cdc_incremental_output_fields(&report, scenario),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn cdc_incremental_output_fields(
    report: &CdcIncrementalPlanningReport,
    scenario: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "cdc_incremental_plan");
    push_field(&mut fields, "scenario", scenario);
    push_field(&mut fields, "cdc_status", report.status.as_str());
    append_cdc_incremental_count_fields(&mut fields, report);
    append_cdc_incremental_requirement_fields(&mut fields, report);
    append_cdc_incremental_side_effect_fields(&mut fields, report);
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    fields
}

fn append_cdc_incremental_count_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_count_field(
        fields,
        "changed_segment_count",
        report.changed_segment_count,
    );
    push_count_field(
        fields,
        "metadata_only_segment_count",
        report.metadata_only_segment_count,
    );
    push_count_field(
        fields,
        "unknown_segment_change_count",
        report.unknown_segment_change_count,
    );
    push_count_field(fields, "insert_count", report.insert_count);
    push_count_field(fields, "update_count", report.update_count);
    push_count_field(fields, "delete_count", report.delete_count);
    push_count_field(fields, "tombstone_count", report.tombstone_count);
    push_count_field(fields, "schema_change_count", report.schema_change_count);
    push_count_field(
        fields,
        "partition_change_count",
        report.partition_change_count,
    );
    push_count_field(fields, "metadata_only_count", report.metadata_only_count);
    push_count_field(fields, "unknown_event_count", report.unknown_event_count);
    push_count_field(
        fields,
        "unsupported_change_count",
        report.unsupported_change_count,
    );
}

fn append_cdc_incremental_requirement_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_bool_field(
        fields,
        "requires_snapshot_pair",
        report.requires_snapshot_pair,
    );
    push_bool_field(
        fields,
        "requires_row_identity",
        report.requires_row_identity,
    );
    push_bool_field(
        fields,
        "requires_delete_handling",
        report.requires_delete_handling,
    );
    push_bool_field(
        fields,
        "requires_schema_compatibility",
        report.requires_schema_compatibility,
    );
    push_bool_field(
        fields,
        "requires_partition_compatibility",
        report.requires_partition_compatibility,
    );
    push_bool_field(
        fields,
        "can_reuse_unchanged_segments",
        report.can_reuse_unchanged_segments,
    );
    push_bool_field(
        fields,
        "can_execute_changed_segments_only",
        report.can_execute_changed_segments_only,
    );
    push_bool_field(
        fields,
        "requires_partial_recompute",
        report.requires_partial_recompute,
    );
    push_bool_field(
        fields,
        "requires_full_recompute",
        report.requires_full_recompute,
    );
}

fn append_cdc_incremental_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &CdcIncrementalPlanningReport,
) {
    push_bool_field(fields, "side_effect_free", report.side_effect_free());
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "catalog_io", report.catalog_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
}

fn cdc_incremental_fixture(
    scenario: &str,
) -> Result<(ChangeSet, Vec<CdcEventSummary>), ShardLoomError> {
    match scenario {
        "append-only" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Added,
                SegmentId::new("segment-added")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Insert, 10)],
            ))
        }
        "metadata-only" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-metadata")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::MetadataOnly, 1)],
            ))
        }
        "delete" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Removed,
                SegmentId::new("segment-removed")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Delete, 1)],
            ))
        }
        "upsert" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Replaced,
                SegmentId::new("segment-replaced")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Update, 4)],
            ))
        }
        "schema-change" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-schema")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::SchemaChange, 1)],
            ))
        }
        "partition-change" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::MetadataOnly,
                SegmentId::new("segment-partition")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::PartitionChange, 1)],
            ))
        }
        "missing-from-snapshot" => {
            let mut change_set = ChangeSet::new(SnapshotId::new("snapshot-current")?);
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Added,
                SegmentId::new("segment-added")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Insert, 1)],
            ))
        }
        "unknown" => {
            let mut change_set = cdc_change_set_between()?;
            change_set.add_change(SegmentChange::new(
                SegmentChangeKind::Unknown,
                SegmentId::new("segment-unknown")?,
            ));
            Ok((
                change_set,
                vec![CdcEventSummary::new(CdcEventKind::Unknown, 1)],
            ))
        }
        value => Err(cli_unknown_arg_error("incremental-plan cdc", value)),
    }
}

fn cdc_change_set_between() -> Result<ChangeSet, ShardLoomError> {
    Ok(ChangeSet::between(
        SnapshotId::new("snapshot-previous")?,
        SnapshotId::new("snapshot-current")?,
    ))
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
            let protocol = CliApiJsonProtocolReport::contract_only();
            let mut diagnostics = plan.diagnostics.clone();
            diagnostics.extend(protocol.diagnostics.clone());
            emit(
                "api-compat-plan",
                format,
                protocol.status(),
                "api compatibility and cli json protocol foundation".to_string(),
                format!("{}\n\n{}", plan.to_human_text(), protocol.to_human_text()),
                diagnostics,
                api_protocol_fields(&protocol),
            );
            ExitCode::SUCCESS
        }
        Some("python-wrapper-plan") => {
            let report = PythonWrapperFoundationReport::contract_only();
            emit(
                "python-wrapper-plan",
                format,
                report.status(),
                "python wrapper foundation".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                python_wrapper_fields(&report),
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

        Some("schema-plan") => handle_schema_plan(args, format),
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
        Some("table-compat-plan") => handle_table_compat_plan(args, format),
        Some("capabilities") => {
            let scope = match CapabilityDiscoveryScope::parse(args.next().as_deref()) {
                Ok(scope) => scope,
                Err(error) => {
                    return emit_error(
                        "capabilities",
                        format,
                        "capability discovery failed",
                        &error,
                    );
                }
            };
            if let Some(extra) = args.next() {
                return emit_error(
                    "capabilities",
                    format,
                    "capability discovery failed",
                    &cli_unknown_arg_error("capabilities", &extra),
                );
            }
            if scope.world_class_dimension().is_some() {
                let report = plan_world_class_sufficiency();
                emit_world_class_surface_capability(scope, format, &report);
                return ExitCode::SUCCESS;
            }
            if scope != CapabilityDiscoveryScope::Engine {
                let report = CapabilityCertificationReport::contract_only();
                emit_capability_certification(scope, format, &report);
                return ExitCode::SUCCESS;
            }
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
            let report = PlanPortabilityReport::native_skeleton(&document);
            emit(
                "plan-ir",
                format,
                CommandStatus::Warning,
                "native plan ir skeleton".to_string(),
                format!("{}\n\n{}", document.to_human_text(), report.to_human_text()),
                report.diagnostics.clone(),
                plan_portability_fields(&report, "plan_ir"),
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
            let request = if format_kind == PlanInteropFormat::ShardLoomNative {
                match PlanImportRequest::from_native_serialized(source_label) {
                    Ok(v) => v,
                    Err(error) => {
                        return emit_error("plan-import", format, "invalid import request", &error);
                    }
                }
            } else {
                match PlanImportRequest::not_implemented(format_kind, source_label) {
                    Ok(v) => v,
                    Err(error) => {
                        return emit_error("plan-import", format, "invalid import request", &error);
                    }
                }
            };
            let report = PlanPortabilityReport::for_import_request(&request);
            let mut fields = plan_portability_fields(&report, "plan_import");
            if let Some(document) = &request.imported_document {
                push_field(&mut fields, "imported_plan_id", document.id.as_str());
                push_count_field(
                    &mut fields,
                    "imported_plan_node_count",
                    document.node_count(),
                );
            }
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "plan-import",
                format,
                command_status,
                "plan import".to_string(),
                format!("{}\n\n{}", request.summary(), report.to_human_text()),
                report.diagnostics.clone(),
                fields,
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("plan-export") => {
            let Some(format_raw) = args.next() else {
                eprintln!("usage: shardloom plan-export <format>");
                return ExitCode::from(2);
            };
            let format_kind = parse_plan_interop_format(&format_raw);
            let mut serialized_plan = None;
            let mut serialized_plan_node_count = None;
            let request = if format_kind == PlanInteropFormat::ShardLoomNative {
                let document = match native_plan_export_document() {
                    Ok(document) => document,
                    Err(error) => {
                        return emit_error("plan-export", format, "invalid native plan", &error);
                    }
                };
                serialized_plan_node_count = Some(document.node_count());
                let request = PlanExportRequest::serialized_native(&document);
                serialized_plan.clone_from(&request.serialized_document);
                request
            } else {
                PlanExportRequest::not_implemented(format_kind)
            };
            let report = PlanPortabilityReport::for_export_request(&request);
            let mut fields = plan_portability_fields(&report, "plan_export");
            if let Some(serialized_plan) = &serialized_plan {
                push_field(&mut fields, "serialized_plan", serialized_plan);
            }
            if let Some(node_count) = serialized_plan_node_count {
                push_count_field(&mut fields, "serialized_plan_node_count", node_count);
            }
            let command_status = if report.has_errors() {
                CommandStatus::Unsupported
            } else {
                CommandStatus::Success
            };
            emit(
                "plan-export",
                format,
                command_status,
                "plan export".to_string(),
                format!("{}\n\n{}", request.summary(), report.to_human_text()),
                report.diagnostics.clone(),
                fields,
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
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
                correctness_plan_fields(&plan),
            );
            ExitCode::SUCCESS
        }
        Some("execution-certificate-plan") => {
            let command = "execution-certificate-plan";
            let report = plan_execution_certificate_evidence_surface();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "execution certificate evidence surface".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                execution_certificate_surface_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("kernel-registry") => {
            let snapshot = KernelRegistrySnapshot::empty();
            let physical_plan = PhysicalKernelRegistryPlan::cg7_foundation();
            let execution_profiles = PhysicalOperatorExecutionProfileMatrix::cg7_foundation();
            emit(
                "kernel-registry",
                format,
                CommandStatus::Success,
                "kernel registry snapshot".to_string(),
                format!("{}\n{}", snapshot.summary(), physical_plan.to_human_text()),
                physical_plan.diagnostics.clone(),
                vec![
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("mode".to_string(), "kernel_registry_snapshot".to_string()),
                    (
                        "status".to_string(),
                        "report_only_missing_required_kernels".to_string(),
                    ),
                    (
                        "registered_kernel_count".to_string(),
                        snapshot.kernel_count().to_string(),
                    ),
                    (
                        "physical_kernel_schema_version".to_string(),
                        physical_plan.schema_version.to_string(),
                    ),
                    (
                        "physical_kernel_registry_id".to_string(),
                        physical_plan.registry_id.clone(),
                    ),
                    (
                        "physical_kernel_required_slot_count".to_string(),
                        physical_plan.required_slot_count().to_string(),
                    ),
                    (
                        "physical_kernel_present_slot_count".to_string(),
                        physical_plan.present_slot_count().to_string(),
                    ),
                    (
                        "physical_kernel_missing_slot_count".to_string(),
                        physical_plan.missing_slot_count().to_string(),
                    ),
                    (
                        "physical_kernel_reference_only_rejected_count".to_string(),
                        physical_plan.reference_only_rejected_count().to_string(),
                    ),
                    (
                        "physical_kernel_runtime_execution_allowed".to_string(),
                        physical_plan.runtime_execution_allowed().to_string(),
                    ),
                    (
                        "physical_kernel_fallback_execution_allowed".to_string(),
                        physical_plan.fallback_execution_allowed().to_string(),
                    ),
                    (
                        "physical_operator_native_execution_level_count".to_string(),
                        execution_profiles
                            .native_execution_level_count()
                            .to_string(),
                    ),
                    (
                        "physical_operator_metadata_only_level_count".to_string(),
                        execution_profiles
                            .allowed_level_count(PhysicalOperatorExecutionLevel::MetadataOnly)
                            .to_string(),
                    ),
                    (
                        "physical_operator_encoded_native_level_count".to_string(),
                        execution_profiles
                            .allowed_level_count(PhysicalOperatorExecutionLevel::EncodedNative)
                            .to_string(),
                    ),
                    (
                        "physical_operator_hybrid_native_level_count".to_string(),
                        execution_profiles
                            .allowed_level_count(PhysicalOperatorExecutionLevel::HybridNative)
                            .to_string(),
                    ),
                    (
                        "physical_operator_native_decoded_level_count".to_string(),
                        execution_profiles
                            .allowed_level_count(PhysicalOperatorExecutionLevel::NativeDecoded)
                            .to_string(),
                    ),
                    (
                        "metadata_physical_kernel_schema_version".to_string(),
                        "shardloom.vortex_metadata_physical_kernel.v1".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_supported_primitives".to_string(),
                        "count_all,count_where,filter_predicate".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_requires_correctness_evidence".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_requires_memory_safety_evidence".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_requires_benchmark_for_production".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_physical_kernel_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_metadata_count_kernel_admission.v1".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_operator_kind".to_string(),
                        "count_aggregate".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_required_kernel_kind".to_string(),
                        "metadata".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_requires_metadata_kernel_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_requires_correctness_evidence".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_count_kernel_admission_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_metadata_filter_kernel_admission.v1".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_operator_kind".to_string(),
                        "filter".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_required_kernel_kind".to_string(),
                        "metadata".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_requires_metadata_kernel_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_requires_correctness_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_filter_kernel_admission_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_metadata_projection_kernel_admission.v1".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_operator_kind".to_string(),
                        "project".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_required_kernel_kind".to_string(),
                        "metadata".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_requires_projection_readiness"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_requires_correctness_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "metadata_projection_kernel_admission_fallback_execution_allowed"
                            .to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_encoded_projection_kernel_admission.v1".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_operator_kind".to_string(),
                        "project".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_required_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_requires_projection_readiness"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_requires_encoded_column_path"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_requires_correctness_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_projection_kernel_admission_fallback_execution_allowed"
                            .to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_schema_version".to_string(),
                        "shardloom.vortex_encoded_count_physical_kernel.v1".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_supported_primitive".to_string(),
                        "count_all".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_operator_kind".to_string(),
                        "count_aggregate".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_execution_level".to_string(),
                        "encoded_native".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_requires_execution_certificate".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_count_physical_kernel_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_encoded_count_kernel_admission.v1".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_operator_kind".to_string(),
                        "count_aggregate".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_required_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_requires_physical_kernel_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_requires_correctness_evidence".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_count_kernel_admission_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_schema_version".to_string(),
                        "shardloom.vortex_encoded_predicate_evaluation.v1".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_id".to_string(),
                        "vortex.query-primitive.filter_predicate.encoded-predicate-evaluation"
                            .to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_operator_kind".to_string(),
                        "filter".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_execution_level".to_string(),
                        "encoded_native".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_emits_selection_vectors".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_supports_metadata_proven_all".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_supports_metadata_proven_none".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_defers_inconclusive_to_encoded_values"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_discovery_reads_data".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "encoded_predicate_evaluation_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_schema_version".to_string(),
                        "shardloom.vortex_selection_vector_filter_kernel.v1".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_id".to_string(),
                        "vortex.query-primitive.filter_predicate.selection-vector-filter-kernel"
                            .to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_operator_kind".to_string(),
                        "filter".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_execution_level".to_string(),
                        "encoded_native".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_requires_encoded_predicate_evaluation"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_requires_selection_vectors".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_requires_correctness_evidence".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_discovery_reads_data".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_schema_version".to_string(),
                        "shardloom.vortex_selection_vector_filter_kernel_admission.v1"
                            .to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_contextual_only".to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_operator_kind".to_string(),
                        "filter".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_required_kernel_kind".to_string(),
                        "encoded".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_requires_filter_kernel_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_requires_correctness_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_requires_memory_safety_evidence"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_requires_benchmark_for_production"
                            .to_string(),
                        "true".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_runtime_execution".to_string(),
                        "false".to_string(),
                    ),
                    (
                        "selection_vector_filter_kernel_admission_fallback_execution_allowed"
                            .to_string(),
                        "false".to_string(),
                    ),
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
            let plan = BenchmarkPlan::default_foundation_plan();
            emit(
                "benchmark-plan",
                format,
                CommandStatus::Success,
                "benchmark plan".to_string(),
                plan.to_human_text(),
                vec![],
                benchmark_plan_fields(&plan),
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
        Some("layout-health-plan") => {
            let scenario = args.next().unwrap_or_else(|| "healthy".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "layout-health-plan",
                    format,
                    "layout health planning failed",
                    &cli_unknown_arg_error("layout-health-plan", &extra),
                );
            }
            emit_layout_health_plan(format, &scenario)
        }
        Some("compaction-plan") => {
            let scenario = args.next().unwrap_or_else(|| "healthy".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "compaction-plan",
                    format,
                    "compaction planning failed",
                    &cli_unknown_arg_error("compaction-plan", &extra),
                );
            }
            emit_compaction_plan(format, &scenario)
        }
        Some("object-store-range-plan") => {
            let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "object-store-range-plan",
                    format,
                    "object-store range planning failed",
                    &cli_unknown_arg_error("object-store-range-plan", &extra),
                );
            }
            emit_object_store_range_plan(format, &scenario)
        }
        Some("object-store-coalesce-plan") => {
            let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "object-store-coalesce-plan",
                    format,
                    "object-store request coalescing failed",
                    &cli_unknown_arg_error("object-store-coalesce-plan", &extra),
                );
            }
            emit_object_store_coalesce_plan(format, &scenario)
        }
        Some("object-store-schedule-plan") => {
            let scenario = args.next().unwrap_or_else(|| "s3-ranges".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "object-store-schedule-plan",
                    format,
                    "object-store scheduling planning failed",
                    &cli_unknown_arg_error("object-store-schedule-plan", &extra),
                );
            }
            emit_object_store_schedule_plan(format, &scenario)
        }
        Some("object-store-checkpoint-retry-plan") => {
            let scenario = args.next().unwrap_or_else(|| "ready".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "object-store-checkpoint-retry-plan",
                    format,
                    "object-store checkpoint/retry planning failed",
                    &cli_unknown_arg_error("object-store-checkpoint-retry-plan", &extra),
                );
            }
            emit_object_store_checkpoint_retry_plan(format, &scenario)
        }
        Some("object-store-commit-plan") => {
            let scenario = args.next().unwrap_or_else(|| "ready".to_string());
            if let Some(extra) = args.next() {
                return emit_error(
                    "object-store-commit-plan",
                    format,
                    "object-store commit planning failed",
                    &cli_unknown_arg_error("object-store-commit-plan", &extra),
                );
            }
            emit_object_store_commit_plan(format, &scenario)
        }
        Some("incremental-plan") => {
            let Some(snapshot_id) = args.next() else {
                eprintln!("usage: shardloom incremental-plan <snapshot_id>|cdc <scenario>");
                return ExitCode::from(2);
            };
            if snapshot_id == "cdc" {
                if let Some(scenario) = args.next() {
                    if let Some(extra) = args.next() {
                        return emit_error(
                            "incremental-plan",
                            format,
                            "CDC incremental plan failed",
                            &cli_unknown_arg_error("incremental-plan cdc", &extra),
                        );
                    }
                    return emit_cdc_incremental_plan(format, &scenario);
                }
            } else if let Some(extra) = args.next() {
                return emit_error(
                    "incremental-plan",
                    format,
                    "incremental plan failed",
                    &cli_unknown_arg_error("incremental-plan", &extra),
                );
            }
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
        Some("stateful-reuse-plan") => {
            let command = "stateful-reuse-plan";
            let report = plan_stateful_reuse();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "stateful reuse plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                stateful_reuse_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("universal-harness-plan") => {
            let command = "universal-harness-plan";
            let report = plan_universal_harness();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "universal harness plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                universal_harness_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("native-io-envelope-plan") => {
            let command = "native-io-envelope-plan";
            let report = plan_native_io_envelope();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "native I/O envelope plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                native_io_envelope_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("world-class-sufficiency-plan") => {
            let command = "world-class-sufficiency-plan";
            let report = plan_world_class_sufficiency();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "world-class sufficiency plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                world_class_sufficiency_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
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
        Some("vortex-manifest-finalization-plan") => {
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
        Some("vortex-output-payload-plan") => {
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
            let payload_content = match VortexOutputPayloadContentDescriptor::synthetic_placeholder(
                payload_name.as_str(),
            ) {
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
            let mut request =
                VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
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
        Some("vortex-finalized-manifest-artifact-write") => {
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
            let options = match parse_vortex_finalized_manifest_artifact_write_options(&options_raw)
            {
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
            let mut write_request =
                match finalized_manifest_artifact_write_request_from_plan(&plan_report) {
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
        Some("vortex-output-payload-artifact-write") => {
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
            let allow_overwrite =
                match parse_vortex_output_payload_artifact_write_options(&options_raw) {
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
            let payload_content = match VortexOutputPayloadContentDescriptor::synthetic_placeholder(
                payload_name.as_str(),
            ) {
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
            let mut request =
                VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
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

        Some("vortex-native-count-payload-write") => {
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
            let allow_overwrite =
                match parse_vortex_output_payload_artifact_write_options(&options_raw) {
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
                match VortexOutputPayloadContentDescriptor::native_vortex_count_result(count_result)
                {
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
            let mut request =
                VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
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
            let mut write_request =
                match native_output_payload_write_request_from_plan(&plan, count_result) {
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
        Some("vortex-local-commit-execute") => {
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
        Some("vortex-local-commit-recovery-plan") => {
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
        Some("vortex-local-commit-rollback-execute") => {
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
                streaming_plan_fields(&plan),
            );
            if plan.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("streaming-batch-plan") => {
            let Some(dataset_uri) = args.next() else {
                eprintln!(
                    "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]"
                );
                return ExitCode::from(2);
            };
            let Some(target_uri) = args.next() else {
                eprintln!(
                    "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]"
                );
                return ExitCode::from(2);
            };
            let Some(memory_gb_text) = args.next() else {
                eprintln!(
                    "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]"
                );
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!(
                    "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]"
                );
                return ExitCode::from(2);
            };
            let batch_mib = match args.next() {
                Some(value) => match value.parse::<u64>() {
                    Ok(parsed) if parsed > 0 => Some(parsed),
                    _ => {
                        return emit_error(
                            "streaming-batch-plan",
                            format,
                            "encoded streaming-batch planning failed",
                            &ShardLoomError::InvalidOperation(
                                "batch_mib must be a positive integer".to_string(),
                            ),
                        );
                    }
                },
                None => None,
            };
            if let Some(extra) = args.next() {
                return emit_error(
                    "streaming-batch-plan",
                    format,
                    "encoded streaming-batch planning failed",
                    &cli_unknown_arg_error("streaming-batch-plan", &extra),
                );
            }
            let dataset_uri = match DatasetUri::new(dataset_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "invalid dataset uri",
                        &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
                    );
                }
            };
            let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
                Ok(dataset_ref) => dataset_ref,
                Err(error) => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "failed to create dataset reference",
                        &error,
                    );
                }
            };
            let target_uri = match DatasetUri::new(target_uri) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "invalid target uri",
                        &ShardLoomError::InvalidOperation(format!("invalid target uri: {error}")),
                    );
                }
            };
            let output_target = OutputTarget::from_uri(target_uri);
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(value) if value > 0 => value,
                _ => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "encoded streaming-batch planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be a positive integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(value) if value > 0 => value,
                _ => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "encoded streaming-batch planning failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be a positive integer".to_string(),
                        ),
                    );
                }
            };
            let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb));
            let mut input = match EncodedStreamingBatchPlanInput::for_vortex_to_target(
                dataset_ref,
                output_target,
                memory,
                max_parallelism,
            ) {
                Ok(input) => input,
                Err(error) => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "encoded streaming-batch planning failed",
                        &error,
                    );
                }
            };
            if let Some(batch_mib) = batch_mib {
                input = input.with_estimated_batch_bytes(ByteSize::from_mib(batch_mib));
            }
            let report = match plan_encoded_streaming_batches(input) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "streaming-batch-plan",
                        format,
                        "encoded streaming-batch planning failed",
                        &error,
                    );
                }
            };
            emit(
                "streaming-batch-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "encoded streaming-batch plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                encoded_streaming_batch_plan_fields(&report, memory_gb, batch_mib),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("backpressure-plan") => {
            let Some(memory_gb_text) = args.next() else {
                eprintln!(
                    "usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]"
                );
                return ExitCode::from(2);
            };
            let Some(max_parallelism_text) = args.next() else {
                eprintln!(
                    "usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]"
                );
                return ExitCode::from(2);
            };
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(value) => value,
                Err(_) => {
                    return emit_error(
                        "backpressure-plan",
                        format,
                        "backpressure planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let max_parallelism: usize = match max_parallelism_text.parse() {
                Ok(value) => value,
                Err(_) => {
                    return emit_error(
                        "backpressure-plan",
                        format,
                        "backpressure planning failed",
                        &ShardLoomError::InvalidOperation(
                            "max_parallelism must be an unsigned integer".to_string(),
                        ),
                    );
                }
            };
            let chunk_mib = match args.next() {
                Some(value) => match value.parse::<u64>() {
                    Ok(parsed) => Some(parsed),
                    Err(_) => {
                        return emit_error(
                            "backpressure-plan",
                            format,
                            "backpressure planning failed",
                            &ShardLoomError::InvalidOperation(
                                "chunk_mib must be an unsigned integer".to_string(),
                            ),
                        );
                    }
                },
                None => None,
            };
            let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb));
            let mut input = match BackpressurePlanInput::new(memory, max_parallelism) {
                Ok(input) => input,
                Err(error) => {
                    return emit_error(
                        "backpressure-plan",
                        format,
                        "backpressure planning failed",
                        &error,
                    );
                }
            };
            if let Some(chunk_mib) = chunk_mib {
                input = input.with_estimated_chunk_bytes(ByteSize::from_mib(chunk_mib));
            }
            let report = match plan_backpressure(input) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "backpressure-plan",
                        format,
                        "backpressure planning failed",
                        &error,
                    );
                }
            };
            emit(
                "backpressure-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "backpressure plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                backpressure_plan_fields(&report),
            );
            if report.has_errors() {
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
        Some("sizing-feedback-plan") => {
            let Some(memory_gb_text) = args.next() else {
                eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
                return ExitCode::from(2);
            };
            let Some(signals_raw) = args.next() else {
                eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
                return ExitCode::from(2);
            };
            if let Some(extra) = args.next() {
                return emit_error(
                    "sizing-feedback-plan",
                    format,
                    "dynamic sizing feedback planning failed",
                    &cli_unknown_arg_error("sizing-feedback-plan", &extra),
                );
            }
            let memory_gb: u64 = match memory_gb_text.parse() {
                Ok(value) if value > 0 => value,
                _ => {
                    return emit_error(
                        "sizing-feedback-plan",
                        format,
                        "dynamic sizing feedback planning failed",
                        &ShardLoomError::InvalidOperation(
                            "memory_gb must be a positive integer".to_string(),
                        ),
                    );
                }
            };
            let signals = match parse_sizing_feedback_signals(&signals_raw) {
                Ok(signals) => signals,
                Err(error) => {
                    return emit_error(
                        "sizing-feedback-plan",
                        format,
                        "dynamic sizing feedback planning failed",
                        &error,
                    );
                }
            };
            let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::memory_limited(
                ByteSize::from_gib(memory_gb),
            ));
            for signal in signals {
                input.add_signal(signal);
            }
            let report = plan_dynamic_sizing_feedback(input);
            emit(
                "sizing-feedback-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "dynamic sizing feedback plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                dynamic_sizing_feedback_fields(&report, memory_gb, &signals_raw),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
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
                adaptive_sizing_report_fields(
                    &report,
                    memory_gb,
                    input_plan.source.is_native_vortex(),
                ),
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
                memory_bridge_report_fields(
                    &report,
                    memory_gb,
                    input_plan.source.is_native_vortex(),
                ),
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
                scheduler_bridge_report_fields(&report, memory_gb, max_parallelism),
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

        Some("vortex-encoded-path-selection-plan") => {
            let command = "vortex-encoded-path-selection-plan";
            let report = plan_vortex_encoded_execution_path_selection();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex encoded path selection plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vortex_encoded_path_selection_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-generalized-encoded-primitive-gate") => {
            let command = "vortex-generalized-encoded-primitive-gate";
            let report = plan_vortex_generalized_encoded_primitive_gate();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex generalized encoded primitive gate".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vortex_generalized_encoded_primitive_gate_fields(&report),
            );
            if report.has_errors() {
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
        Some("vortex-encoded-read-boundary") => {
            let command = "vortex-encoded-read-boundary";
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <target_uri> <signals>");
                return ExitCode::from(2);
            };
            let Some(signals_raw) = args.next() else {
                eprintln!("usage: shardloom {command} <target_uri> <signals>");
                return ExitCode::from(2);
            };
            let target_uri = match DatasetUri::new(target_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read boundary failed",
                        &error,
                    );
                }
            };
            let signals = match parse_vortex_encoded_read_boundary_signals(&signals_raw) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read boundary failed",
                        &error,
                    );
                }
            };
            let mut request = VortexEncodedReadBoundaryRequest::new(target_uri);
            for signal in signals {
                request.add_signal(signal);
            }
            let report = match plan_vortex_encoded_read_boundary(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read boundary failed",
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
                "vortex encoded-read boundary report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vortex_encoded_read_boundary_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-encoded-read-metadata-probe") => {
            let command = "vortex-encoded-read-metadata-probe";
            let Some(target_uri) = args.next() else {
                eprintln!("usage: shardloom {command} <target_uri> <fixture_ref> <signals>");
                return ExitCode::from(2);
            };
            let Some(fixture_ref_raw) = args.next() else {
                return emit_error(
                    command,
                    format,
                    "vortex encoded read metadata probe failed",
                    &cli_missing_arg_error(command, "fixture_ref"),
                );
            };
            let Some(signals_raw) = args.next() else {
                eprintln!("usage: shardloom {command} <target_uri> <fixture_ref> <signals>");
                return ExitCode::from(2);
            };
            let target_uri = match DatasetUri::new(target_uri) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read metadata probe failed",
                        &error,
                    );
                }
            };
            let fixture_ref = match VortexEncodedReadFixtureRef::new(fixture_ref_raw) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read metadata probe failed",
                        &error,
                    );
                }
            };
            let signals = match parse_vortex_encoded_read_metadata_probe_signals(&signals_raw) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read metadata probe failed",
                        &error,
                    );
                }
            };
            let mut request = VortexEncodedReadMetadataProbeRequest::new(target_uri, fixture_ref)
                .fixture_ref_provided(true);
            for signal in signals {
                request.add_signal(signal);
            }
            let report = match probe_vortex_encoded_read_metadata(request) {
                Ok(v) => v,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "vortex encoded-read metadata probe failed",
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
                "vortex encoded-read metadata probe report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vortex_encoded_read_metadata_probe_fields(&report),
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
        Some("vortex-query-primitive-plan") => {
            let Some(primitive_arg) = args.next() else {
                return emit_error(
                    "vortex-query-primitive-plan",
                    format,
                    "missing primitive",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <primitive>".to_string(),
                    ),
                );
            };
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-query-primitive-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let primitive = match primitive_arg.as_str() {
                "count" => shardloom_vortex::VortexQueryPrimitiveBoundaryKind::Count,
                "filtered-count" | "filtered_count" => {
                    shardloom_vortex::VortexQueryPrimitiveBoundaryKind::FilteredCount
                }
                "projection" => shardloom_vortex::VortexQueryPrimitiveBoundaryKind::Projection,
                "predicate-filter" | "predicate_filter" => {
                    shardloom_vortex::VortexQueryPrimitiveBoundaryKind::PredicateFilter
                }
                _ => {
                    return emit_error(
                        "vortex-query-primitive-plan",
                        format,
                        "invalid primitive",
                        &ShardLoomError::InvalidOperation(format!(
                            "invalid primitive: {primitive_arg}"
                        )),
                    );
                }
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-query-primitive-plan",
                        format,
                        "invalid dataset uri",
                        &error,
                    );
                }
            };
            let mut request =
                shardloom_vortex::VortexQueryPrimitiveBoundaryRequest::new(uri, primitive);
            for token in args {
                match token.as_str() {
                    "--feature-gate" => {
                        request.add_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled);
                    }
                    "--metadata-footer-ready" => {
                        request.add_signal(VortexQueryPrimitiveSignal::MetadataFooterReady);
                    }
                    "--encoded-data-path-ready" => {
                        request.add_signal(VortexQueryPrimitiveSignal::EncodedDataPathReady);
                    }
                    "--predicate-provided" => {
                        request.add_signal(VortexQueryPrimitiveSignal::PredicateProvided);
                    }
                    "--projection-provided" => {
                        request.add_signal(VortexQueryPrimitiveSignal::ProjectionProvided);
                    }
                    "--predicate-unsupported" => {
                        request.add_signal(VortexQueryPrimitiveSignal::PredicateUnsupported);
                    }
                    "--object-store-target" => {
                        request.add_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget);
                    }
                    "--decode-risk" => request.add_signal(VortexQueryPrimitiveSignal::DecodeRisk),
                    "--materialization-risk" => {
                        request.add_signal(VortexQueryPrimitiveSignal::MaterializationRisk);
                    }
                    "--arrow-default-risk" => {
                        request.add_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk);
                    }
                    "--write-risk" => request.add_signal(VortexQueryPrimitiveSignal::WriteRisk),
                    "--scan-execution-risk" => {
                        request.add_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk);
                    }
                    "--fallback-policy-blocked" => {
                        request.add_signal(VortexQueryPrimitiveSignal::FallbackPolicyBlocked);
                    }
                    "--format" => {}
                    _ => {
                        return emit_error(
                            "vortex-query-primitive-plan",
                            format,
                            "unknown option",
                            &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                        );
                    }
                }
            }
            let report = match plan_vortex_query_primitive(request) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-query-primitive-plan",
                        format,
                        "query primitive planning failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-query-primitive-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex query primitive planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "primitive".to_string(),
                        report.request.primitive.as_str().to_string(),
                    ),
                    ("query_executed".to_string(), "false".to_string()),
                    (
                        "primitive_ready".to_string(),
                        report.status.primitive_ready().to_string(),
                    ),
                    (
                        "feature_gate_enabled".to_string(),
                        report
                            .request
                            .signals
                            .contains(&VortexQueryPrimitiveSignal::FeatureGateEnabled)
                            .to_string(),
                    ),
                    (
                        "metadata_footer_ready".to_string(),
                        report
                            .request
                            .signals
                            .contains(&VortexQueryPrimitiveSignal::MetadataFooterReady)
                            .to_string(),
                    ),
                    (
                        "encoded_data_path_ready".to_string(),
                        report
                            .request
                            .signals
                            .contains(&VortexQueryPrimitiveSignal::EncodedDataPathReady)
                            .to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("encoded_data_read".to_string(), "false".to_string()),
                    ("row_read".to_string(), "false".to_string()),
                    ("array_decoded".to_string(), "false".to_string()),
                    ("values_materialized".to_string(), "false".to_string()),
                    ("arrow_converted".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("data_written".to_string(), "false".to_string()),
                    ("upstream_scan_called".to_string(), "false".to_string()),
                    ("status".to_string(), report.status.as_str().to_string()),
                    ("mode".to_string(), report.mode.as_str().to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-metadata-physical-kernel-plan") => {
            run_vortex_metadata_physical_kernel_plan(format, args.collect())
        }
        Some("vortex-count-readiness-plan") => {
            let Some(source_arg) = args.next() else {
                return emit_error(
                    "vortex-count-readiness-plan",
                    format,
                    "missing candidate source",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <candidate_source>".to_string(),
                    ),
                );
            };
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-count-readiness-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let candidate_source = match source_arg.as_str() {
                "metadata-footer" | "metadata_footer" => VortexCountCandidateSource::MetadataFooter,
                "encoded-data-path" | "encoded_data_path" => {
                    VortexCountCandidateSource::EncodedDataPath
                }
                "unknown" => VortexCountCandidateSource::Unknown,
                _ => {
                    return emit_error(
                        "vortex-count-readiness-plan",
                        format,
                        "invalid candidate source",
                        &ShardLoomError::InvalidOperation(format!(
                            "invalid candidate source: {source_arg}"
                        )),
                    );
                }
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-count-readiness-plan",
                        format,
                        "invalid dataset uri",
                        &error,
                    );
                }
            };
            let mut request =
                shardloom_vortex::VortexCountReadinessRequest::new(uri, candidate_source);
            for token in args {
                match token.as_str() {
                    "--feature-gate" => {
                        request.add_signal(VortexCountReadinessSignal::FeatureGateEnabled);
                    }
                    "--query-primitive-ready" => {
                        request.add_signal(VortexCountReadinessSignal::QueryPrimitiveReady);
                    }
                    "--metadata-footer-ready" => {
                        request.add_signal(VortexCountReadinessSignal::MetadataFooterReady);
                    }
                    "--encoded-data-path-ready" => {
                        request.add_signal(VortexCountReadinessSignal::EncodedDataPathReady);
                    }
                    "--count-primitive" => {
                        request.add_signal(VortexCountReadinessSignal::CountPrimitive);
                    }
                    "--filtered-count-requested" => {
                        request.add_signal(VortexCountReadinessSignal::FilteredCountRequested);
                    }
                    "--predicate-provided" => {
                        request.add_signal(VortexCountReadinessSignal::PredicateProvided);
                    }
                    "--object-store-target" => {
                        request.add_signal(VortexCountReadinessSignal::ObjectStoreTarget);
                    }
                    "--decode-risk" => request.add_signal(VortexCountReadinessSignal::DecodeRisk),
                    "--materialization-risk" => {
                        request.add_signal(VortexCountReadinessSignal::MaterializationRisk);
                    }
                    "--arrow-default-risk" => {
                        request.add_signal(VortexCountReadinessSignal::ArrowDefaultRisk);
                    }
                    "--write-risk" => request.add_signal(VortexCountReadinessSignal::WriteRisk),
                    "--scan-execution-risk" => {
                        request.add_signal(VortexCountReadinessSignal::ScanExecutionRisk);
                    }
                    "--fallback-policy-blocked" => {
                        request.add_signal(VortexCountReadinessSignal::FallbackPolicyBlocked);
                    }
                    _ => {
                        return emit_error(
                            "vortex-count-readiness-plan",
                            format,
                            "unknown option",
                            &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                        );
                    }
                }
            }
            let report = match plan_vortex_count_readiness(request) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-count-readiness-plan",
                        format,
                        "count readiness planning failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-count-readiness-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex count readiness planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "candidate_source".to_string(),
                        report.request.candidate_source.as_str().to_string(),
                    ),
                    ("status".to_string(), report.status.as_str().to_string()),
                    ("mode".to_string(), report.mode.as_str().to_string()),
                    (
                        "count_ready".to_string(),
                        report.status.count_ready().to_string(),
                    ),
                    ("count_executed".to_string(), "false".to_string()),
                    (
                        "feature_gate_enabled".to_string(),
                        report
                            .request
                            .has_signal(VortexCountReadinessSignal::FeatureGateEnabled)
                            .to_string(),
                    ),
                    (
                        "query_primitive_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexCountReadinessSignal::QueryPrimitiveReady)
                            .to_string(),
                    ),
                    (
                        "metadata_footer_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexCountReadinessSignal::MetadataFooterReady)
                            .to_string(),
                    ),
                    (
                        "encoded_data_path_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexCountReadinessSignal::EncodedDataPathReady)
                            .to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("metadata_read".to_string(), "false".to_string()),
                    ("encoded_data_read".to_string(), "false".to_string()),
                    ("row_read".to_string(), "false".to_string()),
                    ("array_decoded".to_string(), "false".to_string()),
                    ("values_materialized".to_string(), "false".to_string()),
                    ("arrow_converted".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("data_written".to_string(), "false".to_string()),
                    ("upstream_scan_called".to_string(), "false".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-encoded-count-approval-plan") => {
            let command = "vortex-encoded-count-approval-plan";
            let Some(source_arg) = args.next() else {
                return emit_error(
                    command,
                    format,
                    "missing candidate source",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <candidate_source>".to_string(),
                    ),
                );
            };
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    command,
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let candidate_source = match source_arg.as_str() {
                "metadata-footer" | "metadata_footer" => VortexCountCandidateSource::MetadataFooter,
                "encoded-data-path" | "encoded_data_path" => {
                    VortexCountCandidateSource::EncodedDataPath
                }
                "unknown" => VortexCountCandidateSource::Unknown,
                _ => {
                    return emit_error(
                        command,
                        format,
                        "invalid candidate source",
                        &ShardLoomError::InvalidOperation(format!(
                            "invalid candidate source: {source_arg}"
                        )),
                    );
                }
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(command, format, "invalid dataset uri", &error);
                }
            };
            let mut request =
                shardloom_vortex::VortexCountReadinessRequest::new(uri, candidate_source);
            let mut layout_row_count_approved = false;
            for token in args {
                match token.as_str() {
                    "--feature-gate" => {
                        request.add_signal(VortexCountReadinessSignal::FeatureGateEnabled);
                    }
                    "--query-primitive-ready" => {
                        request.add_signal(VortexCountReadinessSignal::QueryPrimitiveReady);
                    }
                    "--metadata-footer-ready" => {
                        request.add_signal(VortexCountReadinessSignal::MetadataFooterReady);
                    }
                    "--encoded-data-path-ready" => {
                        request.add_signal(VortexCountReadinessSignal::EncodedDataPathReady);
                    }
                    "--count-primitive" => {
                        request.add_signal(VortexCountReadinessSignal::CountPrimitive);
                    }
                    "--filtered-count-requested" => {
                        request.add_signal(VortexCountReadinessSignal::FilteredCountRequested);
                    }
                    "--predicate-provided" => {
                        request.add_signal(VortexCountReadinessSignal::PredicateProvided);
                    }
                    "--object-store-target" => {
                        request.add_signal(VortexCountReadinessSignal::ObjectStoreTarget);
                    }
                    "--decode-risk" => request.add_signal(VortexCountReadinessSignal::DecodeRisk),
                    "--materialization-risk" => {
                        request.add_signal(VortexCountReadinessSignal::MaterializationRisk);
                    }
                    "--arrow-default-risk" => {
                        request.add_signal(VortexCountReadinessSignal::ArrowDefaultRisk);
                    }
                    "--write-risk" => request.add_signal(VortexCountReadinessSignal::WriteRisk),
                    "--scan-execution-risk" => {
                        request.add_signal(VortexCountReadinessSignal::ScanExecutionRisk);
                    }
                    "--fallback-policy-blocked" => {
                        request.add_signal(VortexCountReadinessSignal::FallbackPolicyBlocked);
                    }
                    "--layout-row-count-approved" => {
                        layout_row_count_approved = true;
                    }
                    _ => {
                        return emit_error(
                            command,
                            format,
                            "unknown option",
                            &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                        );
                    }
                }
            }
            let count_report = match plan_vortex_count_readiness(request) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(command, format, "count readiness planning failed", &error);
                }
            };
            let api_boundary = vortex_encoded_read_public_api_boundary();
            let report = if layout_row_count_approved {
                let layout_report = match plan_vortex_layout_reader_driver_approval(
                    VortexLayoutReaderDriverApprovalInput::new(api_boundary.clone())
                        .local_fixture_only(true)
                        .caller_session_allowed(true)
                        .runtime_driver_start_allowed(true)
                        .layout_row_count_only_intent(true)
                        .scan_forbidden(true)
                        .evaluation_forbidden(true)
                        .data_read_forbidden(true)
                        .decode_forbidden(true)
                        .materialization_forbidden(true)
                        .arrow_forbidden(true)
                        .object_store_forbidden(true)
                        .write_forbidden(true)
                        .fallback_forbidden(true),
                ) {
                    Ok(report) => report,
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            "layout driver approval planning failed",
                            &error,
                        );
                    }
                };
                plan_vortex_encoded_count_data_path_approval_with_layout_driver(
                    count_report,
                    api_boundary,
                    layout_report,
                )
            } else {
                plan_vortex_encoded_count_data_path_approval(count_report, api_boundary)
            };
            let report = match report {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "encoded count approval planning failed",
                        &error,
                    );
                }
            };
            let local_execution_report = if report.approved() {
                match execute_vortex_count_all_from_encoded_count_data_path_approval(&report) {
                    Ok(local) => Some(local),
                    Err(error) => {
                        return emit_error(
                            command,
                            format,
                            "encoded count local guard planning failed",
                            &error,
                        );
                    }
                }
            } else {
                None
            };
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex encoded count approval planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "candidate_source".to_string(),
                        report
                            .input
                            .count_readiness_report
                            .request
                            .candidate_source
                            .as_str()
                            .to_string(),
                    ),
                    ("status".to_string(), report.status.as_str().to_string()),
                    ("mode".to_string(), report.mode.as_str().to_string()),
                    ("approved".to_string(), report.approved().to_string()),
                    (
                        "metadata_count_surface_ready".to_string(),
                        report.metadata_count_surface_ready.to_string(),
                    ),
                    (
                        "execution_usable_data_path_count".to_string(),
                        report.execution_usable_data_path_count.to_string(),
                    ),
                    (
                        "layout_driver_approval_status".to_string(),
                        report
                            .layout_driver_approval_status
                            .clone()
                            .unwrap_or_else(|| "absent".to_string()),
                    ),
                    (
                        "layout_row_count_path_approved".to_string(),
                        report.layout_row_count_path_approved.to_string(),
                    ),
                    (
                        "api_boundary_blocker_count".to_string(),
                        report.api_boundary_blockers.len().to_string(),
                    ),
                    (
                        "encoded_data_path_ready".to_string(),
                        report
                            .input
                            .count_readiness_report
                            .encoded_data_path_ready()
                            .to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        report.fallback_execution_allowed.to_string(),
                    ),
                    (
                        "count_executed".to_string(),
                        report.count_executed.to_string(),
                    ),
                    (
                        "encoded_data_read".to_string(),
                        report.encoded_data_read.to_string(),
                    ),
                    ("row_read".to_string(), report.row_read.to_string()),
                    (
                        "array_decoded".to_string(),
                        report.array_decoded.to_string(),
                    ),
                    (
                        "values_materialized".to_string(),
                        report.values_materialized.to_string(),
                    ),
                    (
                        "arrow_converted".to_string(),
                        report.arrow_converted.to_string(),
                    ),
                    (
                        "object_store_io".to_string(),
                        report.object_store_io.to_string(),
                    ),
                    ("data_written".to_string(), report.data_written.to_string()),
                    (
                        "upstream_scan_called".to_string(),
                        report.upstream_scan_called.to_string(),
                    ),
                    (
                        "local_execution_status".to_string(),
                        local_execution_report.as_ref().map_or_else(
                            || "not_planned".to_string(),
                            |local| local.status.as_str().to_string(),
                        ),
                    ),
                    (
                        "local_execution_result_known".to_string(),
                        local_execution_report
                            .as_ref()
                            .is_some_and(|local| local.value.is_known())
                            .to_string(),
                    ),
                    (
                        "local_execution_data_read".to_string(),
                        local_execution_report
                            .as_ref()
                            .is_some_and(|local| local.data_read)
                            .to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-layout-driver-approval-plan") => {
            let command = "vortex-layout-driver-approval-plan";
            let Some(signals_raw) = args.next() else {
                return emit_error(
                    command,
                    format,
                    "missing signals",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <signals>".to_string(),
                    ),
                );
            };
            if let Some(extra) = args.next() {
                return emit_error(
                    command,
                    format,
                    "unknown option",
                    &ShardLoomError::InvalidOperation(format!("unknown option: {extra}")),
                );
            }
            let signals = match parse_vortex_layout_driver_approval_signals(&signals_raw) {
                Ok(signals) => signals,
                Err(error) => {
                    return emit_error(command, format, "invalid signals", &error);
                }
            };
            let mut input = VortexLayoutReaderDriverApprovalInput::new(
                vortex_encoded_read_public_api_boundary(),
            );
            for signal in signals {
                input.add_signal(signal);
            }
            let report = match plan_vortex_layout_reader_driver_approval(input) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        command,
                        format,
                        "layout driver approval planning failed",
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
                "vortex layout driver approval planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    ("status".to_string(), report.status.as_str().to_string()),
                    ("mode".to_string(), report.mode.as_str().to_string()),
                    ("approved".to_string(), report.approved().to_string()),
                    (
                        "layout_reader_surface_present".to_string(),
                        report.layout_reader_surface_present.to_string(),
                    ),
                    (
                        "layout_row_count_surface_present".to_string(),
                        report.layout_row_count_surface_present.to_string(),
                    ),
                    (
                        "runtime_driver_risk_present".to_string(),
                        report.runtime_driver_risk_present.to_string(),
                    ),
                    (
                        "layout_reader_constructed".to_string(),
                        report.layout_reader_constructed.to_string(),
                    ),
                    (
                        "runtime_driver_started".to_string(),
                        report.runtime_driver_started.to_string(),
                    ),
                    ("scan_called".to_string(), report.scan_called.to_string()),
                    (
                        "evaluation_called".to_string(),
                        report.evaluation_called.to_string(),
                    ),
                    ("data_read".to_string(), report.data_read.to_string()),
                    ("row_read".to_string(), report.row_read.to_string()),
                    ("data_decoded".to_string(), report.data_decoded.to_string()),
                    (
                        "data_materialized".to_string(),
                        report.data_materialized.to_string(),
                    ),
                    (
                        "arrow_converted".to_string(),
                        report.arrow_converted.to_string(),
                    ),
                    (
                        "object_store_io".to_string(),
                        report.object_store_io.to_string(),
                    ),
                    ("write_io".to_string(), report.write_io.to_string()),
                    (
                        "fallback_execution_allowed".to_string(),
                        report.fallback_execution_allowed.to_string(),
                    ),
                    (
                        "side_effect_free".to_string(),
                        report.is_side_effect_free().to_string(),
                    ),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("vortex-filtered-count-readiness-plan") => {
            let Some(source_arg) = args.next() else {
                return emit_error(
                    "vortex-filtered-count-readiness-plan",
                    format,
                    "missing candidate source",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <candidate_source>".to_string(),
                    ),
                );
            };
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-filtered-count-readiness-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let candidate_source = match source_arg.as_str() {
                "metadata-predicate-proof" | "metadata_predicate_proof" => {
                    VortexFilteredCountCandidateSource::MetadataPredicateProof
                }
                "encoded-predicate-path" | "encoded_predicate_path" => {
                    VortexFilteredCountCandidateSource::EncodedPredicatePath
                }
                "unknown" => VortexFilteredCountCandidateSource::Unknown,
                _ => {
                    return emit_error(
                        "vortex-filtered-count-readiness-plan",
                        format,
                        "invalid candidate source",
                        &ShardLoomError::InvalidOperation(format!(
                            "invalid candidate source: {source_arg}"
                        )),
                    );
                }
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-filtered-count-readiness-plan",
                        format,
                        "invalid dataset uri",
                        &error,
                    );
                }
            };
            let mut request =
                shardloom_vortex::VortexFilteredCountReadinessRequest::new(uri, candidate_source);
            for token in args {
                match token.as_str() {
                    "--feature-gate" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::FeatureGateEnabled);
                    }
                    "--query-primitive-ready" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::QueryPrimitiveReady);
                    }
                    "--metadata-footer-ready" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::MetadataFooterReady);
                    }
                    "--encoded-data-path-ready" => {
                        request
                            .add_signal(VortexFilteredCountReadinessSignal::EncodedDataPathReady);
                    }
                    "--filtered-count-primitive" => request
                        .add_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive),
                    "--predicate-provided" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::PredicateProvided);
                    }
                    "--predicate-metadata-proof-ready" => request.add_signal(
                        VortexFilteredCountReadinessSignal::PredicateMetadataProofReady,
                    ),
                    "--predicate-unsupported" => {
                        request
                            .add_signal(VortexFilteredCountReadinessSignal::PredicateUnsupported);
                    }
                    "--object-store-target" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::ObjectStoreTarget);
                    }
                    "--decode-risk" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::DecodeRisk);
                    }
                    "--materialization-risk" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::MaterializationRisk);
                    }
                    "--arrow-default-risk" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::ArrowDefaultRisk);
                    }
                    "--write-risk" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::WriteRisk);
                    }
                    "--scan-execution-risk" => {
                        request.add_signal(VortexFilteredCountReadinessSignal::ScanExecutionRisk);
                    }
                    "--fallback-policy-blocked" => request
                        .add_signal(VortexFilteredCountReadinessSignal::FallbackPolicyBlocked),
                    _ => {
                        return emit_error(
                            "vortex-filtered-count-readiness-plan",
                            format,
                            "unknown option",
                            &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                        );
                    }
                }
            }
            let report = match plan_vortex_filtered_count_readiness(request) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-filtered-count-readiness-plan",
                        format,
                        "filtered count readiness planning failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-filtered-count-readiness-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex filtered count readiness planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vec![
                    (
                        "candidate_source".to_string(),
                        report.request.candidate_source.as_str().to_string(),
                    ),
                    ("status".to_string(), report.status.as_str().to_string()),
                    ("mode".to_string(), report.mode.as_str().to_string()),
                    (
                        "filtered_count_ready".to_string(),
                        report.status.filtered_count_ready().to_string(),
                    ),
                    ("filtered_count_executed".to_string(), "false".to_string()),
                    ("predicate_evaluated".to_string(), "false".to_string()),
                    (
                        "feature_gate_enabled".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::FeatureGateEnabled)
                            .to_string(),
                    ),
                    (
                        "query_primitive_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::QueryPrimitiveReady)
                            .to_string(),
                    ),
                    (
                        "metadata_footer_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::MetadataFooterReady)
                            .to_string(),
                    ),
                    (
                        "encoded_data_path_ready".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::EncodedDataPathReady)
                            .to_string(),
                    ),
                    (
                        "filtered_count_primitive".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive)
                            .to_string(),
                    ),
                    (
                        "predicate_provided".to_string(),
                        report
                            .request
                            .has_signal(VortexFilteredCountReadinessSignal::PredicateProvided)
                            .to_string(),
                    ),
                    (
                        "predicate_metadata_proof_ready".to_string(),
                        report
                            .request
                            .has_signal(
                                VortexFilteredCountReadinessSignal::PredicateMetadataProofReady,
                            )
                            .to_string(),
                    ),
                    (
                        "fallback_execution_allowed".to_string(),
                        "false".to_string(),
                    ),
                    ("metadata_read".to_string(), "false".to_string()),
                    ("encoded_data_read".to_string(), "false".to_string()),
                    ("row_read".to_string(), "false".to_string()),
                    ("array_decoded".to_string(), "false".to_string()),
                    ("values_materialized".to_string(), "false".to_string()),
                    ("arrow_converted".to_string(), "false".to_string()),
                    ("object_store_io".to_string(), "false".to_string()),
                    ("data_written".to_string(), "false".to_string()),
                    ("upstream_scan_called".to_string(), "false".to_string()),
                ],
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-projection-readiness-plan") => {
            let Some(source_arg) = args.next() else {
                return emit_error(
                    "vortex-projection-readiness-plan",
                    format,
                    "missing candidate source",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <candidate_source>".to_string(),
                    ),
                );
            };
            let Some(uri_arg) = args.next() else {
                return emit_error(
                    "vortex-projection-readiness-plan",
                    format,
                    "missing dataset uri",
                    &ShardLoomError::InvalidOperation(
                        "missing required argument: <dataset_uri>".to_string(),
                    ),
                );
            };
            let candidate_source = match source_arg.as_str() {
                "metadata-schema-projection" | "metadata_schema_projection" => {
                    VortexProjectionCandidateSource::MetadataSchemaProjection
                }
                "encoded-column-path" | "encoded_column_path" => {
                    VortexProjectionCandidateSource::EncodedColumnPath
                }
                "unknown" => VortexProjectionCandidateSource::Unknown,
                _ => {
                    return emit_error(
                        "vortex-projection-readiness-plan",
                        format,
                        "invalid candidate source",
                        &ShardLoomError::InvalidOperation(format!(
                            "invalid candidate source: {source_arg}"
                        )),
                    );
                }
            };
            let uri = match DatasetUri::new(uri_arg) {
                Ok(uri) => uri,
                Err(error) => {
                    return emit_error(
                        "vortex-projection-readiness-plan",
                        format,
                        "invalid dataset uri",
                        &error,
                    );
                }
            };
            let mut request =
                shardloom_vortex::VortexProjectionReadinessRequest::new(uri, candidate_source);
            for token in args {
                match token.as_str() {
                    "--feature-gate" => {
                        request.add_signal(VortexProjectionReadinessSignal::FeatureGateEnabled);
                    }
                    "--query-primitive-ready" => {
                        request.add_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady);
                    }
                    "--metadata-footer-ready" => {
                        request.add_signal(VortexProjectionReadinessSignal::MetadataFooterReady);
                    }
                    "--encoded-data-path-ready" => {
                        request.add_signal(VortexProjectionReadinessSignal::EncodedDataPathReady);
                    }
                    "--projection-primitive" => {
                        request.add_signal(VortexProjectionReadinessSignal::ProjectionPrimitive);
                    }
                    "--projection-provided" => {
                        request.add_signal(VortexProjectionReadinessSignal::ProjectionProvided);
                    }
                    "--projection-supported" => {
                        request.add_signal(VortexProjectionReadinessSignal::ProjectionSupported);
                    }
                    "--projection-unsupported" => {
                        request.add_signal(VortexProjectionReadinessSignal::ProjectionUnsupported);
                    }
                    "--object-store-target" => {
                        request.add_signal(VortexProjectionReadinessSignal::ObjectStoreTarget);
                    }
                    "--decode-risk" => {
                        request.add_signal(VortexProjectionReadinessSignal::DecodeRisk);
                    }
                    "--materialization-risk" => {
                        request.add_signal(VortexProjectionReadinessSignal::MaterializationRisk);
                    }
                    "--arrow-default-risk" => {
                        request.add_signal(VortexProjectionReadinessSignal::ArrowDefaultRisk);
                    }
                    "--write-risk" => {
                        request.add_signal(VortexProjectionReadinessSignal::WriteRisk);
                    }
                    "--scan-execution-risk" => {
                        request.add_signal(VortexProjectionReadinessSignal::ScanExecutionRisk);
                    }
                    "--fallback-policy-blocked" => {
                        request.add_signal(VortexProjectionReadinessSignal::FallbackPolicyBlocked);
                    }
                    _ => {
                        return emit_error(
                            "vortex-projection-readiness-plan",
                            format,
                            "unknown option",
                            &ShardLoomError::InvalidOperation(format!("unknown option: {token}")),
                        );
                    }
                }
            }
            let report = match plan_vortex_projection_readiness(request) {
                Ok(report) => report,
                Err(error) => {
                    return emit_error(
                        "vortex-projection-readiness-plan",
                        format,
                        "projection readiness planning failed",
                        &error,
                    );
                }
            };
            emit(
                "vortex-projection-readiness-plan",
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "vortex projection readiness planning report".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                vortex_projection_readiness_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }

        Some("vortex-count") => handle_vortex_count(args, format),
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
                bounded_local_execution_fields(&report, &primitive_arg, memory_gb, max_parallelism),
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
        Some("optimizer-adaptive-memory-plan") => {
            let command = "optimizer-adaptive-memory-plan";
            let report = plan_adaptive_optimizer_memory();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "adaptive optimizer memory plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                adaptive_optimizer_memory_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
        }
        Some("cpu-specialization-plan") => {
            let command = "cpu-specialization-plan";
            let report = plan_cpu_operator_specialization();
            emit(
                command,
                format,
                if report.has_errors() {
                    CommandStatus::Unsupported
                } else {
                    CommandStatus::Success
                },
                "cpu operator specialization plan".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                cpu_operator_specialization_fields(&report),
            );
            if report.has_errors() {
                ExitCode::from(1)
            } else {
                ExitCode::SUCCESS
            }
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
        Some(command) => {
            eprintln!("{}", cli_usage_line());
            let error = cli_unknown_arg_error("shardloom", command);
            emit_error("cli", format, "unknown command", &error)
        }
        None => {
            eprintln!("{}", cli_usage_line());
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DiagnosticCategory, DiagnosticCode};
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
            let _ = sender.send(super::run(args));
        });
        receiver.recv().expect("receive test exit code")
    }

    fn run(args: Vec<String>) -> ExitCode {
        run_with_larger_stack("cli-test", args)
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
    fn optimizer_adaptive_memory_plan_returns_success() {
        let code = run(vec!["optimizer-adaptive-memory-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
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
    fn incremental_plan_cdc_snapshot_id_returns_success_without_scenario() {
        let code = run(vec!["incremental-plan".to_string(), "cdc".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn stateful_reuse_plan_returns_success() {
        let code = run(vec!["stateful-reuse-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn universal_harness_plan_returns_success() {
        let code = run(vec!["universal-harness-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn native_io_envelope_plan_returns_success() {
        let code = run(vec!["native-io-envelope-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn world_class_sufficiency_plan_returns_success() {
        let code = run(vec!["world-class-sufficiency-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn layout_health_plan_healthy_returns_success() {
        let code = run(vec![
            "layout-health-plan".to_string(),
            "healthy".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn layout_health_plan_empty_returns_non_zero() {
        let code = run(vec!["layout-health-plan".to_string(), "empty".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn compaction_plan_small_files_returns_success() {
        let code = run(vec![
            "compaction-plan".to_string(),
            "small-files".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn compaction_plan_empty_returns_non_zero() {
        let code = run(vec!["compaction-plan".to_string(), "empty".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_range_plan_s3_ranges_returns_success() {
        let code = run(vec![
            "object-store-range-plan".to_string(),
            "s3-ranges".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_range_plan_missing_ranges_returns_non_zero() {
        let code = run(vec![
            "object-store-range-plan".to_string(),
            "missing-ranges".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_coalesce_plan_s3_ranges_returns_success() {
        let code = run(vec![
            "object-store-coalesce-plan".to_string(),
            "s3-ranges".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_coalesce_plan_missing_ranges_returns_non_zero() {
        let code = run(vec![
            "object-store-coalesce-plan".to_string(),
            "missing-ranges".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_coalesce_unknown_scenario_uses_coalesce_command_name() {
        let error = object_store_range_fixture_for_command("object-store-coalesce-plan", "unknown")
            .expect_err("unknown scenario");

        assert!(
            error
                .message()
                .contains("object-store-coalesce-plan unknown argument/value: unknown")
        );
    }

    #[test]
    fn object_store_schedule_plan_s3_ranges_returns_success() {
        let code = run(vec![
            "object-store-schedule-plan".to_string(),
            "s3-ranges".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_schedule_plan_missing_ranges_returns_non_zero() {
        let code = run(vec![
            "object-store-schedule-plan".to_string(),
            "missing-ranges".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_schedule_plan_task_budget_returns_non_zero() {
        let code = run(vec![
            "object-store-schedule-plan".to_string(),
            "task-budget".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_checkpoint_retry_plan_ready_returns_success() {
        let code = run(vec![
            "object-store-checkpoint-retry-plan".to_string(),
            "ready".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_checkpoint_retry_plan_missing_idempotency_returns_non_zero() {
        let code = run(vec![
            "object-store-checkpoint-retry-plan".to_string(),
            "missing-idempotency".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_checkpoint_retry_plan_blocked_scheduling_returns_non_zero() {
        let code = run(vec![
            "object-store-checkpoint-retry-plan".to_string(),
            "blocked-scheduling".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_commit_plan_ready_returns_success() {
        let code = run(vec![
            "object-store-commit-plan".to_string(),
            "ready".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_commit_plan_missing_idempotency_returns_non_zero() {
        let code = run(vec![
            "object-store-commit-plan".to_string(),
            "missing-idempotency".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn incremental_plan_cdc_append_only_returns_success() {
        let code = run(vec![
            "incremental-plan".to_string(),
            "cdc".to_string(),
            "append-only".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn incremental_plan_cdc_delete_returns_non_zero() {
        let code = run(vec![
            "incremental-plan".to_string(),
            "cdc".to_string(),
            "delete".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
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
    fn vortex_encoded_read_boundary_ready_signals_returns_success() {
        let code = run(vec![
            "vortex-encoded-read-boundary".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "upstream-open-options-available,upstream-footer-available,upstream-metadata-surface-available,upstream-scan-surface-deferred,local-path-only,feature-gate-enabled".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_boundary_missing_target_uri_returns_non_zero() {
        let code = run(vec!["vortex-encoded-read-boundary".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_boundary_missing_signals_returns_non_zero() {
        let code = run(vec![
            "vortex-encoded-read-boundary".to_string(),
            "file:///tmp/example.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_boundary_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-encoded-read-boundary".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "upstream-open-options-available,unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_boundary_blocking_signals_return_non_zero() {
        for signals in [
            "upstream-open-options-available,upstream-footer-available,object-store-target,feature-gate-enabled",
            "upstream-open-options-available,upstream-footer-available,decode-risk",
            "upstream-open-options-available,upstream-footer-available,materialization-risk",
            "upstream-open-options-available,upstream-footer-available,arrow-default-risk",
        ] {
            let code = run(vec![
                "vortex-encoded-read-boundary".to_string(),
                "file:///tmp/example.vortex".to_string(),
                signals.to_string(),
            ]);
            assert_ne!(code, ExitCode::SUCCESS);
        }
    }

    #[test]
    fn parse_vortex_encoded_read_boundary_signals_unknown_token_maps_to_invalid_input() {
        let err = parse_vortex_encoded_read_boundary_signals("bad-token").unwrap_err();
        let diagnostic = err.to_diagnostic();

        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, DiagnosticCategory::InvalidInput);
        assert!(!diagnostic.fallback.attempted);
        assert!(!diagnostic.fallback.allowed);
    }

    #[test]
    fn parse_vortex_encoded_read_boundary_signals_dedup_and_trim() {
        let parsed = parse_vortex_encoded_read_boundary_signals(
            " upstream-open-options-available , upstream-footer-available , upstream-footer-available ",
        )
        .expect("parse signals");
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn vortex_encoded_read_boundary_fields_include_required_no_exec_flags() {
        let mut request = VortexEncodedReadBoundaryRequest::new(
            DatasetUri::new("file:///tmp/example.vortex").expect("uri"),
        );
        request.add_signal(VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable);
        request.add_signal(VortexEncodedReadBoundarySignal::UpstreamFooterAvailable);
        let report = plan_vortex_encoded_read_boundary(request).expect("report");
        let fields = vortex_encoded_read_boundary_fields(&report);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        )));
        assert!(fields.contains(&("data_read".to_string(), "false".to_string())));
        assert!(fields.contains(&("array_decoded".to_string(), "false".to_string())));
        assert!(fields.contains(&("values_materialized".to_string(), "false".to_string(),)));
        assert!(fields.contains(&("arrow_converted".to_string(), "false".to_string())));
        assert!(fields.contains(&("object_store_io".to_string(), "false".to_string())));
        assert!(fields.contains(&("upstream_scan_called".to_string(), "false".to_string(),)));
    }
    #[test]
    fn vortex_encoded_read_metadata_probe_ready_local_returns_non_zero() {
        let code = run(vec![
            "vortex-encoded-read-metadata-probe".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "/tmp/example.vortex".to_string(),
            "fixture-ready,fixture-ref-provided,local-path-only,feature-gate-enabled".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_encoded_read_metadata_probe_missing_args_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-encoded-read-metadata-probe".to_string()]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-encoded-read-metadata-probe".to_string(),
                "file:///tmp/example.vortex".to_string()
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-encoded-read-metadata-probe".to_string(),
                "file:///tmp/example.vortex".to_string(),
                "/tmp/example.vortex".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_read_metadata_probe_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-encoded-read-metadata-probe".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "/tmp/example.vortex".to_string(),
            "fixture-ready,unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_encoded_read_metadata_probe_blocking_signals_return_non_zero() {
        for signals in [
            "fixture-ready,fixture-ref-provided,object-store-target,feature-gate-enabled",
            "fixture-ready,fixture-ref-provided,local-path-only,decode-risk",
            "fixture-ready,fixture-ref-provided,local-path-only,materialization-risk",
            "fixture-ready,fixture-ref-provided,local-path-only,arrow-default-risk",
        ] {
            let code = run(vec![
                "vortex-encoded-read-metadata-probe".to_string(),
                "file:///tmp/example.vortex".to_string(),
                "/tmp/example.vortex".to_string(),
                signals.to_string(),
            ]);
            assert_ne!(code, ExitCode::SUCCESS);
        }
    }

    #[test]
    fn parse_vortex_encoded_read_metadata_probe_signals_unknown_token_maps_to_invalid_input() {
        let err = parse_vortex_encoded_read_metadata_probe_signals("bad-token").unwrap_err();
        let diagnostic = err.to_diagnostic();

        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, DiagnosticCategory::InvalidInput);
        assert!(!diagnostic.fallback.attempted);
        assert!(!diagnostic.fallback.allowed);
    }

    #[test]
    fn parse_vortex_encoded_read_metadata_probe_signals_dedup_and_trim() {
        let parsed = parse_vortex_encoded_read_metadata_probe_signals(
            " fixture-ready , fixture-ref-provided , fixture-ref-provided ",
        )
        .expect("parse signals");
        assert_eq!(parsed.len(), 2);
    }
    #[test]
    fn vortex_encoded_read_metadata_probe_fields_include_required_no_exec_flags() {
        let request = VortexEncodedReadMetadataProbeRequest::new(
            DatasetUri::new("file:///tmp/example.vortex").expect("uri"),
            VortexEncodedReadFixtureRef::new("/tmp/example.vortex").expect("fixture"),
        );
        let report = probe_vortex_encoded_read_metadata(request).expect("report");
        let fields = vortex_encoded_read_metadata_probe_fields(&report);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        )));
        assert!(fields.contains(&("metadata_opened".to_string(), "false".to_string())));
        assert!(fields.contains(&("footer_inspected".to_string(), "false".to_string())));
        assert!(fields.contains(&("encoded_data_read".to_string(), "false".to_string())));
        assert!(fields.contains(&("row_read".to_string(), "false".to_string())));
        assert!(fields.contains(&("arrow_converted".to_string(), "false".to_string())));
        assert!(fields.contains(&("object_store_io".to_string(), "false".to_string())));
        assert!(fields.contains(&("upstream_scan_called".to_string(), "false".to_string(),)));
    }

    #[test]
    fn vortex_encoded_read_metadata_probe_s3_fixture_sets_object_store_target_field() {
        let request = VortexEncodedReadMetadataProbeRequest::new(
            DatasetUri::new("file:///tmp/example.vortex").expect("uri"),
            VortexEncodedReadFixtureRef::new("s3://bucket/example.vortex").expect("fixture"),
        )
        .fixture_ready(true)
        .fixture_ref_provided(true)
        .feature_gate_enabled(true);
        let report = probe_vortex_encoded_read_metadata(request).expect("report");
        let fields = vortex_encoded_read_metadata_probe_fields(&report);
        assert!(fields.contains(&("object_store_target".to_string(), "true".to_string())));
        assert!(fields.contains(&("metadata_opened".to_string(), "false".to_string())));
        assert!(fields.contains(&("footer_inspected".to_string(), "false".to_string())));
        assert!(fields.contains(&("encoded_data_read".to_string(), "false".to_string())));
        assert!(fields.contains(&("upstream_scan_called".to_string(), "false".to_string())));
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        )));
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
    fn vortex_output_payload_plan_ready_returns_success() {
        let code = run(vec![
            "vortex-output-payload-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_output_payload_plan_missing_args_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-output-payload-plan".to_string()]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-output-payload-plan".to_string(),
                "file://tmp/out.vortex".to_string()
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-output-payload-plan".to_string(),
                "file://tmp/out.vortex".to_string(),
                "/tmp/stage".to_string()
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_output_payload_plan_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-output-payload-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_output_payload_plan_blocking_signals_return_non_zero() {
        let object_store_code = run(vec![
            "vortex-output-payload-plan".to_string(),
            "s3://bucket/out.vortex".to_string(),
            "s3://bucket/stage".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,object-store-target,feature-gate-enabled".to_string(),
        ]);
        assert_ne!(object_store_code, ExitCode::SUCCESS);
        let upstream_required_code = run(vec![
            "vortex-output-payload-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,upstream-vortex-write-required,feature-gate-enabled".to_string(),
        ]);
        assert_ne!(upstream_required_code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_output_payload_artifact_write_ready_default_build_reports_not_written() {
        let code = run(vec![
            "vortex-output-payload-artifact-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_output_payload_artifact_write_missing_args_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-output-payload-artifact-write".to_string()]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-output-payload-artifact-write".to_string(),
                "file://tmp/out.vortex".to_string()
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-output-payload-artifact-write".to_string(),
                "file://tmp/out.vortex".to_string(),
                "/tmp/stage".to_string()
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-output-payload-artifact-write".to_string(),
                "file://tmp/out.vortex".to_string(),
                "/tmp/stage".to_string(),
                "write-intent-ready".to_string()
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_output_payload_artifact_write_unknown_signal_or_option_returns_non_zero() {
        let signal = run(vec![
            "vortex-output-payload-artifact-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,unknown".to_string(),
            "none".to_string(),
        ]);
        assert_ne!(signal, ExitCode::SUCCESS);
        let option = run(vec![
            "vortex-output-payload-artifact-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(option, ExitCode::SUCCESS);
    }
    #[test]
    fn vortex_output_payload_artifact_write_json_format_includes_required_fields() {
        let code = run(vec![
            "vortex-output-payload-artifact-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_native_count_payload_write_ready_returns_success() {
        let unique = format!(
            "shardloom-cli-native-count-payload-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let workspace = root.join("stage");
        std::fs::create_dir_all(&workspace).unwrap();
        let code = run(vec![
            "vortex-native-count-payload-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            workspace.to_string_lossy().to_string(),
            "42".to_string(),
            "write-intent-ready,staged-output-ready,finalized-manifest-ready,payload-content-available,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
        ]);
        let payload_path = workspace.join("_shardloom_output_payload.vortex");
        assert_eq!(
            payload_path.exists(),
            vortex_native_output_payload_write_feature_enabled()
        );
        let _ = std::fs::remove_file(payload_path);
        let _ = std::fs::remove_dir(&workspace);
        let _ = std::fs::remove_dir(&root);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_native_count_payload_write_invalid_inputs_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-native-count-payload-write".to_string()]),
            ExitCode::SUCCESS
        );
        let invalid_count = run(vec![
            "vortex-native-count-payload-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "not-a-count".to_string(),
            "write-intent-ready".to_string(),
            "none".to_string(),
        ]);
        assert_ne!(invalid_count, ExitCode::SUCCESS);
        let unknown_signal = run(vec![
            "vortex-native-count-payload-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "42".to_string(),
            "write-intent-ready,unknown".to_string(),
            "none".to_string(),
        ]);
        assert_ne!(unknown_signal, ExitCode::SUCCESS);
        let unknown_option = run(vec![
            "vortex-native-count-payload-write".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "42".to_string(),
            "write-intent-ready".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(unknown_option, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_local_commit_execute_ready_returns_success() {
        let unique = format!(
            "shardloom-cli-local-commit-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let workspace = root.join("stage");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(
            workspace.join("_shardloom_finalized_manifest.json"),
            b"{\"finalized\":true}",
        )
        .unwrap();
        std::fs::write(workspace.join(".shardloom-commit-marker"), b"marker=true\n").unwrap();
        std::fs::write(
            workspace.join("_shardloom_output_payload.vortex"),
            b"payload",
        )
        .unwrap();
        let code = run(vec![
            "vortex-local-commit-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            workspace.to_string_lossy().to_string(),
            "commit-protocol-ready,finalized-manifest-written,commit-marker-written,output-payload-written,local-workspace,feature-gate-enabled".to_string(),
        ]);
        let committed_path = workspace.join("_shardloom_committed_manifest.json");
        assert_eq!(
            committed_path.exists(),
            vortex_local_commit_execution_feature_enabled()
        );
        let _ = std::fs::remove_dir_all(root);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_local_commit_execute_invalid_inputs_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-local-commit-execute".to_string()]),
            ExitCode::SUCCESS
        );
        let unknown_signal = run(vec![
            "vortex-local-commit-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-ready,unknown".to_string(),
        ]);
        assert_ne!(unknown_signal, ExitCode::SUCCESS);
        let blocking_signal = run(vec![
            "vortex-local-commit-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "commit-protocol-blocked,finalized-manifest-written,commit-marker-written,output-payload-written,local-workspace,feature-gate-enabled".to_string(),
        ]);
        if vortex_local_commit_execution_feature_enabled() {
            assert_ne!(blocking_signal, ExitCode::SUCCESS);
        } else {
            assert_eq!(blocking_signal, ExitCode::SUCCESS);
        }
    }

    #[test]
    fn vortex_local_commit_recovery_plan_ready_returns_success() {
        let code = run(vec![
            "vortex-local-commit-recovery-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "rollback-requested,committed-manifest-written,local-workspace,cleanup-allowed"
                .to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_local_commit_recovery_plan_invalid_inputs_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-local-commit-recovery-plan".to_string()]),
            ExitCode::SUCCESS
        );
        let unknown_signal = run(vec![
            "vortex-local-commit-recovery-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "committed-manifest-written,unknown".to_string(),
        ]);
        assert_ne!(unknown_signal, ExitCode::SUCCESS);
        let ambiguous = run(vec![
            "vortex-local-commit-recovery-plan".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "committed-manifest-written,ambiguous-commit,local-workspace".to_string(),
        ]);
        assert_ne!(ambiguous, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_local_commit_rollback_execute_ready_returns_success() {
        let unique = format!(
            "shardloom-cli-local-commit-rollback-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let workspace = root.join("stage");
        std::fs::create_dir_all(&workspace).unwrap();
        let committed_path = workspace.join("_shardloom_committed_manifest.json");
        std::fs::write(&committed_path, b"{\"committed\":true}").unwrap();
        let code = run(vec![
            "vortex-local-commit-rollback-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            workspace.to_string_lossy().to_string(),
            "rollback-requested,committed-manifest-written,local-workspace,cleanup-allowed"
                .to_string(),
        ]);
        assert_eq!(
            committed_path.exists(),
            !shardloom_vortex::vortex_local_commit_rollback_execution_feature_enabled()
        );
        let _ = std::fs::remove_dir_all(root);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_local_commit_rollback_execute_invalid_inputs_return_non_zero() {
        assert_ne!(
            run(vec!["vortex-local-commit-rollback-execute".to_string()]),
            ExitCode::SUCCESS
        );
        let unknown_signal = run(vec![
            "vortex-local-commit-rollback-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "committed-manifest-written,unknown".to_string(),
        ]);
        assert_ne!(unknown_signal, ExitCode::SUCCESS);
        let ambiguous = run(vec![
            "vortex-local-commit-rollback-execute".to_string(),
            "file://tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "committed-manifest-written,ambiguous-commit,local-workspace".to_string(),
        ]);
        assert_ne!(ambiguous, ExitCode::SUCCESS);
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

    fn layout_driver_approval_signals(runtime_allowed: bool) -> String {
        let mut signals = vec![
            "local-fixture-only",
            "caller-session-allowed",
            "layout-row-count-only-intent",
            "scan-forbidden",
            "evaluation-forbidden",
            "data-read-forbidden",
            "decode-forbidden",
            "materialization-forbidden",
            "arrow-forbidden",
            "object-store-forbidden",
            "write-forbidden",
            "fallback-forbidden",
        ];
        if runtime_allowed {
            signals.push("runtime-driver-start-allowed");
        }
        signals.join(",")
    }

    #[test]
    fn usage_includes_vortex_count_readiness_plan() {
        assert!(cli_usage_line().contains("vortex-count-readiness-plan"));
    }
    #[test]
    fn usage_includes_vortex_encoded_count_approval_plan() {
        assert!(cli_usage_line().contains("vortex-encoded-count-approval-plan"));
    }
    #[test]
    fn usage_includes_vortex_layout_driver_approval_plan() {
        assert!(cli_usage_line().contains("vortex-layout-driver-approval-plan"));
    }
    #[test]
    fn usage_includes_vortex_filtered_count_readiness_plan() {
        assert!(cli_usage_line().contains("vortex-filtered-count-readiness-plan"));
    }
    #[test]
    fn usage_includes_vortex_projection_readiness_plan() {
        assert!(cli_usage_line().contains("vortex-projection-readiness-plan"));
    }
    #[test]
    fn usage_includes_vortex_metadata_physical_kernel_plan() {
        assert!(cli_usage_line().contains("vortex-metadata-physical-kernel-plan"));
    }
    #[test]
    fn usage_includes_vortex_encoded_path_selection_plan() {
        assert!(cli_usage_line().contains("vortex-encoded-path-selection-plan"));
    }
    #[test]
    fn vortex_encoded_path_selection_plan_returns_success() {
        let code = run(vec!["vortex-encoded-path-selection-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn usage_includes_vortex_generalized_encoded_primitive_gate() {
        assert!(cli_usage_line().contains("vortex-generalized-encoded-primitive-gate"));
    }
    #[test]
    fn vortex_generalized_encoded_primitive_gate_returns_success() {
        let code = run(vec![
            "vortex-generalized-encoded-primitive-gate".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn usage_includes_streaming_plan() {
        assert!(cli_usage_line().contains("streaming-plan"));
    }
    #[test]
    fn usage_includes_streaming_batch_plan() {
        assert!(cli_usage_line().contains("streaming-batch-plan"));
    }
    #[test]
    fn usage_includes_backpressure_plan() {
        assert!(cli_usage_line().contains("backpressure-plan"));
    }
    #[test]
    fn usage_includes_sizing_feedback_plan() {
        assert!(cli_usage_line().contains("sizing-feedback-plan"));
    }
    #[test]
    fn usage_includes_layout_health_plan() {
        assert!(cli_usage_line().contains("layout-health-plan"));
    }
    #[test]
    fn usage_includes_compaction_plan() {
        assert!(cli_usage_line().contains("compaction-plan"));
    }
    #[test]
    fn usage_includes_object_store_range_plan() {
        assert!(cli_usage_line().contains("object-store-range-plan"));
    }
    #[test]
    fn usage_includes_object_store_coalesce_plan() {
        assert!(cli_usage_line().contains("object-store-coalesce-plan"));
    }
    #[test]
    fn usage_includes_object_store_schedule_plan() {
        assert!(cli_usage_line().contains("object-store-schedule-plan"));
    }
    #[test]
    fn usage_includes_object_store_checkpoint_retry_plan() {
        assert!(cli_usage_line().contains("object-store-checkpoint-retry-plan"));
    }
    #[test]
    fn usage_includes_object_store_commit_plan() {
        assert!(cli_usage_line().contains("object-store-commit-plan"));
    }
    #[test]
    fn usage_includes_optimizer_adaptive_memory_plan() {
        assert!(cli_usage_line().contains("optimizer-adaptive-memory-plan"));
    }
    #[test]
    fn usage_includes_cpu_specialization_plan() {
        assert!(cli_usage_line().contains("cpu-specialization-plan"));
    }
    #[test]
    fn cpu_specialization_plan_returns_success() {
        let code = run(vec!["cpu-specialization-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }
    #[test]
    fn usage_includes_execution_certificate_plan() {
        assert!(cli_usage_line().contains("execution-certificate-plan"));
    }
    #[test]
    fn usage_includes_stateful_reuse_plan() {
        assert!(cli_usage_line().contains("stateful-reuse-plan"));
    }
    #[test]
    fn usage_includes_universal_harness_plan() {
        assert!(cli_usage_line().contains("universal-harness-plan"));
    }
    #[test]
    fn usage_includes_native_io_envelope_plan() {
        assert!(cli_usage_line().contains("native-io-envelope-plan"));
    }

    #[test]
    fn usage_includes_world_class_sufficiency_plan() {
        assert!(cli_usage_line().contains("world-class-sufficiency-plan"));
    }

    #[test]
    fn parse_sizing_feedback_signals_rejects_unknown_and_empty() {
        assert!(parse_sizing_feedback_signals("unknown").is_err());
        assert!(parse_sizing_feedback_signals(" ").is_err());
    }
    #[test]
    fn parse_sizing_feedback_signals_deduplicates_and_accepts_aliases() {
        let signals = parse_sizing_feedback_signals(
            "task-too-small,task_too_small,memory-pressure-high,stable",
        )
        .unwrap();
        assert_eq!(signals.len(), 3);
        assert!(
            signals
                .iter()
                .any(|signal| signal.kind == SizingFeedbackSignalKind::TaskTooSmall)
        );
        assert!(
            signals
                .iter()
                .any(|signal| signal.kind == SizingFeedbackSignalKind::MemoryPressureHigh)
        );
        assert!(
            signals
                .iter()
                .any(|signal| signal.kind == SizingFeedbackSignalKind::Stable)
        );
    }
    #[test]
    fn vortex_count_readiness_plan_missing_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec!["vortex-count-readiness-plan".to_string()]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_missing_dataset_uri_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_invalid_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "bad-source".to_string(),
                "file://tmp/in.vortex".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_unknown_extra_token_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--nope".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_bare_json_text_tokens_return_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "text".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_global_format_json_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--metadata-footer-ready".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_metadata_footer_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--metadata-footer-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_encoded_data_path_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "encoded-data-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_unknown_source_with_ready_signals_is_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "unknown".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--metadata-footer-ready".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_count_readiness_plan_filtered_count_requested_is_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-count-readiness-plan".to_string(),
                "metadata-footer".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--metadata-footer-ready".to_string(),
                "--filtered-count-requested".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_count_approval_plan_missing_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec!["vortex-encoded-count-approval-plan".to_string()]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_count_approval_plan_invalid_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-encoded-count-approval-plan".to_string(),
                "bad-source".to_string(),
                "file://tmp/in.vortex".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_count_approval_plan_current_api_boundary_blocks_ready_count() {
        assert_ne!(
            run(vec![
                "vortex-encoded-count-approval-plan".to_string(),
                "encoded-data-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_count_approval_plan_json_current_api_boundary_blocks_ready_count() {
        assert_ne!(
            run(vec![
                "vortex-encoded-count-approval-plan".to_string(),
                "encoded-data-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--encoded-data-path-ready".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_encoded_count_approval_plan_layout_row_count_approval_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-encoded-count-approval-plan".to_string(),
                "encoded-data-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--count-primitive".to_string(),
                "--encoded-data-path-ready".to_string(),
                "--layout-row-count-approved".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn parse_vortex_layout_driver_approval_signals_unknown_token_maps_to_invalid_input() {
        let err = parse_vortex_layout_driver_approval_signals("bad-token").unwrap_err();
        assert!(err.to_string().contains("bad-token"));
    }
    #[test]
    fn parse_vortex_layout_driver_approval_signals_dedup_and_trim() {
        let parsed = parse_vortex_layout_driver_approval_signals(
            "local-fixture-only, local-fixture-only,caller-session-allowed",
        )
        .unwrap();
        assert_eq!(
            parsed,
            vec![
                VortexLayoutReaderDriverApprovalSignal::LocalFixtureOnly,
                VortexLayoutReaderDriverApprovalSignal::CallerSessionAllowed,
            ]
        );
    }
    #[test]
    fn vortex_layout_driver_approval_plan_missing_signals_returns_non_zero() {
        assert_ne!(
            run(vec!["vortex-layout-driver-approval-plan".to_string()]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_layout_driver_approval_plan_unknown_extra_token_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-layout-driver-approval-plan".to_string(),
                layout_driver_approval_signals(true),
                "--nope".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_layout_driver_approval_plan_current_api_boundary_blocks_without_driver_signal() {
        assert_ne!(
            run(vec![
                "vortex-layout-driver-approval-plan".to_string(),
                layout_driver_approval_signals(false),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_layout_driver_approval_plan_json_ready_remains_report_only() {
        assert_eq!(
            run(vec![
                "vortex-layout-driver-approval-plan".to_string(),
                layout_driver_approval_signals(true),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_missing_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec!["vortex-filtered-count-readiness-plan".to_string()]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_missing_dataset_uri_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_invalid_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "bad-source".to_string(),
                "file://tmp/in.vortex".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_unknown_extra_token_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--nope".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_bare_json_text_tokens_return_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "text".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_global_format_json_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--metadata-footer-ready".to_string(),
                "--predicate-metadata-proof-ready".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_encoded_predicate_path_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "encoded-predicate-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_metadata_proof_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--metadata-footer-ready".to_string(),
                "--predicate-metadata-proof-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_missing_encoded_data_path_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "encoded-predicate-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_missing_metadata_proof_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "metadata-predicate-proof".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--metadata-footer-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_unknown_source_ready_signals_is_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "unknown".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--metadata-footer-ready".to_string(),
                "--predicate-metadata-proof-ready".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_filtered_count_readiness_plan_predicate_unsupported_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-filtered-count-readiness-plan".to_string(),
                "encoded-predicate-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--filtered-count-primitive".to_string(),
                "--predicate-provided".to_string(),
                "--encoded-data-path-ready".to_string(),
                "--predicate-unsupported".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_missing_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec!["vortex-projection-readiness-plan".to_string()]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_missing_dataset_uri_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "metadata-schema-projection".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_invalid_candidate_source_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "bad-source".to_string(),
                "file://tmp/in.vortex".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_unknown_extra_token_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded-column-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--nope".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_bare_json_text_tokens_return_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded-column-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded-column-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "text".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_global_format_json_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "metadata-schema-projection".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
                "--projection-supported".to_string(),
                "--metadata-footer-ready".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_fields_remain_report_only() {
        let report = plan_vortex_projection_readiness(
            shardloom_vortex::VortexProjectionReadinessRequest::new(
                DatasetUri::new("file://tmp/in.vortex").expect("uri"),
                VortexProjectionCandidateSource::EncodedColumnPath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .encoded_data_path_ready(true),
        )
        .expect("report");
        let fields = vortex_projection_readiness_fields(&report);
        for (key, value) in [
            ("projection_executed", "false"),
            ("projection_applied", "false"),
            ("metadata_read", "false"),
            ("encoded_data_read", "false"),
            ("row_read", "false"),
            ("array_decoded", "false"),
            ("values_materialized", "false"),
            ("arrow_converted", "false"),
            ("object_store_io", "false"),
            ("data_written", "false"),
            ("upstream_scan_called", "false"),
            ("fallback_execution_allowed", "false"),
        ] {
            assert!(fields.contains(&(key.to_string(), value.to_string())));
        }
    }
    #[test]
    fn vortex_projection_readiness_plan_metadata_schema_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "metadata-schema-projection".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
                "--projection-supported".to_string(),
                "--metadata-footer-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_encoded_column_path_ready_succeeds() {
        assert_eq!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded-column-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_unknown_source_with_ready_signals_is_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "unknown".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
                "--projection-supported".to_string(),
                "--metadata-footer-ready".to_string(),
                "--encoded-data-path-ready".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_missing_encoded_data_path_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "encoded-column-path".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
            ]),
            ExitCode::SUCCESS
        );
    }
    #[test]
    fn vortex_projection_readiness_plan_projection_unsupported_returns_non_zero() {
        assert_ne!(
            run(vec![
                "vortex-projection-readiness-plan".to_string(),
                "metadata-schema-projection".to_string(),
                "file://tmp/in.vortex".to_string(),
                "--feature-gate".to_string(),
                "--query-primitive-ready".to_string(),
                "--projection-primitive".to_string(),
                "--projection-provided".to_string(),
                "--metadata-footer-ready".to_string(),
                "--projection-unsupported".to_string(),
            ]),
            ExitCode::SUCCESS
        );
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
    fn vortex_manifest_finalization_plan_ready_returns_success() {
        let code = run(vec![
            "vortex-manifest-finalization-plan".to_string(),
            "file:///tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "draft-manifest-written,commit-marker-written,commit-protocol-ready,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,local-workspace,feature-gate-enabled".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_manifest_finalization_plan_unknown_signal_returns_non_zero() {
        let code = run(vec![
            "vortex-manifest-finalization-plan".to_string(),
            "file:///tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_finalized_manifest_artifact_write_default_build_returns_success() {
        let code = run(vec![
            "vortex-finalized-manifest-artifact-write".to_string(),
            "file:///tmp/out.vortex".to_string(),
            "/tmp/stage".to_string(),
            "draft-manifest-written,commit-marker-written,commit-protocol-ready,schema-known,schema-compatible,delete-semantics-known,tombstone-semantics-known,local-workspace,feature-gate-enabled".to_string(),
            "none".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
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
    fn streaming_plan_with_vortex_target_returns_success() {
        let code = run(vec![
            "streaming-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
            "file://tmp/out.vortex".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn streaming_batch_plan_with_vortex_target_returns_success() {
        let code = run(vec![
            "streaming-batch-plan".to_string(),
            "file://tmp/test.vortex".to_string(),
            "file://tmp/out.vortex".to_string(),
            "8".to_string(),
            "2".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn release_plan_returns_success() {
        let code = run(vec!["release-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn python_wrapper_plan_returns_success() {
        let code = run(vec!["python-wrapper-plan".to_string()]);
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
    fn table_compat_plan_partition_evolution_add_field_returns_success() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "partition-evolution".to_string(),
            "add-field".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_partition_evolution_unknown_transform_returns_non_zero() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "partition-evolution".to_string(),
            "unknown-transform".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_delete_semantics_file_level_returns_success() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "delete-semantics".to_string(),
            "file-level".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_delete_semantics_equality_delete_returns_non_zero() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "delete-semantics".to_string(),
            "equality-delete".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_aggregate_compatible_returns_success() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "aggregate".to_string(),
            "compatible".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_compat_plan_aggregate_delete_blocked_returns_non_zero() {
        let code = run(vec![
            "table-compat-plan".to_string(),
            "aggregate".to_string(),
            "delete-blocked".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn schema_plan_evolution_add_nullable_returns_success() {
        let code = run(vec![
            "schema-plan".to_string(),
            "evolution".to_string(),
            "add-nullable".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn schema_plan_evolution_rename_without_id_returns_non_zero() {
        let code = run(vec![
            "schema-plan".to_string(),
            "evolution".to_string(),
            "rename-without-id".to_string(),
        ]);
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
        let code = run_with_larger_stack(
            "vortex-input-plan-vortex-uri",
            vec![
                "vortex-input-plan".to_string(),
                "file://tmp/data.vortex".to_string(),
            ],
        );
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_input_plan_with_parquet_uri_returns_non_zero() {
        let code = run_with_larger_stack(
            "vortex-input-plan-parquet-uri",
            vec![
                "vortex-input-plan".to_string(),
                "file://tmp/data.parquet".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_vortex_uri_returns_success() {
        let code = run_with_larger_stack(
            "vortex-task-graph-vortex-uri",
            vec![
                "vortex-task-graph".to_string(),
                "file://tmp/data.vortex".to_string(),
            ],
        );
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_task_graph_with_parquet_uri_returns_non_zero() {
        let code = run_with_larger_stack(
            "vortex-task-graph-parquet-uri",
            vec![
                "vortex-task-graph".to_string(),
                "file://tmp/data.parquet".to_string(),
            ],
        );
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
    fn execution_certificate_plan_returns_success() {
        let code = run(vec!["execution-certificate-plan".to_string()]);
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
    fn plan_export_json_like_returns_non_zero_for_not_implemented() {
        let code = run(vec!["plan-export".to_string(), "json-like".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn plan_export_native_returns_success() {
        let code = run(vec!["plan-export".to_string(), "native".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_schedule_plan_with_vortex_uri_returns_success() {
        let code = run_with_larger_stack(
            "vortex-schedule-plan-vortex-uri",
            vec![
                "vortex-schedule-plan".to_string(),
                "file://tmp/data.vortex".to_string(),
                "8".to_string(),
                "2".to_string(),
            ],
        );
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_execution_readiness_with_vortex_uri_returns_non_zero_when_blocked() {
        let code = run_with_larger_stack(
            "vortex-execution-readiness-vortex-uri",
            vec![
                "vortex-execution-readiness".to_string(),
                "file://tmp/data.vortex".to_string(),
                "8".to_string(),
                "2".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_dry_run_with_vortex_uri_returns_non_zero_when_readiness_blocked() {
        let code = run_with_larger_stack(
            "vortex-dry-run-vortex-uri",
            vec![
                "vortex-dry-run".to_string(),
                "file://tmp/data.vortex".to_string(),
                "8".to_string(),
                "2".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_execution_readiness_with_non_vortex_uri_returns_non_zero() {
        let code = run_with_larger_stack(
            "vortex-execution-readiness-parquet-uri",
            vec![
                "vortex-execution-readiness".to_string(),
                "file://tmp/data.parquet".to_string(),
                "8".to_string(),
                "2".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_schedule_plan_with_non_vortex_uri_returns_non_zero() {
        let code = run_with_larger_stack(
            "vortex-schedule-plan-parquet-uri",
            vec![
                "vortex-schedule-plan".to_string(),
                "file://tmp/data.parquet".to_string(),
                "8".to_string(),
                "2".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cli_usage_line_uses_shardloom_not_crate_name() {
        let usage = cli_usage_line();
        assert!(usage.starts_with("usage: shardloom "));
        assert!(!usage.contains("shardloom-cli"));
    }

    #[test]
    fn cli_missing_arg_error_maps_to_invalid_input_diagnostic() {
        let error = cli_missing_arg_error("vortex-encoded-read-metadata-probe", "fixture_ref");
        let diagnostic = error.to_diagnostic();
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, DiagnosticCategory::InvalidInput);
        assert!(!diagnostic.fallback.attempted);
        assert!(!diagnostic.fallback.allowed);
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("vortex-encoded-read-metadata-probe"))
        );
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("fixture_ref"))
        );
    }

    #[test]
    fn cli_unknown_signal_error_maps_to_invalid_input_diagnostic() {
        let error = cli_unknown_signal_error(
            "vortex-encoded-read-boundary",
            "encoded-read-boundary",
            "bad-token",
        );
        let diagnostic = error.to_diagnostic();

        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, DiagnosticCategory::InvalidInput);
        assert!(!diagnostic.fallback.attempted);
        assert!(!diagnostic.fallback.allowed);
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("vortex-encoded-read-boundary"))
        );
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("encoded-read-boundary"))
        );
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("bad-token"))
        );
    }

    #[test]
    fn cli_unknown_arg_error_maps_to_invalid_input_diagnostic() {
        let error = cli_unknown_arg_error("shardloom", "bad-command");
        let diagnostic = error.to_diagnostic();
        assert_eq!(diagnostic.code, DiagnosticCode::InvalidInput);
        assert_eq!(diagnostic.category, DiagnosticCategory::InvalidInput);
        assert!(!diagnostic.fallback.attempted);
        assert!(!diagnostic.fallback.allowed);
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("shardloom"))
        );
        assert!(
            diagnostic
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("bad-command"))
        );
    }

    #[test]
    fn unknown_command_usage_text_uses_shardloom() {
        let usage = cli_usage_line();
        assert!(usage.contains("usage: shardloom "));
        assert!(!usage.contains("usage: shardloom-cli "));
    }

    #[test]
    fn cli_usage_lists_plan_probe_and_write_command_families() {
        let usage = cli_usage_line();
        assert!(usage.contains("|release-plan|"));
        assert!(usage.contains("|vortex-query-primitive-plan|"));
        assert!(usage.contains("|vortex-encoded-read-metadata-probe|"));
        assert!(usage.contains("|vortex-output-payload-artifact-write|"));
        assert!(usage.contains("|vortex-native-count-payload-write|"));
        assert!(usage.contains("|vortex-local-commit-execute|"));
        assert!(usage.contains("|vortex-local-commit-recovery-plan|"));
        assert!(usage.contains("|vortex-local-commit-rollback-execute|"));
    }

    #[test]
    fn vortex_query_primitive_plan_unknown_primitive_returns_non_zero() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "bogus".to_string(),
            "file:///tmp/example.vortex".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_count_ready_flags_return_success_report_only() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "--feature-gate".to_string(),
            "--metadata-footer-ready".to_string(),
            "--encoded-data-path-ready".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_bare_json_token_returns_non_zero() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "json".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_bare_text_token_returns_non_zero() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "text".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_global_format_json_succeeds() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_ready_flags_with_global_format_json_succeeds() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "--feature-gate".to_string(),
            "--metadata-footer-ready".to_string(),
            "--encoded-data-path-ready".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_query_primitive_plan_unknown_extra_token_returns_non_zero() {
        let code = run(vec![
            "vortex-query-primitive-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "extra".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_spike_parse_execute_local_count_flag() {
        let parsed = parse_vortex_spike_args(
            "vortex-encoded-read-spike",
            vec![
                "file:///tmp/example.vortex".to_string(),
                "1".to_string(),
                "2".to_string(),
                "--execute-local-count".to_string(),
            ]
            .into_iter(),
        )
        .expect("parse");
        assert_eq!(parsed.0.as_str(), "file:///tmp/example.vortex");
        assert_eq!(parsed.1, 1);
        assert_eq!(parsed.2, 2);
        assert!(parsed.3);
    }

    #[test]
    fn vortex_encoded_read_spike_unknown_option_returns_parse_error() {
        let parsed = parse_vortex_spike_args(
            "vortex-encoded-read-spike",
            vec![
                "file:///tmp/example.vortex".to_string(),
                "1".to_string(),
                "2".to_string(),
                "--bogus".to_string(),
            ]
            .into_iter(),
        );
        assert_eq!(parsed, Err(ExitCode::from(2)));
    }

    #[test]
    fn vortex_count_parse_local_encoded_count_flag() {
        let parsed = parse_vortex_count_args(
            vec![
                "file:///tmp/example.vortex".to_string(),
                "--execute-local-encoded-count".to_string(),
                "1".to_string(),
                "2".to_string(),
            ]
            .into_iter(),
        )
        .expect("parse");
        assert_eq!(parsed.0.as_str(), "file:///tmp/example.vortex");
        assert_eq!(
            parsed.1,
            VortexCountExecutionRequest::LocalEncodedCount {
                memory_gb: 1,
                max_parallelism: 2
            }
        );
    }

    #[test]
    fn vortex_count_local_encoded_evidence_matches_workspace_fixture_path() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");

        let fixture =
            local_encoded_count_correctness_fixture_for_target(&uri).expect("fixture match");

        assert_eq!(fixture.id.as_str(), "vortex-local-encoded-count-u64-20000");
    }

    #[test]
    fn vortex_count_local_encoded_evidence_matches_relative_fixture_path() {
        let uri = DatasetUri::new(
            ".\\shardloom-vortex\\tests\\fixtures\\metadata_footer_u64_20000.vortex",
        )
        .expect("uri");

        let fixture =
            local_encoded_count_correctness_fixture_for_target(&uri).expect("fixture match");

        assert_eq!(fixture.id.as_str(), "vortex-local-encoded-count-u64-20000");
    }

    #[test]
    fn vortex_count_local_encoded_evidence_rejects_unrelated_path() {
        let uri = DatasetUri::new("file:///tmp/unrelated.vortex").expect("uri");

        let fixture = local_encoded_count_correctness_fixture_for_target(&uri);

        assert!(fixture.is_none());
    }

    #[test]
    fn vortex_count_local_encoded_evidence_rejects_suffix_match_outside_workspace() {
        let outside_root = std::env::temp_dir().join(format!(
            "shardloom-outside-fixture-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let outside_fixture = outside_root
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        std::fs::create_dir_all(outside_fixture.parent().expect("outside fixture parent"))
            .expect("outside fixture directory");
        std::fs::write(&outside_fixture, b"copied fixture placeholder")
            .expect("outside fixture file");
        let uri = DatasetUri::new(outside_fixture.to_string_lossy().to_string()).expect("uri");

        let fixture = local_encoded_count_correctness_fixture_for_target(&uri);

        assert!(fixture.is_none());
        std::fs::remove_dir_all(outside_root).expect("outside fixture cleanup");
    }

    #[test]
    fn vortex_count_unknown_option_returns_non_zero() {
        let code = run(vec![
            "vortex-count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "--bogus".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_encoded_read_spike_execute_local_count_bridges_when_feature_enabled() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");

        let code = run(vec![
            "vortex-encoded-read-spike".to_string(),
            fixture_path.to_string_lossy().to_string(),
            "1".to_string(),
            "2".to_string(),
            "--execute-local-count".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);

        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_count_local_encoded_count_bridges_when_feature_enabled() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");

        let code = run(vec![
            "vortex-count".to_string(),
            fixture_path.to_string_lossy().to_string(),
            "--execute-local-encoded-count".to_string(),
            "1".to_string(),
            "2".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);

        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_physical_kernel_plan_ready_count_succeeds() {
        let code = run(vec![
            "vortex-metadata-physical-kernel-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "5".to_string(),
            "--correctness-evidence".to_string(),
            "--memory-safe".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_physical_kernel_plan_filter_admission_succeeds() {
        let code = run(vec![
            "vortex-metadata-physical-kernel-plan".to_string(),
            "filter".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "false".to_string(),
            "--correctness-evidence".to_string(),
            "--memory-safe".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_physical_kernel_plan_filter_admission_missing_evidence_blocks() {
        let code = run(vec![
            "vortex-metadata-physical-kernel-plan".to_string(),
            "filter".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "false".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_physical_kernel_plan_missing_evidence_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-physical-kernel-plan".to_string(),
            "count".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "5".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_metadata_physical_kernel_plan_unknown_option_returns_non_zero() {
        let code = run(vec![
            "vortex-metadata-physical-kernel-plan".to_string(),
            "filter".to_string(),
            "file:///tmp/example.vortex".to_string(),
            "false".to_string(),
            "--bogus".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cli_usage_preserves_specific_probe_and_artifact_write_names() {
        let usage = cli_usage_line();
        assert!(usage.contains("vortex-encoded-read-metadata-probe"));
        assert!(usage.contains("vortex-output-payload-artifact-write"));
        assert!(usage.contains("vortex-native-count-payload-write"));
    }

    #[test]
    fn cli_usage_execute_command_names_are_explicitly_scoped() {
        let usage = cli_usage_line();
        let execute_commands = usage.matches("-execute").count();
        assert_eq!(execute_commands, 4);
        assert!(usage.contains("vortex-encoded-read-execute"));
        assert!(usage.contains("vortex-metadata-execute"));
        assert!(usage.contains("vortex-local-commit-execute"));
        assert!(usage.contains("vortex-local-commit-rollback-execute"));
    }
    #[test]
    fn cli_contract_name_is_shardloom() {
        assert_eq!(cli_command_name(), "shardloom");
    }

    #[test]
    fn cli_contract_core_commands_dispatch_without_unknown_command_usage() {
        run_test_with_larger_stack("cli-contract-core-commands-dispatch", || {
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
        });
    }

    #[test]
    fn capabilities_certification_scope_dispatches_report_only() {
        let code = run(vec![
            "capabilities".to_string(),
            "certification".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn capabilities_sql_scope_dispatches_report_only() {
        let code = run(vec!["capabilities".to_string(), "sql".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn capabilities_unknown_scope_returns_non_zero() {
        let code = run(vec!["capabilities".to_string(), "unknown".to_string()]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn capabilities_extra_arg_returns_non_zero() {
        let code = run(vec![
            "capabilities".to_string(),
            "sql".to_string(),
            "extra".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn capabilities_usage_lists_certification_scopes() {
        let usage = cli_usage_line();
        assert!(usage.contains("capabilities [sql|functions|operators|adapters"));
        assert!(usage.contains("semantic-profiles|migration|certification"));
        assert!(usage.contains("data-etl|python|dataframe|notebook|udfs"));
        assert!(usage.contains("universal-adapters|event-api-saas-adapters"));
        assert!(usage.contains("unstructured-media|api-surfaces|observability"));
        assert!(usage.contains("deployment|extensions|security-governance"));
    }

    #[test]
    fn usage_includes_python_wrapper_plan() {
        assert!(cli_usage_line().contains("python-wrapper-plan"));
    }

    #[test]
    fn certification_discovery_fields_are_side_effect_free() {
        let report = CapabilityCertificationReport::contract_only();
        let fields = certification_fields(&report, CapabilityDiscoveryScope::Certification);
        assert!(
            fields
                .iter()
                .any(|(key, value)| { key == "fallback_execution_allowed" && value == "false" })
        );
        assert!(
            fields
                .iter()
                .any(|(key, value)| key == "side_effect_free" && value == "true")
        );
        assert!(
            fields
                .iter()
                .any(|(key, value)| key == "parser_executed" && value == "false")
        );
        assert!(
            fields
                .iter()
                .any(|(key, value)| key == "adapter_probe" && value == "false")
        );
        assert!(
            fields
                .iter()
                .any(|(key, value)| key == "runtime_execution" && value == "false")
        );
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
        let code = run_with_larger_stack(
            "spill-payload-roundtrip-test",
            vec![
                "spill-payload-roundtrip".to_string(),
                "/tmp/shardloom_spill_payload".to_string(),
                "payload-1".to_string(),
                "hello".to_string(),
            ],
        );
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
        let code = run_with_larger_stack(
            "cleanup-synthetic-payload-valid",
            vec![
                "cleanup-synthetic-payload".to_string(),
                "/tmp/shardloom_spill_payload".to_string(),
                "payload-1".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::from(2));
    }

    #[test]
    fn cleanup_synthetic_payload_invalid_payload_id_returns_non_zero() {
        let code = run_with_larger_stack(
            "cleanup-synthetic-payload-invalid-id",
            vec![
                "cleanup-synthetic-payload".to_string(),
                "/tmp/shardloom_spill_payload".to_string(),
                "../bad".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_missing_args_returns_non_zero() {
        let code = run_with_larger_stack(
            "cleanup-synthetic-payload-missing-args",
            vec!["cleanup-synthetic-payload".to_string()],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_too_many_args_returns_non_zero() {
        let code = run_with_larger_stack(
            "cleanup-synthetic-payload-too-many-args",
            vec![
                "cleanup-synthetic-payload".to_string(),
                "/tmp/shardloom_spill_payload".to_string(),
                "payload-1".to_string(),
                "extra".to_string(),
            ],
        );
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn cleanup_synthetic_payload_json_format_dispatches() {
        let code = run_with_larger_stack(
            "cleanup-synthetic-payload-json",
            vec![
                "cleanup-synthetic-payload".to_string(),
                "/tmp/shardloom_spill_payload".to_string(),
                "payload-1".to_string(),
                "--format".to_string(),
                "json".to_string(),
            ],
        );
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
