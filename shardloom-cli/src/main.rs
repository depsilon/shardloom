//! Command-line entry point and current JSON/text protocol surface for `ShardLoom`.
//!
//! The `CLI` exposes a mix of narrow executable local Vortex paths, explicit
//! feature-gated local artifact helpers, and many report-only planning or
//! promotion-gate commands. Commands must surface unsupported behavior
//! deterministically and keep external engines as baselines/oracles only, never
//! fallback execution. Handler modularization is tracked separately; this file
//! documents the current public posture while shared rendering and envelope
//! routing move into focused modules.

use std::process::ExitCode;

mod benchmark_planning;
mod benchmark_runtime;
mod cli_output;
mod cli_time;
mod command_family;
mod diagnostics;
mod engine_runtime_planning;
mod evidence_certificates;
mod extension_planning;
mod input_planning;
mod object_store_planning;
mod operational_hardening;
mod optimizer_planning;
mod packaging_deployment;
mod prepared_source_backed_execution;
mod rest_api_planning;
mod status_capabilities;
mod typed_envelope;
mod vortex_output_commit;
mod vortex_planning;
mod vortex_primitive_execution;
mod vortex_runtime_planning;
mod workflow_planning;

use cli_output::{emit, emit_error};
use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, ColumnRef, CommandStatus, ComparisonOp,
    CorrectnessFixture, CorrectnessValidationPlan, DatasetRef, DatasetUri, ExecutionCertificate,
    ExpectedOutcome, NativeIoCertificate, OperatorMemoryCertification, OutputFormat, PredicateExpr,
    ShardLoomError, StatValue,
};
use shardloom_exec::{
    AdaptiveSizingPolicy, BoundedMemoryPolicy, ByteSize, EncodedStreamingBatchPlanInput,
    EncodedStreamingBatchPlanReport, MemoryBudget, StreamingSink, StreamingSource,
    plan_encoded_streaming_batches,
};
use shardloom_plan::ProjectionRequest;
use shardloom_vortex::{
    VortexBoundedExecutionReport, VortexCommitIntentSignal, VortexCommitMarkerSignal,
    VortexCommitMarkerWriteOption, VortexCommitProtocolSignal, VortexCommitProtocolState,
    VortexCommitProtocolTransition, VortexCountCandidateSource, VortexCountReadinessRequest,
    VortexEncodedCountKernelAdmissionReport, VortexEncodedCountPhysicalKernelReport,
    VortexEncodedPredicateEvaluationReport, VortexEncodedPredicateEvaluationStatus,
    VortexEncodedReadBoundaryReport, VortexEncodedReadBoundarySignal,
    VortexEncodedReadExecutionMode, VortexEncodedReadExecutionStatus,
    VortexEncodedReadExecutorFeatureStatus, VortexEncodedReadMetadataProbeReport,
    VortexEncodedReadMetadataProbeSignal, VortexExecutionReadinessStatus,
    VortexFinalizedManifestArtifactWriteOption, VortexFinalizedManifestContent,
    VortexLayoutReaderDriverApprovalSignal, VortexLocalCommitExecutionSignal,
    VortexLocalCommitRecoverySignal, VortexLocalEngineWhyReport, VortexLocalExecutionReport,
    VortexLocalExecutionStatus, VortexLocalPrimitiveExecutionPolicy,
    VortexLocalPrimitiveExecutionReport, VortexManifestFinalizationSignal,
    VortexMetadataCountKernelAdmissionReport, VortexMetadataFilterKernelAdmissionReport,
    VortexMetadataSummaryReport, VortexOutputPayloadSignal, VortexProjectionReadinessSignal,
    VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult, VortexQueryPrimitiveValue,
    VortexSelectionVectorFilterKernelAdmissionReport, VortexSelectionVectorFilterKernelReport,
    VortexStagedManifestDraftContent, VortexStagedManifestFileSignal,
    VortexStagedManifestFileWriteOption, VortexStagedManifestFileWriteSignal,
    VortexStagedMarkerOption, VortexStagedWorkspaceSetupOption, VortexStreamingBatchRuntimeReport,
    VortexTaskSchedulingDecision, VortexWorkAvoidedMetricKind, VortexWorkAvoidedReport,
    admit_vortex_encoded_count_kernel, admit_vortex_metadata_count_kernel,
    admit_vortex_metadata_filter_kernel, admit_vortex_selection_vector_filter_kernel,
    build_vortex_runtime_task_graph, evaluate_vortex_encoded_predicate_segments,
    evaluate_vortex_encoded_read_readiness, evaluate_vortex_local_encoded_count_physical_kernel,
    evaluate_vortex_metadata_physical_kernels, evaluate_vortex_selection_vector_filter_kernel,
    execute_vortex_count_all_from_approved_local_scan,
    execute_vortex_count_all_from_approved_local_scan_result, execute_vortex_encoded_read_spike,
    execute_vortex_local_primitive_with_policy, local_encoded_count_execution_certificate,
    local_encoded_count_native_io_certificate, local_primitive_execution_certificate,
    local_primitive_native_io_certificate, plan_native_vortex_universal_input,
    plan_vortex_count_readiness, plan_vortex_encoded_count_data_path_approval,
    plan_vortex_encoded_read_probe, plan_vortex_memory_safety,
    plan_vortex_query_primitive_result_physical_operators_with_evidence,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    size_vortex_runtime_task_graph, vortex_encoded_read_local_scan_count_api_boundary,
    vortex_encoded_read_public_api_boundary, vortex_encoded_read_spike_feature_enabled,
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
        "usage: {} <status|release-plan|package-plan|api-compat-plan|agent-contract-pack|python-wrapper-plan|capabilities [sql|functions|operators|adapters|semantic-profiles|migration|certification|data-etl|python|dataframe|notebook|udfs|universal-adapters|event-api-saas-adapters|unstructured-media|api-surfaces|observability|deployment|extensions|security-governance]|security-plan|security-governance-evidence-gate|effect-budget-plan|agent-safety-plan|redaction-plan|kernel-registry|feature-footprint|doctor|manifest-plan|incremental-plan|stateful-reuse-plan|cg17-stateful-reuse-gate|universal-harness-plan|rfc-coverage-followthrough-plan|native-io-envelope-plan|world-class-sufficiency-plan|cg20-user-capability-gate|cg20-approx-sketch-gate|layout-health-plan|compaction-plan|table-intelligence-plan|cg9-catalog-metadata-gate|object-store-request-plan|cg10-object-store-runtime-gate|object-store-range-plan|object-store-coalesce-plan|object-store-schedule-plan|object-store-checkpoint-retry-plan|object-store-commit-plan|write-intent|scan-plan|streaming-plan|streaming-batch-plan|backpressure-plan|runtime-plan|task-plan|sizing-plan|sizing-feedback-plan|dynamic-work-shaping-plan [balanced|memory-pressure|object-store-throttled|small-tasks]|cg8-runtime-promotion-gate|translation-plan|vortex-plan|vortex-output-plan|vortex-readiness|vortex-api-inventory|vortex-dtype-mapping|vortex-encoding-layout-mapping|vortex-statistics-mapping|vortex-metadata-probe|vortex-file-metadata-open|vortex-metadata-summary|vortex-metadata-plan|vortex-pruning-plan|optimizer-plan|optimizer-adaptive-memory-plan|cpu-specialization-plan|explain|estimate|benchmark-plan|benchmark-claim-evidence-plan [foundation|traditional-analytics]|traditional-analytics-run|traditional-analytics-vortex-run|vortex-count-benchmark|correctness-plan|correctness-harness-plan|execution-certificate-plan|recovery-plan|commit-execution-promotion-gate|fault-tolerance-promotion-gate|cancellation-plan|retry-plan|observability-plan|observability-schema-coverage|runtime-report|profile-plan|plan-ir|plan-import|plan-export|table-compat-plan [aggregate|partition-evolution|delete-semantics]|schema-plan|input-adapters|input-plan|vortex-input-plan|vortex-read-plan|vortex-task-graph|vortex-adaptive-sizing|vortex-memory-plan|vortex-schedule-plan|vortex-execution-readiness|vortex-encoded-path-selection-plan|vortex-generalized-encoded-primitive-gate|vortex-encoded-read-api|vortex-encoded-read-boundary|vortex-encoded-read-metadata-probe|vortex-encoded-read-readiness|vortex-encoded-read-probe|vortex-encoded-read-execute|vortex-encoded-read-spike|vortex-dry-run|vortex-metadata-execute|vortex-query-primitive-plan|vortex-metadata-physical-kernel-plan|vortex-count-readiness-plan|vortex-encoded-count-approval-plan|vortex-layout-driver-approval-plan|vortex-filtered-count-readiness-plan|vortex-projection-readiness-plan|vortex-count|vortex-count-where|vortex-staged-workspace-setup|vortex-staged-marker-write|vortex-staged-manifest-file-plan|vortex-staged-manifest-file-write|vortex-output-payload-plan|vortex-output-payload-artifact-write|vortex-native-count-payload-write|vortex-manifest-finalization-plan|vortex-finalized-manifest-artifact-write|vortex-commit-marker-plan|vortex-commit-marker-write|vortex-commit-intent-plan|vortex-commit-protocol-plan|vortex-local-commit-execute|vortex-local-commit-recovery-plan|vortex-local-commit-rollback-execute|vortex-project|vortex-filter|vortex-filter-project|vortex-query-trace|vortex-local-exec|vortex-bounded-local-exec|vortex-run|operator-memory-spill-declarations|cg14-memory-runtime-hardening-gate|spill-lifecycle|spill-reservation-plan|spill-payload-roundtrip|cleanup-synthetic-payload|retry-gate-plan <signals>|cancellation-gate-plan <signals>> [--format text|json]",
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

pub(crate) fn parse_vortex_encoded_read_boundary_signals(
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

pub(crate) fn parse_vortex_encoded_read_metadata_probe_signals(
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

pub(crate) fn parse_vortex_layout_driver_approval_signals(
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

pub(crate) fn vortex_encoded_read_boundary_fields(
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

pub(crate) fn vortex_encoded_read_metadata_probe_fields(
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

pub(crate) fn cli_missing_arg_error(command: &str, arg: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("{command} missing required argument: <{arg}>"))
}

pub(crate) fn cli_unknown_arg_error(command: &str, value: &str) -> ShardLoomError {
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
pub(crate) fn run_vortex_metadata_physical_kernel_plan(
    format: OutputFormat,
    args: Vec<String>,
) -> ExitCode {
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

pub(crate) fn append_vortex_work_avoided_fields(
    fields: &mut Vec<(String, String)>,
    report: Option<&VortexWorkAvoidedReport>,
) {
    let Some(report) = report else {
        push_count_field(fields, "work_avoided_metrics", 0);
        push_count_field(fields, "work_avoided_known_metrics", 0);
        push_count_field(fields, "work_avoided_unknown_metrics", 0);
        return;
    };
    push_count_field(fields, "work_avoided_metrics", report.metric_count());
    push_count_field(
        fields,
        "work_avoided_known_metrics",
        report.known_metric_count(),
    );
    push_count_field(
        fields,
        "work_avoided_unknown_metrics",
        report.unknown_metric_count(),
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::DecodeAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::MaterializationAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::ObjectStoreRequestsAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::SpillAvoided,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::FallbackBlocked,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::RowsNotScanned,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::SegmentsPruned,
    );
    append_vortex_work_avoided_metric_fields(
        fields,
        report,
        VortexWorkAvoidedMetricKind::BytesNotRead,
    );
}

fn append_vortex_work_avoided_metric_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexWorkAvoidedReport,
    kind: VortexWorkAvoidedMetricKind,
) {
    let stem = format!("work_avoided_{}", kind.as_str());
    push_field(fields, &stem, &report.metric_value_summary(kind));
    push_field(
        fields,
        &format!("{stem}_known"),
        &report.metric_known_summary(kind),
    );
    push_field(
        fields,
        &format!("{stem}_reason"),
        &report.metric_reason_summary(kind),
    );
}

pub(crate) fn reconcile_vortex_local_engine_why_with_execution_certificate(
    report: &mut VortexLocalEngineWhyReport,
    certificate: Option<&ExecutionCertificate>,
) {
    if !certificate.is_some_and(ExecutionCertificate::is_certified) {
        return;
    }
    report
        .next_actions
        .retain(|action| action != "attach CG-16 execution certificate evidence");
    if !report
        .supporting_evidence
        .iter()
        .any(|evidence| evidence == "cg16_execution_certificate=certified")
    {
        report
            .supporting_evidence
            .push("cg16_execution_certificate=certified".to_string());
    }
}

pub(crate) fn append_vortex_local_engine_why_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexLocalEngineWhyReport,
) {
    push_field(fields, "why_report_present", "true");
    push_field(fields, "why_schema_version", report.schema_version);
    push_field(fields, "why_report_id", report.report_id);
    push_field(fields, "why_claim_gate_status", report.claim_gate_status);
    push_field(fields, "why_primary_reason", &report.primary_reason);
    push_count_field(fields, "why_blocker_count", report.blocker_count());
    push_field(fields, "why_blockers", &report.blockers_summary());
    push_count_field(
        fields,
        "why_supporting_evidence_count",
        report.supporting_evidence_count(),
    );
    push_field(
        fields,
        "why_supporting_evidence",
        &report.supporting_evidence_summary(),
    );
    push_count_field(fields, "why_next_action_count", report.next_action_count());
    push_field(fields, "why_next_actions", &report.next_actions_summary());
    push_count_field(
        fields,
        "decision_trace_entries",
        report.decision_trace_entries,
    );
    push_count_field(
        fields,
        "why_work_avoided_metrics",
        report.work_avoided_metrics,
    );
    push_bool_field(fields, "why_fallback_attempted", report.fallback_attempted);
}

pub(crate) fn bounded_local_execution_fields(
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

pub(crate) fn readiness_is_blocked(status: VortexExecutionReadinessStatus) -> bool {
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

pub(crate) fn parse_tiny_predicate(value: &str) -> Result<PredicateExpr, ShardLoomError> {
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

pub(crate) struct VortexCountWhereFilterEvidence {
    predicate_evaluation: VortexEncodedPredicateEvaluationReport,
    filter_kernel: VortexSelectionVectorFilterKernelReport,
    filter_kernel_admission: VortexSelectionVectorFilterKernelAdmissionReport,
}

pub(crate) struct VortexLocalPrimitiveCliExecutionRequest {
    memory_gb: u64,
    max_parallelism: usize,
}

pub(crate) type VortexCountWhereLocalExecutionRequest = VortexLocalPrimitiveCliExecutionRequest;
pub(crate) type VortexCountWhereLocalExecutionEvidence = VortexLocalPrimitiveCliExecutionEvidence;

pub(crate) struct VortexLocalPrimitiveCliExecutionEvidence {
    pub(crate) memory_gb: u64,
    pub(crate) max_parallelism: usize,
    pub(crate) report: VortexLocalPrimitiveExecutionReport,
    pub(crate) native_io_certificate: NativeIoCertificate,
    pub(crate) execution_certificate: Option<ExecutionCertificate>,
}
impl VortexLocalPrimitiveCliExecutionEvidence {
    pub(crate) fn has_errors(&self) -> bool {
        self.report.has_errors() || !self.native_io_certificate.is_certified()
    }

    pub(crate) fn count(&self) -> Option<u64> {
        self.report.rows_selected
    }

    pub(crate) fn selected_rows(&self) -> Option<u64> {
        self.report.rows_selected
    }

    pub(crate) fn projected_rows(&self) -> Option<u64> {
        self.report.rows_projected
    }

    pub(crate) fn selection_vector_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->selection_vector_encoded"
            && self.report.filter_pushdown_applied
            && self.report.upstream_filter_expression_used
            && self.report.rows_selected.is_some()
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    fn projection_encoded_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->vortex_encoded"
            && self.projection_evidence_guaranteed()
            && self.report.rows_projected.is_some()
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    fn filter_project_encoded_guaranteed(&self) -> bool {
        self.native_io_certificate.is_certified()
            && self.native_io_certificate.representation_transition_order()
                == "vortex_encoded->selection_vector_encoded"
            && self.report.filter_pushdown_applied
            && self.report.upstream_filter_expression_used
            && self.projection_evidence_guaranteed()
            && self.report.rows_selected.is_some()
            && self.report.rows_projected.is_some()
            && self.report.rows_selected == self.report.rows_projected
            && !self.report.data_decoded
            && !self.report.data_materialized
            && !self.report.row_read
            && !self.report.arrow_converted
            && !self.report.object_store_io
            && !self.report.write_io
            && !self.report.spill_io_performed
            && !self.report.external_effects_executed
            && !self.report.fallback_execution_allowed
    }

    fn projection_evidence_guaranteed(&self) -> bool {
        !self.report.projected_columns.is_empty()
            && ((self.report.projection_pushdown_applied
                && self.report.upstream_projection_expression_used)
                || (self.report.projected_columns.len() == 1
                    && self.report.projected_columns[0] == "value"
                    && !self.report.projection_pushdown_applied
                    && !self.report.upstream_projection_expression_used))
    }
}

pub(crate) fn parse_vortex_count_where_local_execution_args(
    mut args: impl Iterator<Item = String>,
) -> shardloom_core::Result<Option<VortexCountWhereLocalExecutionRequest>> {
    parse_vortex_local_primitive_cli_execution_args(&mut args)
}

pub(crate) fn parse_vortex_local_primitive_cli_execution_args(
    args: &mut impl Iterator<Item = String>,
) -> shardloom_core::Result<Option<VortexLocalPrimitiveCliExecutionRequest>> {
    let Some(option) = args.next() else {
        return Ok(None);
    };
    if option != "--execute-local-primitive" {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unknown option: {option}"
        )));
    }
    let Some(memory_gb_text) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(
            "missing memory_gb after --execute-local-primitive".to_string(),
        ));
    };
    let Some(max_parallelism_text) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(
            "missing max_parallelism after --execute-local-primitive".to_string(),
        ));
    };
    if let Some(extra) = args.next() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "unknown option: {extra}"
        )));
    }
    let memory_gb = memory_gb_text.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation("memory_gb must be an unsigned integer".to_string())
    })?;
    if memory_gb == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "memory_gb must be >= 1".to_string(),
        ));
    }
    let max_parallelism = max_parallelism_text.parse::<usize>().map_err(|_| {
        ShardLoomError::InvalidOperation("max_parallelism must be an unsigned integer".to_string())
    })?;
    VortexLocalPrimitiveExecutionPolicy::new(max_parallelism)?;
    Ok(Some(VortexLocalPrimitiveCliExecutionRequest {
        memory_gb,
        max_parallelism,
    }))
}

pub(crate) fn vortex_count_where_filter_evidence(
    predicate: &PredicateExpr,
    summary: &VortexMetadataSummaryReport,
) -> shardloom_core::Result<VortexCountWhereFilterEvidence> {
    let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(predicate, summary);
    let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);
    let filter_kernel_admission = admit_vortex_selection_vector_filter_kernel(&filter_kernel)?;
    Ok(VortexCountWhereFilterEvidence {
        predicate_evaluation,
        filter_kernel,
        filter_kernel_admission,
    })
}

pub(crate) fn vortex_count_where_local_execution_evidence(
    request: &VortexQueryPrimitiveRequest,
    local_request: &VortexCountWhereLocalExecutionRequest,
) -> shardloom_core::Result<VortexCountWhereLocalExecutionEvidence> {
    vortex_local_primitive_cli_execution_evidence(request, local_request)
}

pub(crate) fn vortex_local_primitive_cli_execution_evidence(
    request: &VortexQueryPrimitiveRequest,
    local_request: &VortexLocalPrimitiveCliExecutionRequest,
) -> shardloom_core::Result<VortexLocalPrimitiveCliExecutionEvidence> {
    let _memory_budget = MemoryBudget::from_gib(local_request.memory_gb)?;
    let policy = VortexLocalPrimitiveExecutionPolicy::new(local_request.max_parallelism)?;
    let report = execute_vortex_local_primitive_with_policy(request, policy)?;
    let native_io_certificate = local_primitive_native_io_certificate(request, &report)?;
    let execution_certificate = local_primitive_correctness_fixture_for_request(request, &report)
        .map(|fixture| local_primitive_execution_certificate(&fixture, request, &report))
        .transpose()?;
    Ok(VortexLocalPrimitiveCliExecutionEvidence {
        memory_gb: local_request.memory_gb,
        max_parallelism: local_request.max_parallelism,
        report,
        native_io_certificate,
        execution_certificate,
    })
}

pub(crate) fn vortex_count_where_human_text(
    result: &VortexQueryPrimitiveResult,
    evidence: &VortexCountWhereFilterEvidence,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) -> String {
    let mut sections = vec![
        result.to_human_text(),
        evidence.predicate_evaluation.to_human_text(),
        evidence.filter_kernel.to_human_text(),
        evidence.filter_kernel_admission.to_human_text(),
    ];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_count_where_fields(
    result: &VortexQueryPrimitiveResult,
    count: Option<u64>,
    predicate_arg: String,
    evidence: &VortexCountWhereFilterEvidence,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let execution = local_execution.map_or(
        "metadata_or_selection_vector_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_count_where_primitive_performed".to_string()
            } else {
                "local_vortex_count_where_primitive_not_performed".to_string()
            }
        },
    );
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_count_where".to_string()),
        ("primitive".to_string(), "count_where".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        (
            "metadata_query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), count.is_some().to_string()),
        (
            "count".to_string(),
            count.map_or_else(|| "unknown".to_string(), |v| v.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
    ];
    append_vortex_count_where_filter_evidence_fields(&mut fields, evidence);
    append_vortex_count_where_local_execution_fields(&mut fields, local_execution);
    fields
}

fn append_vortex_count_where_filter_evidence_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountWhereFilterEvidence,
) {
    append_vortex_count_where_predicate_evidence_fields(fields, &evidence.predicate_evaluation);
    append_vortex_count_where_filter_kernel_fields(fields, &evidence.filter_kernel);
    append_vortex_count_where_filter_admission_fields(fields, &evidence.filter_kernel_admission);
    push_bool_field(
        fields,
        "filtered_count_selection_vector_evidence_present",
        evidence
            .filter_kernel
            .is_safe_native_filter_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "filtered_count_generalized_execution_allowed",
        false,
    );
    push_bool_field(fields, "filtered_count_production_claim_allowed", false);
    push_bool_field(
        fields,
        "filtered_count_requires_encoded_value_kernel",
        evidence.predicate_evaluation.status
            == VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues,
    );
    push_bool_field(fields, "filtered_count_requires_benchmark_evidence", true);
    push_bool_field(fields, "filtered_count_cg2_closeout_allowed", false);
    push_bool_field(fields, "filtered_count_cg13_closeout_allowed", false);
}

fn append_vortex_count_where_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) {
    append_vortex_count_where_local_execution_request_fields(fields, local_execution);
    match local_execution {
        Some(local) => append_vortex_count_where_local_execution_present_fields(fields, local),
        None => append_vortex_count_where_local_execution_absent_fields(fields),
    }
}

fn append_vortex_count_where_local_execution_request_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexCountWhereLocalExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
}

fn append_vortex_count_where_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "filtered_count_local_execution_status",
        "not_requested",
    );
    push_field(
        fields,
        "filtered_count_local_execution_mode",
        "not_requested",
    );
    push_u64_field(fields, "filtered_count_local_execution_memory_gb", 0);
    push_count_field(fields, "filtered_count_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filtered_count_local_execution_result_known", false);
    push_field(fields, "filtered_count_local_execution_count", "unknown");
    append_vortex_count_where_local_execution_absent_effect_fields(fields);
    append_vortex_count_where_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_count_where_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    append_vortex_count_where_local_execution_report_fields(fields, local);
    append_vortex_count_where_local_execution_effect_fields(fields, local);
    append_vortex_count_where_local_execution_claim_fields(
        fields,
        local.selection_vector_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_count_where_local_execution_report_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    push_field(
        fields,
        "filtered_count_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(
        fields,
        "filtered_count_local_execution_memory_gb",
        local.memory_gb,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_result_known",
        local.count().is_some(),
    );
    push_field(
        fields,
        "filtered_count_local_execution_count",
        &local
            .count()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_u64_field(
        fields,
        "filtered_count_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_field(
        fields,
        "filtered_count_local_execution_rows_selected",
        &local
            .report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filtered_count_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
}

fn append_vortex_count_where_local_execution_absent_effect_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_bool_field(fields, "filtered_count_local_execution_data_read", false);
    push_bool_field(fields, "filtered_count_local_execution_data_decoded", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_materialized",
        false,
    );
    push_bool_field(fields, "filtered_count_local_execution_row_read", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_arrow_converted",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_object_store_io",
        false,
    );
    push_bool_field(fields, "filtered_count_local_execution_write_io", false);
    push_bool_field(
        fields,
        "filtered_count_local_execution_spill_io_performed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_attempted",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_count_where_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexCountWhereLocalExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    selection_vector_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "filtered_count_local_execution_selection_vector_guarantee",
        selection_vector_guarantee,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filtered_count_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_project_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_project_fields(
    result: &VortexQueryPrimitiveResult,
    columns_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let row_read = local_execution.is_some_and(|local| local.report.row_read);
    let arrow_converted = local_execution.is_some_and(|local| local.report.arrow_converted);
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_or_projection_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_project_primitive_performed".to_string()
            } else {
                "local_vortex_project_primitive_not_performed".to_string()
            }
        },
    );
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_project".to_string()),
        ("primitive".to_string(), "project_columns".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("row_read".to_string(), row_read.to_string()),
        ("arrow_converted".to_string(), arrow_converted.to_string()),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_projected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("columns".to_string(), columns_arg),
    ];
    append_vortex_project_local_execution_fields(&mut fields, local_execution);
    fields
}

fn append_vortex_project_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "project_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "project_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "project_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_project_local_execution_present_fields(fields, local),
        None => append_vortex_project_local_execution_absent_fields(fields),
    }
}

fn append_vortex_project_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(fields, "project_local_execution_status", "not_requested");
    push_field(fields, "project_local_execution_mode", "not_requested");
    push_u64_field(fields, "project_local_execution_memory_gb", 0);
    push_count_field(fields, "project_local_execution_max_parallelism", 0);
    push_bool_field(fields, "project_local_execution_result_known", false);
    push_field(fields, "project_local_execution_rows_projected", "unknown");
    push_field(fields, "project_local_execution_projected_columns", "");
    append_vortex_project_local_execution_absent_effect_fields(fields);
    append_vortex_project_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_project_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "project_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "project_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(fields, "project_local_execution_memory_gb", local.memory_gb);
    push_count_field(
        fields,
        "project_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "project_local_execution_result_known",
        local.projected_rows().is_some(),
    );
    push_field(
        fields,
        "project_local_execution_rows_projected",
        &local
            .projected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "project_local_execution_projected_columns",
        &local.report.projected_columns.join(","),
    );
    push_u64_field(
        fields,
        "project_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "project_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "project_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "project_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "project_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "project_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "project_local_execution_projection_pushdown_applied",
        local.report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "project_local_execution_upstream_projection_expression_used",
        local.report.upstream_projection_expression_used,
    );
    append_vortex_project_local_execution_effect_fields(fields, local);
    append_vortex_project_local_execution_claim_fields(
        fields,
        local.projection_encoded_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_project_local_execution_absent_effect_fields(fields: &mut Vec<(String, String)>) {
    push_bool_field(fields, "project_local_execution_data_read", false);
    push_bool_field(fields, "project_local_execution_data_decoded", false);
    push_bool_field(fields, "project_local_execution_data_materialized", false);
    push_bool_field(fields, "project_local_execution_row_read", false);
    push_bool_field(fields, "project_local_execution_arrow_converted", false);
    push_bool_field(fields, "project_local_execution_object_store_io", false);
    push_bool_field(fields, "project_local_execution_write_io", false);
    push_bool_field(fields, "project_local_execution_spill_io_performed", false);
    push_bool_field(fields, "project_local_execution_fallback_attempted", false);
    push_bool_field(
        fields,
        "project_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_project_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "project_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "project_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "project_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "project_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "project_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "project_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "project_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "project_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "project_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "project_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "project_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_project_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    encoded_projection_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "project_local_execution_encoded_projection_guarantee",
        encoded_projection_guarantee,
    );
    push_bool_field(
        fields,
        "project_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "project_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "project_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "project_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_filter_project_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_filter_project_fields(
    result: &VortexQueryPrimitiveResult,
    predicate_arg: String,
    columns_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let row_read = local_execution.is_some_and(|local| local.report.row_read);
    let arrow_converted = local_execution.is_some_and(|local| local.report.arrow_converted);
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_filter_project_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_filter_project_primitive_performed".to_string()
            } else {
                "local_vortex_filter_project_primitive_not_performed".to_string()
            }
        },
    );
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_filter_project".to_string()),
        ("primitive".to_string(), "filter_and_project".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("row_read".to_string(), row_read.to_string()),
        ("arrow_converted".to_string(), arrow_converted.to_string()),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_selected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        (
            "rows_projected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::projected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
        ("columns".to_string(), columns_arg),
    ];
    append_vortex_filter_project_local_execution_fields(&mut fields, local_execution);
    fields
}

fn append_vortex_filter_project_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filter_project_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filter_project_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_filter_project_local_execution_present_fields(fields, local),
        None => append_vortex_filter_project_local_execution_absent_fields(fields),
    }
}

fn append_vortex_filter_project_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(
        fields,
        "filter_project_local_execution_status",
        "not_requested",
    );
    push_field(
        fields,
        "filter_project_local_execution_mode",
        "not_requested",
    );
    push_u64_field(fields, "filter_project_local_execution_memory_gb", 0);
    push_count_field(fields, "filter_project_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filter_project_local_execution_result_known", false);
    push_field(
        fields,
        "filter_project_local_execution_rows_selected",
        "unknown",
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_projected",
        "unknown",
    );
    push_field(
        fields,
        "filter_project_local_execution_projected_columns",
        "",
    );
    append_vortex_filter_project_local_execution_absent_effect_fields(fields);
    append_vortex_filter_project_local_execution_claim_fields(fields, None);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_filter_project_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "filter_project_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filter_project_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(
        fields,
        "filter_project_local_execution_memory_gb",
        local.memory_gb,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_result_known",
        local.projected_rows().is_some(),
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_selected",
        &local
            .selected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "filter_project_local_execution_rows_projected",
        &local
            .projected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "filter_project_local_execution_projected_columns",
        &local.report.projected_columns.join(","),
    );
    append_vortex_filter_project_local_execution_scan_fields(fields, local);
    append_vortex_filter_project_local_execution_effect_fields(fields, local);
    append_vortex_filter_project_local_execution_claim_fields(fields, Some(local));
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_filter_project_local_execution_scan_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_u64_field(
        fields,
        "filter_project_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filter_project_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_projection_pushdown_applied",
        local.report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_upstream_projection_expression_used",
        local.report.upstream_projection_expression_used,
    );
}

fn append_vortex_filter_project_local_execution_absent_effect_fields(
    fields: &mut Vec<(String, String)>,
) {
    push_bool_field(fields, "filter_project_local_execution_data_read", false);
    push_bool_field(fields, "filter_project_local_execution_data_decoded", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_data_materialized",
        false,
    );
    push_bool_field(fields, "filter_project_local_execution_row_read", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_arrow_converted",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_object_store_io",
        false,
    );
    push_bool_field(fields, "filter_project_local_execution_write_io", false);
    push_bool_field(
        fields,
        "filter_project_local_execution_spill_io_performed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_attempted",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_filter_project_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filter_project_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_filter_project_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    let encoded_guarantee = local_execution
        .is_some_and(VortexLocalPrimitiveCliExecutionEvidence::filter_project_encoded_guaranteed);
    let native_io_certified =
        local_execution.is_some_and(|local| local.native_io_certificate.is_certified());
    let correctness_certified = local_execution.is_some_and(|local| {
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified)
    });
    push_bool_field(
        fields,
        "filter_project_local_execution_selection_vector_guarantee",
        encoded_guarantee,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_projection_pushdown_guarantee",
        encoded_guarantee,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_project_local_execution_cg13_closeout_allowed",
        false,
    );
}

pub(crate) fn vortex_filter_human_text(
    result: &VortexQueryPrimitiveResult,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> String {
    let mut sections = vec![result.to_human_text()];
    if let Some(local) = local_execution {
        sections.push(local.report.to_human_text());
        sections.push(local_primitive_native_io_certificate_human_text(
            &local.native_io_certificate,
        ));
        if let Some(certificate) = &local.execution_certificate {
            sections.push(certificate.to_human_text());
        }
    }
    sections.join("\n\n")
}

pub(crate) fn vortex_filter_fields(
    result: &VortexQueryPrimitiveResult,
    predicate_arg: String,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) -> Vec<(String, String)> {
    let data_read = local_execution.map_or(result.data_read, |local| local.report.data_read);
    let data_decoded =
        local_execution.map_or(result.data_decoded, |local| local.report.data_decoded);
    let data_materialized = local_execution.map_or(result.data_materialized, |local| {
        local.report.data_materialized
    });
    let object_store_io =
        local_execution.map_or(result.object_store_io, |local| local.report.object_store_io);
    let write_io = local_execution.map_or(result.write_io, |local| local.report.write_io);
    let spill_io_performed = local_execution.map_or(result.spill_io_performed, |local| {
        local.report.spill_io_performed
    });
    let result_known = local_execution
        .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
        .is_some()
        || result.value.is_known();
    let execution = local_execution.map_or(
        "metadata_or_selection_vector_evidence_only".to_string(),
        |local| {
            if local.report.data_read {
                "local_vortex_filter_primitive_performed".to_string()
            } else {
                "local_vortex_filter_primitive_not_performed".to_string()
            }
        },
    );
    let mut fields = vec![
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("mode".to_string(), "vortex_filter".to_string()),
        ("primitive".to_string(), "filter_predicate".to_string()),
        ("data_read".to_string(), data_read.to_string()),
        ("data_decoded".to_string(), data_decoded.to_string()),
        (
            "data_materialized".to_string(),
            data_materialized.to_string(),
        ),
        ("object_store_io".to_string(), object_store_io.to_string()),
        ("write_io".to_string(), write_io.to_string()),
        (
            "spill_io_performed".to_string(),
            spill_io_performed.to_string(),
        ),
        ("execution".to_string(), execution),
        (
            "query_primitive_status".to_string(),
            result.status.as_str().to_string(),
        ),
        ("result_known".to_string(), result_known.to_string()),
        (
            "rows_selected".to_string(),
            local_execution
                .and_then(VortexLocalPrimitiveCliExecutionEvidence::selected_rows)
                .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
        ),
        ("predicate".to_string(), predicate_arg),
    ];
    append_vortex_filter_local_execution_fields(&mut fields, local_execution);
    fields
}

fn append_vortex_filter_local_execution_fields(
    fields: &mut Vec<(String, String)>,
    local_execution: Option<&VortexLocalPrimitiveCliExecutionEvidence>,
) {
    push_bool_field(
        fields,
        "filter_local_execution_requested",
        local_execution.is_some(),
    );
    push_field(
        fields,
        "filter_local_execution_feature_gate",
        "vortex-local-primitives",
    );
    push_bool_field(
        fields,
        "filter_local_execution_feature_enabled",
        cfg!(feature = "vortex-local-primitives"),
    );
    match local_execution {
        Some(local) => append_vortex_filter_local_execution_present_fields(fields, local),
        None => append_vortex_filter_local_execution_absent_fields(fields),
    }
}

fn append_vortex_filter_local_execution_absent_fields(fields: &mut Vec<(String, String)>) {
    push_field(fields, "filter_local_execution_status", "not_requested");
    push_field(fields, "filter_local_execution_mode", "not_requested");
    push_u64_field(fields, "filter_local_execution_memory_gb", 0);
    push_count_field(fields, "filter_local_execution_max_parallelism", 0);
    push_bool_field(fields, "filter_local_execution_result_known", false);
    push_field(fields, "filter_local_execution_rows_selected", "unknown");
    append_vortex_filter_local_execution_absent_effect_fields(fields);
    append_vortex_filter_local_execution_claim_fields(fields, false, false, false);
    append_vortex_local_primitive_native_io_certificate_fields(fields, None);
    append_vortex_local_primitive_execution_certificate_fields(fields, None);
}

fn append_vortex_filter_local_execution_present_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_field(
        fields,
        "filter_local_execution_status",
        local.report.status.as_str(),
    );
    push_field(
        fields,
        "filter_local_execution_mode",
        local.report.mode.as_str(),
    );
    push_u64_field(fields, "filter_local_execution_memory_gb", local.memory_gb);
    push_count_field(
        fields,
        "filter_local_execution_max_parallelism",
        local.max_parallelism,
    );
    push_bool_field(
        fields,
        "filter_local_execution_result_known",
        local.selected_rows().is_some(),
    );
    push_field(
        fields,
        "filter_local_execution_rows_selected",
        &local
            .selected_rows()
            .map_or_else(|| "unknown".to_string(), |value| value.to_string()),
    );
    push_u64_field(
        fields,
        "filter_local_execution_rows_scanned",
        local.report.rows_scanned,
    );
    push_count_field(
        fields,
        "filter_local_execution_arrays_read_count",
        local.report.arrays_read_count,
    );
    push_count_field(
        fields,
        "filter_local_execution_max_chunk_rows",
        local.report.max_chunk_rows,
    );
    push_count_field(
        fields,
        "filter_local_execution_scan_concurrency_per_worker",
        local.report.scan_concurrency_per_worker,
    );
    push_bool_field(
        fields,
        "filter_local_execution_streaming_scan_used",
        local.report.streaming_scan_used,
    );
    push_bool_field(
        fields,
        "filter_local_execution_full_stream_collected",
        local.report.full_stream_collected,
    );
    push_bool_field(
        fields,
        "filter_local_execution_filter_pushdown_applied",
        local.report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "filter_local_execution_upstream_filter_expression_used",
        local.report.upstream_filter_expression_used,
    );
    append_vortex_filter_local_execution_effect_fields(fields, local);
    append_vortex_filter_local_execution_claim_fields(
        fields,
        local.selection_vector_guaranteed(),
        local.native_io_certificate.is_certified(),
        local
            .execution_certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&local.native_io_certificate),
    );
    append_vortex_local_primitive_execution_certificate_fields(
        fields,
        local.execution_certificate.as_ref(),
    );
}

fn append_vortex_filter_local_execution_absent_effect_fields(fields: &mut Vec<(String, String)>) {
    push_bool_field(fields, "filter_local_execution_data_read", false);
    push_bool_field(fields, "filter_local_execution_data_decoded", false);
    push_bool_field(fields, "filter_local_execution_data_materialized", false);
    push_bool_field(fields, "filter_local_execution_row_read", false);
    push_bool_field(fields, "filter_local_execution_arrow_converted", false);
    push_bool_field(fields, "filter_local_execution_object_store_io", false);
    push_bool_field(fields, "filter_local_execution_write_io", false);
    push_bool_field(fields, "filter_local_execution_spill_io_performed", false);
    push_bool_field(fields, "filter_local_execution_fallback_attempted", false);
    push_bool_field(
        fields,
        "filter_local_execution_fallback_execution_allowed",
        false,
    );
}

fn append_vortex_filter_local_execution_effect_fields(
    fields: &mut Vec<(String, String)>,
    local: &VortexLocalPrimitiveCliExecutionEvidence,
) {
    push_bool_field(
        fields,
        "filter_local_execution_data_read",
        local.report.data_read,
    );
    push_bool_field(
        fields,
        "filter_local_execution_data_decoded",
        local.report.data_decoded,
    );
    push_bool_field(
        fields,
        "filter_local_execution_data_materialized",
        local.report.data_materialized,
    );
    push_bool_field(
        fields,
        "filter_local_execution_row_read",
        local.report.row_read,
    );
    push_bool_field(
        fields,
        "filter_local_execution_arrow_converted",
        local.report.arrow_converted,
    );
    push_bool_field(
        fields,
        "filter_local_execution_object_store_io",
        local.report.object_store_io,
    );
    push_bool_field(
        fields,
        "filter_local_execution_write_io",
        local.report.write_io,
    );
    push_bool_field(
        fields,
        "filter_local_execution_spill_io_performed",
        local.report.spill_io_performed,
    );
    push_bool_field(
        fields,
        "filter_local_execution_external_effects_executed",
        local.report.external_effects_executed,
    );
    push_bool_field(
        fields,
        "filter_local_execution_fallback_attempted",
        local.native_io_certificate.side_effects.fallback_attempted
            || local.native_io_certificate.fallback_attempted
            || local
                .execution_certificate
                .as_ref()
                .is_some_and(|certificate| certificate.fallback_attempted),
    );
    push_bool_field(
        fields,
        "filter_local_execution_fallback_execution_allowed",
        local.report.fallback_execution_allowed,
    );
}

fn append_vortex_filter_local_execution_claim_fields(
    fields: &mut Vec<(String, String)>,
    selection_vector_guarantee: bool,
    native_io_certified: bool,
    correctness_certified: bool,
) {
    push_bool_field(
        fields,
        "filter_local_execution_selection_vector_guarantee",
        selection_vector_guarantee,
    );
    push_bool_field(
        fields,
        "filter_local_execution_native_io_certified",
        native_io_certified,
    );
    push_bool_field(
        fields,
        "filter_local_execution_correctness_certified",
        correctness_certified,
    );
    push_bool_field(
        fields,
        "filter_local_execution_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "filter_local_execution_generalized_claim_allowed",
        false,
    );
    push_bool_field(fields, "filter_local_execution_cg2_closeout_allowed", false);
    push_bool_field(
        fields,
        "filter_local_execution_cg13_closeout_allowed",
        false,
    );
}

fn append_vortex_count_where_predicate_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexEncodedPredicateEvaluationReport,
) {
    push_bool_field(fields, "encoded_predicate_evidence_emitted", true);
    push_field(fields, "encoded_predicate_status", report.status.as_str());
    push_count_field(
        fields,
        "encoded_predicate_segment_report_count",
        report.segment_report_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selected_all_count",
        report.selected_all_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selected_none_count",
        report.selected_none_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_needs_encoded_values_count",
        report.needs_encoded_values_count,
    );
    push_count_field(
        fields,
        "encoded_predicate_selection_vectors_emitted",
        report.selection_vectors_emitted,
    );
    push_field(
        fields,
        "encoded_predicate_selected_rows_metadata_count",
        &report
            .selected_rows_metadata_count
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(fields, "encoded_predicate_data_read", report.data_read);
    push_bool_field(
        fields,
        "encoded_predicate_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "encoded_predicate_data_materialized",
        report.data_materialized,
    );
    push_bool_field(fields, "encoded_predicate_row_read", report.row_read);
    push_bool_field(
        fields,
        "encoded_predicate_arrow_converted",
        report.arrow_converted,
    );
    push_bool_field(
        fields,
        "encoded_predicate_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "encoded_predicate_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_filter_kernel_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexSelectionVectorFilterKernelReport,
) {
    push_bool_field(fields, "selection_vector_filter_kernel_emitted", true);
    push_field(
        fields,
        "selection_vector_filter_kernel_status",
        report.status.as_str(),
    );
    push_count_field(
        fields,
        "selection_vector_filter_kernel_segment_count",
        report.segment_count,
    );
    push_count_field(
        fields,
        "selection_vector_filter_kernel_selection_vector_count",
        report.selection_vector_count,
    );
    push_field(
        fields,
        "selection_vector_filter_kernel_selected_row_count",
        &report
            .selected_row_count
            .map_or_else(|| "unknown".to_string(), |count| count.to_string()),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_safe_evidence",
        report.is_safe_native_filter_kernel_evidence(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_read",
        report.data_read,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_decoded",
        report.data_decoded,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_data_materialized",
        report.data_materialized,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_attempted",
        report.fallback_attempted,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_kernel_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

fn append_vortex_count_where_filter_admission_fields(
    fields: &mut Vec<(String, String)>,
    report: &VortexSelectionVectorFilterKernelAdmissionReport,
) {
    push_bool_field(fields, "selection_vector_filter_admission_emitted", true);
    push_field(
        fields,
        "selection_vector_filter_admission_status",
        report.status.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_slot_marked_present",
        report.slot_marked_present,
    );
    push_field(
        fields,
        "selection_vector_filter_admission_correctness_evidence",
        report.correctness_evidence.as_str(),
    );
    push_field(
        fields,
        "selection_vector_filter_admission_benchmark_evidence",
        report.benchmark_evidence.as_str(),
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_runtime_execution_allowed",
        report.runtime_execution_allowed,
    );
    push_bool_field(
        fields,
        "selection_vector_filter_admission_fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
}

pub(crate) fn parse_projection_columns(value: &str) -> Result<ProjectionRequest, ShardLoomError> {
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

pub(crate) fn parse_vortex_primitive_request(
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
        for prefix in ["filter-project:", "filter-and-project:"] {
            if let Some(value) = primitive_arg.strip_prefix(prefix) {
                let Some((predicate, columns)) = value.split_once('|') else {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project requires <predicate>|<columns>".to_string(),
                    ));
                };
                if predicate.is_empty() {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project predicate must not be empty".to_string(),
                    ));
                }
                if columns.is_empty() {
                    return Err(ShardLoomError::InvalidOperation(
                        "filter-project columns must not be empty".to_string(),
                    ));
                }
                return Ok(
                    shardloom_vortex::VortexQueryPrimitiveRequest::filter_and_project(
                        uri,
                        parse_tiny_predicate(predicate)?,
                        parse_projection_columns(columns)?,
                    ),
                );
            }
        }
        Err(ShardLoomError::InvalidOperation("invalid primitive; expected count, count-where:<predicate>, project:<columns>, filter:<predicate>, filter-project:<predicate>|<columns>".to_string()))
    }
}

pub(crate) fn vortex_projection_readiness_fields(
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

pub(crate) fn vortex_encoded_read_spike_fields(
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

pub(crate) fn parse_vortex_spike_args(
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

pub(crate) fn run_vortex_encoded_read_spike(
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

pub(crate) fn run_vortex_approved_local_encoded_count(
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

pub(crate) fn build_vortex_count_local_streaming_batch_plan(
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

pub(crate) struct VortexCountLocalEncodedEvidence {
    target_policy: VortexCountLocalEncodedTargetPolicy,
    fixture_id: Option<String>,
    fixture_source_ref: Option<String>,
    native_io_certificate: Option<NativeIoCertificate>,
    certificate: Option<ExecutionCertificate>,
    physical_kernel: Option<VortexEncodedCountPhysicalKernelReport>,
    kernel_admission: Option<VortexEncodedCountKernelAdmissionReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VortexCountLocalEncodedTargetPolicy {
    KnownFixtureCertified,
    LocalVortexUncertified,
    Blocked,
}

impl VortexCountLocalEncodedTargetPolicy {
    const fn as_str(self) -> &'static str {
        match self {
            Self::KnownFixtureCertified => "known_fixture_certified",
            Self::LocalVortexUncertified => "local_vortex_uncertified",
            Self::Blocked => "blocked",
        }
    }

    const fn reason(self) -> &'static str {
        match self {
            Self::KnownFixtureCertified => {
                "target matches the repository encoded CountAll correctness fixture and has certified execution evidence"
            }
            Self::LocalVortexUncertified => {
                "target is an approved local .vortex CountAll execution but lacks fixture correctness and benchmark certification"
            }
            Self::Blocked => {
                "target does not have successful side-effect-free local encoded CountAll execution evidence"
            }
        }
    }

    const fn execution_allowed(self) -> bool {
        matches!(
            self,
            Self::KnownFixtureCertified | Self::LocalVortexUncertified
        )
    }

    const fn non_fixture_target(self) -> bool {
        matches!(self, Self::LocalVortexUncertified)
    }
}

impl VortexCountLocalEncodedEvidence {
    fn unavailable(
        encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
        local_report: &VortexLocalExecutionReport,
    ) -> shardloom_core::Result<Self> {
        let target_policy =
            if local_encoded_count_target_execution_allowed(encoded_report, local_report) {
                VortexCountLocalEncodedTargetPolicy::LocalVortexUncertified
            } else {
                VortexCountLocalEncodedTargetPolicy::Blocked
            };
        let native_io_certificate = target_policy
            .execution_allowed()
            .then(|| local_encoded_count_native_io_certificate(encoded_report, local_report))
            .transpose()?;
        Ok(Self {
            target_policy,
            fixture_id: None,
            fixture_source_ref: None,
            native_io_certificate,
            certificate: None,
            physical_kernel: None,
            kernel_admission: None,
        })
    }

    fn from_fixture(
        fixture: &CorrectnessFixture,
        native_io_certificate: NativeIoCertificate,
        certificate: ExecutionCertificate,
        physical_kernel: VortexEncodedCountPhysicalKernelReport,
        kernel_admission: VortexEncodedCountKernelAdmissionReport,
    ) -> Self {
        let target_policy = if certificate.is_certified() {
            VortexCountLocalEncodedTargetPolicy::KnownFixtureCertified
        } else {
            VortexCountLocalEncodedTargetPolicy::Blocked
        };
        Self {
            target_policy,
            fixture_id: Some(fixture.id.as_str().to_string()),
            fixture_source_ref: fixture.source_ref.clone(),
            native_io_certificate: Some(native_io_certificate),
            certificate: Some(certificate),
            physical_kernel: Some(physical_kernel),
            kernel_admission: Some(kernel_admission),
        }
    }

    pub(crate) fn has_errors(&self) -> bool {
        self.target_policy == VortexCountLocalEncodedTargetPolicy::Blocked
            || self
                .native_io_certificate
                .as_ref()
                .is_some_and(NativeIoCertificate::has_errors)
            || self
                .certificate
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

    pub(crate) fn diagnostics(&self) -> Vec<shardloom_core::Diagnostic> {
        let mut diagnostics = Vec::new();
        if let Some(native_io_certificate) = &self.native_io_certificate {
            diagnostics.extend(native_io_certificate.diagnostics.clone());
        }
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

    pub(crate) fn human_sections(&self) -> Vec<String> {
        let mut sections = vec![format!(
            "Vortex local encoded CountAll target policy\npolicy: {}\nreason: {}\nexecution allowed: {}\ncorrectness certified: {}\nproduction claim allowed: false\nCG-2 closeout allowed: false\nCG-13 closeout allowed: false",
            self.target_policy.as_str(),
            self.target_policy.reason(),
            self.target_policy.execution_allowed(),
            self.certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        )];
        if let Some(native_io_certificate) = &self.native_io_certificate {
            sections.push(local_count_native_io_certificate_human_text(
                native_io_certificate,
            ));
        }
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

pub(crate) fn vortex_count_local_encoded_evidence(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
) -> shardloom_core::Result<VortexCountLocalEncodedEvidence> {
    let Some(fixture) = local_encoded_count_correctness_fixture_for_report(encoded_report) else {
        return VortexCountLocalEncodedEvidence::unavailable(encoded_report, local_report);
    };
    let native_io_certificate =
        local_encoded_count_native_io_certificate(encoded_report, local_report)?;
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
        native_io_certificate,
        certificate,
        physical_kernel,
        kernel_admission,
    ))
}

fn local_encoded_count_target_execution_allowed(
    encoded_report: &shardloom_vortex::VortexEncodedReadExecutionReport,
    local_report: &VortexLocalExecutionReport,
) -> bool {
    let count_result_matches = encoded_report
        .count_result
        .is_some_and(|count| encoded_report.rows_counted == count);
    encoded_report.feature_status == VortexEncodedReadExecutorFeatureStatus::Enabled
        && encoded_report.status == VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        && encoded_report.mode == VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount
        && !encoded_report.has_errors()
        && encoded_report.data_read
        && encoded_report.upstream_scan_called
        && !encoded_report.data_decoded
        && !encoded_report.data_materialized
        && !encoded_report.row_read
        && !encoded_report.arrow_converted
        && !encoded_report.object_store_io
        && !encoded_report.write_io
        && !encoded_report.spill_io_performed
        && !encoded_report.external_effects_executed
        && !encoded_report.fallback_execution_allowed
        && encoded_report.local_scan_source_uri_matches_target
        && encoded_report.local_scan_target_uri.is_some()
        && encoded_report.local_scan_readiness_source_uri == encoded_report.local_scan_target_uri
        && count_result_matches
        && local_report.status == VortexLocalExecutionStatus::LocalEncodedCountExecuted
        && !local_report.has_errors()
        && local_report.tasks_executed
        && local_report.data_read
        && !local_report.data_decoded
        && !local_report.data_materialized
        && !local_report.object_store_io
        && !local_report.write_io
        && !local_report.spill_io_performed
        && !local_report.external_effects_executed
        && !local_report.fallback_execution_allowed
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
            matches!(fixture.expected, ExpectedOutcome::EncodedCount { .. })
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

fn local_foundation_fixture_for_target(
    target_uri: &DatasetUri,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            fixture.id.as_str() == fixture_id
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

pub(crate) fn local_primitive_correctness_fixture_for_request(
    request: &VortexQueryPrimitiveRequest,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) -> Option<CorrectnessFixture> {
    if request.kind != report.primitive_kind
        || report.status != shardloom_vortex::VortexLocalPrimitiveExecutionStatus::Executed
        || report.has_errors()
    {
        return None;
    }
    match request.kind {
        shardloom_vortex::VortexQueryPrimitiveKind::CountAll => request
            .source_uri
            .as_ref()
            .and_then(local_encoded_count_correctness_fixture_for_target),
        shardloom_vortex::VortexQueryPrimitiveKind::CountWhere => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request),
            "vortex-local-count-where-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::ProjectColumns => local_primitive_fixture_if(
            request,
            local_struct_metric_projection(request),
            "vortex-local-project-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::FilterPredicate => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request),
            "vortex-local-filter-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::FilterAndProject => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request)
                && local_struct_metric_projection(request),
            "vortex-local-filter-project-struct-five",
        ),
        shardloom_vortex::VortexQueryPrimitiveKind::SimpleAggregate
        | shardloom_vortex::VortexQueryPrimitiveKind::Unsupported => None,
    }
}

fn local_primitive_fixture_if(
    request: &VortexQueryPrimitiveRequest,
    matches_fixture_shape: bool,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    matches_fixture_shape
        .then_some(request.source_uri.as_ref())
        .flatten()
        .and_then(|source_uri| local_foundation_fixture_for_target(source_uri, fixture_id))
}

fn local_struct_value_gte_three_predicate(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        request.predicate.as_ref(),
        Some(PredicateExpr::Compare {
            column,
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3)
        }) if column.as_str() == "value"
    )
}

fn local_struct_metric_projection(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        &request.projection,
        ProjectionRequest::Columns(columns)
            if columns.len() == 1 && columns[0].as_str() == "metric"
    )
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

fn local_count_native_io_certificate_human_text(certificate: &NativeIoCertificate) -> String {
    format!(
        "Vortex local encoded CountAll Native I/O certificate\ncertificate: {}\npath: {}\nstatus: {}\nsource adapter: {}\npushdown guarantee: {}\nrepresentation transitions: {}\nmaterialization boundaries: {}\nfallback execution allowed: {}",
        certificate.certificate_id,
        certificate.path_id,
        certificate.status(),
        certificate.source_capability_report.adapter_id,
        certificate.source_pushdown_report.guarantee,
        certificate.representation_transition_order(),
        certificate.materialization_boundary_order(),
        certificate.side_effects.fallback_execution_allowed
    )
}

fn local_primitive_native_io_certificate_human_text(certificate: &NativeIoCertificate) -> String {
    format!(
        "Vortex local primitive Native I/O certificate\ncertificate: {}\npath: {}\nstatus: {}\nsource adapter: {}\npushdown guarantee: {}\naccepted operations: {}\nrepresentation transitions: {}\nmaterialization boundaries: {}\nfallback execution allowed: {}",
        certificate.certificate_id,
        certificate.path_id,
        certificate.status(),
        certificate.source_capability_report.adapter_id,
        certificate.source_pushdown_report.guarantee,
        certificate
            .source_pushdown_report
            .accepted_operation_order(),
        certificate.representation_transition_order(),
        certificate.materialization_boundary_order(),
        certificate.side_effects.fallback_execution_allowed
    )
}

pub(crate) fn vortex_count_local_encoded_fields(
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
    append_vortex_count_local_encoded_target_policy_fields(fields, evidence);
    append_vortex_count_local_native_io_certificate_fields(fields, evidence);
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

fn append_vortex_count_local_native_io_certificate_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    let Some(certificate) = &evidence.native_io_certificate else {
        push_bool_field(fields, "local_count_native_io_certificate_emitted", false);
        push_field(
            fields,
            "local_count_native_io_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "local_count_native_io_certified", false);
        return;
    };
    append_vortex_count_local_native_io_identity_fields(fields, certificate);
    append_vortex_count_local_native_io_source_fields(fields, certificate);
    append_vortex_count_local_native_io_pushdown_fields(fields, certificate);
    append_vortex_count_local_native_io_sink_fields(fields, certificate);
    append_vortex_count_local_native_io_side_effect_fields(fields, certificate);
}

fn append_vortex_count_local_native_io_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_bool_field(fields, "local_count_native_io_certificate_emitted", true);
    push_field(
        fields,
        "local_count_native_io_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_path_id",
        &certificate.path_id,
    );
    push_field(
        fields,
        "local_count_native_io_certificate_status",
        certificate.status(),
    );
    push_bool_field(
        fields,
        "local_count_native_io_certified",
        certificate.is_certified(),
    );
}

fn append_vortex_count_local_native_io_source_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let source = &certificate.source_capability_report;
    push_field(
        fields,
        "local_count_native_io_source_kind",
        &source.source_kind,
    );
    push_field(
        fields,
        "local_count_native_io_adapter_id",
        &source.adapter_id,
    );
    push_bool_field(
        fields,
        "local_count_native_io_encoded_representation_preserved",
        source.encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "local_count_native_io_streaming_capability",
        source.streaming_capability,
    );
}

fn append_vortex_count_local_native_io_pushdown_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let pushdown = &certificate.source_pushdown_report;
    push_field(
        fields,
        "local_count_native_io_pushdown_accepted_operations",
        &pushdown.accepted_operation_order(),
    );
    push_field(
        fields,
        "local_count_native_io_pushdown_rejected_operations",
        &pushdown.rejected_operation_order(),
    );
    push_field(
        fields,
        "local_count_native_io_pushdown_guarantee",
        &pushdown.guarantee,
    );
    push_field(
        fields,
        "local_count_native_io_representation_transitions",
        &certificate.representation_transition_order(),
    );
    push_field(
        fields,
        "local_count_native_io_materialization_boundaries",
        &certificate.materialization_boundary_order(),
    );
    push_bool_field(
        fields,
        "local_count_native_io_materializing_transitions_have_boundaries",
        certificate.materializing_transitions_have_boundaries(),
    );
}

fn append_vortex_count_local_native_io_sink_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let sink = &certificate.sink_requirement_report;
    let fidelity = &certificate.adapter_fidelity_report;
    push_field(
        fields,
        "local_count_native_io_sink_target_format",
        &sink.target_format,
    );
    push_bool_field(
        fields,
        "local_count_native_io_sink_accepts_encoded",
        sink.accepts_encoded,
    );
    push_bool_field(
        fields,
        "local_count_native_io_adapter_materialization_required",
        fidelity.materialization_required,
    );
}

fn append_vortex_count_local_native_io_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let side_effects = &certificate.side_effects;
    push_bool_field(
        fields,
        "local_count_native_io_data_read",
        side_effects.data_read,
    );
    push_bool_field(
        fields,
        "local_count_native_io_data_decoded",
        side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "local_count_native_io_data_materialized",
        side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "local_count_native_io_row_read",
        side_effects.row_read,
    );
    push_bool_field(
        fields,
        "local_count_native_io_arrow_converted",
        side_effects.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_count_native_io_object_store_io",
        side_effects.object_store_io,
    );
    push_bool_field(
        fields,
        "local_count_native_io_write_io",
        side_effects.write_io,
    );
    push_bool_field(
        fields,
        "local_count_native_io_spill_io_performed",
        side_effects.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_count_native_io_fallback_attempted",
        side_effects.fallback_attempted || certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_count_native_io_fallback_execution_allowed",
        side_effects.fallback_execution_allowed,
    );
}

pub(crate) fn append_vortex_local_primitive_native_io_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: Option<&NativeIoCertificate>,
) {
    let Some(certificate) = certificate else {
        push_bool_field(
            fields,
            "local_primitive_native_io_certificate_emitted",
            false,
        );
        push_field(
            fields,
            "local_primitive_native_io_certificate_status",
            "evidence_unavailable",
        );
        push_bool_field(fields, "local_primitive_native_io_certified", false);
        return;
    };

    append_vortex_local_primitive_native_io_identity_fields(fields, certificate);
    append_vortex_local_primitive_native_io_source_fields(fields, certificate);
    append_vortex_local_primitive_native_io_pushdown_fields(fields, certificate);
    append_vortex_local_primitive_native_io_sink_fields(fields, certificate);
    append_vortex_local_primitive_native_io_side_effect_fields(fields, certificate);
}

fn append_vortex_local_primitive_native_io_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_native_io_certificate_emitted",
        true,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_path_id",
        &certificate.path_id,
    );
    push_field(
        fields,
        "local_primitive_native_io_certificate_status",
        certificate.status(),
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_certified",
        certificate.is_certified(),
    );
}

fn append_vortex_local_primitive_native_io_source_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_source_kind",
        &certificate.source_capability_report.source_kind,
    );
    push_field(
        fields,
        "local_primitive_native_io_adapter_id",
        &certificate.source_capability_report.adapter_id,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_encoded_representation_preserved",
        certificate
            .source_capability_report
            .encoded_representation_preserved,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_streaming_capability",
        certificate.source_capability_report.streaming_capability,
    );
}

fn append_vortex_local_primitive_native_io_pushdown_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_pushdown_accepted_operations",
        &certificate
            .source_pushdown_report
            .accepted_operation_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_pushdown_rejected_operations",
        &certificate
            .source_pushdown_report
            .rejected_operation_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_pushdown_guarantee",
        &certificate.source_pushdown_report.guarantee,
    );
    push_field(
        fields,
        "local_primitive_native_io_representation_transitions",
        &certificate.representation_transition_order(),
    );
    push_field(
        fields,
        "local_primitive_native_io_materialization_boundaries",
        &certificate.materialization_boundary_order(),
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_materializing_transitions_have_boundaries",
        certificate.materializing_transitions_have_boundaries(),
    );
}

fn append_vortex_local_primitive_native_io_sink_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    push_field(
        fields,
        "local_primitive_native_io_sink_target_format",
        &certificate.sink_requirement_report.target_format,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_accepts_encoded",
        certificate.sink_requirement_report.accepts_encoded,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_requires_decoded_columnar",
        certificate
            .sink_requirement_report
            .requires_decoded_columnar,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_requires_rows",
        certificate.sink_requirement_report.requires_rows,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_sink_supports_streaming",
        certificate.sink_requirement_report.supports_streaming,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_adapter_materialization_required",
        certificate.adapter_fidelity_report.materialization_required,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_adapter_encoded_representation_preserved",
        certificate
            .adapter_fidelity_report
            .encoded_representation_preserved,
    );
}

fn append_vortex_local_primitive_native_io_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &NativeIoCertificate,
) {
    let side_effects = &certificate.side_effects;
    push_bool_field(
        fields,
        "local_primitive_native_io_data_read",
        side_effects.data_read,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_data_decoded",
        side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_data_materialized",
        side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_row_read",
        side_effects.row_read,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_arrow_converted",
        side_effects.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_object_store_io",
        side_effects.object_store_io,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_write_io",
        side_effects.write_io,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_spill_io_performed",
        side_effects.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_fallback_attempted",
        side_effects.fallback_attempted || certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_primitive_native_io_fallback_execution_allowed",
        side_effects.fallback_execution_allowed,
    );
}

pub(crate) fn append_vortex_local_primitive_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: Option<&ExecutionCertificate>,
) {
    let Some(certificate) = certificate else {
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_emitted",
            false,
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_schema_version",
            "none",
        );
        push_field(fields, "local_primitive_execution_certificate_id", "none");
        push_field(
            fields,
            "local_primitive_execution_certificate_execution_kind",
            "none",
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_status",
            "not_available",
        );
        push_field(
            fields,
            "local_primitive_execution_certificate_fixture_id",
            "none",
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_correctness_passed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_read",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_decoded",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_data_materialized",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_row_read",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_arrow_converted",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_object_store_io",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_write_io",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_spill_io_performed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_external_effects_executed",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_unsafe_effect_detected",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_fallback_attempted",
            false,
        );
        push_bool_field(
            fields,
            "local_primitive_execution_certificate_fallback_execution_allowed",
            false,
        );
        return;
    };

    append_vortex_local_primitive_execution_certificate_identity_fields(fields, certificate);
    append_vortex_local_primitive_execution_certificate_effect_fields(fields, certificate);
}

fn append_vortex_local_primitive_execution_certificate_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_emitted",
        true,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_execution_kind",
        &certificate.execution_kind,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_kind",
        certificate.execution_provider_kind.as_str(),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_scope",
        &certificate.provider_scope,
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_crate",
        certificate.provider_crate.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_version",
        certificate.provider_version.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_provider_api_surface",
        certificate
            .provider_api_surface
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_shardloom_admission_policy",
        certificate
            .shardloom_admission_policy
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "local_primitive_execution_certificate_fixture_id",
        certificate
            .correctness_fixture_id
            .as_deref()
            .unwrap_or("none"),
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_correctness_passed",
        certificate.correctness_passed,
    );
}

fn append_vortex_local_primitive_execution_certificate_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_read",
        certificate.data_read,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_decoded",
        certificate.data_decoded,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_data_materialized",
        certificate.data_materialized,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_row_read",
        certificate.row_read,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_arrow_converted",
        certificate.arrow_converted,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_object_store_io",
        certificate.object_store_io,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_write_io",
        certificate.write_io,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_spill_io_performed",
        certificate.spill_io_performed,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_external_query_engine_invoked",
        certificate.external_query_engine_invoked,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_external_effects_executed",
        certificate.external_effects_executed,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_unsafe_effect_detected",
        certificate.unsafe_effect_detected,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "local_primitive_execution_certificate_fallback_execution_allowed",
        certificate.fallback_execution_allowed,
    );
}

fn append_vortex_count_local_encoded_target_policy_fields(
    fields: &mut Vec<(String, String)>,
    evidence: &VortexCountLocalEncodedEvidence,
) {
    push_bool_field(
        fields,
        "generalized_local_count_target_policy_report_emitted",
        true,
    );
    push_field(
        fields,
        "generalized_local_count_target_policy",
        evidence.target_policy.as_str(),
    );
    push_field(
        fields,
        "generalized_local_count_target_policy_reason",
        evidence.target_policy.reason(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_execution_allowed",
        evidence.target_policy.execution_allowed(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_non_fixture_target",
        evidence.target_policy.non_fixture_target(),
    );
    push_bool_field(
        fields,
        "generalized_local_count_correctness_certified",
        evidence
            .certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    push_bool_field(
        fields,
        "generalized_local_count_requires_correctness_fixture",
        !evidence
            .certificate
            .as_ref()
            .is_some_and(ExecutionCertificate::is_certified),
    );
    push_bool_field(
        fields,
        "generalized_local_count_requires_benchmark_evidence",
        true,
    );
    push_bool_field(
        fields,
        "generalized_local_count_production_claim_allowed",
        false,
    );
    push_bool_field(
        fields,
        "generalized_local_count_cg2_closeout_allowed",
        false,
    );
    push_bool_field(
        fields,
        "generalized_local_count_cg13_closeout_allowed",
        false,
    );
}

fn append_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(fields, "execution_certificate_emitted", true);
    append_execution_certificate_identity_fields(fields, certificate);
    append_execution_certificate_provider_fields(fields, certificate);
    append_execution_certificate_io_fields(fields, certificate);
    append_execution_certificate_effect_fields(fields, certificate);
}

fn append_execution_certificate_identity_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
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
}

fn append_execution_certificate_provider_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_field(
        fields,
        "execution_certificate_provider_kind",
        certificate.execution_provider_kind.as_str(),
    );
    push_field(
        fields,
        "execution_certificate_provider_scope",
        &certificate.provider_scope,
    );
    push_field(
        fields,
        "execution_certificate_provider_crate",
        certificate.provider_crate.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_provider_version",
        certificate.provider_version.as_deref().unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_provider_api_surface",
        certificate
            .provider_api_surface
            .as_deref()
            .unwrap_or("none"),
    );
    push_field(
        fields,
        "execution_certificate_shardloom_admission_policy",
        certificate
            .shardloom_admission_policy
            .as_deref()
            .unwrap_or("none"),
    );
}

fn append_execution_certificate_io_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
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
}

fn append_execution_certificate_effect_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ExecutionCertificate,
) {
    push_bool_field(
        fields,
        "execution_certificate_external_query_engine_invoked",
        certificate.external_query_engine_invoked,
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
        Some("spill-lifecycle") => operational_hardening::handle_spill_lifecycle(args, format),
        Some("spill-reservation-plan") => {
            operational_hardening::handle_spill_reservation_plan(args, format)
        }
        Some("spill-payload-roundtrip") => {
            operational_hardening::handle_spill_payload_roundtrip(args, format)
        }
        Some("cleanup-synthetic-payload") => {
            operational_hardening::handle_cleanup_synthetic_payload(args, format)
        }

        Some("status") => status_capabilities::handle_status(format),
        Some("release-plan") => packaging_deployment::handle_release_plan(format),
        Some("package-plan") => packaging_deployment::handle_package_plan(format),
        Some("api-compat-plan") => rest_api_planning::handle_api_compat_plan(format),
        Some("agent-contract-pack") => packaging_deployment::handle_agent_contract_pack(format),
        Some("python-wrapper-plan") => packaging_deployment::handle_python_wrapper_plan(format),

        Some("input-adapters") => input_planning::handle_input_adapters(format),
        Some("input-plan") => input_planning::handle_input_plan(args, format),
        Some("vortex-input-plan") => input_planning::handle_vortex_input_plan(args, format),
        Some("vortex-read-plan") => input_planning::handle_vortex_read_plan(args, format),
        Some("vortex-task-graph") => input_planning::handle_vortex_task_graph(args, format),

        Some("schema-plan") => workflow_planning::handle_schema_plan(args, format),
        Some("catalog-plan") => workflow_planning::handle_catalog_plan(args, format),
        Some("table-compat-plan") => workflow_planning::handle_table_compat_plan(args, format),
        Some("capabilities") => status_capabilities::handle_capabilities(args, format),
        Some("extension-registry") => extension_planning::handle_extension_registry(format),
        Some("extension-inspect") => extension_planning::handle_extension_inspect(args, format),
        Some("udf-runtime-plan") => extension_planning::handle_udf_runtime_plan(args, format),
        Some("security-plan") => operational_hardening::handle_security_plan(format),
        Some("security-governance-evidence-gate") => {
            operational_hardening::handle_security_governance_evidence_gate(format)
        }
        Some("effect-budget-plan") => operational_hardening::handle_effect_budget_plan(format),
        Some("agent-safety-plan") => operational_hardening::handle_agent_safety_plan(format),
        Some("redaction-plan") => operational_hardening::handle_redaction_plan(format),
        Some("plan-ir") => workflow_planning::handle_plan_ir(format),
        Some("plan-import") => workflow_planning::handle_plan_import(args, format),
        Some("plan-export") => workflow_planning::handle_plan_export(args, format),
        Some("memory-plan") => operational_hardening::handle_memory_plan(args, format),
        Some("operator-memory-spill-declarations") => {
            operational_hardening::handle_operator_memory_spill_declarations(format)
        }
        Some("cg14-memory-runtime-hardening-gate") => {
            operational_hardening::handle_memory_runtime_hardening_gate(format)
        }
        Some("spill-plan") => operational_hardening::handle_spill_plan(args, format),
        Some("correctness-plan") => evidence_certificates::handle_correctness_plan(format),
        Some("correctness-harness-plan") => {
            evidence_certificates::handle_correctness_harness_plan(format)
        }
        Some("execution-certificate-plan") => {
            evidence_certificates::handle_execution_certificate_plan(format)
        }
        Some("kernel-registry") => optimizer_planning::handle_kernel_registry(format),
        Some("recovery-plan") => operational_hardening::handle_recovery_plan(format),
        Some("commit-execution-promotion-gate") => {
            operational_hardening::handle_commit_execution_promotion_gate(format)
        }
        Some("fault-tolerance-promotion-gate") => {
            operational_hardening::handle_fault_tolerance_promotion_gate(format)
        }
        Some("cancellation-plan") => operational_hardening::handle_cancellation_plan(args, format),
        Some("retry-plan") => operational_hardening::handle_retry_plan(args, format),
        Some("retry-gate-plan") => operational_hardening::handle_retry_gate_plan(args, format),
        Some("cancellation-gate-plan") => {
            operational_hardening::handle_cancellation_gate_plan(args, format)
        }
        Some("observability-plan") => diagnostics::handle_observability_plan(format),
        Some("observability-schema-coverage") => {
            diagnostics::handle_observability_schema_coverage(format)
        }
        Some("runtime-report") => diagnostics::handle_runtime_report(format),
        Some("profile-plan") => diagnostics::handle_profile_plan(format),
        Some("feature-footprint") => diagnostics::handle_feature_footprint(format),
        Some("doctor") => diagnostics::handle_doctor(format),
        Some("explain") => diagnostics::handle_explain(args, format),
        Some("benchmark-plan") => benchmark_planning::handle_benchmark_plan(args, format),
        Some("benchmark-claim-evidence-plan") => {
            benchmark_planning::handle_benchmark_claim_evidence_plan(args, format)
        }
        Some("manifest-plan") => workflow_planning::handle_manifest_plan(args, format),
        Some("layout-health-plan") => workflow_planning::handle_layout_health_plan(args, format),
        Some("compaction-plan") => workflow_planning::handle_compaction_plan(args, format),
        Some("table-intelligence-plan") => {
            workflow_planning::handle_table_intelligence_plan(args, format)
        }
        Some("cg9-catalog-metadata-gate") => {
            workflow_planning::handle_catalog_metadata_gate(args, format)
        }
        Some("object-store-request-plan") => {
            object_store_planning::handle_object_store_request_plan(args, format)
        }
        Some("cg10-object-store-runtime-gate") => {
            object_store_planning::handle_cg10_object_store_runtime_gate(args, format)
        }
        Some("object-store-range-plan") => {
            object_store_planning::handle_object_store_range_plan(args, format)
        }
        Some("object-store-coalesce-plan") => {
            object_store_planning::handle_object_store_coalesce_plan(args, format)
        }
        Some("object-store-schedule-plan") => {
            object_store_planning::handle_object_store_schedule_plan(args, format)
        }
        Some("object-store-checkpoint-retry-plan") => {
            object_store_planning::handle_object_store_checkpoint_retry_plan(args, format)
        }
        Some("object-store-commit-plan") => {
            object_store_planning::handle_object_store_commit_plan(args, format)
        }
        Some("incremental-plan") => workflow_planning::handle_incremental_plan(args, format),
        Some("stateful-reuse-plan") => workflow_planning::handle_stateful_reuse_plan(format),
        Some("cg17-stateful-reuse-gate") => workflow_planning::handle_stateful_reuse_gate(format),
        Some("universal-harness-plan") => {
            evidence_certificates::handle_universal_harness_plan(format)
        }
        Some("rfc-coverage-followthrough-plan") => {
            evidence_certificates::handle_rfc_coverage_followthrough_plan(format)
        }
        Some("native-io-envelope-plan") => {
            evidence_certificates::handle_native_io_envelope_plan(format)
        }
        Some("world-class-sufficiency-plan") => {
            evidence_certificates::handle_world_class_sufficiency_plan(format)
        }
        Some("cg20-user-capability-gate") => {
            evidence_certificates::handle_cg20_user_capability_gate(args, format)
        }
        Some("cg20-approx-sketch-gate") => {
            evidence_certificates::handle_cg20_approx_sketch_gate(args, format)
        }
        Some("vortex-write-intent-plan") => {
            vortex_output_commit::handle_vortex_write_intent_plan(args, format)
        }
        Some("vortex-commit-intent-plan") => {
            vortex_output_commit::handle_vortex_commit_intent_plan(args, format)
        }
        Some("vortex-manifest-finalization-plan") => {
            vortex_output_commit::handle_vortex_manifest_finalization_plan(args, format)
        }
        Some("vortex-output-payload-plan") => {
            vortex_output_commit::handle_vortex_output_payload_plan(args, format)
        }
        Some("vortex-finalized-manifest-artifact-write") => {
            vortex_output_commit::handle_vortex_finalized_manifest_artifact_write(args, format)
        }
        Some("vortex-output-payload-artifact-write") => {
            vortex_output_commit::handle_vortex_output_payload_artifact_write(args, format)
        }

        Some("vortex-native-count-payload-write") => {
            vortex_output_commit::handle_vortex_native_count_payload_write(args, format)
        }

        Some("vortex-commit-marker-plan") => {
            vortex_output_commit::handle_vortex_commit_marker_plan(args, format)
        }
        Some("vortex-commit-marker-write") => {
            vortex_output_commit::handle_vortex_commit_marker_write(args, format)
        }
        Some("vortex-commit-protocol-plan") => {
            vortex_output_commit::handle_vortex_commit_protocol_plan(args, format)
        }
        Some("vortex-local-commit-execute") => {
            vortex_output_commit::handle_vortex_local_commit_execute(args, format)
        }
        Some("vortex-local-commit-recovery-plan") => {
            vortex_output_commit::handle_vortex_local_commit_recovery_plan(args, format)
        }
        Some("vortex-local-commit-rollback-execute") => {
            vortex_output_commit::handle_vortex_local_commit_rollback_execute(args, format)
        }
        Some("vortex-staged-workspace-setup") => {
            vortex_output_commit::handle_vortex_staged_workspace_setup(args, format)
        }

        Some("vortex-staged-marker-write") => {
            vortex_output_commit::handle_vortex_staged_marker_write(args, format)
        }
        Some("vortex-staged-manifest-file-plan") => {
            vortex_output_commit::handle_vortex_staged_manifest_file_plan(args, format)
        }
        Some("vortex-staged-manifest-file-write") => {
            vortex_output_commit::handle_vortex_staged_manifest_file_write(args, format)
        }
        Some("write-intent") => workflow_planning::handle_write_intent(args, format),
        Some("scan-plan") => workflow_planning::handle_scan_plan(args, format),
        Some("streaming-plan") => engine_runtime_planning::handle_streaming_plan(args, format),
        Some("streaming-batch-plan") => {
            engine_runtime_planning::handle_streaming_batch_plan(args, format)
        }
        Some("backpressure-plan") => {
            engine_runtime_planning::handle_backpressure_plan(args, format)
        }
        Some("runtime-plan") => engine_runtime_planning::handle_runtime_plan(args, format),
        Some("sizing-plan") => engine_runtime_planning::handle_sizing_plan(args, format),
        Some("sizing-feedback-plan") => {
            engine_runtime_planning::handle_sizing_feedback_plan(args, format)
        }
        Some("dynamic-work-shaping-plan") => {
            engine_runtime_planning::handle_dynamic_work_shaping_plan(args, format)
        }
        Some("cg8-runtime-promotion-gate") => {
            engine_runtime_planning::handle_dynamic_runtime_gate(format)
        }
        Some("task-plan") => engine_runtime_planning::handle_task_plan(args, format),

        Some("vortex-adaptive-sizing") => {
            vortex_runtime_planning::handle_vortex_adaptive_sizing(args, format)
        }
        Some("vortex-memory-plan") => {
            vortex_runtime_planning::handle_vortex_memory_plan(args, format)
        }
        Some("vortex-schedule-plan") => {
            vortex_runtime_planning::handle_vortex_schedule_plan(args, format)
        }

        Some("vortex-execution-readiness") => {
            vortex_runtime_planning::handle_vortex_execution_readiness(args, format)
        }

        Some("vortex-encoded-path-selection-plan") => {
            vortex_planning::handle_vortex_encoded_path_selection_plan(format)
        }
        Some("vortex-generalized-encoded-primitive-gate") => {
            vortex_planning::handle_vortex_generalized_encoded_primitive_gate(format)
        }
        Some("vortex-encoded-read-api") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_api(format)
        }
        Some("vortex-encoded-read-boundary") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_boundary(args, format)
        }
        Some("vortex-encoded-read-metadata-probe") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_metadata_probe(
                args, format,
            )
        }
        Some("vortex-encoded-read-readiness") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_readiness(args, format)
        }
        Some("vortex-encoded-read-probe") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_probe(args, format)
        }
        Some("vortex-encoded-read-spike") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_spike(args, format)
        }

        Some("vortex-encoded-read-execute") => {
            prepared_source_backed_execution::handle_vortex_encoded_read_execute(args, format)
        }
        Some("vortex-metadata-execute") => {
            vortex_planning::handle_vortex_metadata_execute(args, format)
        }
        Some("vortex-dry-run") => vortex_planning::handle_vortex_dry_run(args, format),
        Some("vortex-plan") => vortex_planning::handle_vortex_plan(args, format),
        Some("translation-plan") => vortex_planning::handle_translation_plan(args, format),
        Some("vortex-output-plan") => vortex_planning::handle_vortex_output_plan(args, format),
        Some("vortex-readiness") => vortex_planning::handle_vortex_readiness(format),
        Some("vortex-dtype-mapping") => vortex_planning::handle_vortex_dtype_mapping(format),
        Some("vortex-encoding-layout-mapping") => {
            vortex_planning::handle_vortex_encoding_layout_mapping(format)
        }
        Some("vortex-statistics-mapping") => {
            vortex_planning::handle_vortex_statistics_mapping(format)
        }
        Some("vortex-file-metadata-open") => {
            vortex_planning::handle_vortex_file_metadata_open(args, format)
        }
        Some("vortex-metadata-summary") => {
            vortex_planning::handle_vortex_metadata_summary(args, format)
        }
        Some("vortex-query-primitive-plan") => {
            vortex_planning::handle_vortex_query_primitive_plan(args, format)
        }
        Some("vortex-metadata-physical-kernel-plan") => {
            vortex_planning::handle_vortex_metadata_physical_kernel_plan(args, format)
        }
        Some("vortex-count-readiness-plan") => {
            vortex_planning::handle_vortex_count_readiness_plan(args, format)
        }
        Some("vortex-encoded-count-approval-plan") => {
            vortex_planning::handle_vortex_encoded_count_approval_plan(args, format)
        }
        Some("vortex-layout-driver-approval-plan") => {
            vortex_planning::handle_vortex_layout_driver_approval_plan(args, format)
        }
        Some("vortex-filtered-count-readiness-plan") => {
            vortex_planning::handle_vortex_filtered_count_readiness_plan(args, format)
        }
        Some("vortex-projection-readiness-plan") => {
            vortex_planning::handle_vortex_projection_readiness_plan(args, format)
        }
        Some("traditional-analytics-run") => {
            benchmark_runtime::handle_traditional_analytics_run(args, format)
        }
        Some("traditional-analytics-vortex-run") => {
            benchmark_runtime::handle_traditional_analytics_vortex_run(args, format)
        }
        Some("vortex-count") => vortex_primitive_execution::handle_vortex_count(args, format),
        Some("vortex-count-benchmark") => {
            benchmark_runtime::handle_vortex_count_benchmark(args, format)
        }
        Some("vortex-count-where") => {
            vortex_primitive_execution::handle_vortex_count_where(args, format)
        }
        Some("vortex-project") => vortex_primitive_execution::handle_vortex_project(args, format),
        Some("vortex-filter-project") => {
            vortex_primitive_execution::handle_vortex_filter_project(args, format)
        }
        Some("vortex-filter") => vortex_primitive_execution::handle_vortex_filter(args, format),
        Some("vortex-local-exec") => {
            vortex_primitive_execution::handle_vortex_local_exec(args, format)
        }
        Some("vortex-bounded-local-exec") => {
            vortex_primitive_execution::handle_vortex_bounded_local_exec(args, format)
        }
        Some("vortex-run") => vortex_primitive_execution::handle_vortex_run(args, format),
        Some("vortex-query-trace") => {
            vortex_primitive_execution::handle_vortex_query_trace(args, format)
        }

        Some("vortex-metadata-plan") => vortex_planning::handle_vortex_metadata_plan(args, format),
        Some("vortex-pruning-plan") => vortex_planning::handle_vortex_pruning_plan(args, format),
        Some("vortex-metadata-probe") => {
            vortex_planning::handle_vortex_metadata_probe(args, format)
        }
        Some("vortex-api-inventory") => vortex_planning::handle_vortex_api_inventory(format),
        Some("optimizer-plan") => optimizer_planning::handle_optimizer_plan(format),
        Some("optimizer-adaptive-memory-plan") => {
            optimizer_planning::handle_optimizer_adaptive_memory_plan(format)
        }
        Some("cpu-specialization-plan") => {
            optimizer_planning::handle_cpu_specialization_plan(format)
        }
        Some("estimate") => diagnostics::handle_estimate(args, format),
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
    use shardloom_core::{
        DiagnosticCategory, DiagnosticCode, EncodingKind, ExecutionCertificateInput, LayoutKind,
        LogicalDType, NativeIoAdapterFidelityReport, NativeIoRepresentationTransition,
        NativeIoSideEffectReport, NativeIoSinkRequirementReport, NativeIoSourceCapabilityReport,
        NativeIoSourcePushdownReport, RepresentationState, SegmentStats,
        plan_rfc_coverage_followthrough,
    };
    use shardloom_exec::{
        SizingFeedbackSignalKind, plan_cancellation_execution_gate, plan_retry_execution_gate,
    };
    use shardloom_vortex::{
        VortexEncodedReadBoundaryRequest, VortexEncodedReadFixtureRef,
        VortexEncodedReadMetadataProbeRequest, VortexProjectionCandidateSource,
        evaluate_vortex_query_primitive, plan_vortex_encoded_read_boundary,
        plan_vortex_projection_readiness, probe_vortex_encoded_read_metadata,
        vortex_local_commit_execution_feature_enabled,
        vortex_native_output_payload_write_feature_enabled,
    };
    #[cfg(feature = "vortex-local-primitives")]
    use std::path::PathBuf;
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

    fn fake_vortex_file_plans_should_succeed() -> bool {
        !cfg!(any(
            feature = "vortex-encoded-read-spike",
            feature = "vortex-traditional-analytics-benchmark"
        ))
    }

    fn assert_fake_vortex_file_io_plan_code(code: ExitCode) {
        if fake_vortex_file_plans_should_succeed() {
            assert_eq!(code, ExitCode::SUCCESS);
        } else {
            assert_ne!(code, ExitCode::SUCCESS);
        }
    }

    #[test]
    fn vortex_work_avoided_fields_include_runtime_metric_details() {
        let mut report = VortexWorkAvoidedReport::empty();
        report.add_metric(shardloom_vortex::VortexWorkAvoidedMetric::known_bool(
            VortexWorkAvoidedMetricKind::DecodeAvoided,
            true,
            "decode skipped",
        ));
        report.add_metric(shardloom_vortex::VortexWorkAvoidedMetric::unknown(
            VortexWorkAvoidedMetricKind::BytesNotRead,
            "not safely estimated",
        ));
        let mut fields = Vec::new();
        append_vortex_work_avoided_fields(&mut fields, Some(&report));

        assert!(fields.contains(&("work_avoided_metrics".to_string(), "2".to_string())));
        assert!(fields.contains(&("work_avoided_known_metrics".to_string(), "1".to_string())));
        assert!(fields.contains(&(
            "work_avoided_decode_avoided".to_string(),
            "true".to_string()
        )));
        assert!(fields.contains(&(
            "work_avoided_bytes_not_read".to_string(),
            "unknown".to_string()
        )));
        assert!(fields.contains(&(
            "work_avoided_bytes_not_read_known".to_string(),
            "false".to_string()
        )));
    }

    #[test]
    fn vortex_local_engine_why_fields_include_claim_blockers() {
        let uri = DatasetUri::new("file://tmp/data.vortex").expect("uri");
        let request = shardloom_vortex::VortexLocalEngineRequest::new(
            uri,
            shardloom_vortex::VortexLocalEnginePrimitive::Count,
            1,
            1,
        )
        .expect("request");
        let report =
            shardloom_vortex::VortexLocalEngineReport::unsupported(request, "test", "blocked");
        let why = report.why_report();
        let mut fields = Vec::new();
        append_vortex_local_engine_why_fields(&mut fields, &why);

        assert!(fields.contains(&("why_report_present".to_string(), "true".to_string())));
        assert!(fields.contains(&(
            "why_claim_gate_status".to_string(),
            "unsupported".to_string()
        )));
        assert!(output_field(&fields, "why_blockers").contains("CG-5 global correctness"));
        assert!(output_field(&fields, "why_next_actions").contains("CG-6 comparison"));
        assert_eq!(output_field(&fields, "why_fallback_attempted"), "false");
    }

    fn sample_local_primitive_native_io_certificate() -> NativeIoCertificate {
        NativeIoCertificate::new(
            "cg19.local_primitive.filter_and_project.native_io",
            "native_vortex_source_to_filtered_projected_result",
            sample_local_primitive_source_capability_report(),
            sample_local_primitive_source_pushdown_report(),
            vec![NativeIoRepresentationTransition::new(
                RepresentationState::VortexEncoded,
                RepresentationState::SelectionVectorEncoded,
                false,
            )],
            sample_local_primitive_sink_requirement_report(),
            sample_local_primitive_adapter_fidelity_report(),
            Vec::new(),
            sample_local_primitive_side_effect_report(),
            Vec::new(),
        )
        .expect("certificate")
    }

    fn sample_local_primitive_source_capability_report() -> NativeIoSourceCapabilityReport {
        NativeIoSourceCapabilityReport {
            source_kind: "vortex".to_string(),
            adapter_id: "shardloom.adapter.vortex.local_primitive.v1".to_string(),
            schema_discovery_status: "vortex_scan_schema_available".to_string(),
            statistics_availability: "row_count_available".to_string(),
            pushdown_capabilities: "filter,project".to_string(),
            encoded_representation_preserved: true,
            range_read_capability: false,
            streaming_capability: true,
            object_store_capability: false,
            fallback_attempted: false,
        }
    }

    fn sample_local_primitive_source_pushdown_report() -> NativeIoSourcePushdownReport {
        NativeIoSourcePushdownReport {
            accepted_operations: vec!["filter".to_string(), "project".to_string()],
            rejected_operations: Vec::new(),
            guarantee: "exact_filter_project_from_single_vortex_scan_pushdown".to_string(),
            proof_basis: "test".to_string(),
            residual_expression: None,
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        }
    }

    fn sample_local_primitive_sink_requirement_report() -> NativeIoSinkRequirementReport {
        NativeIoSinkRequirementReport {
            target_format: "local_filtered_projected_stream_summary".to_string(),
            accepts_encoded: true,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: true,
            requires_ordering: false,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: true,
            max_chunk_size: Some(3),
            backpressure_policy: "bounded_local_scan_chunks".to_string(),
        }
    }

    fn sample_local_primitive_adapter_fidelity_report() -> NativeIoAdapterFidelityReport {
        NativeIoAdapterFidelityReport {
            adapter_id: "shardloom.adapter.vortex.local_primitive.v1".to_string(),
            source_kind: "vortex".to_string(),
            sink_kind: "local_filtered_projected_stream_summary".to_string(),
            metadata_preserved: true,
            statistics_preserved: true,
            encoded_representation_preserved: true,
            materialization_required: false,
            fidelity_loss: "none".to_string(),
            metadata_loss: "none".to_string(),
            fallback_attempted: false,
        }
    }

    fn sample_local_primitive_side_effect_report() -> NativeIoSideEffectReport {
        NativeIoSideEffectReport {
            data_read: true,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        }
    }

    fn sample_local_primitive_execution_certificate() -> ExecutionCertificate {
        let mut input = ExecutionCertificateInput::new(
            "vortex-local-encoded-count-u64-20000.count_all.execution-certificate",
            "vortex.local_primitive.count_all",
        )
        .expect("input");
        input.plan_ref = Some("vortex-run:count_all".to_string());
        input.input_ref =
            Some("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex".to_string());
        input.output_ref = Some("count_result=20000".to_string());
        input.correctness_fixture_id = Some("vortex-local-encoded-count-u64-20000".to_string());
        input.expected_outcome = Some(ExpectedOutcome::EncodedCount { count: 20000 });
        input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count: 20000 });
        input.side_effects_performed = vec!["local_vortex_scan".to_string()];
        input.data_read = true;
        input.correctness_passed = true;
        ExecutionCertificate::evaluate(input)
    }

    #[test]
    fn vortex_local_primitive_native_io_fields_include_certificate_evidence() {
        let certificate = sample_local_primitive_native_io_certificate();
        let mut fields = Vec::new();
        append_vortex_local_primitive_native_io_certificate_fields(&mut fields, Some(&certificate));

        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_pushdown_accepted_operations"
            ),
            "filter,project"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_representation_transitions"
            ),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_sink_target_format"),
            "local_filtered_projected_stream_summary"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_data_materialized"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_fallback_attempted"),
            "false"
        );
    }

    #[test]
    fn vortex_local_primitive_execution_certificate_fields_include_correctness_evidence() {
        let certificate = sample_local_primitive_execution_certificate();
        let mut fields = Vec::new();
        append_vortex_local_primitive_execution_certificate_fields(&mut fields, Some(&certificate));

        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_emitted"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_fixture_id"),
            "vortex-local-encoded-count-u64-20000"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_execution_certificate_correctness_passed"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_execution_certificate_data_decoded"
            ),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_execution_certificate_fallback_attempted"
            ),
            "false"
        );
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
    fn rfc_coverage_followthrough_plan_returns_success() {
        let code = run(vec!["rfc-coverage-followthrough-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn universal_harness_fields_expose_import_deployment_and_baseline_maturity() {
        let report = shardloom_core::plan_universal_harness();
        let fields = evidence_certificates::universal_harness_fields(&report);

        assert_eq!(
            output_field(&fields, "universal_harness_status"),
            "evidence_incomplete"
        );
        assert_eq!(output_field(&fields, "harness_environment_count"), "5");
        assert_eq!(output_field(&fields, "external_baseline_count"), "6");
        assert_eq!(
            output_field(&fields, "harness_environment_kind_order"),
            "local,ci,container,foundry_optional,benchmark_extras_optional"
        );
        assert_eq!(
            output_field(&fields, "baseline_engine_order"),
            "spark,datafusion,polars,duckdb,dask,pandas"
        );
        assert_eq!(output_field(&fields, "local_harness_required"), "true");
        assert_eq!(output_field(&fields, "ci_harness_required"), "true");
        assert_eq!(output_field(&fields, "container_harness_required"), "true");
        assert_eq!(
            output_field(&fields, "foundry_optional_harness_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "optional_benchmark_environment_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "external_engines_as_runtime_dependencies_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "baselines_comparison_only_runtime_dependency_free"),
            "true"
        );
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
    }

    #[test]
    fn rfc_coverage_followthrough_fields_expose_rfc_safety_gates() {
        let report = plan_rfc_coverage_followthrough();
        let fields = evidence_certificates::rfc_coverage_followthrough_fields(&report);

        assert_eq!(
            output_field(&fields, "rfc_coverage_status"),
            "evidence_required"
        );
        assert_eq!(output_field(&fields, "rfc_coverage_entry_count"), "5");
        assert_eq!(
            output_field(&fields, "rfc_order"),
            "rfc_0010,rfc_0011,rfc_0020,rfc_0022,rfc_0023"
        );
        assert_eq!(
            output_field(&fields, "area_order"),
            "developer_agent_usability,modular_extensibility,schema_catalog_table_compatibility,native_plan_ir_interop,extension_plugin_sandboxing"
        );
        assert_eq!(output_field(&fields, "rfc0010_status"), "evidence_required");
        assert_eq!(output_field(&fields, "rfc0011_status"), "evidence_required");
        assert_eq!(output_field(&fields, "rfc0020_status"), "evidence_required");
        assert_eq!(output_field(&fields, "rfc0022_status"), "evidence_required");
        assert_eq!(output_field(&fields, "rfc0023_status"), "evidence_required");
        assert_eq!(
            output_field(&fields, "deterministic_machine_readable_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "import_discovery_dry_run_safety_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "typed_effect_materialization_metadata_required"),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "metadata_discovery_separate_from_read_write_commit"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "imported_plan_execution_blocked"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "extension_manifest_inspection_only"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "extension_code_execution_blocked"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "all_entries_runtime_expansion_blocked"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "all_entries_dependency_expansion_blocked"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "all_entries_external_effects_blocked"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "runtime_expansion_performed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "dependency_expansion_performed"),
            "false"
        );
        assert_eq!(output_field(&fields, "external_effect_performed"), "false");
        assert_eq!(output_field(&fields, "external_engine_invoked"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
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
    fn table_intelligence_plan_returns_success() {
        let code = run(vec!["table-intelligence-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn table_intelligence_plan_rejects_extra_args() {
        let code = run(vec![
            "table-intelligence-plan".to_string(),
            "extra".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_request_plan_ready_returns_success() {
        let code = run(vec![
            "object-store-request-plan".to_string(),
            "ready".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn object_store_request_plan_missing_ranges_returns_non_zero() {
        let code = run(vec![
            "object-store-request-plan".to_string(),
            "missing-ranges".to_string(),
        ]);
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
    fn usage_includes_dynamic_work_shaping_plan() {
        assert!(cli_usage_line().contains("dynamic-work-shaping-plan"));
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
    fn usage_includes_table_intelligence_plan() {
        assert!(cli_usage_line().contains("table-intelligence-plan"));
    }
    #[test]
    fn usage_includes_object_store_request_plan() {
        assert!(cli_usage_line().contains("object-store-request-plan"));
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
    fn usage_includes_correctness_harness_plan() {
        assert!(cli_usage_line().contains("correctness-harness-plan"));
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
    fn usage_includes_cg17_stateful_reuse_gate() {
        assert!(cli_usage_line().contains("cg17-stateful-reuse-gate"));
    }
    #[test]
    fn usage_includes_universal_harness_plan() {
        assert!(cli_usage_line().contains("universal-harness-plan"));
    }
    #[test]
    fn usage_includes_rfc_coverage_followthrough_plan() {
        assert!(cli_usage_line().contains("rfc-coverage-followthrough-plan"));
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
    fn parse_sizing_feedback_signals_rejects_unknown_and_allows_empty() {
        assert!(engine_runtime_planning::parse_sizing_feedback_signals("unknown").is_err());
        assert!(
            engine_runtime_planning::parse_sizing_feedback_signals(" ")
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn parse_sizing_feedback_signals_deduplicates_and_accepts_aliases() {
        let signals = engine_runtime_planning::parse_sizing_feedback_signals(
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
    fn release_plan_fields_expose_release_readiness_blockers_without_publish() {
        let plan = shardloom_core::ReleasePlan::default_foundation_plan();
        let evidence = plan.release_readiness_evidence();
        let publication = plan.publication_boundary_report();
        let fields = packaging_deployment::release_plan_fields(
            &plan,
            &evidence,
            &publication,
            "release_plan",
        );

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.release_readiness_evidence.v1"
        );
        assert_eq!(output_field(&fields, "mode"), "release_plan");
        assert_eq!(output_field(&fields, "schema_version_check"), "present");
        assert_eq!(output_field(&fields, "api_stability_check"), "present");
        assert_eq!(output_field(&fields, "dependency_license_check"), "missing");
        assert_eq!(output_field(&fields, "sbom_check"), "missing");
        assert_eq!(
            output_field(&fields, "provenance_attestation_check"),
            "missing"
        );
        assert_eq!(
            output_field(&fields, "no_fallback_release_check"),
            "present"
        );
        assert_eq!(
            output_field(&fields, "public_release_claim_allowed"),
            "false"
        );
        assert_eq!(output_field(&fields, "external_publish_performed"), "false");
        assert_eq!(output_field(&fields, "runtime_execution"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(
            output_field(&fields, "conda_certification_schema_version"),
            "shardloom.conda_build_install_certification.v1"
        );
        assert_eq!(output_field(&fields, "conda_package_count"), "3");
        assert_eq!(output_field(&fields, "conda_recipe_scaffold_count"), "3");
        assert_eq!(output_field(&fields, "conda_certified_package_count"), "0");
        assert_eq!(
            output_field(&fields, "conda_release_gate_blocking_count"),
            "5"
        );
        assert_eq!(
            output_field(&fields, "conda_clean_build_certified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_clean_install_certified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_package_publication_allowed"),
            "false"
        );
        assert_eq!(output_field(&fields, "conda_build_invoked"), "false");
        assert_eq!(output_field(&fields, "conda_install_invoked"), "false");
        assert_eq!(output_field(&fields, "conda_release_gated"), "true");
        assert_eq!(output_field(&fields, "conda_side_effect_free"), "true");
    }

    #[test]
    fn release_plan_fields_keep_publication_boundaries_distinct() {
        let plan = shardloom_core::ReleasePlan::default_foundation_plan();
        let evidence = plan.release_readiness_evidence();
        let publication = plan.publication_boundary_report();
        let fields = packaging_deployment::release_plan_fields(
            &plan,
            &evidence,
            &publication,
            "package_plan",
        );

        assert_eq!(
            output_field(&fields, "publication_boundary_schema_version"),
            "shardloom.release_publication_boundaries.v1"
        );
        assert_eq!(
            output_field(&fields, "local_development_boundary"),
            "enabled"
        );
        assert_eq!(output_field(&fields, "public_package_boundary"), "planned");
        assert_eq!(output_field(&fields, "github_release_boundary"), "planned");
        assert_eq!(
            output_field(&fields, "container_image_boundary"),
            "disabled"
        );
        assert_eq!(output_field(&fields, "server_mode_boundary"), "disabled");
        assert_eq!(
            output_field(&fields, "benchmark_extras_boundary"),
            "planned"
        );
        assert_eq!(
            output_field(
                &fields,
                "package_publication_distinct_from_local_development"
            ),
            "true"
        );
        assert_eq!(output_field(&fields, "benchmark_extras_optional"), "true");
        assert_eq!(
            output_field(&fields, "benchmark_extras_core_dependency"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "publication_fallback_dependency_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_tagged_archive_required"),
            "true"
        );
        assert_eq!(output_field(&fields, "conda_source_hash_required"), "true");
        assert_eq!(
            output_field(&fields, "conda_version_alignment_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "conda_provenance_attestation_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "conda_human_approval_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "conda_tagged_archive_present"),
            "false"
        );
        assert_eq!(output_field(&fields, "conda_source_hash_verified"), "false");
        assert_eq!(
            output_field(&fields, "conda_version_alignment_verified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_provenance_attestation_present"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_human_approval_present"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "conda_fallback_dependency_allowed"),
            "false"
        );
    }

    #[test]
    fn python_wrapper_plan_returns_success() {
        let code = run(vec!["python-wrapper-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn agent_contract_pack_returns_success() {
        let code = run(vec!["agent-contract-pack".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn security_plan_returns_success() {
        let code = run(vec!["security-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn effect_budget_plan_returns_success() {
        let code = run(vec!["effect-budget-plan".to_string()]);
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
        assert_fake_vortex_file_io_plan_code(code);
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
        assert_fake_vortex_file_io_plan_code(code);
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
    fn correctness_harness_plan_returns_success() {
        let code = run(vec!["correctness-harness-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn benchmark_claim_evidence_plan_returns_success() {
        let code = run(vec!["benchmark-claim-evidence-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn benchmark_claim_evidence_traditional_plan_returns_success() {
        let code = run(vec![
            "benchmark-claim-evidence-plan".to_string(),
            "traditional-analytics".to_string(),
        ]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn dynamic_work_shaping_plan_returns_success() {
        let code = run(vec!["dynamic-work-shaping-plan".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn dynamic_work_shaping_plan_unknown_profile_returns_non_zero() {
        let code = run(vec![
            "dynamic-work-shaping-plan".to_string(),
            "unknown".to_string(),
        ]);
        assert_ne!(code, ExitCode::SUCCESS);
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
        assert_fake_vortex_file_io_plan_code(code);
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
        assert!(usage.contains("|benchmark-claim-evidence-plan"));
        assert!(usage.contains("|dynamic-work-shaping-plan"));
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
        let parsed = vortex_primitive_execution::parse_vortex_count_args(
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
            vortex_primitive_execution::VortexCountExecutionRequest::LocalEncodedCount {
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
    fn vortex_count_local_encoded_evidence_matches_struct_count_fixture() {
        let fixture =
            local_encoded_count_correctness_fixture_for_target(&local_struct_fixture_uri())
                .expect("fixture match");

        assert_eq!(fixture.id.as_str(), "vortex-local-count-all-struct-five");
        assert_eq!(fixture.expected, ExpectedOutcome::EncodedCount { count: 5 });
    }

    fn local_struct_fixture_uri() -> DatasetUri {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri")
    }

    fn executed_local_primitive_report(
        kind: shardloom_vortex::VortexQueryPrimitiveKind,
    ) -> shardloom_vortex::VortexLocalPrimitiveExecutionReport {
        let mut report =
            shardloom_vortex::VortexLocalPrimitiveExecutionReport::feature_disabled(kind);
        report.status = shardloom_vortex::VortexLocalPrimitiveExecutionStatus::Executed;
        report
    }

    #[test]
    fn vortex_run_local_primitive_fixture_matching_covers_struct_count_project_paths() {
        let uri = local_struct_fixture_uri();
        let cases = [
            (
                VortexQueryPrimitiveRequest::count_all(uri.clone()),
                "vortex-local-count-all-struct-five",
            ),
            (
                VortexQueryPrimitiveRequest::count_where(
                    uri.clone(),
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                ),
                "vortex-local-count-where-struct-five",
            ),
            (
                VortexQueryPrimitiveRequest::project(
                    uri.clone(),
                    ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
                ),
                "vortex-local-project-struct-five",
            ),
            (
                VortexQueryPrimitiveRequest::filter(
                    uri.clone(),
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                ),
                "vortex-local-filter-struct-five",
            ),
            (
                VortexQueryPrimitiveRequest::filter_and_project(
                    uri,
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                    ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
                ),
                "vortex-local-filter-project-struct-five",
            ),
        ];

        for (request, expected_fixture_id) in cases {
            let report = executed_local_primitive_report(request.kind);
            let fixture = local_primitive_correctness_fixture_for_request(&request, &report)
                .expect("fixture match");

            assert_eq!(fixture.id.as_str(), expected_fixture_id);
            assert_eq!(
                fixture.source_ref.as_deref(),
                Some("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
            );
        }
    }

    #[test]
    fn vortex_run_local_primitive_fixture_matching_rejects_non_fixture_shape() {
        let request = VortexQueryPrimitiveRequest::count_where(
            local_struct_fixture_uri(),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(4),
            },
        );
        let report = executed_local_primitive_report(request.kind);

        let fixture = local_primitive_correctness_fixture_for_request(&request, &report);

        assert!(fixture.is_none());
    }

    fn synthetic_local_encoded_count_reports(
        uri: DatasetUri,
        count: u64,
    ) -> (
        shardloom_vortex::VortexEncodedReadExecutionReport,
        VortexLocalExecutionReport,
    ) {
        let readiness = build_vortex_encoded_count_readiness(uri.clone(), 1, 1).expect("readiness");
        let mut encoded_report =
            shardloom_vortex::VortexEncodedReadExecutionReport::feature_disabled(
                shardloom_vortex::VortexEncodedReadExecutionInput::new(readiness)
                    .allow_encoded_read_execution(true),
            );
        encoded_report.feature_status = VortexEncodedReadExecutorFeatureStatus::Enabled;
        encoded_report.status = VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted;
        encoded_report.mode = VortexEncodedReadExecutionMode::LocalScanEncodedArrayLengthCount;
        encoded_report.data_read = true;
        encoded_report.upstream_scan_called = true;
        encoded_report.arrays_read_count = 1;
        encoded_report.rows_counted = count;
        encoded_report.count_result = Some(count);
        encoded_report.local_scan_target_uri = Some(uri.clone());
        encoded_report.local_scan_readiness_source_uri = Some(uri.clone());
        encoded_report.local_scan_source_uri_matches_target = true;

        let input = shardloom_vortex::VortexLocalExecutionInput::new(
            VortexQueryPrimitiveRequest::count_all(uri),
        )
        .allow_encoded_read(true);
        let local_report = VortexLocalExecutionReport::local_encoded_count_executed(input, count);
        (encoded_report, local_report)
    }

    #[test]
    fn vortex_count_local_encoded_actual_non_fixture_executes_but_stays_uncertified() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let temp_path = std::env::temp_dir().join(format!(
            "shardloom-local-countall-non-fixture-{}-{nanos}.vortex",
            std::process::id()
        ));
        std::fs::copy(&fixture_path, &temp_path).expect("copy fixture");
        let uri = DatasetUri::new(temp_path.to_string_lossy().to_string()).expect("uri");

        let (encoded_report, local_report) =
            run_vortex_approved_local_encoded_count(uri, 1, 2).expect("local count");
        let evidence =
            vortex_count_local_encoded_evidence(&encoded_report, &local_report).expect("evidence");
        let _ = std::fs::remove_file(&temp_path);

        assert_eq!(encoded_report.count_result, Some(5));
        assert_eq!(
            encoded_report.status,
            VortexEncodedReadExecutionStatus::LocalScanEncodedCountExecuted
        );
        assert_eq!(
            local_report.status,
            VortexLocalExecutionStatus::LocalEncodedCountExecuted
        );
        assert_eq!(
            evidence.target_policy,
            VortexCountLocalEncodedTargetPolicy::LocalVortexUncertified
        );
        assert!(evidence.native_io_certificate.is_some());
        assert!(evidence.certificate.is_none());
        assert!(evidence.physical_kernel.is_none());
        assert!(evidence.kernel_admission.is_none());
        assert!(!evidence.has_errors());

        let mut fields = Vec::new();
        append_vortex_count_local_encoded_evidence_fields(&mut fields, &evidence);
        assert_eq!(
            output_field(&fields, "generalized_local_count_execution_allowed"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_correctness_certified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(&fields, "execution_certificate_emitted"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "encoded_count_physical_kernel_emitted"),
            "false"
        );
    }

    fn output_field<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields.iter().find(|(name, _)| name == key).map_or_else(
            || panic!("missing output field {key}"),
            |(_, value)| value.as_str(),
        )
    }

    fn vortex_count_where_filter_summary(stats: SegmentStats) -> VortexMetadataSummaryReport {
        let mut segment =
            shardloom_vortex::VortexSegmentMetadataSummary::unknown().with_row_count(5);
        segment.add_column(
            shardloom_vortex::VortexColumnMetadataSummary::new(
                ColumnRef::new("x").expect("column"),
            )
            .with_dtype(LogicalDType::Int64)
            .with_encoding(EncodingKind::VortexNative("test".to_string()))
            .with_layout(LayoutKind::Flat)
            .with_stats(stats)
            .with_statistics_available(true),
        );
        let mut summary = shardloom_vortex::VortexFileMetadataSummary::empty();
        summary.add_segment(segment);
        VortexMetadataSummaryReport {
            status: shardloom_vortex::VortexMetadataSummaryStatus::Summarized,
            summary,
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn vortex_count_where_filter_evidence_reports_selection_vector_admission() {
        let uri = DatasetUri::new("file:///tmp/filter-evidence.vortex").expect("uri");
        let mut stats = SegmentStats::with_row_count(5);
        stats.null_count = Some(0);
        let summary = vortex_count_where_filter_summary(stats);
        let predicate = PredicateExpr::IsNotNull {
            column: ColumnRef::new("x").expect("column"),
        };
        let request = VortexQueryPrimitiveRequest::count_where(uri, predicate.clone());
        let result = evaluate_vortex_query_primitive(request, &summary).expect("result");
        let evidence = vortex_count_where_filter_evidence(&predicate, &summary).expect("evidence");
        let count = match &result.value {
            VortexQueryPrimitiveValue::Count(value) => Some(value.to_owned()),
            _ => None,
        };

        assert_eq!(
            evidence.predicate_evaluation.status,
            VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        );
        assert!(
            evidence
                .filter_kernel
                .is_safe_native_filter_kernel_evidence()
        );
        assert!(evidence.filter_kernel_admission.slot_marked_present);

        let fields =
            vortex_count_where_fields(&result, count, "is_not_null:x".to_string(), &evidence, None);

        assert_eq!(
            output_field(&fields, "encoded_predicate_status"),
            "evaluated_selections"
        );
        assert_eq!(
            output_field(&fields, "selection_vector_filter_kernel_status"),
            "evaluated_selection_vectors"
        );
        assert_eq!(
            output_field(&fields, "selection_vector_filter_admission_status"),
            "registry_ready"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_selection_vector_evidence_present"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_generalized_execution_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_cg13_closeout_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_local_execution_requested"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_emitted"),
            "false"
        );
    }

    #[test]
    fn vortex_count_where_filter_evidence_reports_encoded_value_blocker() {
        let uri = DatasetUri::new("file:///tmp/filter-evidence.vortex").expect("uri");
        let mut stats = SegmentStats::with_row_count(5);
        stats.min_value = Some(StatValue::Int64(1));
        stats.max_value = Some(StatValue::Int64(9));
        let summary = vortex_count_where_filter_summary(stats);
        let predicate = PredicateExpr::Compare {
            column: ColumnRef::new("x").expect("column"),
            op: ComparisonOp::Eq,
            value: StatValue::Int64(4),
        };
        let request = VortexQueryPrimitiveRequest::count_where(uri, predicate.clone());
        let result = evaluate_vortex_query_primitive(request, &summary).expect("result");
        let evidence = vortex_count_where_filter_evidence(&predicate, &summary).expect("evidence");
        let count = match &result.value {
            VortexQueryPrimitiveValue::Count(value) => Some(value.to_owned()),
            _ => None,
        };

        assert_eq!(
            evidence.predicate_evaluation.status,
            VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues
        );
        assert!(
            !evidence
                .filter_kernel
                .is_safe_native_filter_kernel_evidence()
        );
        assert!(!evidence.filter_kernel_admission.slot_marked_present);

        let fields =
            vortex_count_where_fields(&result, count, "eq:x:4".to_string(), &evidence, None);

        assert_eq!(
            output_field(&fields, "encoded_predicate_status"),
            "needs_encoded_values"
        );
        assert_eq!(
            output_field(&fields, "selection_vector_filter_kernel_status"),
            "needs_encoded_values"
        );
        assert_eq!(
            output_field(&fields, "selection_vector_filter_kernel_safe_evidence"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_requires_encoded_value_kernel"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "filtered_count_cg2_closeout_allowed"),
            "false"
        );
    }

    #[test]
    fn vortex_count_where_local_execution_arg_parser_accepts_optional_execution() {
        let none = parse_vortex_count_where_local_execution_args(Vec::<String>::new().into_iter())
            .expect("none");
        assert!(none.is_none());

        let parsed = parse_vortex_count_where_local_execution_args(
            [
                "--execute-local-primitive".to_string(),
                "2".to_string(),
                "4".to_string(),
            ]
            .into_iter(),
        )
        .expect("parsed")
        .expect("request");

        assert_eq!(parsed.memory_gb, 2);
        assert_eq!(parsed.max_parallelism, 4);
    }

    #[test]
    fn vortex_count_where_local_execution_arg_parser_rejects_bad_values() {
        assert!(
            parse_vortex_count_where_local_execution_args(["--bad".to_string()].into_iter())
                .is_err()
        );
        assert!(
            parse_vortex_count_where_local_execution_args(
                ["--execute-local-primitive".to_string(), "1".to_string()].into_iter(),
            )
            .is_err()
        );
        assert!(
            parse_vortex_count_where_local_execution_args(
                [
                    "--execute-local-primitive".to_string(),
                    "0".to_string(),
                    "1".to_string(),
                ]
                .into_iter(),
            )
            .is_err()
        );
        assert!(
            parse_vortex_count_where_local_execution_args(
                [
                    "--execute-local-primitive".to_string(),
                    "1".to_string(),
                    "0".to_string(),
                ]
                .into_iter(),
            )
            .is_err()
        );
        assert!(
            parse_vortex_count_where_local_execution_args(
                [
                    "--execute-local-primitive".to_string(),
                    "1".to_string(),
                    "1".to_string(),
                    "--execute-local-primitive".to_string(),
                ]
                .into_iter(),
            )
            .is_err()
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_count_where_local_execution_certifies_checked_in_struct_fixture() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::count_where(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );
        let local_request = VortexCountWhereLocalExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_count_where_local_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_count_where_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.count(), Some(3));
        assert!(evidence.selection_vector_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(
            evidence
                .execution_certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        );
        assert_eq!(
            output_field(&fields, "filtered_count_local_execution_status"),
            "executed"
        );
        assert_eq!(
            output_field(
                &fields,
                "filtered_count_local_execution_selection_vector_guarantee"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_representation_transitions"
            ),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_fixture_id"),
            "vortex-local-count-where-struct-five"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_project_local_execution_certifies_checked_in_struct_fixture() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_project_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.projected_rows(), Some(5));
        assert!(evidence.projection_encoded_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(
            evidence
                .execution_certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        );
        assert_eq!(
            output_field(&fields, "project_local_execution_status"),
            "executed"
        );
        assert_eq!(
            output_field(&fields, "project_local_execution_rows_projected"),
            "5"
        );
        assert_eq!(
            output_field(&fields, "project_local_execution_projected_columns"),
            "metric"
        );
        assert_eq!(
            output_field(
                &fields,
                "project_local_execution_encoded_projection_guarantee"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_representation_transitions"
            ),
            "vortex_encoded->vortex_encoded"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_fixture_id"),
            "vortex-local-project-struct-five"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_project_local_execution_leaves_non_fixture_shape_uncertified() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_project_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.projected_rows(), Some(5));
        assert!(evidence.projection_encoded_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(evidence.execution_certificate.is_none());
        assert_eq!(
            output_field(&fields, "project_local_execution_correctness_certified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_emitted"),
            "false"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_filter_project_local_execution_certifies_checked_in_struct_fixture() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_filter_project_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.selected_rows(), Some(3));
        assert_eq!(evidence.projected_rows(), Some(3));
        assert!(evidence.filter_project_encoded_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(
            evidence
                .execution_certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        );
        assert_eq!(
            output_field(&fields, "filter_project_local_execution_status"),
            "executed"
        );
        assert_eq!(
            output_field(&fields, "filter_project_local_execution_rows_selected"),
            "3"
        );
        assert_eq!(
            output_field(&fields, "filter_project_local_execution_rows_projected"),
            "3"
        );
        assert_eq!(
            output_field(&fields, "filter_project_local_execution_projected_columns"),
            "metric"
        );
        assert_eq!(
            output_field(
                &fields,
                "filter_project_local_execution_selection_vector_guarantee"
            ),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "filter_project_local_execution_projection_pushdown_guarantee"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_representation_transitions"
            ),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_fixture_id"),
            "vortex-local-filter-project-struct-five"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_filter_project_local_execution_leaves_non_fixture_shape_uncertified() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(4),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_filter_project_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.selected_rows(), Some(2));
        assert_eq!(evidence.projected_rows(), Some(2));
        assert!(evidence.filter_project_encoded_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(evidence.execution_certificate.is_none());
        assert_eq!(
            output_field(
                &fields,
                "filter_project_local_execution_correctness_certified"
            ),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_emitted"),
            "false"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_filter_local_execution_certifies_checked_in_struct_fixture() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_filter_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.selected_rows(), Some(3));
        assert!(evidence.selection_vector_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(
            evidence
                .execution_certificate
                .as_ref()
                .is_some_and(ExecutionCertificate::is_certified)
        );
        assert_eq!(
            output_field(&fields, "filter_local_execution_status"),
            "executed"
        );
        assert_eq!(
            output_field(&fields, "filter_local_execution_rows_selected"),
            "3"
        );
        assert_eq!(
            output_field(&fields, "filter_local_execution_selection_vector_guarantee"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_primitive_native_io_representation_transitions"
            ),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_fixture_id"),
            "vortex-local-filter-struct-five"
        );
    }

    #[cfg(feature = "vortex-local-primitives")]
    #[test]
    fn vortex_filter_local_execution_leaves_non_fixture_shape_uncertified() {
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        let request = VortexQueryPrimitiveRequest::filter(
            DatasetUri::new(fixture_path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(4),
            },
        );
        let local_request = VortexLocalPrimitiveCliExecutionRequest {
            memory_gb: 1,
            max_parallelism: 2,
        };

        let evidence = vortex_local_primitive_cli_execution_evidence(&request, &local_request)
            .expect("evidence");
        let mut fields = Vec::new();
        append_vortex_filter_local_execution_fields(&mut fields, Some(&evidence));

        assert_eq!(evidence.report.status.as_str(), "executed");
        assert_eq!(evidence.selected_rows(), Some(2));
        assert!(evidence.selection_vector_guaranteed());
        assert!(evidence.native_io_certificate.is_certified());
        assert!(evidence.execution_certificate.is_none());
        assert_eq!(
            output_field(&fields, "filter_local_execution_correctness_certified"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "local_primitive_execution_certificate_emitted"),
            "false"
        );
    }

    #[test]
    fn vortex_count_local_encoded_evidence_reports_uncertified_non_fixture_policy() {
        let uri = DatasetUri::new("file:///tmp/non-fixture.vortex").expect("uri");
        let (encoded_report, local_report) = synthetic_local_encoded_count_reports(uri, 42);

        let evidence =
            vortex_count_local_encoded_evidence(&encoded_report, &local_report).expect("evidence");

        assert_eq!(
            evidence.target_policy,
            VortexCountLocalEncodedTargetPolicy::LocalVortexUncertified
        );
        assert!(!evidence.has_errors());

        let mut fields = Vec::new();
        append_vortex_count_local_encoded_evidence_fields(&mut fields, &evidence);

        assert_eq!(
            output_field(&fields, "generalized_local_count_target_policy"),
            "local_vortex_uncertified"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_execution_allowed"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_certificate_path_id"),
            "native_vortex_source_to_scalar_count_result"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_count_native_io_pushdown_accepted_operations"
            ),
            "count_all"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_representation_transitions"),
            "vortex_encoded->vortex_encoded"
        );
        assert_eq!(
            output_field(
                &fields,
                "local_count_native_io_encoded_representation_preserved"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_data_decoded"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_non_fixture_target"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_correctness_certified"),
            "false"
        );
        assert_eq!(
            output_field(
                &fields,
                "generalized_local_count_requires_correctness_fixture"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_production_claim_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_cg13_closeout_allowed"),
            "false"
        );
    }

    #[test]
    fn vortex_count_local_encoded_evidence_reports_certified_fixture_policy() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let (encoded_report, local_report) = synthetic_local_encoded_count_reports(uri, 20_000);

        let evidence =
            vortex_count_local_encoded_evidence(&encoded_report, &local_report).expect("evidence");

        assert_eq!(
            evidence.target_policy,
            VortexCountLocalEncodedTargetPolicy::KnownFixtureCertified
        );
        assert!(!evidence.has_errors());

        let mut fields = Vec::new();
        append_vortex_count_local_encoded_evidence_fields(&mut fields, &evidence);

        assert_eq!(
            output_field(&fields, "generalized_local_count_target_policy"),
            "known_fixture_certified"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_execution_allowed"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_certificate_status"),
            "certified"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_certified"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "local_count_native_io_pushdown_guarantee"),
            "exact_array_length_count"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_non_fixture_target"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_correctness_certified"),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "generalized_local_count_requires_correctness_fixture"
            ),
            "false"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_cg2_closeout_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_cg13_closeout_allowed"),
            "false"
        );
    }

    #[test]
    fn vortex_count_local_encoded_evidence_certifies_struct_count_fixture() {
        let uri = local_struct_fixture_uri();
        let (encoded_report, local_report) = synthetic_local_encoded_count_reports(uri, 5);

        let evidence =
            vortex_count_local_encoded_evidence(&encoded_report, &local_report).expect("evidence");

        assert_eq!(
            evidence.target_policy,
            VortexCountLocalEncodedTargetPolicy::KnownFixtureCertified
        );
        assert_eq!(
            evidence
                .certificate
                .as_ref()
                .and_then(|certificate| certificate.correctness_fixture_id.as_deref()),
            Some("vortex-local-count-all-struct-five")
        );
        assert!(!evidence.has_errors());

        let mut fields = Vec::new();
        append_vortex_count_local_encoded_evidence_fields(&mut fields, &evidence);

        assert_eq!(
            output_field(&fields, "generalized_local_count_target_policy"),
            "known_fixture_certified"
        );
        assert_eq!(
            output_field(&fields, "generalized_local_count_correctness_certified"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "correctness_fixture_id"),
            "vortex-local-count-all-struct-five"
        );
    }

    #[test]
    fn vortex_count_benchmark_report_blocks_claims_without_external_results() {
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("metadata_footer_u64_20000.vortex");
        let uri = DatasetUri::new(fixture_path.to_string_lossy().to_string()).expect("uri");
        let iterations = vec![
            benchmark_runtime::VortexCountBenchmarkIterationSummary::synthetic_success(
                1_000, 20_000,
            ),
            benchmark_runtime::VortexCountBenchmarkIterationSummary::synthetic_success(
                2_000, 20_000,
            ),
        ];

        let report = benchmark_runtime::VortexCountBenchmarkReport::from_iterations(
            uri, 1, 2, 2, iterations,
        )
        .expect("report");
        let fields = benchmark_runtime::vortex_count_benchmark_fields(&report);

        assert!(!report.has_errors());
        assert_eq!(report.count_result(), Some(20_000));
        assert_eq!(report.correctness_evidence, BenchmarkEvidenceState::Present);
        assert_eq!(
            output_field(&fields, "benchmark_execution_implemented"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "external_baselines"),
            "pandas,polars,duckdb,spark,datafusion,dask"
        );
        assert_eq!(
            output_field(&fields, "external_baseline_execution"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "comparison_missing_result_count"),
            "6"
        );
        assert_eq!(
            output_field(&fields, "claim_gate_status"),
            "evidence_missing"
        );
        assert_eq!(output_field(&fields, "performance_claim_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
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
    fn vortex_project_local_primitive_bridges_when_feature_enabled() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");

        let code = run(vec![
            "vortex-project".to_string(),
            fixture_path.to_string_lossy().to_string(),
            "metric".to_string(),
            "--execute-local-primitive".to_string(),
            "1".to_string(),
            "2".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);

        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_filter_project_local_primitive_bridges_when_feature_enabled() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");

        let code = run(vec![
            "vortex-filter-project".to_string(),
            fixture_path.to_string_lossy().to_string(),
            "gte:value:3".to_string(),
            "metric".to_string(),
            "--execute-local-primitive".to_string(),
            "1".to_string(),
            "2".to_string(),
            "--format".to_string(),
            "json".to_string(),
        ]);

        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn vortex_filter_local_primitive_bridges_when_feature_enabled() {
        if !vortex_encoded_read_spike_feature_enabled() {
            return;
        }
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace crate parent")
            .join("shardloom-vortex")
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");

        let code = run(vec![
            "vortex-filter".to_string(),
            fixture_path.to_string_lossy().to_string(),
            "gte:value:3".to_string(),
            "--execute-local-primitive".to_string(),
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
                "agent-contract-pack",
                "feature-footprint",
                "effect-budget-plan",
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
    fn usage_includes_feature_footprint() {
        assert!(cli_usage_line().contains("feature-footprint"));
    }

    #[test]
    fn usage_includes_effect_budget_plan() {
        assert!(cli_usage_line().contains("effect-budget-plan"));
    }

    #[test]
    fn usage_includes_agent_contract_pack() {
        assert!(cli_usage_line().contains("agent-contract-pack"));
    }

    #[test]
    fn agent_contract_pack_fields_include_no_probe_and_no_fallback() {
        let report = shardloom_core::AgentContractPack::default_pack();
        let fields = packaging_deployment::agent_contract_pack_fields(&report);

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.agent_contract_pack.v1"
        );
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "fallback_allowed_surface_count"), "0");
        assert_eq!(output_field(&fields, "text_is_authoritative"), "false");
        assert_eq!(output_field(&fields, "no_probe_default"), "true");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
        assert!(output_field(&fields, "surface_order").contains("feature_footprint"));
        assert!(output_field(&fields, "surface_order").contains("effect_budget"));
        assert!(output_field(&fields, "recommended_sequence").contains("doctor --format json"));
    }

    #[test]
    fn effect_budget_fields_include_no_effects_and_no_fallback() {
        let report = shardloom_core::EffectBudgetReport::planning_default();
        let fields = operational_hardening::effect_budget_fields(&report);

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.effect_budget.v1"
        );
        assert_eq!(
            output_field(&fields, "budget_mode"),
            "deny_external_effects_by_default"
        );
        assert_eq!(output_field(&fields, "approved_scope_count"), "0");
        assert_eq!(output_field(&fields, "external_effects_allowed"), "false");
        assert_eq!(output_field(&fields, "network_egress_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
        assert!(output_field(&fields, "scope_order").contains("llm_call"));
        assert!(output_field(&fields, "scope_order").contains("network_egress"));
    }

    #[test]
    fn correctness_harness_fields_include_fixture_and_oracle_gaps() {
        let report = shardloom_core::plan_correctness_differential_harness(
            CorrectnessValidationPlan::default_foundation_plan(),
        );
        let fields = evidence_certificates::correctness_harness_fields(&report);

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.correctness_differential_harness.v1"
        );
        assert_eq!(
            output_field(&fields, "report_id"),
            "cg5.correctness_differential_harness.aggregate"
        );
        assert_eq!(output_field(&fields, "harness_status"), "needs_evidence");
        assert_eq!(output_field(&fields, "planned_surface_count"), "9");
        assert_eq!(output_field(&fields, "blocked_surface_count"), "2");
        assert_eq!(
            output_field(&fields, "blocked_surface_order"),
            "deferred_fixture_family_artifacts,benchmark_claim_gate"
        );
        assert_eq!(output_field(&fields, "baseline_count"), "7");
        assert!(output_field(&fields, "baseline_engine_order").contains("dask"));
        assert_eq!(
            output_field(&fields, "fixtures_with_source_ref_count"),
            "18"
        );
        assert_eq!(
            output_field(&fields, "source_backed_edge_fixture_count"),
            "11"
        );
        assert_eq!(output_field(&fields, "not_yet_defined_fixture_count"), "0");
        assert_eq!(output_field(&fields, "deferred_fixture_family_count"), "8");
        assert!(
            output_field(&fields, "deferred_fixture_family_id_order")
                .contains("encoded-vs-decoded-reference")
        );
        assert_eq!(
            output_field(&fields, "deferred_fixture_family_artifact_count"),
            "8"
        );
        assert_eq!(
            output_field(&fields, "deferred_fixture_family_artifact_populated_count"),
            "0"
        );
        assert_eq!(
            output_field(&fields, "deferred_fixture_family_artifacts_populated"),
            "false"
        );
        assert!(
            output_field(&fields, "deferred_fixture_family_artifact_id_order")
                .contains("encoded-vs-decoded-reference.deferred-fixture-family.declared-evidence")
        );
        assert_eq!(
            output_field(&fields, "deferred_fixture_family_artifact_status_order"),
            "declared_not_populated"
        );
        assert_eq!(
            output_field(&fields, "deferred_fixture_family_artifacts_test_only"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_result_artifact_count"),
            "77"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_result_populated_count"),
            "0"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_results_populated"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_result_artifact_status_order"),
            "declared_not_executed"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_artifacts_test_only"),
            "true"
        );
    }

    #[test]
    fn correctness_harness_fields_include_claim_closeout_blockers_and_no_execution() {
        let report = shardloom_core::plan_correctness_differential_harness(
            CorrectnessValidationPlan::default_foundation_plan(),
        );
        let fields = evidence_certificates::correctness_harness_fields(&report);

        assert_eq!(
            output_field(&fields, "benchmark_claim_blocker_order"),
            "deferred_fixture_family_artifacts_not_populated,external_oracle_results_not_populated,property_fuzz_execution_not_performed"
        );
        assert_eq!(
            output_field(&fields, "claim_grade_correctness_closeout_required"),
            "true"
        );
        assert_eq!(
            output_field(&fields, "claim_grade_correctness_closeout_allowed"),
            "false"
        );
        assert_eq!(
            output_field(&fields, "claim_grade_correctness_closeout_blocker_order"),
            "deferred_fixture_family_artifacts_not_populated,external_oracle_results_not_populated,property_fuzz_execution_not_performed"
        );
        assert_eq!(
            output_field(&fields, "external_oracle_execution_required"),
            "true"
        );
        assert_eq!(
            output_field(
                &fields,
                "deferred_fixture_family_artifact_population_required"
            ),
            "true"
        );
        assert_eq!(
            output_field(&fields, "property_fuzz_execution_performed"),
            "false"
        );
        assert_eq!(output_field(&fields, "production_claim_allowed"), "false");
        assert_eq!(
            output_field(&fields, "benchmark_claims_blocked_by_correctness"),
            "true"
        );
        assert_eq!(output_field(&fields, "query_execution"), "false");
        assert_eq!(output_field(&fields, "external_engine_execution"), "false");
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(output_field(&fields, "fallback_attempted"), "false");
        assert_eq!(output_field(&fields, "side_effect_free"), "true");
    }

    #[test]
    fn feature_footprint_fields_include_no_fallback_and_gate_counts() {
        let report = shardloom_core::FeatureFootprintReport::contract_only();
        let fields = diagnostics::feature_footprint_fields(&report);

        assert_eq!(
            output_field(&fields, "schema_version"),
            "shardloom.feature_footprint.v1"
        );
        assert_eq!(output_field(&fields, "fallback_execution_allowed"), "false");
        assert_eq!(
            output_field(&fields, "external_baseline_runtime_fallback_count"),
            "0"
        );
        assert!(
            output_field(&fields, "gate_status_order").contains("vortex_file_io"),
            "gate_status_order should expose deterministic feature gate names"
        );
    }

    #[test]
    fn feature_footprint_command_returns_success() {
        let code = run(vec!["feature-footprint".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
    }

    #[test]
    fn doctor_command_returns_success_through_feature_footprint() {
        let code = run(vec!["doctor".to_string()]);
        assert_eq!(code, ExitCode::SUCCESS);
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
        let report = shardloom_core::CapabilityCertificationReport::contract_only();
        let fields = status_capabilities::certification_fields(
            &report,
            status_capabilities::CapabilityDiscoveryScope::Certification,
        );
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
            operational_hardening::parse_retry_gate_signals(
                "retry-requested,retry-allowed,retry-requires-cleanup,unknown-artifact,external-effects,cancellation-requested",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!blocked.retry_gate_open());
        let fields = operational_hardening::retry_gate_plan_fields(&blocked);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&("retry_executed".to_string(), "false".to_string())));

        let open = plan_retry_execution_gate(
            operational_hardening::parse_retry_gate_signals(
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
            operational_hardening::parse_cancellation_gate_signals(
                "cancellation-requested,cleanup-required",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!cleanup_required.cancellation_gate_open());

        let open = plan_cancellation_execution_gate(
            operational_hardening::parse_cancellation_gate_signals(
                "cancellation-requested,cleanup-required,cleanup-completed",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(open.cancellation_gate_open());

        let unknown_closed = plan_cancellation_execution_gate(
            operational_hardening::parse_cancellation_gate_signals(
                "cancellation-requested,unknown-artifact",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!unknown_closed.cancellation_gate_open());

        let external_closed = plan_cancellation_execution_gate(
            operational_hardening::parse_cancellation_gate_signals(
                "cancellation-requested,external-effects",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!external_closed.cancellation_gate_open());

        let retry_closed = plan_cancellation_execution_gate(
            operational_hardening::parse_cancellation_gate_signals(
                "cancellation-requested,retry-in-progress",
            )
            .expect("request"),
        )
        .expect("report");
        assert!(!retry_closed.cancellation_gate_open());

        let fields = operational_hardening::cancellation_gate_plan_fields(&retry_closed);
        assert!(fields.contains(&(
            "fallback_execution_allowed".to_string(),
            "false".to_string()
        )));
        assert!(fields.contains(&("cancellation_executed".to_string(), "false".to_string())));
    }
}
