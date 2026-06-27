//! Vortex-facing execution, planning, and evidence surfaces for `ShardLoom`.
//!
//! Narrow local, prepared-encoded, source-backed, and reader-backed paths have
//! executable evidence. Broader Scan API, layout/write strategy, object-store,
//! device/GPU, extension-type, table/catalog, and integration behavior remains
//! report-only or blocked until explicit provider/certificate evidence exists.
//!
//! Public exports are grouped by posture: Vortex compatibility/provider reports,
//! narrow executable encoded/local/source-backed paths, readiness and promotion
//! gates, write/commit artifact helpers, benchmark-only surfaces, and future
//! runtime bridges. Vortex query-engine integrations and external engines are
//! never fallback execution paths.
// Intentionally scoped to `shardloom-vortex`: report-contract modules naturally
// use pairs like request/report/status/mode/plan. Keep strict `-D warnings`
// for all other lints, and do not add broader lint exceptions.
#![allow(clippy::similar_names)]

/// Current optional upstream `vortex` provider version recorded in `ShardLoom`
/// native-provider evidence. This is the Vortex crate line, not the
/// `shardloom-vortex` crate version.
pub const UPSTREAM_VORTEX_PROVIDER_VERSION: &str =
    env!("SHARDLOOM_UPSTREAM_VORTEX_PROVIDER_VERSION");

// Report-only adapter/API inventory and metadata posture.
pub use adapter::{
    Vortex075HeavyOperatorDispositionRow, Vortex075HeavyOperatorDispositionStatus,
    Vortex075HeavyOperatorProviderDispositionReport, Vortex075HeavyOperatorSurface,
    Vortex075LocalIoDispositionRow, Vortex075LocalIoDispositionStatus,
    Vortex075LocalIoProviderDispositionReport, Vortex075LocalIoSurface, VortexAdapterCapability,
    VortexAdapterCapabilityReport, VortexAdapterCapabilityStatus, VortexApiArea,
    VortexApiInventoryItem, VortexApiSupportStatus, VortexDTypeMappingReport,
    VortexEncodingLayoutMappingReport, VortexEncodingMappingStatus, VortexLayoutMappingStatus,
    VortexLocalIoCoverageReport, VortexLocalIoCoverageRow, VortexLocalIoLaneStatus,
    VortexMetadataIoMode, VortexMetadataIoStatus, VortexMetadataProbeReport,
    VortexNativeWriterCertificationStatus, VortexNativeWriterSchemaCertificationReport,
    VortexNativeWriterSchemaCertificationRow, VortexObjectStoreIoGateReport,
    VortexObjectStoreIoGateRow, VortexObjectStoreIoGateStatus, VortexObjectStoreIoGateSurface,
    VortexStatisticsMappingReport, VortexStatisticsMappingStatus, VortexTypedMappingStatus,
    can_map_statistics_without_io, empty_vortex_segment_stats_placeholder,
    map_known_vortex_dtype_name, map_known_vortex_encoding_name, map_known_vortex_layout_name,
    probe_vortex_metadata_only, row_count_stats_placeholder, typed_vortex_dtype_mapping_available,
    typed_vortex_statistics_mapping_available,
};

pub mod bounded_execution;
pub mod commit_execution_gate;
pub mod commit_intent;
pub mod commit_marker;
pub mod commit_protocol;
pub mod composite_pushdown;
pub mod count_readiness;
pub mod device_residency;
pub mod encoded_count_approval;
pub mod encoded_count_physical_kernel;
pub mod encoded_path_selection;
pub mod encoded_predicate_evaluation;
pub mod encoded_projection_execution;
pub mod encoded_read_api;
pub mod encoded_read_boundary;
pub mod encoded_read_executor;
pub mod encoded_read_fixture;
pub mod encoded_read_metadata_probe;
pub mod encoded_read_probe;
pub mod encoded_read_readiness;
pub mod execute_step_evidence;
pub mod execution_readiness;
pub mod extension_type_capability;
pub mod file_io;
pub mod filtered_count_readiness;
pub mod generalized_encoded_filter_execution;
pub mod generalized_encoded_primitive_gate;
pub mod generalized_encoded_projection_execution;
pub mod generalized_filter_execution;
pub mod generalized_projection_execution;
pub mod manifest_finalization;
pub mod memory_bridge;
pub mod metadata_async_boundary;
pub mod metadata_executor;
pub mod metadata_physical_kernel;
pub mod metadata_planning;
pub mod metadata_pruning;
pub mod metadata_summary;
pub mod output_payload;
pub mod physical_operator_bridge;
pub mod projection_readiness;
pub mod query_primitive;
pub mod query_primitives;
pub mod query_trace;
pub mod read_planning;
pub mod runtime_bridge;
pub mod runtime_utilization;
pub mod scheduler_bridge;
pub mod selection_vector_filter_kernel;
pub mod source_backed_benchmark_matrix;
pub mod source_backed_encoded_execution;
pub mod staged_manifest;
pub mod staged_output;
pub mod streaming_batch_runtime;
pub mod top_level_facade;
pub mod traditional_analytics;
#[cfg(feature = "universal-format-io")]
pub mod universal_format_io;
#[cfg(any(feature = "universal-format-io", feature = "vortex-local-primitives"))]
mod url_domain;
pub mod vortex_compatibility;
pub mod vortex_compute_provider;
pub mod vortex_ingest;
pub mod vortex_operational_facets;
pub mod vortex_scan_compatibility;
pub mod write_intent;

// Report-only compatibility, provider-boundary, runtime-utilization, and
// benchmark-matrix surfaces.
pub use composite_pushdown::{
    CompositePushdownCapabilityMatrix, CompositePushdownCapabilityRow, CompositePushdownStatus,
    plan_composite_pushdown_capability_matrix,
};
pub use device_residency::{
    DeviceResidencyKind, DeviceResidencyOutputBoundary, DeviceResidencyReport,
    plan_device_residency_report,
};
pub use execute_step_evidence::{ExecuteStepEvidence, plan_execute_step_evidence};
pub use extension_type_capability::{
    ExtensionTypeCapabilityMatrix, ExtensionTypeCapabilityRow, ExtensionTypeSupportStatus,
    plan_extension_type_capability_matrix,
};
pub use filtered_count_readiness::*;
pub use generalized_encoded_filter_execution::{
    VortexGeneralizedEncodedFilterExecutionReport, VortexGeneralizedEncodedFilterExecutionStatus,
    execute_vortex_generalized_filter_from_encoded_value_batches,
};
pub use generalized_encoded_primitive_gate::{
    VortexGeneralizedEncodedPrimitiveGateEntry, VortexGeneralizedEncodedPrimitiveGateReport,
    VortexGeneralizedEncodedPrimitiveGateStatus, VortexGeneralizedEncodedPrimitiveKind,
    VortexGeneralizedEncodedPrimitiveStatus, plan_vortex_generalized_encoded_primitive_gate,
};
pub use generalized_encoded_projection_execution::{
    VortexGeneralizedEncodedProjectionExecutionReport,
    VortexGeneralizedEncodedProjectionExecutionStatus,
    execute_vortex_generalized_projection_from_encoded_projection_batches,
};
pub use generalized_filter_execution::{
    VortexGeneralizedFilterExecutionReport, VortexGeneralizedFilterExecutionStatus,
    execute_vortex_generalized_filter_from_local_scan_pushdown,
};
pub use generalized_projection_execution::{
    VortexGeneralizedProjectionExecutionReport, VortexGeneralizedProjectionExecutionStatus,
    execute_vortex_generalized_projection_from_local_scan_pushdown,
};
pub use runtime_utilization::{
    VortexArrayExecutionCertificate, VortexBoundarySupportStatus, VortexCapabilityUse,
    VortexCapabilityUtilizationReport, VortexCapabilityUtilizationRow, VortexFieldMaskEvidence,
    VortexLayoutAdvisorReport, VortexLayoutDeviceManagedBoundaryMatrix,
    VortexLayoutDeviceManagedBoundaryRow, VortexLayoutDeviceManagedBoundarySurface,
    VortexPredicateOrderingEvidence, VortexRuntimeCapabilityArea,
    VortexRuntimeUtilizationAuditReport, VortexScanExecutionSpineReport,
    plan_vortex_runtime_utilization_audit,
};
pub use source_backed_benchmark_matrix::{
    SourceBackedBenchmarkLane, SourceBackedBenchmarkMatrixReport, SourceBackedBenchmarkMatrixRow,
    SourceBackedBenchmarkMeasuredRow, SourceBackedBenchmarkOperation,
    SourceBackedBenchmarkRowStatus, measure_source_backed_benchmark_matrix_smoke,
    plan_source_backed_benchmark_matrix,
};
pub use source_backed_encoded_execution::{
    VortexNativeProviderBoundary, VortexReaderBackedEncodedExecutionStatus,
    VortexReaderBackedEncodedFilterExecutionReport,
    VortexReaderBackedEncodedProjectionExecutionReport, VortexReaderBackedSplitEvidence,
    VortexReaderGeneratedConjunctiveSelectionVectorBridgeReport,
    VortexReaderGeneratedConjunctiveSelectionVectorStatus, VortexReaderGeneratedEncodedKernelInput,
    VortexReaderGeneratedPreparedBatchEvidence, VortexReaderGeneratedPreparedBatchReport,
    VortexReaderGeneratedPreparedBatchStatus, VortexResidualBoundaryReport,
    VortexSourceBackedCertificatePairReport, VortexSourceBackedEncodedExecutionStatus,
    VortexSourceBackedEncodedFilterExecutionReport, VortexSourceBackedEncodedProjectionColumn,
    VortexSourceBackedEncodedProjectionExecutionReport,
    VortexSourceBackedEncodedValuePredicateBatch, VortexSourceBackedExpansionEvidenceReport,
    execute_vortex_reader_backed_filter_from_encoded_value_batches,
    execute_vortex_reader_backed_projection_from_encoded_projection_batches,
    execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs,
    execute_vortex_reader_generated_filter_from_encoded_kernel_inputs,
    execute_vortex_reader_generated_projection_from_encoded_kernel_inputs,
    execute_vortex_source_backed_filter_from_encoded_value_batches,
    execute_vortex_source_backed_projection_from_encoded_projection_batches,
    plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs,
};
pub use vortex_compatibility::{
    VortexCompatibilityMatrixReport, VortexCompatibilityMatrixRow, VortexCompatibilityStatus,
    plan_vortex_compatibility_matrix,
};
pub use vortex_compute_provider::{
    VortexComputeProviderAlignmentReport, VortexComputeProviderReport,
    VortexIntegrationBoundaryReport, VortexIntegrationBoundaryRow, VortexIntegrationRole,
    plan_vortex_compute_provider_alignment_report,
};
pub use vortex_operational_facets::{
    ApproxAnalyticsCertificate, CompressionAdvisorReport, ExecutionTelemetryFacet,
    ForeignRuntimePosture, ForeignRuntimeStatus, ForeignRuntimeSurface,
    IntegrityAndEncryptionReport, IoBackendEvidence, IoBackendKind, PythonVortexInteropReport,
    StreamingSinkCertificate, StreamingSinkWriterMode, VortexBenchmarkInterop,
    VortexBenchmarkInteropRow, VortexOperationalHardeningReport,
    plan_vortex_operational_hardening_report,
};
pub use vortex_scan_compatibility::{
    VortexScanCompatibilityReport, VortexScanCompatibilityStatus, VortexScanPushdownDecision,
    VortexScanResidualExecutor, VortexSegmentExtractionAdmissionReport,
    VortexSegmentExtractionAdmissionRow, VortexSegmentExtractionAdmissionStatus,
    VortexSourceSplitAdmissionStatus, VortexSourceSplitRuntimeAdmissionProof,
    plan_vortex_scan_compatibility,
};

// Feature-gated metadata/readiness and plan-only I/O surfaces.
pub use file_io::{
    VortexFileIoFeatureStatus, VortexMetadataOpenMode, VortexMetadataOpenReport,
    VortexMetadataOpenRequest, VortexMetadataOpenStatus, open_vortex_metadata_only,
    vortex_file_io_feature_enabled,
};

pub use metadata_planning::{
    VortexMetadataPlanningMode, VortexMetadataPlanningReport, VortexMetadataPlanningStatus,
    metadata_planning_is_side_effect_free, plan_from_vortex_metadata_summary,
};

pub use metadata_pruning::{
    VortexMetadataPruningInput, VortexMetadataPruningMode, VortexMetadataPruningReport,
    VortexMetadataPruningStatus, VortexSegmentPruningResult, metadata_pruning_is_side_effect_free,
    plan_vortex_metadata_pruning, prove_predicate_from_segment_stats,
};

pub use input_bridge::{
    VortexUniversalInputMode, VortexUniversalInputPlan, VortexUniversalInputStatus,
    plan_native_vortex_universal_input, vortex_universal_input_plan_is_side_effect_free,
};

pub use adaptive_sizing::{
    VortexAdaptiveSizingInput, VortexAdaptiveSizingMode, VortexAdaptiveSizingReport,
    VortexAdaptiveSizingStatus, VortexSegmentSizingInput, VortexSizingEstimateSource,
    size_vortex_runtime_task_graph, vortex_adaptive_sizing_is_side_effect_free,
};

// Encoded-read, metadata, physical-kernel, and selection-vector readiness or
// narrow local execution surfaces.
#[cfg(feature = "vortex-file-io")]
pub use encoded_read_api::vortex_encoded_read_public_api_compile_probe_summary;
pub use encoded_read_api::{
    VortexEncodedReadApiArea, VortexEncodedReadApiBoundaryReport,
    VortexEncodedReadApiBoundaryStatus, VortexEncodedReadApiItem, VortexEncodedReadApiRisk,
    VortexEncodedReadApiStatus, vortex_encoded_read_api_allows_future_probe,
    vortex_encoded_read_local_scan_count_api_boundary, vortex_encoded_read_public_api_boundary,
};
pub use encoded_read_boundary::{
    VortexEncodedReadBoundaryEffect, VortexEncodedReadBoundaryMode,
    VortexEncodedReadBoundaryReport, VortexEncodedReadBoundaryRequest,
    VortexEncodedReadBoundarySignal, VortexEncodedReadBoundaryStatus,
    plan_vortex_encoded_read_boundary, vortex_encoded_read_boundary_is_side_effect_free,
};
pub use encoded_read_fixture::{
    VortexEncodedReadFixtureEffect, VortexEncodedReadFixtureMode, VortexEncodedReadFixtureRef,
    VortexEncodedReadFixtureReport, VortexEncodedReadFixtureRequest,
    VortexEncodedReadFixtureSignal, VortexEncodedReadFixtureStatus,
    encoded_read_fixture_request_from_boundary_report, plan_vortex_encoded_read_fixture,
    vortex_encoded_read_fixture_is_side_effect_free,
};

pub use encoded_read_metadata_probe::{
    VortexEncodedReadMetadataProbeEffect, VortexEncodedReadMetadataProbeMode,
    VortexEncodedReadMetadataProbeReport, VortexEncodedReadMetadataProbeRequest,
    VortexEncodedReadMetadataProbeSignal, VortexEncodedReadMetadataProbeStatus,
    encoded_read_metadata_probe_request_from_fixture_report, probe_vortex_encoded_read_metadata,
    vortex_encoded_read_metadata_probe_is_side_effect_free,
};

#[cfg(feature = "vortex-encoded-read-spike")]
pub use encoded_read_executor::execute_vortex_count_all_from_local_scan_with_session;
pub use encoded_read_executor::{
    VortexEncodedReadExecutionDecision, VortexEncodedReadExecutionDecisionKind,
    VortexEncodedReadExecutionInput, VortexEncodedReadExecutionMode,
    VortexEncodedReadExecutionReport, VortexEncodedReadExecutionStatus,
    VortexEncodedReadExecutorFeatureStatus, execute_vortex_count_all_from_approved_local_scan,
    execute_vortex_encoded_read_contract, execute_vortex_encoded_read_spike,
    vortex_encoded_read_execution_is_side_effect_free,
    vortex_encoded_read_executor_feature_enabled, vortex_encoded_read_spike_feature_enabled,
};

pub use encoded_read_probe::{
    VortexEncodedReadProbeCandidate, VortexEncodedReadProbeCandidateKind,
    VortexEncodedReadProbeInput, VortexEncodedReadProbeMode, VortexEncodedReadProbeReport,
    VortexEncodedReadProbeStatus, VortexProbeCounts, VortexProbeRequirement, VortexProbeSideEffect,
    plan_vortex_encoded_read_probe, vortex_encoded_read_probe_is_side_effect_free,
};

pub use encoded_read_readiness::{
    VortexEncodedReadCandidate, VortexEncodedReadCandidateKind, VortexEncodedReadReadinessInput,
    VortexEncodedReadReadinessMode, VortexEncodedReadReadinessReport,
    VortexEncodedReadReadinessStatus, evaluate_vortex_encoded_read_readiness,
    vortex_encoded_read_readiness_is_side_effect_free,
};

pub use execution_readiness::{
    VortexDryRunContract, VortexDryRunMode, VortexExecutionReadinessInput,
    VortexExecutionReadinessReport, VortexExecutionReadinessStatus, VortexReadinessGateKind,
    VortexReadinessGateResult, VortexReadinessGateStatus, evaluate_vortex_execution_readiness,
    vortex_execution_readiness_is_side_effect_free,
};
pub use metadata_executor::{
    VortexMetadataExecutableDecisionKind, VortexMetadataExecutionDecision,
    VortexMetadataExecutionInput, VortexMetadataExecutionMode, VortexMetadataExecutionReport,
    VortexMetadataExecutionStatus, VortexMetadataExecutorFeatureStatus,
    execute_vortex_metadata_only, vortex_metadata_execution_is_side_effect_free,
    vortex_metadata_executor_feature_enabled,
};

pub use encoded_count_physical_kernel::{
    VortexEncodedCountKernelAdmissionReport, VortexEncodedCountPhysicalKernelDiscoveryReport,
    VortexEncodedCountPhysicalKernelReport, VortexEncodedCountPhysicalKernelStatus,
    admit_vortex_encoded_count_kernel, evaluate_vortex_local_encoded_count_physical_kernel,
    vortex_encoded_count_physical_kernel_discovery_report,
};
pub use encoded_path_selection::{
    VortexEncodedExecutionPathSelectionEntry, VortexEncodedExecutionPathSelectionReport,
    VortexEncodedExecutionPathSelectionStatus, plan_vortex_encoded_execution_path_selection,
};
pub use encoded_predicate_evaluation::{
    VortexEncodedPredicateEvaluationDiscoveryReport, VortexEncodedPredicateEvaluationReport,
    VortexEncodedPredicateEvaluationStatus, VortexEncodedValuePredicateBatch,
    evaluate_vortex_encoded_predicate_segments, evaluate_vortex_encoded_value_predicate_batch,
    evaluate_vortex_encoded_value_predicate_batches,
    vortex_encoded_predicate_evaluation_discovery_report,
};
pub use encoded_projection_execution::{
    VortexEncodedProjectionExecutionReport, VortexEncodedProjectionExecutionStatus,
    VortexPreparedEncodedProjectionColumn, evaluate_vortex_prepared_encoded_projection,
};
pub use metadata_async_boundary::{
    VortexMetadataAsyncBoundaryEffect, VortexMetadataAsyncBoundaryMode,
    VortexMetadataAsyncBoundaryReport, VortexMetadataAsyncBoundaryRequest,
    VortexMetadataAsyncBoundarySignal, VortexMetadataAsyncBoundaryStatus,
    VortexMetadataAsyncInvocationEffect, VortexMetadataAsyncInvocationReport,
    VortexMetadataAsyncInvocationStatus, invoke_vortex_metadata_footer_probe_async,
    metadata_async_boundary_request_from_metadata_probe_report,
    plan_vortex_metadata_async_boundary, vortex_metadata_async_boundary_is_side_effect_free,
};
#[cfg(feature = "vortex-file-io")]
pub use metadata_async_boundary::{
    VortexMetadataAsyncInvocationInput, invoke_vortex_metadata_footer_probe_with_session_async,
};
pub use metadata_physical_kernel::{
    VortexMetadataCountKernelAdmissionReport, VortexMetadataFilterKernelAdmissionReport,
    VortexMetadataPhysicalKernelReport, VortexMetadataPhysicalKernelStatus,
    admit_vortex_metadata_count_kernel, admit_vortex_metadata_filter_kernel,
    evaluate_vortex_metadata_physical_kernels,
};
pub use selection_vector_filter_kernel::{
    VortexSelectionVectorFilterKernelAdmissionReport,
    VortexSelectionVectorFilterKernelDiscoveryReport, VortexSelectionVectorFilterKernelReport,
    VortexSelectionVectorFilterKernelStatus, admit_vortex_selection_vector_filter_kernel,
    evaluate_vortex_selection_vector_filter_kernel,
    vortex_selection_vector_filter_kernel_discovery_report,
};

// Memory, physical-operator, primitive, and decision-trace bridge surfaces.
pub use memory_bridge::{
    VortexMemoryBridgeInput, VortexMemoryBridgeMode, VortexMemoryBridgeReport,
    VortexMemoryBridgeStatus, VortexTaskMemoryClass, VortexTaskMemoryDecision,
    VortexTaskMemoryDecisionKind, plan_vortex_memory_safety, plan_vortex_memory_spill_reservation,
    vortex_memory_bridge_is_side_effect_free,
};
pub use projection_readiness::*;

pub use physical_operator_bridge::{
    VortexPhysicalOperatorBridgeReport, VortexPhysicalOperatorBridgeStatus,
    physical_operator_plan_for_vortex_query_primitive,
    physical_operator_plan_for_vortex_query_primitive_result,
    plan_vortex_query_primitive_physical_operators,
    plan_vortex_query_primitive_result_physical_operators,
    plan_vortex_query_primitive_result_physical_operators_with_evidence,
};

pub use query_primitive::{
    VortexAggregateExpression, VortexAggregateHavingExpr, VortexAggregateOrderExpr,
    VortexDuplicateKeepPolicy, VortexExplodeProjectionRequest, VortexExpressionProjectionRequest,
    VortexExpressionRewrite, VortexMeltProjectionRequest, VortexPivotProjectionRequest,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveMode, VortexQueryPrimitiveRequest,
    VortexQueryPrimitiveResult, VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
    VortexRollingWindowRequest, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest,
    VortexSortRowsRequest, VortexSortTiePolicy, VortexStructuredProjectionColumn,
    VortexStructuredProjectionExpr, VortexStructuredProjectionRequest,
    evaluate_vortex_count_all_from_summary, evaluate_vortex_query_primitive,
};
pub use query_primitives::{
    VortexQueryPrimitiveEffect, VortexQueryPrimitiveKind as VortexQueryPrimitiveBoundaryKind,
    VortexQueryPrimitiveMode as VortexQueryPrimitiveBoundaryMode, VortexQueryPrimitiveReport,
    VortexQueryPrimitiveRequest as VortexQueryPrimitiveBoundaryRequest, VortexQueryPrimitiveSignal,
    VortexQueryPrimitiveStatus as VortexQueryPrimitiveBoundaryStatus, plan_vortex_query_primitive,
    query_primitive_request_from_metadata_async_boundary,
    query_primitive_request_from_metadata_async_invocation,
    vortex_query_primitive_is_side_effect_free,
};

pub use query_trace::{
    VortexQueryDecisionKind, VortexQueryDecisionTrace, VortexQueryDecisionTraceEntry,
    VortexQueryPrimitiveAnalysisReport, VortexWorkAvoidedMetric, VortexWorkAvoidedMetricKind,
    VortexWorkAvoidedReport, VortexWorkAvoidedValue, analyze_vortex_query_primitive_result,
    evaluate_vortex_query_primitive_with_analysis,
};

// Write/output/commit readiness surfaces and narrow feature-gated local artifact helpers.
pub use staged_manifest::{
    VortexStagedManifestDraftContent, VortexStagedManifestDraftEffect,
    VortexStagedManifestDraftMode, VortexStagedManifestDraftReport,
    VortexStagedManifestDraftRequest, VortexStagedManifestDraftSignal,
    VortexStagedManifestDraftStatus, VortexStagedManifestFileEffect, VortexStagedManifestFileMode,
    VortexStagedManifestFileName, VortexStagedManifestFileRef, VortexStagedManifestFileReport,
    VortexStagedManifestFileRequest, VortexStagedManifestFileSignal,
    VortexStagedManifestFileStatus, VortexStagedManifestFileWriteEffect,
    VortexStagedManifestFileWriteMode, VortexStagedManifestFileWriteOption,
    VortexStagedManifestFileWriteReport, VortexStagedManifestFileWriteRequest,
    VortexStagedManifestFileWriteSignal, VortexStagedManifestFileWriteStatus,
    plan_vortex_staged_manifest_draft, plan_vortex_staged_manifest_file,
    plan_vortex_staged_manifest_file_write, staged_manifest_file_request_from_reports,
    staged_manifest_file_write_request_from_plan, staged_manifest_request_from_reports,
    vortex_staged_manifest_draft_is_side_effect_free,
    vortex_staged_manifest_file_is_side_effect_free,
    vortex_staged_manifest_file_write_is_side_effect_free, write_vortex_staged_manifest_file,
};

pub use manifest_finalization::{
    VortexFinalizedManifestArtifactWriteMode, VortexFinalizedManifestArtifactWriteOption,
    VortexFinalizedManifestArtifactWriteReport, VortexFinalizedManifestArtifactWriteRequest,
    VortexFinalizedManifestArtifactWriteStatus, VortexFinalizedManifestContent,
    VortexFinalizedManifestFileName, VortexFinalizedManifestFileRef,
    VortexManifestFinalizationEffect, VortexManifestFinalizationMode,
    VortexManifestFinalizationReport, VortexManifestFinalizationRequest,
    VortexManifestFinalizationSignal, VortexManifestFinalizationStatus,
    finalized_manifest_artifact_write_request_from_plan,
    manifest_finalization_request_from_reports, plan_vortex_manifest_finalization,
    vortex_finalized_manifest_artifact_write_is_side_effect_free,
    vortex_manifest_finalization_is_side_effect_free, write_vortex_finalized_manifest_artifact,
};

pub use commit_marker::{
    VortexCommitMarkerContent, VortexCommitMarkerEffect, VortexCommitMarkerFileName,
    VortexCommitMarkerFileRef, VortexCommitMarkerMode, VortexCommitMarkerReport,
    VortexCommitMarkerRequest, VortexCommitMarkerSignal, VortexCommitMarkerStatus,
    VortexCommitMarkerWriteMode, VortexCommitMarkerWriteOption, VortexCommitMarkerWriteReport,
    VortexCommitMarkerWriteRequest, VortexCommitMarkerWriteSignal, VortexCommitMarkerWriteStatus,
    commit_marker_request_from_protocol_report, commit_marker_write_request_from_plan,
    plan_vortex_commit_marker, vortex_commit_marker_is_side_effect_free,
    vortex_commit_marker_write_is_side_effect_free, write_vortex_commit_marker,
};

pub use count_readiness::{
    VortexCountCandidateSource, VortexCountReadinessEffect, VortexCountReadinessMode,
    VortexCountReadinessReport, VortexCountReadinessRequest, VortexCountReadinessSignal,
    VortexCountReadinessStatus, count_readiness_request_from_encoded_read_probe_report,
    count_readiness_request_from_encoded_read_readiness_report,
    count_readiness_request_from_query_primitive_report, plan_vortex_count_readiness,
    vortex_count_readiness_is_side_effect_free,
};
pub use encoded_count_approval::{
    VortexEncodedCountDataPathApprovalInput, VortexEncodedCountDataPathApprovalMode,
    VortexEncodedCountDataPathApprovalReport, VortexEncodedCountDataPathApprovalStatus,
    plan_vortex_encoded_count_data_path_approval,
    plan_vortex_encoded_count_data_path_approval_with_layout_driver,
    vortex_encoded_count_data_path_approval_is_side_effect_free,
};

pub use commit_protocol::{
    VortexCommitProtocolEffect, VortexCommitProtocolMode, VortexCommitProtocolReport,
    VortexCommitProtocolRequest, VortexCommitProtocolSignal, VortexCommitProtocolState,
    VortexCommitProtocolStatus, VortexCommitProtocolTransition, VortexCommittedManifestFileName,
    VortexCommittedManifestFileRef, VortexLocalCommitExecutionMode,
    VortexLocalCommitExecutionReport, VortexLocalCommitExecutionRequest,
    VortexLocalCommitExecutionSignal, VortexLocalCommitExecutionStatus,
    VortexLocalCommitRecoveryMode, VortexLocalCommitRecoveryReport,
    VortexLocalCommitRecoveryRequest, VortexLocalCommitRecoverySignal,
    VortexLocalCommitRecoveryStatus, VortexLocalCommitRollbackExecutionMode,
    VortexLocalCommitRollbackExecutionReport, VortexLocalCommitRollbackExecutionStatus,
    commit_protocol_request_from_commit_intent, execute_vortex_local_commit,
    execute_vortex_local_commit_rollback, local_commit_recovery_request_from_execution_report,
    plan_vortex_commit_protocol, plan_vortex_commit_protocol_from_commit_intent,
    plan_vortex_local_commit_recovery, vortex_commit_protocol_is_side_effect_free,
    vortex_local_commit_execution_feature_enabled,
    vortex_local_commit_execution_is_side_effect_free,
    vortex_local_commit_recovery_is_side_effect_free,
    vortex_local_commit_rollback_execution_feature_enabled,
    vortex_local_commit_rollback_execution_is_side_effect_free,
};

pub use commit_execution_gate::{
    VortexLocalCommitExecutionGateEffect, VortexLocalCommitExecutionGateMode,
    VortexLocalCommitExecutionGateReport, VortexLocalCommitExecutionGateRequest,
    VortexLocalCommitExecutionGateSignal, VortexLocalCommitExecutionGateStatus,
    local_commit_execution_gate_request_from_reports, plan_vortex_local_commit_execution_gate,
    vortex_local_commit_execution_gate_is_side_effect_free,
};

pub use commit_intent::{
    VortexCommitIntentEffect, VortexCommitIntentMode, VortexCommitIntentReport,
    VortexCommitIntentRequest, VortexCommitIntentSignal, VortexCommitIntentStatus,
    commit_intent_request_from_reports, plan_vortex_commit_intent,
    vortex_commit_intent_is_side_effect_free,
};

pub use staged_output::{
    VortexStagedMarkerMode, VortexStagedMarkerOption, VortexStagedMarkerReport,
    VortexStagedMarkerRequest, VortexStagedMarkerStatus, VortexStagedOutputEffect,
    VortexStagedOutputMode, VortexStagedOutputReport, VortexStagedOutputRequest,
    VortexStagedOutputSignal, VortexStagedOutputStatus, VortexStagedWorkspaceId,
    VortexStagedWorkspacePath, VortexStagedWorkspaceSetupMode, VortexStagedWorkspaceSetupOption,
    VortexStagedWorkspaceSetupReport, VortexStagedWorkspaceSetupRequest,
    VortexStagedWorkspaceSetupStatus, plan_vortex_staged_output, setup_vortex_staged_workspace,
    staged_output_request_from_write_intent, vortex_staged_marker_is_side_effect_free,
    vortex_staged_output_is_side_effect_free, vortex_staged_workspace_setup_is_side_effect_free,
    write_vortex_staged_marker,
};

pub use output_payload::{
    VortexNativeOutputPayloadWriteMode, VortexNativeOutputPayloadWriteReport,
    VortexNativeOutputPayloadWriteRequest, VortexNativeOutputPayloadWriteStatus,
    VortexOutputPayloadArtifactWriteMode, VortexOutputPayloadArtifactWriteOption,
    VortexOutputPayloadArtifactWriteReport, VortexOutputPayloadArtifactWriteRequest,
    VortexOutputPayloadArtifactWriteStatus, VortexOutputPayloadContentDescriptor,
    VortexOutputPayloadContentKind, VortexOutputPayloadEffect, VortexOutputPayloadFileName,
    VortexOutputPayloadFileRef, VortexOutputPayloadMode, VortexOutputPayloadReport,
    VortexOutputPayloadRequest, VortexOutputPayloadSignal, VortexOutputPayloadStatus,
    native_output_payload_write_request_from_plan, output_payload_artifact_write_request_from_plan,
    output_payload_request_from_reports, plan_vortex_output_payload,
    vortex_native_output_payload_write_feature_enabled,
    vortex_native_output_payload_write_is_side_effect_free,
    vortex_output_payload_artifact_write_is_side_effect_free,
    vortex_output_payload_is_side_effect_free, write_vortex_native_count_output_payload,
    write_vortex_output_payload_artifact,
};

// Benchmark-only surfaces. External engines remain comparison-only and never fallback.
pub use traditional_analytics::{
    TraditionalAnalyticsInputFormat, TraditionalAnalyticsPreparedBatchReport,
    TraditionalAnalyticsPreparedBatchRequest, TraditionalAnalyticsReport,
    TraditionalAnalyticsRequest, TraditionalAnalyticsResourcePolicy, TraditionalAnalyticsScenario,
    TraditionalAnalyticsVortexBatchReport, TraditionalAnalyticsVortexBatchRequest,
    TraditionalAnalyticsVortexReport, TraditionalAnalyticsVortexRequest,
    TraditionalDirectTransientReport, TraditionalRuntimeEvidenceLevel,
    TraditionalRuntimeEvidenceTier, run_traditional_analytics_benchmark,
    run_traditional_analytics_prepared_batch_benchmark,
    run_traditional_analytics_vortex_batch_benchmark, run_traditional_analytics_vortex_benchmark,
    run_traditional_direct_transient_csv_smoke, run_traditional_direct_transient_local_input_smoke,
};
#[cfg(feature = "universal-format-io")]
pub use universal_format_io::{
    FlatLocalColumnarSource, FlatLocalColumnarStreamSource, FlatLocalSourceTable,
    encode_flat_arrow_ipc_rows, encode_flat_arrow_ipc_rows_with_arrow_dtypes,
    encode_flat_arrow_ipc_rows_with_dtypes, encode_flat_avro_rows,
    encode_flat_avro_rows_with_arrow_dtypes, encode_flat_avro_rows_with_dtypes,
    encode_flat_orc_rows, encode_flat_orc_rows_with_arrow_dtypes, encode_flat_orc_rows_with_dtypes,
    encode_flat_parquet_rows, encode_flat_parquet_rows_with_arrow_dtypes,
    encode_flat_parquet_rows_with_dtypes, materialize_flat_columnar_source_to_scalar_table,
    read_flat_arrow_ipc_columnar_source, read_flat_arrow_ipc_columnar_source_with_projection,
    read_flat_arrow_ipc_source, read_flat_arrow_ipc_source_with_projection,
    read_flat_avro_columnar_source, read_flat_avro_columnar_source_with_projection,
    read_flat_avro_source, read_flat_avro_source_with_projection, read_flat_orc_columnar_source,
    read_flat_orc_columnar_source_with_projection, read_flat_orc_source,
    read_flat_orc_source_with_projection, read_flat_parquet_columnar_source,
    read_flat_parquet_columnar_source_with_projection, read_flat_parquet_source,
    read_flat_parquet_source_with_projection, stream_flat_arrow_ipc_columnar_source,
    stream_flat_avro_columnar_source, stream_flat_orc_columnar_source,
    stream_flat_parquet_columnar_source, stream_flat_parquet_columnar_source_with_parallelism,
    with_capillary_prefetch_columnar_stream_source,
};
pub use vortex_ingest::{
    VORTEX_CAPILLARY_PREPARATION_SCHEMA_VERSION, VORTEX_COPY_BUDGET_SCHEMA_VERSION,
    VORTEX_DIFFERENTIAL_PREPARATION_SCHEMA_VERSION,
    VORTEX_DIFFERENTIAL_REFINEMENT_MANIFEST_SCHEMA_VERSION, VORTEX_DIFFERENTIAL_REFINEMENT_POLICY,
    VORTEX_LAYOUT_WRITE_ADVISOR_SCHEMA_VERSION, VORTEX_PREPARATION_SPINE_SCHEMA_VERSION,
    VORTEX_PREPARATION_SPINE_VORTEX_CRATE_VERSION, VORTEX_PREPARED_OLAP_STATE_POLICY,
    VORTEX_PREPARED_OLAP_STATE_SCHEMA_VERSION, VORTEX_PREPARED_STATE_REUSE_POLICY,
    VORTEX_PREPARED_STATE_REUSE_SCHEMA_VERSION, VORTEX_SCOUT_INGRESS_SCHEMA_VERSION,
    VortexCapillaryPreparationInput, VortexCapillaryPreparationReport, VortexCopyBudgetInput,
    VortexCopyBudgetReport, VortexDifferentialPreparationInput,
    VortexDifferentialPreparationReport, VortexDifferentialRefinementManifestReport,
    VortexDifferentialUpdateMode, VortexIngestCertificationLevel, VortexLayoutWriteAdvisorInput,
    VortexLayoutWriteAdvisorReport, VortexLayoutWriteRuntimeDecision, VortexPreparationSpineReport,
    VortexPreparedOlapStateReport, VortexPreparedOlapStateWriteRequest,
    VortexPreparedStateAppendOnlyRefinementDecision, VortexPreparedStateReuseReport,
    VortexPreparedStateReuseRequest, VortexPreparedStateReuseWriteEvidence,
    VortexPreparedStateWriteReport, VortexPreparedStateWriteRequest, VortexScoutIngressInput,
    VortexScoutIngressReport, evaluate_vortex_capillary_preparation, evaluate_vortex_copy_budget,
    evaluate_vortex_differential_preparation, evaluate_vortex_layout_write_advisor,
    evaluate_vortex_prepared_olap_single_artifact_state,
    evaluate_vortex_prepared_state_append_only_refinement, evaluate_vortex_prepared_state_reuse,
    evaluate_vortex_scout_ingress, publish_vortex_prepared_olap_single_artifact_state,
    vortex_differential_refinement_manifest_path, vortex_ingest_write_feature_enabled,
    vortex_prepared_state_reuse_manifest_path, write_flat_scalar_vortex_prepared_state,
    write_vortex_differential_refinement_manifest, write_vortex_prepared_olap_state_bundle,
    write_vortex_prepared_state_reuse_manifest,
};
#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
pub use vortex_ingest::{
    VortexPreparedStateColumnarStreamWriteRequest, VortexPreparedStateColumnarWriteRequest,
    write_flat_columnar_vortex_prepared_state, write_flat_columnar_vortex_prepared_state_streaming,
};

// Runtime bridge, scheduler, bounded execution, and narrow local engine/provider exports.
pub use write_intent::{
    VortexWriteIntentEffect, VortexWriteIntentMode, VortexWriteIntentReport,
    VortexWriteIntentRequest, VortexWriteIntentSignal, VortexWriteIntentStatus,
    plan_vortex_write_intent, vortex_write_intent_is_side_effect_free,
};

pub use read_planning::{
    VortexByteRangeIntent, VortexReadIntentStatus, VortexReadPlanningInput, VortexReadPlanningMode,
    VortexReadPlanningReport, VortexReadSplitDescriptor, VortexSegmentReadIntent,
    plan_vortex_read_from_universal_input, vortex_read_planning_is_side_effect_free,
};
pub use runtime_bridge::{
    VortexRuntimeBridgeInput, VortexRuntimeBridgeMode, VortexRuntimeBridgeReport,
    VortexRuntimeBridgeStatus, VortexTaskMapping, VortexTaskMappingKind,
    build_vortex_runtime_task_graph, vortex_runtime_bridge_is_side_effect_free,
};

pub use scheduler_bridge::{
    VortexSchedulerBridgeInput, VortexSchedulerBridgeMode, VortexSchedulerBridgeReport,
    VortexSchedulerBridgeStatus, VortexSchedulingDecisionKind, VortexTaskBatchPlan,
    VortexTaskQueueClass, VortexTaskSchedulingDecision, plan_vortex_scheduler_queue,
    vortex_scheduler_bridge_is_side_effect_free,
};

pub use bounded_execution::{
    VortexBoundedExecutionDecision, VortexBoundedExecutionDecisionKind,
    VortexBoundedExecutionInput, VortexBoundedExecutionMode, VortexBoundedExecutionPolicy,
    VortexBoundedExecutionReport, VortexBoundedExecutionStatus, VortexBoundedSpillIntegrationMode,
    VortexBoundedSpillIntegrationReport, VortexBoundedSpillIntegrationRequest,
    VortexBoundedSpillIntegrationStatus, execute_vortex_bounded_local_query,
    plan_bounded_execution_spill_payload_integration,
    plan_bounded_execution_spill_payload_roundtrip, plan_bounded_execution_spill_reservation,
    vortex_bounded_execution_is_side_effect_free,
};

pub use layout_driver_approval::{
    VortexLayoutReaderDriverApprovalInput, VortexLayoutReaderDriverApprovalMode,
    VortexLayoutReaderDriverApprovalReport, VortexLayoutReaderDriverApprovalSignal,
    VortexLayoutReaderDriverApprovalStatus, plan_vortex_layout_reader_driver_approval,
    vortex_layout_reader_driver_approval_is_side_effect_free,
};
pub use local_engine::{
    VortexLocalEngineMode, VortexLocalEnginePrimitive, VortexLocalEngineReport,
    VortexLocalEngineRequest, VortexLocalEngineStatus, VortexLocalEngineWhyReport,
    parse_vortex_local_engine_primitive, run_vortex_local_engine,
    vortex_local_engine_is_side_effect_free,
};
pub use local_execution::{
    VortexEncodedCountLocalGuardDiscoveryReport, VortexLocalExecutionInput,
    VortexLocalExecutionMode, VortexLocalExecutionReport, VortexLocalExecutionStatus,
    VortexLocalExecutionStep, VortexLocalExecutionStepKind, VortexLocalExecutionValue,
    execute_vortex_count_all_from_approved_local_scan_result,
    execute_vortex_count_all_from_encoded_count_data_path_approval,
    execute_vortex_count_all_from_encoded_data_candidate,
    execute_vortex_count_all_from_metadata_footer_invocation,
    execute_vortex_count_where_from_filtered_count_metadata_proof,
    execute_vortex_local_query_primitive, local_encoded_count_execution_certificate,
    local_encoded_count_native_io_certificate, vortex_encoded_count_local_guard_discovery_report,
    vortex_local_execution_is_side_effect_free,
};
pub use local_primitives::{
    VortexLocalPrimitiveEmbeddedLayoutReport, VortexLocalPrimitiveExecutionMode,
    VortexLocalPrimitiveExecutionPolicy, VortexLocalPrimitiveExecutionReport,
    VortexLocalPrimitiveExecutionStatus, VortexLocalPrimitiveResourceEnvelope,
    VortexLocalPrimitiveRowExportFormat, VortexLocalPrimitiveRowExportReport,
    VortexLocalPrimitiveStateBudgetReport, execute_vortex_local_partitioned_primitive_with_policy,
    execute_vortex_local_primitive, execute_vortex_local_primitive_row_export_with_policy,
    execute_vortex_local_primitive_with_policy, local_primitive_correctness_fixture_for_request,
    local_primitive_execution_certificate, local_primitive_native_io_certificate,
};
pub use streaming_batch_runtime::{
    VortexStreamingBatchRuntimeMode, VortexStreamingBatchRuntimeReport,
    VortexStreamingBatchRuntimeStatus, execute_vortex_streaming_batches_from_local_encoded_count,
    vortex_streaming_batch_runtime_is_side_effect_free,
    vortex_streaming_batch_runtime_schema_version,
};
pub use top_level_facade::VortexTopLevelExecutionProvider;

// Metadata-summary helpers remain local metadata evidence, not broad scan execution.
pub use metadata_summary::{
    VortexColumnMetadataSummary, VortexFileMetadataSummary, VortexMetadataAvailability,
    VortexMetadataSummaryReport, VortexMetadataSummaryStatus, VortexSegmentMetadataSummary,
    metadata_summary_is_plan_only, summarize_vortex_metadata_probe,
};

pub mod adapter;
pub mod adaptive_sizing;
pub mod input_bridge;
pub mod layout_driver_approval;
pub mod local_engine;
pub mod local_execution;
pub mod local_primitives;

use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, DatasetRef, DatasetUri, Diagnostic, DiagnosticCode, EncodedSegment, FallbackStatus,
    Result, ShardLoomError,
};

/// Planning-time reference to a Vortex-native dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexFileRef {
    pub dataset: DatasetRef,
}

/// Planning-only status of upstream Vortex dependency readiness.
///
/// This enum reports dependency posture only. It does not perform IO, does not
/// call upstream Vortex APIs, and does not permit fallback execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexDependencyStatus {
    NotAdded,
    ReviewRequired,
    ApprovedForFuturePr,
    Added,
    Unsupported,
}
impl VortexDependencyStatus {
    /// Returns a stable machine-readable label for diagnostics/reporting.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotAdded => "not_added",
            Self::ReviewRequired => "review_required",
            Self::ApprovedForFuturePr => "approved_for_future_pr",
            Self::Added => "added",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether upstream Vortex API use is currently allowed.
    ///
    /// Only `Added` returns `true`.
    #[must_use]
    pub const fn allows_upstream_api_use(&self) -> bool {
        matches!(self, Self::Added)
    }
}

/// Planning/readiness report for upstream Vortex dependency and adapter posture.
///
/// This type is reporting-only. It does not add dependencies, probe files, or
/// promote broad adapter reads/writes.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexAdapterReadiness {
    pub dependency_status: VortexDependencyStatus,
    pub license_review_complete: bool,
    pub provenance_review_complete: bool,
    pub public_api_review_complete: bool,
    pub fallback_dependencies_absent: bool,
    pub diagnostics: Vec<shardloom_core::Diagnostic>,
}
impl VortexAdapterReadiness {
    /// Returns the default not-ready readiness state.
    #[must_use]
    pub fn not_ready() -> Self {
        Self {
            dependency_status: VortexDependencyStatus::ReviewRequired,
            license_review_complete: false,
            provenance_review_complete: false,
            public_api_review_complete: false,
            fallback_dependencies_absent: true,
            diagnostics: Vec::new(),
        }
    }

    /// Returns a planning-only readiness state for a future dependency PR.
    #[must_use]
    pub fn ready_for_dependency_pr() -> Self {
        Self {
            dependency_status: VortexDependencyStatus::ApprovedForFuturePr,
            license_review_complete: true,
            provenance_review_complete: true,
            public_api_review_complete: true,
            fallback_dependencies_absent: true,
            diagnostics: Vec::new(),
        }
    }

    /// Returns readiness state while public upstream API discovery is in progress.
    ///
    /// This is still non-IO planning mode; broad Vortex adapter reads/writes
    /// remain blocked unless a narrower feature-gated helper explicitly
    /// authorizes them.
    #[must_use]
    pub fn api_discovery_in_progress() -> Self {
        Self {
            dependency_status: if cfg!(feature = "upstream-vortex") {
                VortexDependencyStatus::Added
            } else {
                VortexDependencyStatus::ApprovedForFuturePr
            },
            license_review_complete: true,
            provenance_review_complete: true,
            public_api_review_complete: false,
            fallback_dependencies_absent: true,
            diagnostics: Vec::new(),
        }
    }

    /// Returns a compile-only readiness state after adding upstream Vortex.
    ///
    /// This confirms dependency posture only. It does not by itself approve
    /// broad Vortex metadata/file I/O or adapter API integration.
    #[must_use]
    pub fn dependency_added_compile_only() -> Self {
        Self {
            dependency_status: if cfg!(feature = "upstream-vortex") {
                VortexDependencyStatus::Added
            } else {
                VortexDependencyStatus::ApprovedForFuturePr
            },
            license_review_complete: true,
            provenance_review_complete: true,
            public_api_review_complete: false,
            fallback_dependencies_absent: true,
            diagnostics: Vec::new(),
        }
    }

    /// Adds a deterministic diagnostic to the readiness report.
    pub fn add_diagnostic(&mut self, diagnostic: shardloom_core::Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    /// Returns whether dependency readiness gates for a future PR are complete.
    #[must_use]
    pub fn is_ready_for_dependency_pr(&self) -> bool {
        matches!(
            self.dependency_status,
            VortexDependencyStatus::ApprovedForFuturePr | VortexDependencyStatus::Added
        ) && self.license_review_complete
            && self.provenance_review_complete
            && self.public_api_review_complete
            && self.fallback_dependencies_absent
            && !self.has_errors()
    }

    /// Returns whether any error diagnostics are present.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|diagnostic| matches!(diagnostic.severity.as_str(), "error" | "fatal"))
    }

    /// Renders a human-readable readiness summary.
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = format!(
            "Vortex adapter readiness\nupstream dependency status: {}\nfallback execution: disabled\nlicense review complete: {}\nprovenance review complete: {}\npublic API review complete: {}\nfallback dependencies absent: {}\nready for dependency PR: {}",
            self.dependency_status.as_str(),
            self.license_review_complete,
            self.provenance_review_complete,
            self.public_api_review_complete,
            self.fallback_dependencies_absent,
            self.is_ready_for_dependency_pr(),
        );
        if !cfg!(feature = "upstream-vortex") {
            text.push_str("\nupstream Vortex dependency feature is not enabled in this build");
        } else if self.dependency_status != VortexDependencyStatus::Added {
            text.push_str("\nupstream Vortex dependency is not added yet");
        } else if !self.public_api_review_complete {
            text.push_str(
                "\nupstream Vortex dependency is added in compile-only readiness mode; broad Vortex adapter IO is not promoted",
            );
        }
        if self.diagnostics.is_empty() {
            text.push_str("\ndiagnostics: none");
        } else {
            text.push_str("\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                let _ = write!(text, "\n- {}", diagnostic.to_human_text());
                if let Some(feature) = &diagnostic.feature {
                    let _ = write!(text, " feature={feature}");
                }
                if let Some(reason) = &diagnostic.reason {
                    let _ = write!(text, " reason={reason}");
                }
                if let Some(next_step) = &diagnostic.suggested_next_step {
                    let _ = write!(text, " next_step={next_step}");
                }
            }
        }
        text
    }
}

/// Compile-time marker that upstream `vortex` is linked for adapter readiness.
///
/// This confirms dependency presence only. It does not read/write Vortex files,
/// inspect Vortex metadata, run object-store IO, decode to Arrow by default, or
/// call any fallback execution engine.
#[must_use]
pub const fn upstream_vortex_dependency_linked() -> bool {
    cfg!(feature = "upstream-vortex")
}

/// Adapter boundary kinds used to stage future upstream Vortex integration.
///
/// These labels are planning-only and do not execute real Vortex IO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexAdapterBoundaryKind {
    MetadataInspection,
    DTypeMapping,
    EncodingMapping,
    LayoutMapping,
    StatisticsMapping,
    ReadPlanning,
    OutputPlanning,
    ActualRead,
    ActualWrite,
    Unsupported,
}
impl VortexAdapterBoundaryKind {
    /// Returns a stable machine-readable label.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataInspection => "metadata_inspection",
            Self::DTypeMapping => "dtype_mapping",
            Self::EncodingMapping => "encoding_mapping",
            Self::LayoutMapping => "layout_mapping",
            Self::StatisticsMapping => "statistics_mapping",
            Self::ReadPlanning => "read_planning",
            Self::OutputPlanning => "output_planning",
            Self::ActualRead => "actual_read",
            Self::ActualWrite => "actual_write",
            Self::Unsupported => "unsupported",
        }
    }

    /// Returns whether this boundary currently requires upstream dependency.
    #[must_use]
    pub const fn requires_upstream_dependency(&self) -> bool {
        matches!(self, Self::ActualRead | Self::ActualWrite)
    }
}

impl VortexFileRef {
    /// Creates a validated Vortex file reference from a dataset reference.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the dataset is not Vortex-native.
    pub fn new(dataset: DatasetRef) -> Result<Self> {
        if !dataset.format.is_native_vortex() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "dataset is not Vortex-native: format={}",
                dataset.format.as_str()
            )));
        }
        Ok(Self { dataset })
    }

    /// Creates a Vortex file reference by deriving a dataset reference from URI.
    ///
    /// # Errors
    /// Returns an error when URI parsing fails or when the format is not Vortex-native.
    pub fn from_uri(uri: DatasetUri) -> Result<Self> {
        let dataset = DatasetRef::from_uri(uri)?;
        Self::new(dataset)
    }

    /// Returns the dataset URI.
    #[must_use]
    pub fn uri(&self) -> &DatasetUri {
        &self.dataset.uri
    }

    /// Returns a concise planning-time summary.
    #[must_use]
    pub fn summary(&self) -> String {
        format!("vortex_file(uri={})", self.uri().as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VortexFileVersion {
    Unknown,
    V1,
    Extension(String),
}
impl VortexFileVersion {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::V1 => "v1",
            Self::Extension(_) => "extension",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOpenMode {
    MetadataOnly,
    PlanRead,
    NativeRead,
}
impl VortexOpenMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::PlanRead => "plan_read",
            Self::NativeRead => "native_read",
        }
    }
}

/// Options for planning Vortex file open/read behavior; no IO occurs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexOpenOptions {
    pub mode: VortexOpenMode,
    pub required_columns: Vec<ColumnRef>,
    pub require_statistics: bool,
    pub allow_partial_decode: bool,
}

impl VortexOpenOptions {
    #[must_use]
    pub fn metadata_only() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn plan_read() -> Self {
        Self {
            mode: VortexOpenMode::PlanRead,
            ..Self::default()
        }
    }
    #[must_use]
    pub fn native_read() -> Self {
        Self {
            mode: VortexOpenMode::NativeRead,
            ..Self::default()
        }
    }
    #[must_use]
    pub fn with_required_columns(mut self, columns: Vec<ColumnRef>) -> Self {
        self.required_columns = columns;
        self
    }
    #[must_use]
    pub fn require_statistics(mut self, value: bool) -> Self {
        self.require_statistics = value;
        self
    }
    #[must_use]
    pub fn allow_partial_decode(mut self, value: bool) -> Self {
        self.allow_partial_decode = value;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "mode={} required_columns={} require_statistics={} allow_partial_decode={}",
            self.mode.as_str(),
            self.required_columns.len(),
            self.require_statistics,
            self.allow_partial_decode
        )
    }
}

impl Default for VortexOpenOptions {
    fn default() -> Self {
        Self {
            mode: VortexOpenMode::MetadataOnly,
            required_columns: Vec::new(),
            require_statistics: false,
            allow_partial_decode: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFeatureStatus {
    Supported,
    Planned,
    NotImplemented,
    Unsupported,
}
impl VortexFeatureStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Planned => "planned",
            Self::NotImplemented => "not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputFidelity {
    NativeFullFidelity,
    NativePartialFidelity,
    Unsupported,
}
impl VortexOutputFidelity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeFullFidelity => "native_full_fidelity",
            Self::NativePartialFidelity => "native_partial_fidelity",
            Self::Unsupported => "unsupported",
        }
    }

    /// Maps adapter-local Vortex fidelity to canonical core fidelity.
    ///
    /// This helper preserves layer boundaries and does not perform IO or execution.
    #[must_use]
    pub const fn to_core_fidelity(&self) -> shardloom_core::FidelityLevel {
        match self {
            Self::NativeFullFidelity => shardloom_core::FidelityLevel::NativeFullFidelity,
            Self::NativePartialFidelity => shardloom_core::FidelityLevel::NativePartialFidelity,
            Self::Unsupported => shardloom_core::FidelityLevel::Unsupported,
        }
    }

    /// Returns canonical terminology label for this fidelity concept.
    ///
    /// This helper is label-only and intended for stable diagnostics output.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSegmentDescriptor {
    pub segment: EncodedSegment,
    pub source_file: VortexFileRef,
}
impl VortexSegmentDescriptor {
    #[must_use]
    pub fn new(segment: EncodedSegment, source_file: VortexFileRef) -> Self {
        Self {
            segment,
            source_file,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "source={} {}",
            self.source_file.uri().as_str(),
            self.segment.execution_summary()
        )
    }
    #[must_use]
    pub fn can_use_metadata(&self) -> bool {
        self.segment.can_use_metadata()
    }
    #[must_use]
    pub fn has_byte_ranges(&self) -> bool {
        self.segment.has_byte_ranges()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexFileMetadata {
    pub file: VortexFileRef,
    pub version: VortexFileVersion,
    pub row_count: Option<u64>,
    pub segments: Vec<VortexSegmentDescriptor>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFileMetadata {
    #[must_use]
    pub fn empty(file: VortexFileRef) -> Self {
        Self {
            file,
            version: VortexFileVersion::Unknown,
            row_count: None,
            segments: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_segment(&mut self, segment: VortexSegmentDescriptor) {
        self.segments.push(segment);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    #[must_use]
    pub fn has_statistics(&self) -> bool {
        self.segments
            .iter()
            .any(VortexSegmentDescriptor::can_use_metadata)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "file={} version={} rows={} segments={} has_statistics={} diagnostics={}",
            self.file.uri().as_str(),
            self.version.as_str(),
            self.row_count
                .map_or_else(|| "unknown".to_string(), |v| v.to_string()),
            self.segment_count(),
            self.has_statistics(),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReadPlanningStatus {
    Planned,
    MetadataOnly,
    NativeReadNotImplemented,
    Unsupported,
}
impl VortexReadPlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataOnly => "metadata_only",
            Self::NativeReadNotImplemented => "native_read_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexReadPlan {
    pub file: VortexFileRef,
    pub options: VortexOpenOptions,
    pub status: VortexReadPlanningStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexReadPlan {
    #[must_use]
    pub fn metadata_only(file: VortexFileRef) -> Self {
        Self {
            file,
            options: VortexOpenOptions::metadata_only(),
            status: VortexReadPlanningStatus::MetadataOnly,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn plan_read(file: VortexFileRef, options: VortexOpenOptions) -> Self {
        Self {
            file,
            options,
            status: VortexReadPlanningStatus::Planned,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn native_read_not_implemented(file: VortexFileRef, options: VortexOpenOptions) -> Self {
        let mut plan = Self {
            file,
            options,
            status: VortexReadPlanningStatus::NativeReadNotImplemented,
            diagnostics: Vec::new(),
        };
        plan.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEncoding,
            "vortex_native_read",
            "Vortex native read is not implemented yet. Fallback execution was not attempted.",
            Some("Use metadata-only or read-plan mode until native Vortex read is implemented. Spark/DataFusion/etc. are not fallback engines.".to_string()),
        ));
        plan
    }
    #[must_use]
    pub fn unsupported(
        file: VortexFileRef,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut plan = Self {
            file,
            options: VortexOpenOptions::metadata_only(),
            status: VortexReadPlanningStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        plan.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEncoding,
            feature,
            format!(
                "Unsupported Vortex read planning feature: {reason}. Fallback execution was not attempted."
            ),
            Some("Adjust request to supported Vortex-native planning modes. Spark/DataFusion/etc. are not fallback engines.".to_string()),
        ));
        plan
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error")
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = format!(
            "Vortex-native input plan\nfile: {}\nmode: {}\nstatus: {}\nfallback execution: disabled",
            self.file.uri().as_str(),
            self.options.mode.as_str(),
            self.status.as_str()
        );
        if self.diagnostics.is_empty() {
            text.push_str("\ndiagnostics: none");
        } else {
            text.push_str("\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                text.push_str("\n- ");
                text.push_str(&diagnostic.to_human_text());
            }
        }
        text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexWriteOptions {
    pub preserve_statistics: bool,
    pub preserve_layout_hints: bool,
    pub emit_manifest_linkage: bool,
    pub overwrite: bool,
}
impl VortexWriteOptions {
    #[must_use]
    pub fn native_defaults() -> Self {
        Self {
            preserve_statistics: true,
            preserve_layout_hints: true,
            emit_manifest_linkage: true,
            overwrite: false,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "preserve_statistics={} preserve_layout_hints={} emit_manifest_linkage={} overwrite={}",
            self.preserve_statistics,
            self.preserve_layout_hints,
            self.emit_manifest_linkage,
            self.overwrite
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWritePlanningStatus {
    Planned,
    NativeWriteNotImplemented,
    Unsupported,
}
impl VortexWritePlanningStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::NativeWriteNotImplemented => "native_write_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexWritePlan {
    pub target: VortexFileRef,
    pub options: VortexWriteOptions,
    pub fidelity: VortexOutputFidelity,
    pub status: VortexWritePlanningStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexWritePlan {
    #[must_use]
    pub fn planned(target: VortexFileRef, options: VortexWriteOptions) -> Self {
        Self {
            target,
            options,
            fidelity: VortexOutputFidelity::NativeFullFidelity,
            status: VortexWritePlanningStatus::Planned,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn native_write_not_implemented(
        target: VortexFileRef,
        options: VortexWriteOptions,
    ) -> Self {
        let mut plan = Self {
            target,
            options,
            fidelity: VortexOutputFidelity::NativePartialFidelity,
            status: VortexWritePlanningStatus::NativeWriteNotImplemented,
            diagnostics: Vec::new(),
        };
        plan.add_diagnostic(Diagnostic::new(
            DiagnosticCode::UnsupportedOutputFormat,
            shardloom_core::DiagnosticSeverity::Error,
            shardloom_core::DiagnosticCategory::VortexIo,
            "Vortex native write is not implemented yet. Fallback execution was not attempted.",
            Some("vortex_native_write".to_string()),
            Some("Spark/DataFusion/etc. are not fallback engines.".to_string()),
            Some(
                "Use planning-only output mode until native Vortex write is implemented."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ));
        plan
    }
    #[must_use]
    pub fn unsupported(
        target: VortexFileRef,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let mut plan = Self {
            target,
            options: VortexWriteOptions::native_defaults(),
            fidelity: VortexOutputFidelity::Unsupported,
            status: VortexWritePlanningStatus::Unsupported,
            diagnostics: Vec::new(),
        };
        plan.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            feature,
            format!("Unsupported Vortex output planning feature: {reason}. Fallback execution was not attempted."),
            Some("Adjust options to supported Vortex-native output planning. Spark/DataFusion/etc. are not fallback engines.".to_string()),
        ));
        plan
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error")
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = format!(
            "Vortex-native output plan\ntarget: {}\nhighest-fidelity target: vortex\nfidelity: {}\nstatus: {}\nfallback execution: disabled",
            self.target.uri().as_str(),
            self.fidelity.as_str(),
            self.status.as_str(),
        );
        if self.diagnostics.is_empty() {
            text.push_str("\ndiagnostics: none");
        } else {
            text.push_str("\ndiagnostics:");
            for diagnostic in &self.diagnostics {
                text.push_str("\n- ");
                text.push_str(&diagnostic.to_human_text());
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vortex_file_ref_from_uri_accepts_vortex_extension() {
        let uri = DatasetUri::new("file://tmp/data.vortex").expect("valid uri");
        let file = VortexFileRef::from_uri(uri).expect("vortex uri should be accepted");
        assert!(file.uri().as_str().ends_with(".vortex"));
    }

    #[test]
    fn vortex_file_ref_from_uri_accepts_vortex_directory() {
        let uri = DatasetUri::new("s3://bucket/path/data.vortex/").expect("valid uri");
        let file = VortexFileRef::from_uri(uri).expect("vortex uri should be accepted");
        assert!(file.uri().as_str().contains(".vortex/"));
    }

    #[test]
    fn vortex_file_ref_from_uri_rejects_parquet() {
        let uri = DatasetUri::new("file://tmp/data.parquet").expect("valid uri");
        let error = VortexFileRef::from_uri(uri).expect_err("parquet should be rejected");
        assert!(
            error
                .to_string()
                .contains("dataset is not Vortex-native: format=parquet")
        );
    }

    #[test]
    fn open_options_factories_set_modes() {
        assert_eq!(
            VortexOpenOptions::metadata_only().mode,
            VortexOpenMode::MetadataOnly
        );
        assert_eq!(
            VortexOpenOptions::plan_read().mode,
            VortexOpenMode::PlanRead
        );
        assert_eq!(
            VortexOpenOptions::native_read().mode,
            VortexOpenMode::NativeRead
        );
    }
    #[test]
    fn vortex_output_fidelity_native_full_maps_to_core_native_full() {
        assert_eq!(
            VortexOutputFidelity::NativeFullFidelity.to_core_fidelity(),
            shardloom_core::FidelityLevel::NativeFullFidelity
        );
    }
    #[test]
    fn vortex_output_fidelity_canonical_label_works() {
        assert_eq!(
            VortexOutputFidelity::NativePartialFidelity.canonical_label(),
            "native_partial_fidelity"
        );
    }

    #[test]
    fn write_options_defaults_preserve_stats_and_layout_hints() {
        let options = VortexWriteOptions::native_defaults();
        assert!(options.preserve_statistics);
        assert!(options.preserve_layout_hints);
    }

    #[test]
    fn file_metadata_empty_starts_with_zero_segments() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let metadata = VortexFileMetadata::empty(file);
        assert_eq!(metadata.segment_count(), 0);
    }

    #[test]
    fn file_metadata_segment_count_tracks_added_segment() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let mut metadata = VortexFileMetadata::empty(file.clone());
        let segment = shardloom_core::EncodedSegment::new(
            shardloom_core::SegmentId::new("seg0").expect("valid"),
            shardloom_core::ColumnRef::new("c0").expect("valid"),
            shardloom_core::LogicalDType::Int64,
            shardloom_core::Nullability::Nullable,
            shardloom_core::SegmentLayout::new(
                shardloom_core::EncodingKind::Plain,
                shardloom_core::LayoutKind::Flat,
            ),
            shardloom_core::SegmentStats::with_row_count(1),
        );
        metadata.add_segment(VortexSegmentDescriptor::new(segment, file));
        assert_eq!(metadata.segment_count(), 1);
    }

    #[test]
    fn read_plan_metadata_only_status() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let plan = VortexReadPlan::metadata_only(file);
        assert_eq!(plan.status, VortexReadPlanningStatus::MetadataOnly);
    }

    #[test]
    fn read_plan_native_read_not_implemented_has_errors() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let plan =
            VortexReadPlan::native_read_not_implemented(file, VortexOpenOptions::native_read());
        assert!(plan.has_errors());
    }

    #[test]
    fn read_plan_human_text_mentions_fallback_disabled() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let text = VortexReadPlan::metadata_only(file).to_human_text();
        assert!(text.contains("fallback execution: disabled"));
    }

    #[test]
    fn write_plan_planned_uses_native_full_fidelity() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let plan = VortexWritePlan::planned(file, VortexWriteOptions::native_defaults());
        assert_eq!(plan.fidelity, VortexOutputFidelity::NativeFullFidelity);
    }

    #[test]
    fn write_plan_native_write_not_implemented_has_errors() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let plan = VortexWritePlan::native_write_not_implemented(
            file,
            VortexWriteOptions::native_defaults(),
        );
        assert!(plan.has_errors());
    }

    #[test]
    fn write_plan_human_text_mentions_highest_fidelity() {
        let file =
            VortexFileRef::from_uri(DatasetUri::new("file://tmp/data.vortex").expect("valid"))
                .expect("vortex uri should be accepted");
        let text =
            VortexWritePlan::planned(file, VortexWriteOptions::native_defaults()).to_human_text();
        assert!(text.contains("highest-fidelity target"));
    }

    #[test]
    fn dependency_status_not_added_disallows_upstream_api_use() {
        assert!(!VortexDependencyStatus::NotAdded.allows_upstream_api_use());
    }

    #[test]
    fn dependency_status_added_allows_upstream_api_use() {
        assert!(VortexDependencyStatus::Added.allows_upstream_api_use());
    }

    #[test]
    fn upstream_vortex_dependency_linked_matches_feature_gate() {
        assert_eq!(
            upstream_vortex_dependency_linked(),
            cfg!(feature = "upstream-vortex")
        );
    }

    #[test]
    fn adapter_readiness_not_ready_is_not_ready_for_dependency_pr() {
        assert!(!VortexAdapterReadiness::not_ready().is_ready_for_dependency_pr());
    }

    #[test]
    fn adapter_readiness_ready_is_ready_for_dependency_pr() {
        assert!(VortexAdapterReadiness::ready_for_dependency_pr().is_ready_for_dependency_pr());
    }

    #[test]
    fn adapter_readiness_text_mentions_fallback_execution_disabled() {
        let text = VortexAdapterReadiness::not_ready().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
    }

    #[test]
    fn adapter_readiness_text_mentions_dependency_not_added_when_not_ready() {
        let text = VortexAdapterReadiness::not_ready().to_human_text();
        assert!(
            text.contains("upstream Vortex dependency feature is not enabled in this build")
                || text.contains("upstream Vortex dependency is not added yet")
        );
    }

    #[test]
    fn adapter_readiness_dependency_added_compile_only_not_ready_for_dependency_pr() {
        let readiness = VortexAdapterReadiness::dependency_added_compile_only();
        assert!(!readiness.is_ready_for_dependency_pr());
        assert!(readiness.fallback_dependencies_absent);
    }

    #[test]
    fn adapter_readiness_dependency_added_compile_only_text_mentions_blocked_adapter_io() {
        let text = VortexAdapterReadiness::dependency_added_compile_only().to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(
            text.contains("upstream Vortex dependency feature is not enabled in this build")
                || text.contains("broad Vortex adapter IO is not promoted")
        );
    }

    #[test]
    fn adapter_readiness_not_ready_when_dependency_status_not_added() {
        let readiness = VortexAdapterReadiness {
            dependency_status: VortexDependencyStatus::NotAdded,
            license_review_complete: true,
            provenance_review_complete: true,
            public_api_review_complete: true,
            fallback_dependencies_absent: true,
            diagnostics: Vec::new(),
        };
        assert!(!readiness.is_ready_for_dependency_pr());
    }

    #[test]
    fn adapter_readiness_has_errors_treats_fatal_as_error() {
        let mut readiness = VortexAdapterReadiness::ready_for_dependency_pr();
        readiness.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            shardloom_core::DiagnosticSeverity::Fatal,
            shardloom_core::DiagnosticCategory::Planning,
            "fatal readiness check",
            Some("vortex_adapter_readiness".to_string()),
            Some("fatal test diagnostic".to_string()),
            None,
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        assert!(readiness.has_errors());
        assert!(!readiness.is_ready_for_dependency_pr());
    }
    #[test]
    fn adapter_readiness_human_text_renders_non_empty_diagnostics() {
        let mut readiness = VortexAdapterReadiness::dependency_added_compile_only();
        readiness.add_diagnostic(Diagnostic::configuration_error(
            "vortex_adapter_readiness",
            "readiness probe pending",
            "readiness unresolved",
        ));
        let text = readiness.to_human_text();
        assert!(text.contains("readiness unresolved"));
        assert!(text.contains("fallback execution: disabled"));
    }
    #[test]
    fn adapter_readiness_has_errors_false_without_diagnostics() {
        let readiness = VortexAdapterReadiness::not_ready();
        assert!(!readiness.has_errors());
    }

    #[test]
    fn adapter_boundary_actual_read_requires_upstream_dependency() {
        assert!(VortexAdapterBoundaryKind::ActualRead.requires_upstream_dependency());
    }

    #[test]
    fn adapter_boundary_actual_write_requires_upstream_dependency() {
        assert!(VortexAdapterBoundaryKind::ActualWrite.requires_upstream_dependency());
    }

    #[test]
    fn adapter_boundary_metadata_inspection_not_required_for_now() {
        assert!(!VortexAdapterBoundaryKind::MetadataInspection.requires_upstream_dependency());
    }
}
