"""Thin Python client for the ShardLoom CLI JSON protocol.

The package does not provide a native binding or fallback execution engine.
It lazily exposes explicit ShardLoom CLI/front-door helpers so `import
shardloom` does not eagerly load the whole SQL/DataFrame/session surface.
"""

from __future__ import annotations

from importlib import import_module
from sys import modules as _sys_modules
from types import ModuleType

from ._version import __version__

_EXPORT_GROUPS: dict[str, tuple[str, ...]] = {
    "models": (
        "Diagnostic",
        "ClaimSummary",
        "EvidenceSummary",
        "FallbackStatus",
        "FieldEntry",
        "OutputEnvelope",
        "RuntimeActivationSummary",
        "RuntimeEnvelopeValidationIssue",
        "RuntimeEnvelopeValidationReport",
        "validate_runtime_execution_envelope",
        "validate_runtime_execution_fields",
    ),
    "quickstart": (
        "QuickstartProofReport",
        "quickstart_proof",
    ),
    "client": (
        "ClaimGateCloseoutReport",
        "CommandMetadataReport",
        "ComputeCapabilityMatrix",
        "ComputeCapabilityRow",
        "CompatibilitySourcePlan",
        "CompatibilitySourceSmokeReport",
        "DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS",
        "ETL_INPUT_FORMATS",
        "EngineCapabilityMatrix",
        "EngineSelectionPlan",
        "EvidenceSchemaRegistryReport",
        "EvidenceAwareOptimizerTraceReport",
        "ExecutionEvidenceSlot",
        "ExecutionResultEnvelopeView",
        "GeneratedSourceWriteReport",
        "HybridOverlayRunReport",
        "LiveChangeContractPlan",
        "LiveEtlReplayResult",
        "LiveFixtureRunReport",
        "LiveHybridDurableCheckpointReport",
        "LiveHybridStateTransitionReport",
        "LocalDistributedFixtureRunReport",
        "LocalVortexPrimitiveSmokeReport",
        "NativeUnsupportedCoverageRow",
        "NativeVortexAdmissionLane",
        "OperatorFamilyCoverageRow",
        "PreparedVortexArtifacts",
        "PreparedVortexBatchResult",
        "PreparedVortexScanPushdownRow",
        "PredicateDtypeCoverageRow",
        "ProductionUnsupportedDiagnosticRow",
        "PublicWorkflowExecution",
        "PublicWorkflowRoute",
        "PythonClientSmokeReport",
        "RestApiContractPlan",
        "RestApiDataPlane",
        "RestApiDiscoveryContract",
        "RestApiEventStream",
        "RestApiLocalLifecycle",
        "RestApiPlanPreview",
        "RestApiSecurityGovernance",
        "RunsTodaySupportMatrix",
        "RunsTodaySupportRow",
        "SemanticConformanceRow",
        "SemanticConformanceSuite",
        "SessionCacheSmokeReport",
        "ShardLoomClient",
        "SqlLocalSourceSmokeReport",
        "VortexIngestSmokeReport",
        "WorkloadCertificationDossier",
        "WorkflowReadinessPlan",
        "WorkflowReadinessSmokeReport",
    ),
    "runtime_defaults": (
        "DEFAULT_INTERNAL_SMOKE_MAX_PARALLELISM",
        "DEFAULT_INTERNAL_SMOKE_MEMORY_GB",
        "DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM",
        "DEFAULT_LOCAL_RUNTIME_MEMORY_GB",
        "SHARDLOOM_MAX_PARALLELISM_ENV",
        "SHARDLOOM_MEMORY_GB_ENV",
    ),
    "context": (
        "CapabilityPosture",
        "CapabilityView",
        "ContextCapabilities",
        "DataFrameFutureContractClassification",
        "DataFrameFutureContractClassificationMatrix",
        "DataFrameMethodCapability",
        "DataFrameMethodCapabilityMatrix",
        "DataFrameNotebookPackageReadinessReport",
        "DataFrameNotebookPackageReadinessRow",
        "DatabaseWarehouseBoundaryMatrix",
        "DatabaseWarehouseBoundaryMatrixRow",
        "ETLWorkflowCapabilityMatrix",
        "ETLWorkflowCapabilityRow",
        "FrontDoorParityMatrix",
        "FrontDoorParityRow",
        "FrontDoorSemanticSurfaceMatrix",
        "FrontDoorSemanticSurfaceRow",
        "FoundryGeneratedOutputReport",
        "GeneratedPartitionedObjectStoreOutputReport",
        "GeneratedSourceApiAdmissionMatrix",
        "GeneratedSourceApiAdmissionRow",
        "GeneratedSourceCaseCapability",
        "GeneratedSourceCertificateContract",
        "GeneratedSourceEvidenceAlignmentReport",
        "GeneratedSourceEvidenceAlignmentRow",
        "GeneratedObjectStoreOutputReport",
        "ObjectStoreAdmissionLadder",
        "ObjectStoreAdmissionLadderRow",
        "LocalFileBenchmarkRouteReport",
        "LocalFileBenchmarkRouteRow",
        "LocalOutputSinkScopeReport",
        "LocalVortexPrimitiveRouteReport",
        "LocalVortexPrimitiveRouteRow",
        "NativeVortexProviderRouteCertificateReport",
        "NativeVortexProviderRouteCertificateRow",
        "SourcePreparedStateScopeReport",
        "V1_LOCAL_OUTPUT_SINK_DEFAULT_OUTPUT_FORMATS",
        "V1_LOCAL_OUTPUT_SINK_FEATURE_GATED_OUTPUT_FORMATS",
        "V1_LOCAL_OUTPUT_SINK_GOLDEN_FIXTURE_PATHS",
        "V1_LOCAL_OUTPUT_SINK_REQUIRED_RUNTIME_FIELDS",
        "V1_LOCAL_OUTPUT_SINK_ROUTE_IDS",
        "V1_LOCAL_OUTPUT_SINK_SCOPE_DOCUMENT",
        "V1_LOCAL_OUTPUT_SINK_SUPPORTED_OUTPUT_FORMATS",
        "V1_LOCAL_OUTPUT_SINK_UNSUPPORTED_BOUNDARY_IDS",
        "V1_LOCAL_OUTPUT_SINK_USER_WRITE_METHODS",
        "V1_LOCAL_OUTPUT_SINK_WRITE_POLICY_IDS",
        "V1_SOURCE_PREPARED_CANONICAL_ROUTE",
        "V1_SOURCE_PREPARED_GENERATED_ROUTE_IDS",
        "V1_SOURCE_PREPARED_GOLDEN_FIXTURE_PATHS",
        "V1_SOURCE_PREPARED_INTERNAL_SMOKE_ROUTE",
        "V1_SOURCE_PREPARED_INTERNAL_SMOKE_ROUTE_IDS",
        "V1_SOURCE_PREPARED_INVALIDATION_CASE_IDS",
        "V1_SOURCE_PREPARED_REQUIRED_RUNTIME_FIELDS",
        "V1_SOURCE_PREPARED_ROUTE_IDS",
        "V1_SOURCE_PREPARED_STATE_SCOPE_DOCUMENT",
        "V1_SOURCE_PREPARED_SUPPORTED_INPUT_FORMATS",
        "V1_SOURCE_PREPARED_UNSUPPORTED_BOUNDARY_IDS",
        "V1_VORTEX_FEATURE_PROFILE_DECISION",
        "V1_VORTEX_PROVIDER_ROUTE_IDS",
        "V1_VORTEX_PROVIDER_SCENARIO_IDS",
        "V1_VORTEX_RUNTIME_SCOPE_DOCUMENT",
        "V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS",
        "V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS",
        "V1_VORTEX_SUPPORTED_STARTING_STATES",
        "V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS",
        "OpenLineageFacetMappingReport",
        "OpenLineageFacetMappingRow",
        "OpenTelemetryTraceExportContractReport",
        "OpenTelemetryTraceExportSpanRow",
        "ShardLoomContext",
        "SourceFreeGeneratedOutputCompatibilityContract",
        "SourceFreeGeneratedOutputCompatibilityRow",
        "TableFormatBoundaryMatrix",
        "TableFormatBoundaryMatrixRow",
        "UniversalCompatibilityRow",
        "UniversalCompatibilityScoreboard",
        "UserSurfaceGraduationMatrix",
        "UserSurfaceGraduationRow",
        "UserRouteCapabilityReport",
        "UserRouteCapabilityRow",
        "context",
        "session",
    ),
    "session": (
        "LocalFileFingerprint",
        "SessionGroupedLazyFrame",
        "SessionLazyFrame",
        "SessionPreparedState",
        "SessionSqlWorkflow",
        "SessionSqlResult",
        "ShardLoomSession",
    ),
    "native_route": (
        "NativeVortexQuery",
        "NativeVortexRoute",
    ),
    "prepared_route": (
        "CompatibilityPreparedVortexRoute",
        "PreparedVortexQuery",
    ),
    "query": (
        "ColumnExpression",
        "ComplexProjectionExpression",
        "GroupedLazyFrame",
        "GeneratedRangeQuerySource",
        "GeneratedRangeSource",
        "GeneratedRowsSource",
        "GeneratedSqlSource",
        "IntervalLiteral",
        "LazyFrame",
        "PredicateExpression",
        "SqlWorkflow",
        "UnsupportedWorkflowOperationReport",
        "UnsupportedWorkflowReport",
        "VortexWorkflowExecutionReport",
        "WindowExpression",
        "WorkflowCertificationReport",
        "WorkflowColumnTransform",
        "WorkflowDataQualityCheckResult",
        "WorkflowDataQualityReport",
        "WorkflowNotebookPreview",
        "WorkflowOperation",
        "WorkflowPlanTransform",
        "WorkflowProfileReport",
        "WorkflowQuarantineReport",
        "WorkflowRowTransform",
        "WorkflowSchemaField",
        "WorkflowSchemaMismatch",
        "WorkflowSchemaReport",
        "WorkflowSchemaValidationReport",
        "WorkflowSource",
        "abs",
        "all_source",
        "any_source",
        "array",
        "byte_length",
        "calendar",
        "case_when",
        "ceil",
        "col",
        "column",
        "column_transform",
        "concat",
        "count_distinct",
        "dataframe_generated_with_column",
        "dataframe_source_free_projection",
        "dense_rank",
        "exists_source",
        "fixture_double_i64",
        "floor",
        "from_arrow_ipc",
        "from_arrow_table",
        "from_base64",
        "from_pandas",
        "from_rows",
        "interval_days",
        "interval_hours",
        "interval_minutes",
        "interval_seconds",
        "left",
        "length",
        "literal_table",
        "not_exists_source",
        "null_if",
        "outer",
        "plan_transform",
        "range",
        "rank",
        "read",
        "read_arrow_ipc",
        "read_avro",
        "read_csv",
        "read_json",
        "read_orc",
        "read_parquet",
        "read_vortex",
        "replace",
        "right",
        "round",
        "row_in",
        "row_in_source",
        "row_not_in",
        "row_not_in_source",
        "row_number",
        "row_transform",
        "sequence",
        "sql",
        "sql_literal_select",
        "sql_values",
        "struct",
        "substr",
        "substring",
        "try_cast",
        "typed_scalar_udf",
        "unhex",
    ),
    "errors": (
        "ShardLoomBinaryNotFoundError",
        "ShardLoomCommandError",
        "ShardLoomProtocolError",
    ),
}

_EXPORT_MODULES = {
    name: module_name
    for module_name, names in _EXPORT_GROUPS.items()
    for name in names
}

_STAR_EXPORT_EXCLUSIONS = frozenset(
    (
        "LiveHybridDurableCheckpointReport",
        "LiveHybridStateTransitionReport",
        "LocalDistributedFixtureRunReport",
        "CompatibilityPreparedVortexRoute",
        "NativeVortexProviderRouteCertificateReport",
        "NativeVortexProviderRouteCertificateRow",
        "NativeVortexQuery",
        "NativeVortexRoute",
        "OpenTelemetryTraceExportContractReport",
        "OpenTelemetryTraceExportSpanRow",
        "PreparedVortexQuery",
        "V1_VORTEX_FEATURE_PROFILE_DECISION",
        "V1_VORTEX_PROVIDER_ROUTE_IDS",
        "V1_VORTEX_PROVIDER_SCENARIO_IDS",
        "V1_VORTEX_RUNTIME_SCOPE_DOCUMENT",
        "V1_VORTEX_SUPPORTED_BENCHMARK_SCENARIO_IDS",
        "V1_VORTEX_SUPPORTED_PRIMITIVE_ROUTE_IDS",
        "V1_VORTEX_SUPPORTED_STARTING_STATES",
        "V1_VORTEX_UNSUPPORTED_BOUNDARY_IDS",
    )
)

__all__ = ("__version__",) + tuple(
    name for name in _EXPORT_MODULES if name not in _STAR_EXPORT_EXCLUSIONS
)

_MISSING = object()


def _load_export(name: str) -> object:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f".{module_name}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


class _ShardLoomPackageModule(ModuleType):
    """Prefer public lazy exports over same-named implementation submodules."""

    def __getattribute__(self, name: str) -> object:
        namespace = ModuleType.__getattribute__(self, "__dict__")
        export_modules = namespace.get("_EXPORT_MODULES", {})
        if name in export_modules:
            current = namespace.get(name, _MISSING)
            if current is _MISSING:
                return _load_export(name)
            if isinstance(current, ModuleType):
                package_name = ModuleType.__getattribute__(self, "__name__")
                if current.__name__ == f"{package_name}.{name}":
                    return _load_export(name)
            return current
        return ModuleType.__getattribute__(self, name)


def __getattr__(name: str) -> object:
    """Load public ShardLoom symbols on first access."""

    return _load_export(name)


def __dir__() -> list[str]:
    """Return the lazily exported public API for interactive discovery."""

    return sorted(set(globals()) | set(__all__))


_sys_modules[__name__].__class__ = _ShardLoomPackageModule
