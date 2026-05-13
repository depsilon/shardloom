"""Thin Python client for the ShardLoom CLI JSON protocol.

The package does not provide a native binding or fallback execution engine.
It invokes explicit ShardLoom CLI commands and parses their JSON envelopes.
"""

from .client import (
    CompatibilitySourcePlan,
    CompatibilitySourceSmokeReport,
    DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS,
    ETL_INPUT_FORMATS,
    EngineCapabilityMatrix,
    EngineSelectionPlan,
    HybridOverlayRunReport,
    LiveChangeContractPlan,
    LiveEtlReplayResult,
    LiveFixtureRunReport,
    LocalVortexPrimitiveSmokeReport,
    PythonClientSmokeReport,
    RestApiContractPlan,
    RestApiDiscoveryContract,
    RestApiEventStream,
    RestApiLocalLifecycle,
    RestApiPlanPreview,
    ShardLoomClient,
    WorkflowReadinessPlan,
    WorkflowReadinessSmokeReport,
)
from .context import (
    CapabilityView,
    ContextCapabilities,
    ShardLoomContext,
    context,
)
from ._version import __version__
from .errors import (
    ShardLoomBinaryNotFoundError,
    ShardLoomCommandError,
    ShardLoomProtocolError,
)
from .models import Diagnostic, FieldEntry, FallbackStatus, OutputEnvelope
from .quickstart import QuickstartProofReport, quickstart_proof
from .query import (
    LazyFrame,
    UnsupportedWorkflowReport,
    WorkflowCertificationReport,
    WorkflowOperation,
    WorkflowSource,
    read_csv,
    read_json,
    read_parquet,
    read_vortex,
)

__all__ = [
    "__version__",
    "Diagnostic",
    "FallbackStatus",
    "FieldEntry",
    "OutputEnvelope",
    "QuickstartProofReport",
    "DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS",
    "ETL_INPUT_FORMATS",
    "EngineCapabilityMatrix",
    "EngineSelectionPlan",
    "HybridOverlayRunReport",
    "LiveChangeContractPlan",
    "LiveFixtureRunReport",
    "CapabilityView",
    "CompatibilitySourcePlan",
    "CompatibilitySourceSmokeReport",
    "ContextCapabilities",
    "LiveEtlReplayResult",
    "LocalVortexPrimitiveSmokeReport",
    "PythonClientSmokeReport",
    "RestApiContractPlan",
    "RestApiDiscoveryContract",
    "RestApiEventStream",
    "RestApiLocalLifecycle",
    "RestApiPlanPreview",
    "ShardLoomClient",
    "WorkflowReadinessPlan",
    "WorkflowReadinessSmokeReport",
    "ShardLoomContext",
    "context",
    "quickstart_proof",
    "LazyFrame",
    "UnsupportedWorkflowReport",
    "WorkflowCertificationReport",
    "WorkflowOperation",
    "WorkflowSource",
    "read_vortex",
    "read_csv",
    "read_json",
    "read_parquet",
    "ShardLoomBinaryNotFoundError",
    "ShardLoomCommandError",
    "ShardLoomProtocolError",
]
