"""Subprocess client for ShardLoom's CLI JSON protocol."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from ._version import __version__
from .errors import (
    ShardLoomBinaryNotFoundError,
    ShardLoomCommandError,
    ShardLoomProtocolError,
)
from .models import OutputEnvelope

CommandPart = str | os.PathLike[str]
Binary = CommandPart | Sequence[CommandPart]
DEFAULT_PROFILE_ORDER = ("release", "debug")
ETL_INPUT_FORMATS = frozenset(
    {"csv", "jsonl", "ndjson", "parquet", "arrow-ipc", "arrow_ipc", "avro", "orc", "vortex"}
)
ENV_BINARY = "SHARDLOOM_BIN"
ENV_REPO_ROOT = "SHARDLOOM_REPO_ROOT"
ENV_PROFILE_ORDER = "SHARDLOOM_PROFILE_ORDER"
ENV_TIMEOUT_SECONDS = "SHARDLOOM_TIMEOUT_SECONDS"
DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS = (
    ("csv", "examples/local/fact.csv"),
    ("jsonl", "examples/local/events.jsonl"),
    ("parquet", "examples/local/fact.parquet"),
    ("arrow_ipc", "examples/local/fact.arrow"),
)
DEFAULT_WORKFLOW_READINESS_TARGET_URI = "file://tmp/shardloom-output-readiness/out.vortex"
DEFAULT_WORKFLOW_READINESS_COMPATIBILITY_TARGET_URI = (
    "file://tmp/shardloom-output-readiness/out.parquet"
)
DEFAULT_WORKFLOW_READINESS_WORKSPACE = "target/shardloom-output-readiness-stage"
DEFAULT_WORKFLOW_READINESS_REMOTE_SOURCES = (
    ("s3_parquet", "s3://bucket/data.parquet"),
    ("gcs_parquet", "gs://bucket/data.parquet"),
    ("azure_parquet", "abfs://container/data.parquet"),
    ("http_parquet", "https://example.invalid/data.parquet"),
)
DEFAULT_VORTEX_WRITE_INTENT_SIGNALS = (
    "native-vortex-target",
    "schema-known",
    "schema-compatible",
    "delete-semantics-known",
    "tombstone-semantics-known",
    "commit-protocol-available",
    "staged-output-required",
)
DEFAULT_VORTEX_OUTPUT_PAYLOAD_SIGNALS = (
    "write-intent-ready",
    "staged-output-ready",
    "finalized-manifest-ready",
    "payload-content-available",
    "local-workspace",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_STAGED_MANIFEST_SIGNALS = (
    "draft-ready",
    "workspace-known",
    "marker-written",
    "local-workspace",
)
DEFAULT_VORTEX_COMMIT_MARKER_SIGNALS = (
    "commit-protocol-ready",
    "manifest-finalization-available",
    "local-workspace",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_COMMIT_INTENT_SIGNALS = (
    "commit-requested",
    "staged-manifest-draft-written",
    "manifest-finalization-available",
    "commit-protocol-available",
    "schema-known",
    "schema-compatible",
    "delete-semantics-known",
    "tombstone-semantics-known",
    "recovery-ready",
    "retry-gate-open",
    "cancellation-gate-open",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_COMMIT_PROTOCOL_SIGNALS = (
    "commit-intent-ready",
    "draft-manifest-ready",
    "manifest-finalization-available",
    "commit-marker-available",
    "recovery-ready",
    "feature-gate-enabled",
)
DEFAULT_VORTEX_LOCAL_COMMIT_RECOVERY_SIGNALS = (
    "rollback-requested",
    "committed-manifest-written",
    "local-workspace",
    "cleanup-allowed",
)


@dataclass(frozen=True, slots=True)
class LiveEtlReplayResult:
    """Result of a CSV universal-I/O run and optional native Vortex replay."""

    csv_import: OutputEnvelope
    native_vortex: OutputEnvelope | None

    @property
    def fact_vortex_path(self) -> str:
        """Return the fact-table Vortex artifact path emitted by CSV import."""

        return _required_field(self.csv_import, "fact_vortex_path")

    @property
    def dim_vortex_path(self) -> str:
        """Return the dimension-table Vortex artifact path emitted by CSV import."""

        return _required_field(self.csv_import, "dim_vortex_path")

    @property
    def fallback_attempted(self) -> bool:
        """Whether either step reported attempted fallback execution."""

        return self.csv_import.fallback.attempted or (
            self.native_vortex.fallback.attempted
            if self.native_vortex is not None
            else False
        )

    @property
    def native_replay_ran(self) -> bool:
        """Whether the native Vortex replay command was executed."""

        return self.native_vortex is not None


@dataclass(frozen=True, slots=True)
class LocalVortexPrimitiveSmokeReport:
    """Result of the explicit local Vortex primitive smoke workflow."""

    count: OutputEnvelope
    count_where: OutputEnvelope
    filter: OutputEnvelope
    project: OutputEnvelope
    filter_project: OutputEnvelope

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return command envelopes in execution order."""

        return (
            self.count,
            self.count_where,
            self.filter,
            self.project,
            self.filter_project,
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return the local primitive commands executed by the smoke workflow."""

        return tuple(envelope.command for envelope in self.envelopes)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any local primitive command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or _any_bool_field(envelope, LOCAL_VORTEX_FALLBACK_ATTEMPTED_FIELDS)
            for envelope in self.envelopes
        )

    @property
    def all_certified(self) -> bool:
        """Whether every command emitted the expected certified local evidence fields."""

        return all(
            _all_bool_fields(envelope, LOCAL_VORTEX_CERTIFIED_FIELDS)
            for envelope in self.envelopes
        )

    @property
    def uncertified_commands(self) -> tuple[str, ...]:
        """Return commands that did not emit both Native I/O and correctness certification."""

        return tuple(
            envelope.command
            for envelope in self.envelopes
            if not _all_bool_fields(envelope, LOCAL_VORTEX_CERTIFIED_FIELDS)
        )


@dataclass(frozen=True, slots=True)
class CompatibilitySourcePlan:
    """One report-only compatibility-source input plan."""

    source_name: str
    dataset_uri: str
    plan: OutputEnvelope


@dataclass(frozen=True, slots=True)
class CompatibilitySourceSmokeReport:
    """Report-only compatibility-source planning smoke envelopes."""

    input_adapters: OutputEnvelope
    native_io_envelope: OutputEnvelope
    sources: tuple[CompatibilitySourcePlan, ...]

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return smoke envelopes in execution order."""

        return (
            self.input_adapters,
            self.native_io_envelope,
            *(source.plan for source in self.sources),
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return commands executed by the compatibility-source smoke workflow."""

        return tuple(envelope.command for envelope in self.envelopes)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any report-only command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or envelope.field_bool("fallback_attempted", False) is True
            for envelope in self.envelopes
        )

    @property
    def all_plan_only(self) -> bool:
        """Whether every source plan stayed side-effect-free and report-only."""

        return all(
            source.plan.field_bool("plan_only", False) is True
            and source.plan.field_bool("data_read", True) is False
            and source.plan.field_bool("data_materialized", True) is False
            and source.plan.field_bool("write_io", True) is False
            and source.plan.field_bool("native_vortex", True) is False
            and source.plan.field_bool("fallback_execution_allowed", True) is False
            for source in self.sources
        )

    @property
    def compatibility_source_names(self) -> tuple[str, ...]:
        """Return source labels that planned as structured compatibility inputs."""

        return tuple(
            source.source_name
            for source in self.sources
            if source.plan.field_bool("compatibility_structured", False) is True
            and source.plan.field_bool("native_vortex", True) is False
        )

    @property
    def planned_source_names(self) -> tuple[str, ...]:
        """Return source labels that are visible but not certified for execution."""

        return tuple(
            source.source_name
            for source in self.sources
            if source.plan.field("capability_status") == "planned"
        )


@dataclass(frozen=True, slots=True)
class WorkflowReadinessPlan:
    """One named envelope in the no-write workflow readiness smoke."""

    name: str
    envelope: OutputEnvelope


@dataclass(frozen=True, slots=True)
class WorkflowReadinessSmokeReport:
    """No-write planning smoke for output, remote, and evidence readiness."""

    output_commit: tuple[WorkflowReadinessPlan, ...]
    table_remote: tuple[WorkflowReadinessPlan, ...]
    evidence: tuple[WorkflowReadinessPlan, ...]

    @property
    def plans(self) -> tuple[WorkflowReadinessPlan, ...]:
        """Return all readiness plans in execution order."""

        return (*self.output_commit, *self.table_remote, *self.evidence)

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all readiness envelopes in execution order."""

        return tuple(plan.envelope for plan in self.plans)

    @property
    def plan_names(self) -> tuple[str, ...]:
        """Return stable readiness plan labels in execution order."""

        return tuple(plan.name for plan in self.plans)

    @property
    def commands(self) -> tuple[str, ...]:
        """Return CLI commands executed by the readiness smoke."""

        return tuple(envelope.command for envelope in self.envelopes)

    @property
    def fallback_attempted(self) -> bool:
        """Whether any readiness command reported attempted fallback execution."""

        return any(
            envelope.fallback.attempted
            or envelope.field_bool("fallback_attempted", False) is True
            for envelope in self.envelopes
        )

    @property
    def all_no_write(self) -> bool:
        """Whether every readiness envelope preserved the no-write/no-effect contract."""

        return all(_envelope_preserves_no_write(envelope) for envelope in self.envelopes)

    @property
    def all_report_only_or_planned(self) -> bool:
        """Whether every envelope stayed in a planning/report-only lifecycle."""

        return all(_envelope_is_report_only_or_planned(envelope) for envelope in self.envelopes)

    @property
    def blocked_plan_names(self) -> tuple[str, ...]:
        """Return readiness plans that report blockers or incomplete evidence."""

        return tuple(
            plan.name
            for plan in self.plans
            if _envelope_reports_blocked_or_incomplete(plan.envelope)
        )


@dataclass(frozen=True, slots=True)
class EngineSelectionPlan:
    """Typed convenience view over the CG-22 engine-selection report."""

    envelope: OutputEnvelope

    @property
    def requested_engine_mode(self) -> str | None:
        """Return the requested engine mode."""

        return self.envelope.field("requested_engine_mode")

    @property
    def selected_engine_mode(self) -> str | None:
        """Return the selected engine mode, or `none` when rejected."""

        return self.envelope.field("selected_engine_mode")

    @property
    def selection_status(self) -> str | None:
        """Return the engine-selection status."""

        return self.envelope.field("selection_status")

    @property
    def rejection_reasons(self) -> tuple[str, ...]:
        """Return deterministic engine rejection reasons."""

        value = self.envelope.field("rejection_reasons", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split(";") if part.strip())

    @property
    def fallback_attempted(self) -> bool:
        """Whether engine selection reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether engine selection invoked an external engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True


@dataclass(frozen=True, slots=True)
class EngineCapabilityMatrix:
    """Typed convenience view over the CG-22 engine capability matrix."""

    envelope: OutputEnvelope

    @property
    def engine_modes(self) -> tuple[str, ...]:
        """Return engine modes represented in the matrix."""

        value = self.envelope.field("engine_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def live_hybrid_claim_blocked_count(self) -> int:
        """Return the number of live/hybrid production claims still blocked."""

        return self.envelope.field_int("live_hybrid_claim_blocked_count", 0) or 0

    @property
    def fallback_attempted(self) -> bool:
        """Whether matrix discovery reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether matrix discovery invoked an external engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True


@dataclass(frozen=True, slots=True)
class RestApiContractPlan:
    """Typed convenience view over the CG-23 REST/OpenAPI contract report."""

    envelope: OutputEnvelope

    @property
    def api_version(self) -> str | None:
        """Return the REST API version."""

        return self.envelope.field("api_version")

    @property
    def openapi_version(self) -> str | None:
        """Return the declared OpenAPI version."""

        return self.envelope.field("openapi_version")

    @property
    def openapi_contract_path(self) -> str | None:
        """Return the checked-in OpenAPI contract path."""

        return self.envelope.field("openapi_contract_path")

    @property
    def represented_resources(self) -> tuple[str, ...]:
        """Return represented REST resource groups."""

        value = self.envelope.field("represented_resources", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def discovery_endpoint_paths(self) -> tuple[str, ...]:
        """Return contract-only discovery endpoint paths."""

        value = self.envelope.field("discovery_endpoint_paths", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def contract_artifact_checked_in(self) -> bool:
        """Whether the checked-in OpenAPI contract artifact is reported present."""

        return self.envelope.field_bool("openapi_contract_artifact_checked_in", False) is True

    @property
    def server_started(self) -> bool:
        """Whether the command started a server."""

        return self.envelope.field_bool("server_started", False) is True

    @property
    def network_listener_opened(self) -> bool:
        """Whether the command opened a network listener."""

        return self.envelope.field_bool("network_listener_opened", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )


@dataclass(frozen=True, slots=True)
class RestApiDiscoveryContract(RestApiContractPlan):
    """Typed view over `serve --mode discovery` contract-only output."""

    @property
    def bind(self) -> str | None:
        """Return the requested bind address."""

        return self.envelope.field("bind")

    @property
    def server_mode(self) -> str | None:
        """Return the requested server mode."""

        return self.envelope.field("server_mode")

    @property
    def contract_only(self) -> bool:
        """Whether `serve` stayed in contract-only mode."""

        return self.envelope.field_bool("serve_command_contract_only", False) is True


@dataclass(frozen=True, slots=True)
class RestApiPlanPreview:
    """Typed view over the CG-23 plan/explain/validate/certification preview."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic preview fixture scenario."""

        return self.envelope.field("scenario")

    @property
    def preview_status(self) -> str | None:
        """Return the preview status."""

        return self.envelope.field("preview_status")

    @property
    def plan_handle(self) -> str | None:
        """Return the stable plan handle."""

        return self.envelope.field("plan_handle")

    @property
    def operations(self) -> tuple[str, ...]:
        """Return supported plan-preview operations."""

        value = self.envelope.field("preview_operations", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def stage_order(self) -> tuple[str, ...]:
        """Return the preview stage order."""

        value = self.envelope.field("stage_order", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def stage_statuses(self) -> Mapping[str, str]:
        """Return per-stage status values keyed by stage id."""

        statuses: dict[str, str] = {}
        for stage in self.stage_order:
            value = self.envelope.field(f"{stage}_stage_status")
            if value is not None:
                statuses[stage] = value
        return statuses

    @property
    def problem_details_emitted(self) -> bool:
        """Whether the preview emitted problem-details fields."""

        return self.envelope.field_bool("problem_details_emitted", False) is True

    @property
    def problem_details_type(self) -> str | None:
        """Return the problem-details type URI, if any."""

        value = self.envelope.field("problem_details_type")
        return None if value in {None, "none"} else value

    @property
    def problem_details_status(self) -> int | None:
        """Return the problem-details HTTP status, if any."""

        value = self.envelope.field("problem_details_status")
        if value in {None, "none"}:
            return None
        return int(value)

    @property
    def problem_details_diagnostic_code(self) -> str | None:
        """Return the problem-details diagnostic code, if any."""

        value = self.envelope.field("problem_details_diagnostic_code")
        return None if value in {None, "none"} else value

    @property
    def unsupported_reason(self) -> str | None:
        """Return the deterministic unsupported reason, if any."""

        value = self.envelope.field("unsupported_reason")
        return None if value in {None, "none"} else value

    @property
    def server_started(self) -> bool:
        """Whether the preview started a server."""

        return self.envelope.field_bool("server_started", False) is True

    @property
    def network_listener_opened(self) -> bool:
        """Whether the preview opened a listener."""

        return self.envelope.field_bool("network_listener_opened", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether the preview executed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the preview attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether the preview delegated execution to any external engine."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiLocalLifecycle:
    """Typed view over the CG-23 certified local lifecycle and result delivery bundle."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic lifecycle scenario."""

        return self.envelope.field("scenario")

    @property
    def lifecycle_status(self) -> str | None:
        """Return the lifecycle status."""

        return self.envelope.field("lifecycle_status")

    @property
    def query_id(self) -> str | None:
        """Return the query handle."""

        return self.envelope.field("query_id")

    @property
    def result_ref(self) -> str | None:
        """Return the result reference, if available."""

        value = self.envelope.field("result_ref")
        return None if value in {None, "none"} else value

    @property
    def lifecycle_operations(self) -> tuple[str, ...]:
        """Return supported lifecycle operations."""

        value = self.envelope.field("lifecycle_operations", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def result_policies(self) -> tuple[str, ...]:
        """Return result policy materialization summaries."""

        value = self.envelope.field("result_policies", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def inline_json_available(self) -> bool:
        """Whether inline JSON result delivery is available."""

        return self.envelope.field_bool("inline_json_available", False) is True

    @property
    def vortex_artifact_available(self) -> bool:
        """Whether a high-fidelity Vortex artifact result is available."""

        return self.envelope.field_bool("vortex_artifact_available", False) is True

    @property
    def arrow_ipc_materialization(self) -> str | None:
        """Return the Arrow IPC materialization classification."""

        return self.envelope.field("arrow_ipc_materialization")

    @property
    def arrow_ipc_certified_native(self) -> bool:
        """Whether Arrow IPC is certified as native full-fidelity output."""

        return self.envelope.field_bool("arrow_ipc_certified_native", False) is True

    @property
    def result_ttl_seconds(self) -> int:
        """Return local result TTL in seconds."""

        return self.envelope.field_int("result_ttl_seconds", 0) or 0

    @property
    def cleanup_required(self) -> bool:
        """Whether lifecycle cleanup is required."""

        return self.envelope.field_bool("cleanup_required", False) is True

    @property
    def non_certified_path_blocked(self) -> bool:
        """Whether an uncertified path was blocked before execution."""

        return self.envelope.field_bool("non_certified_path_blocked", False) is True

    @property
    def cancellation_status(self) -> str | None:
        """Return cancellation state."""

        return self.envelope.field("cancellation_status")

    @property
    def retry_status(self) -> str | None:
        """Return retry state."""

        return self.envelope.field("retry_status")

    @property
    def query_execution(self) -> bool:
        """Whether query execution was performed."""

        return self.envelope.field_bool("query_execution", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether runtime execution was performed."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def local_execution_performed(self) -> bool:
        """Whether the lifecycle used a certified local execution path."""

        return self.envelope.field_bool("local_execution_performed", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether lifecycle handling attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether lifecycle handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiEventStream:
    """Typed view over the CG-23 live/hybrid event stream contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic event-stream scenario."""

        return self.envelope.field("scenario")

    @property
    def event_stream_status(self) -> str | None:
        """Return the event stream status."""

        return self.envelope.field("event_stream_status")

    @property
    def stream_id(self) -> str | None:
        """Return the stream handle."""

        return self.envelope.field("stream_id")

    @property
    def stream_ref(self) -> str | None:
        """Return the stream reference."""

        value = self.envelope.field("stream_ref")
        return None if value in {None, "none"} else value

    @property
    def engine_mode(self) -> str | None:
        """Return the event stream engine-mode posture."""

        return self.envelope.field("engine_mode")

    @property
    def delivery_protocols(self) -> tuple[str, ...]:
        """Return declared event delivery protocols."""

        value = self.envelope.field("delivery_protocols", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def event_types(self) -> tuple[str, ...]:
        """Return declared event type names."""

        value = self.envelope.field("event_types", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def certificate_refs(self) -> tuple[str, ...]:
        """Return available event evidence certificate refs."""

        value = (
            self.envelope.field("certificate_ref_summary")
            or self.envelope.field("certificate_refs")
            or ""
        )
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def asyncapi_contract_path(self) -> str | None:
        """Return the checked-in AsyncAPI contract path."""

        return self.envelope.field("asyncapi_contract_path")

    @property
    def sse_first(self) -> bool:
        """Whether SSE is the default event delivery protocol."""

        return self.envelope.field_bool("sse_first", False) is True

    @property
    def websocket_required(self) -> bool:
        """Whether WebSocket is required for this contract."""

        return self.envelope.field_bool("websocket_required", False) is True

    @property
    def event_count(self) -> int:
        """Return the deterministic event count for the scenario."""

        return self.envelope.field_int("event_count", 0) or 0

    @property
    def workload_certified(self) -> bool:
        """Whether the referenced workload is certified for event streaming."""

        return self.envelope.field_bool("workload_certified", False) is True

    @property
    def production_claim_allowed(self) -> bool:
        """Whether production event-streaming claims are allowed."""

        return self.envelope.field_bool("production_claim_allowed", False) is True

    @property
    def broker_required(self) -> bool:
        """Whether the contract requires a broker."""

        return self.envelope.field_bool("broker_required", False) is True

    @property
    def broker_io(self) -> bool:
        """Whether broker I/O was performed."""

        return self.envelope.field_bool("broker_io", False) is True

    @property
    def object_store_io(self) -> bool:
        """Whether object-store I/O was performed."""

        return self.envelope.field_bool("object_store_io", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether event-stream handling attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether event-stream handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class LiveChangeContractPlan:
    """Typed convenience view over the CG-22 live change contract."""

    envelope: OutputEnvelope

    @property
    def change_record_fields(self) -> tuple[str, ...]:
        """Return required `ChangeRecord` field names in contract order."""

        value = self.envelope.field("change_record_field_order", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def operations(self) -> tuple[str, ...]:
        """Return supported fixture change operations."""

        value = self.envelope.field("change_operation_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def fixture_operators(self) -> tuple[str, ...]:
        """Return supported in-memory live fixture operators."""

        value = self.envelope.field("fixture_operator_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def fallback_attempted(self) -> bool:
        """Whether the contract plan reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether the contract plan executed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True


@dataclass(frozen=True, slots=True)
class LiveFixtureRunReport:
    """Typed convenience view over the CG-22 in-memory live fixture run."""

    envelope: OutputEnvelope

    @property
    def operator(self) -> str | None:
        """Return the executed fixture operator."""

        return self.envelope.field("fixture_operator")

    @property
    def input_change_record_count(self) -> int:
        """Return the number of deterministic fixture change records."""

        return self.envelope.field_int("input_change_record_count", 0) or 0

    @property
    def active_state_key_count(self) -> int:
        """Return the active state key count after applying changes."""

        return self.envelope.field_int("active_state_key_count", 0) or 0

    @property
    def output_row_count(self) -> int:
        """Return the emitted fixture output row count."""

        return self.envelope.field_int("output_row_count", 0) or 0

    @property
    def output_rows(self) -> tuple[str, ...]:
        """Return deterministic output rows from the fixture report."""

        value = self.envelope.field("output_rows", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split("|") if part.strip())

    @property
    def all_certified(self) -> bool:
        """Whether live, execution, and Native I/O certificates are all certified."""

        return all(
            self.envelope.field(key) == "certified"
            for key in (
                "freshness_certificate_status",
                "state_certificate_status",
                "continuous_view_certificate_status",
                "execution_certificate_status",
                "native_io_certificate_status",
            )
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True

    @property
    def data_read(self) -> bool:
        """Whether the fixture command read external data."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the fixture command wrote output or checkpoints."""

        return self.envelope.field_bool("write_io", False) is True


@dataclass(frozen=True, slots=True)
class HybridOverlayRunReport:
    """Typed convenience view over the CG-22 hybrid overlay fixture run."""

    envelope: OutputEnvelope

    @property
    def operator(self) -> str | None:
        """Return the executed fixture operator."""

        return self.envelope.field("fixture_operator")

    @property
    def base_row_count(self) -> int:
        """Return the declared local Vortex base row count."""

        return self.envelope.field_int("base_row_count", 0) or 0

    @property
    def hot_change_record_count(self) -> int:
        """Return the hot delta change-record count."""

        return self.envelope.field_int("hot_change_record_count", 0) or 0

    @property
    def merged_row_count(self) -> int:
        """Return the merged base-plus-hot row count."""

        return self.envelope.field_int("merged_row_count", 0) or 0

    @property
    def output_rows(self) -> tuple[str, ...]:
        """Return deterministic output rows from the hybrid fixture report."""

        value = self.envelope.field("output_rows", "") or ""
        if value == "none":
            return ()
        return tuple(part.strip() for part in value.split("|") if part.strip())

    @property
    def all_certified(self) -> bool:
        """Whether overlay, flush, freshness, execution, and Native I/O evidence is certified."""

        return all(
            self.envelope.field(key) == "certified"
            for key in (
                "delta_overlay_certificate_status",
                "micro_segment_flush_evidence_status",
                "freshness_certificate_status",
                "execution_certificate_status",
                "native_io_certificate_status",
            )
        )

    @property
    def layout_health_status(self) -> str | None:
        """Return the bundled layout-health status."""

        return self.envelope.field("layout_health_bundle_status")

    @property
    def runtime_execution(self) -> bool:
        """Whether this explicit fixture command performed runtime work."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether the fixture command reported fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the fixture command invoked an external engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True

    @property
    def data_read(self) -> bool:
        """Whether the fixture command read external data."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the fixture command wrote output or checkpoints."""

        return self.envelope.field_bool("write_io", False) is True


@dataclass(frozen=True, slots=True)
class PythonClientSmokeReport:
    """No-dataset Python client smoke-check envelopes."""

    binary_command: tuple[str, ...]
    python_package_version: str
    platform: str
    status: OutputEnvelope
    python_capabilities: OutputEnvelope
    deployment_capabilities: OutputEnvelope
    input_adapters: OutputEnvelope

    @property
    def fallback_attempted(self) -> bool:
        """Whether any smoke-check command reported attempted fallback execution."""

        return (
            self.status.fallback.attempted
            or self.python_capabilities.fallback.attempted
            or self.deployment_capabilities.fallback.attempted
            or self.input_adapters.fallback.attempted
        )

    @property
    def commands(self) -> tuple[str, ...]:
        """Return the commands executed by the smoke check."""

        return (
            self.status.command,
            self.python_capabilities.command,
            self.deployment_capabilities.command,
            self.input_adapters.command,
        )

    @property
    def protocol_version(self) -> str:
        """Return the parsed ShardLoom output protocol version."""

        return self.status.schema_version

    @property
    def cli_version(self) -> str | None:
        """Return the CLI version field when the status surface exposes it."""

        return self.status.field("cli_binary_version") or self.status.field("version")

    @property
    def resolved_cli_path(self) -> str | None:
        """Return the resolved CLI executable path or command head."""

        return self.binary_command[0] if self.binary_command else None

    @property
    def feature_gates(self) -> tuple[str, ...]:
        """Return feature-gate-like runtime fields exposed by smoke envelopes."""

        values: list[str] = []
        for envelope in (
            self.status,
            self.python_capabilities,
            self.deployment_capabilities,
            self.input_adapters,
        ):
            for key in (
                "feature_gates",
                "enabled_feature_gates",
                "disabled_feature_gates",
                "surface_components",
            ):
                value = envelope.field(key)
                if value:
                    values.extend(part.strip() for part in value.split(",") if part.strip())
        return tuple(dict.fromkeys(values))


class ShardLoomClient:
    """Thin client that invokes the ShardLoom CLI with `--format json`.

    The client does not inspect datasets, probe catalogs, load external engines,
    or provide fallback execution. It only runs explicit CLI commands requested
    by the caller and parses the resulting JSON envelope.
    """

    def __init__(
        self,
        binary: Binary | None = None,
        *,
        env: Mapping[str, str] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        repo_root: str | os.PathLike[str] | None = None,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        timeout: float | None = None,
    ) -> None:
        self._binary = binary
        self._env = dict(env) if env is not None else None
        self._cwd = Path(cwd) if cwd is not None else None
        self._repo_root = Path(repo_root) if repo_root is not None else None
        self._profile_order = tuple(profile_order)
        self._timeout = timeout

    @classmethod
    def from_repo(
        cls,
        repo_root: str | os.PathLike[str] | None = None,
        *,
        profile_order: Sequence[str] = DEFAULT_PROFILE_ORDER,
        **kwargs: object,
    ) -> "ShardLoomClient":
        """Create a client that resolves `target/<profile>/shardloom` lazily.

        This is intended for source-tree development and local ETL testing. It
        does not run commands or probe anything at import time.
        """

        root = Path.cwd() if repo_root is None else Path(repo_root)
        return cls(repo_root=root, profile_order=profile_order, **kwargs)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        profile_order: Sequence[str] | None = None,
        **kwargs: object,
    ) -> "ShardLoomClient":
        """Create a client from ShardLoom Python environment variables.

        Supported variables:
        `SHARDLOOM_BIN`, `SHARDLOOM_REPO_ROOT`, `SHARDLOOM_PROFILE_ORDER`, and
        `SHARDLOOM_TIMEOUT_SECONDS`. The method only reads configuration; it
        does not run the CLI or inspect datasets.
        """

        effective_env = dict(os.environ)
        if env is not None:
            effective_env.update(env)
        repo_root = effective_env.get(ENV_REPO_ROOT)
        configured_profile_order = profile_order or _profile_order_from_env(effective_env)
        timeout = kwargs.pop("timeout", _timeout_from_env(effective_env))
        return cls(
            env=effective_env,
            repo_root=repo_root,
            profile_order=configured_profile_order,
            timeout=timeout,
            **kwargs,
        )

    def status(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI status envelope."""

        return self.run(["status"], check=check)

    def api_compat_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CLI/API JSON compatibility plan envelope."""

        return self.run(["api-compat-plan"], check=check)

    def rest_api_contract_plan(self, *, check: bool = True) -> RestApiContractPlan:
        """Return the CG-23 REST/OpenAPI contract plan envelope."""

        return RestApiContractPlan(self.run(["rest-api-contract-plan"], check=check))

    def serve_discovery_contract(
        self,
        *,
        bind: str = "127.0.0.1:8787",
        check: bool = True,
    ) -> RestApiDiscoveryContract:
        """Return `serve --mode discovery` contract output without starting a server."""

        return RestApiDiscoveryContract(
            self.run(["serve", "--mode", "discovery", "--bind", bind], check=check)
        )

    def rest_api_plan_preview(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiPlanPreview:
        """Return a CG-23 plan/explain/validate/certification preview envelope."""

        return RestApiPlanPreview(
            self.run(["rest-api-plan-preview", scenario], check=check)
        )

    def rest_api_local_lifecycle(
        self,
        scenario: str = "certified-local-batch",
        *,
        check: bool = True,
    ) -> RestApiLocalLifecycle:
        """Return a CG-23 certified local lifecycle/result delivery envelope."""

        return RestApiLocalLifecycle(
            self.run(["rest-api-local-lifecycle", scenario], check=check)
        )

    def rest_api_event_stream(
        self,
        scenario: str = "certified-live-fixture",
        *,
        check: bool = True,
    ) -> RestApiEventStream:
        """Return a CG-23 live/hybrid event stream contract envelope."""

        return RestApiEventStream(
            self.run(["rest-api-event-stream", scenario], check=check)
        )

    def python_wrapper_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the Python wrapper foundation plan envelope."""

        return self.run(["python-wrapper-plan"], check=check)

    def capabilities(self, scope: str | None = None, *, check: bool = True) -> OutputEnvelope:
        """Return a capability-discovery envelope for the optional scope."""

        args = ["capabilities"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def engine_selection_plan(
        self,
        engine: str = "auto",
        *,
        boundedness: str = "snapshot",
        update_mode: str = "snapshot",
        output_mode: str = "snapshot",
        check: bool = True,
    ) -> EngineSelectionPlan:
        """Return the CG-22 report-only engine-selection plan."""

        return EngineSelectionPlan(
            self.run(
                [
                    "engine-selection-plan",
                    engine,
                    boundedness,
                    update_mode,
                    output_mode,
                ],
                check=check,
            )
        )

    def engine_capability_matrix(self, *, check: bool = True) -> EngineCapabilityMatrix:
        """Return the CG-22 report-only per-engine capability matrix."""

        return EngineCapabilityMatrix(self.run(["engine-capability-matrix"], check=check))

    def live_change_contract_plan(self, *, check: bool = True) -> LiveChangeContractPlan:
        """Return the CG-22 report-only live change contract."""

        return LiveChangeContractPlan(self.run(["live-change-contract-plan"], check=check))

    def live_fixture_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        check: bool = True,
    ) -> LiveFixtureRunReport:
        """Run the explicit CG-22 in-memory live fixture command."""

        args = ["live-fixture-run", operator]
        if argument is not None:
            args.append(str(argument) if isinstance(argument, str) else _columns_arg(argument))
        return LiveFixtureRunReport(self.run(args, check=check))

    def hybrid_overlay_run(
        self,
        operator: str = "filter",
        argument: str | Sequence[str] | None = None,
        *,
        check: bool = True,
    ) -> HybridOverlayRunReport:
        """Run the explicit CG-22 in-memory hybrid overlay fixture command."""

        args = ["hybrid-overlay-run", operator]
        if argument is not None:
            args.append(str(argument) if isinstance(argument, str) else _columns_arg(argument))
        return HybridOverlayRunReport(self.run(args, check=check))

    def explain(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only explain envelope for an operation summary."""

        return self.run(["explain", operation], check=check)

    def estimate(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only estimate envelope for an operation summary."""

        return self.run(["estimate", operation], check=check)

    def execution_certificate_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only execution certificate planning envelope."""

        return self.run(["execution-certificate-plan"], check=check)

    def native_io_envelope_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only Native I/O envelope planning envelope."""

        return self.run(["native-io-envelope-plan"], check=check)

    def vortex_run(
        self,
        dataset_uri: str | os.PathLike[str],
        primitive: str,
        *,
        memory_gb: int = 4,
        max_parallelism: int = 1,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit `vortex-run` CLI command and parse its envelope."""

        return self.run(
            [
                "vortex-run",
                str(dataset_uri),
                primitive,
                str(memory_gb),
                str(max_parallelism),
            ],
            check=check,
        )

    def vortex_count(
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        execute_local_encoded_count: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-count` with optional explicit local encoded execution."""

        args = ["vortex-count", str(dataset_uri)]
        _append_resource_execution_args(
            args,
            option="--execute-local-encoded-count",
            enabled=execute_local_encoded_count,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return self.run(args, check=check)

    def vortex_count_where(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        *,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-count-where` with optional explicit local execution."""

        args = ["vortex-count-where", str(dataset_uri), predicate]
        _append_resource_execution_args(
            args,
            option="--execute-local-primitive",
            enabled=execute_local_primitive,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return self.run(args, check=check)

    def vortex_filter(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        *,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter` with optional explicit local execution."""

        args = ["vortex-filter", str(dataset_uri), predicate]
        _append_resource_execution_args(
            args,
            option="--execute-local-primitive",
            enabled=execute_local_primitive,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return self.run(args, check=check)

    def vortex_project(
        self,
        dataset_uri: str | os.PathLike[str],
        columns: str | Sequence[str],
        *,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-project` with optional explicit local execution."""

        args = ["vortex-project", str(dataset_uri), _columns_arg(columns)]
        _append_resource_execution_args(
            args,
            option="--execute-local-primitive",
            enabled=execute_local_primitive,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return self.run(args, check=check)

    def vortex_filter_project(
        self,
        dataset_uri: str | os.PathLike[str],
        predicate: str,
        columns: str | Sequence[str],
        *,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter-project` with optional explicit local execution."""

        args = [
            "vortex-filter-project",
            str(dataset_uri),
            predicate,
            _columns_arg(columns),
        ]
        _append_resource_execution_args(
            args,
            option="--execute-local-primitive",
            enabled=execute_local_primitive,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return self.run(args, check=check)

    def local_vortex_primitive_smoke(
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        predicate: str = "gte:value:3",
        columns: str | Sequence[str] = ("metric",),
        memory_gb: int = 1,
        max_parallelism: int = 2,
        check: bool = True,
    ) -> LocalVortexPrimitiveSmokeReport:
        """Run the certified local Vortex primitive workflow through explicit CLI flags."""

        memory_gb = _positive_int("memory_gb", memory_gb)
        max_parallelism = _positive_int("max_parallelism", max_parallelism)
        return LocalVortexPrimitiveSmokeReport(
            count=self.vortex_run(
                dataset_uri,
                "count",
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ),
            count_where=self.vortex_count_where(
                dataset_uri,
                predicate,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ),
            filter=self.vortex_filter(
                dataset_uri,
                predicate,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ),
            project=self.vortex_project(
                dataset_uri,
                columns,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ),
            filter_project=self.vortex_filter_project(
                dataset_uri,
                predicate,
                columns,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ),
        )

    def traditional_analytics_run(
        self,
        scenario: str,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        compatibility_output_format: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit traditional analytics universal-I/O smoke command."""

        args = [
            "traditional-analytics-run",
            scenario,
            str(fact_input),
            str(dim_input),
        ]
        if workspace is not None:
            args.extend(["--workspace", str(workspace)])
        if input_format is not None:
            args.extend(["--input-format", input_format])
        if compatibility_output_format is not None:
            args.extend(["--compat-output-format", compatibility_output_format])
        if memory_gb is not None:
            args.extend(["--memory-gb", str(memory_gb)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        return self.run(args, check=check)

    def traditional_analytics_vortex_run(
        self,
        scenario: str,
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit native Vortex traditional analytics smoke command."""

        return self.run(
            [
                "traditional-analytics-vortex-run",
                scenario,
                str(fact_vortex),
                str(dim_vortex),
            ],
            check=check,
        )

    def live_etl_smoke(
        self,
        scenario: str,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        input_format: str = "csv",
        workspace: str | os.PathLike[str] | None = None,
        compatibility_output_format: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the current live ETL smoke surface for CSV or native Vortex inputs.

        Compatibility-file modes import deterministic local inputs into
        temporary Vortex files, reopen them, and run the temporary benchmark
        operator. Vortex mode starts from existing `.vortex` inputs. All modes
        are explicit CLI invocations and preserve returned materialization and
        certificate fields.
        """

        normalized_format = input_format.lower().replace("_", "-")
        if normalized_format not in ETL_INPUT_FORMATS:
            raise ValueError(
                f"input_format must be one of {sorted(ETL_INPUT_FORMATS)}; "
                f"got {input_format!r}"
            )
        if normalized_format != "vortex":
            return self.traditional_analytics_run(
                scenario,
                fact_input,
                dim_input,
                workspace=workspace,
                input_format=normalized_format,
                compatibility_output_format=compatibility_output_format,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        if workspace is not None:
            raise ValueError("workspace is only supported for compatibility-file live ETL smoke runs")
        return self.traditional_analytics_vortex_run(
            scenario,
            fact_input,
            dim_input,
            check=check,
        )

    def live_etl_csv_to_vortex_replay(
        self,
        scenario: str,
        fact_csv: str | os.PathLike[str],
        dim_csv: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str],
        replay_native: bool = True,
        compatibility_output_format: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> LiveEtlReplayResult:
        """Run CSV universal I/O, then optionally replay from native Vortex artifacts.

        This helper keeps the two timing/behavior surfaces distinct: CSV import
        is the current universal-I/O boundary path, while native replay starts
        from the emitted `.vortex` files and reflects the current steady-state
        Vortex path more closely.
        """

        csv_import = self.traditional_analytics_run(
            scenario,
            fact_csv,
            dim_csv,
            workspace=workspace,
            input_format="csv",
            compatibility_output_format=compatibility_output_format,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )
        native_vortex = None
        if replay_native:
            native_vortex = self.traditional_analytics_vortex_run(
                scenario,
                _required_field(csv_import, "fact_vortex_path"),
                _required_field(csv_import, "dim_vortex_path"),
                check=check,
            )
        return LiveEtlReplayResult(csv_import=csv_import, native_vortex=native_vortex)

    def dynamic_work_shaping_plan(
        self, profile: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the advisory dynamic work-shaping plan for an optional profile."""

        args = ["dynamic-work-shaping-plan"]
        if profile is not None:
            args.append(profile)
        return self.run(args, check=check)

    def sizing_feedback_plan(
        self,
        memory_gb: int,
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return the advisory dynamic sizing feedback plan."""

        if isinstance(signals, str):
            signals_text = signals
        else:
            signals_text = ",".join(signals)
        return self.run(
            ["sizing-feedback-plan", str(memory_gb), signals_text],
            check=check,
        )

    def benchmark_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return the benchmark plan for the optional scope."""

        args = ["benchmark-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def benchmark_claim_evidence_plan(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return benchmark claim-evidence planning for the optional scope."""

        args = ["benchmark-claim-evidence-plan"]
        if scope is not None:
            args.append(scope)
        return self.run(args, check=check)

    def world_class_sufficiency_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the current CG-20 world-class sufficiency evidence envelope."""

        return self.run(["world-class-sufficiency-plan"], check=check)

    def translation_plan(
        self, target_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a target translation or compatibility-output planning envelope."""

        return self.run(["translation-plan", str(target_uri)], check=check)

    def plan_export(self, format_kind: str = "native", *, check: bool = True) -> OutputEnvelope:
        """Return a plan export portability envelope for the requested format."""

        return self.run(["plan-export", format_kind], check=check)

    def vortex_output_plan(
        self, target_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a Vortex output target preview envelope."""

        return self.run(["vortex-output-plan", str(target_uri)], check=check)

    def vortex_write_intent_plan(
        self,
        target_uri: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex write-intent readiness without writing output data."""

        return self.run(
            ["vortex-write-intent-plan", str(target_uri), _signals_arg(signals)],
            check=check,
        )

    def vortex_output_payload_plan(
        self,
        target_uri: str | os.PathLike[str],
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex output payload readiness without writing an artifact."""

        return self.run(
            [
                "vortex-output-payload-plan",
                str(target_uri),
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_staged_manifest_file_plan(
        self,
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return staged manifest file readiness without writing a manifest."""

        return self.run(
            [
                "vortex-staged-manifest-file-plan",
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_commit_marker_plan(
        self,
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return commit marker readiness without writing a marker."""

        return self.run(
            [
                "vortex-commit-marker-plan",
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_commit_intent_plan(
        self,
        target_uri: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex commit-intent readiness without committing manifests."""

        return self.run(
            ["vortex-commit-intent-plan", str(target_uri), _signals_arg(signals)],
            check=check,
        )

    def vortex_commit_protocol_plan(
        self,
        target_uri: str | os.PathLike[str],
        current_state: str,
        transition: str,
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return Vortex commit protocol state transition readiness."""

        return self.run(
            [
                "vortex-commit-protocol-plan",
                str(target_uri),
                current_state,
                transition,
                _signals_arg(signals),
            ],
            check=check,
        )

    def vortex_local_commit_recovery_plan(
        self,
        target_uri: str | os.PathLike[str],
        workspace_path: str | os.PathLike[str],
        signals: str | Sequence[str],
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return local commit recovery readiness without cleanup or rollback effects."""

        return self.run(
            [
                "vortex-local-commit-recovery-plan",
                str(target_uri),
                str(workspace_path),
                _signals_arg(signals),
            ],
            check=check,
        )

    def table_compat_plan(
        self,
        format_or_mode: str | None = None,
        scenario: str | None = None,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return table-format compatibility planning for a format or table mode."""

        args = ["table-compat-plan"]
        if format_or_mode is not None:
            args.append(format_or_mode)
        if scenario is not None:
            args.append(scenario)
        return self.run(args, check=check)

    def table_intelligence_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return table-intelligence report-only readiness."""

        return self.run(["table-intelligence-plan"], check=check)

    def layout_health_plan(
        self, scenario: str = "healthy", *, check: bool = True
    ) -> OutputEnvelope:
        """Return layout-health planning for the requested fixture scenario."""

        return self.run(["layout-health-plan", scenario], check=check)

    def compaction_plan(
        self, scenario: str = "small-files", *, check: bool = True
    ) -> OutputEnvelope:
        """Return compaction planning for the requested fixture scenario."""

        return self.run(["compaction-plan", scenario], check=check)

    def catalog_metadata_gate(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CG-9 catalog metadata gate report."""

        return self.run(["cg9-catalog-metadata-gate"], check=check)

    def object_store_request_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store request planning without remote IO."""

        return self.run(["object-store-request-plan", scenario], check=check)

    def object_store_range_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store byte-range planning without remote IO."""

        return self.run(["object-store-range-plan", scenario], check=check)

    def object_store_coalesce_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store request coalescing planning without remote IO."""

        return self.run(["object-store-coalesce-plan", scenario], check=check)

    def object_store_schedule_plan(
        self, scenario: str = "s3-ranges", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store scheduling planning without remote IO."""

        return self.run(["object-store-schedule-plan", scenario], check=check)

    def object_store_checkpoint_retry_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store checkpoint/retry planning without remote IO."""

        return self.run(["object-store-checkpoint-retry-plan", scenario], check=check)

    def object_store_commit_plan(
        self, scenario: str = "ready", *, check: bool = True
    ) -> OutputEnvelope:
        """Return object-store commit planning without remote IO or writes."""

        return self.run(["object-store-commit-plan", scenario], check=check)

    def correctness_plan(self, *, check: bool = True) -> OutputEnvelope:
        """Return the correctness evidence planning surface."""

        return self.run(["correctness-plan"], check=check)

    def workflow_readiness_smoke(
        self,
        *,
        target_uri: str | os.PathLike[str] = DEFAULT_WORKFLOW_READINESS_TARGET_URI,
        workspace_path: str | os.PathLike[str] = DEFAULT_WORKFLOW_READINESS_WORKSPACE,
        compatibility_target_uri: str | os.PathLike[str] = (
            DEFAULT_WORKFLOW_READINESS_COMPATIBILITY_TARGET_URI
        ),
        remote_sources: Mapping[str, str | os.PathLike[str]] | None = None,
        check: bool = True,
    ) -> WorkflowReadinessSmokeReport:
        """Preview output, remote-data, and evidence readiness without side effects."""

        remote_items = _workflow_remote_source_items(remote_sources)
        output_commit = (
            WorkflowReadinessPlan(
                "vortex_output_target",
                self.vortex_output_plan(target_uri, check=check),
            ),
            WorkflowReadinessPlan(
                "compatibility_export_target",
                self.translation_plan(compatibility_target_uri, check=check),
            ),
            WorkflowReadinessPlan(
                "native_plan_export",
                self.plan_export("native", check=check),
            ),
            WorkflowReadinessPlan(
                "vortex_write_intent",
                self.vortex_write_intent_plan(
                    target_uri,
                    DEFAULT_VORTEX_WRITE_INTENT_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_output_payload",
                self.vortex_output_payload_plan(
                    target_uri,
                    workspace_path,
                    DEFAULT_VORTEX_OUTPUT_PAYLOAD_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_staged_manifest",
                self.vortex_staged_manifest_file_plan(
                    workspace_path,
                    DEFAULT_VORTEX_STAGED_MANIFEST_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_commit_marker",
                self.vortex_commit_marker_plan(
                    workspace_path,
                    DEFAULT_VORTEX_COMMIT_MARKER_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_commit_intent",
                self.vortex_commit_intent_plan(
                    target_uri,
                    DEFAULT_VORTEX_COMMIT_INTENT_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_commit_protocol",
                self.vortex_commit_protocol_plan(
                    target_uri,
                    "not-started",
                    "validate-intent",
                    DEFAULT_VORTEX_COMMIT_PROTOCOL_SIGNALS,
                    check=check,
                ),
            ),
            WorkflowReadinessPlan(
                "vortex_local_commit_recovery",
                self.vortex_local_commit_recovery_plan(
                    target_uri,
                    workspace_path,
                    DEFAULT_VORTEX_LOCAL_COMMIT_RECOVERY_SIGNALS,
                    check=check,
                ),
            ),
        )
        table_remote = (
            WorkflowReadinessPlan(
                "table_intelligence",
                self.table_intelligence_plan(check=check),
            ),
            WorkflowReadinessPlan(
                "table_compat_iceberg",
                self.table_compat_plan("iceberg", check=check),
            ),
            WorkflowReadinessPlan(
                "table_compat_delta",
                self.table_compat_plan("delta", check=check),
            ),
            WorkflowReadinessPlan(
                "layout_health",
                self.layout_health_plan("healthy", check=check),
            ),
            WorkflowReadinessPlan(
                "compaction",
                self.compaction_plan("small-files", check=check),
            ),
            WorkflowReadinessPlan(
                "catalog_metadata_gate",
                self.catalog_metadata_gate(check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_request",
                self.object_store_request_plan("ready", check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_range",
                self.object_store_range_plan("s3-ranges", check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_coalesce",
                self.object_store_coalesce_plan("s3-ranges", check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_schedule",
                self.object_store_schedule_plan("s3-ranges", check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_checkpoint_retry",
                self.object_store_checkpoint_retry_plan("ready", check=check),
            ),
            WorkflowReadinessPlan(
                "object_store_commit",
                self.object_store_commit_plan("ready", check=check),
            ),
            *(
                WorkflowReadinessPlan(
                    f"remote_input_{name}",
                    self.input_plan(uri, check=check),
                )
                for name, uri in remote_items
            ),
        )
        evidence = (
            WorkflowReadinessPlan(
                "migration_capabilities",
                self.capabilities("migration", check=check),
            ),
            WorkflowReadinessPlan(
                "correctness_plan",
                self.correctness_plan(check=check),
            ),
            WorkflowReadinessPlan(
                "benchmark_claim_evidence",
                self.benchmark_claim_evidence_plan("foundation", check=check),
            ),
            WorkflowReadinessPlan(
                "world_class_sufficiency",
                self.world_class_sufficiency_plan(check=check),
            ),
        )
        return WorkflowReadinessSmokeReport(
            output_commit=output_commit,
            table_remote=table_remote,
            evidence=evidence,
        )

    def input_adapters(self, *, check: bool = True) -> OutputEnvelope:
        """Return the universal input adapter registry snapshot."""

        return self.run(["input-adapters"], check=check)

    def input_plan(
        self, dataset_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a side-effect-free universal input plan for a dataset URI."""

        return self.run(["input-plan", str(dataset_uri)], check=check)

    def compatibility_source_smoke(
        self,
        sources: Mapping[str, str | os.PathLike[str]] | None = None,
        *,
        check: bool = True,
    ) -> CompatibilitySourceSmokeReport:
        """Plan compatibility file sources without reading, writing, or materializing data."""

        source_items = _compatibility_source_items(sources)
        return CompatibilitySourceSmokeReport(
            input_adapters=self.input_adapters(check=check),
            native_io_envelope=self.native_io_envelope_plan(check=check),
            sources=tuple(
                CompatibilitySourcePlan(
                    source_name=name,
                    dataset_uri=str(uri),
                    plan=self.input_plan(uri, check=check),
                )
                for name, uri in source_items
            ),
        )

    def vortex_input_plan(
        self, dataset_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a side-effect-free native Vortex input planning envelope."""

        return self.run(["vortex-input-plan", str(dataset_uri)], check=check)

    def vortex_read_plan(
        self, dataset_uri: str | os.PathLike[str], *, check: bool = True
    ) -> OutputEnvelope:
        """Return a side-effect-free native Vortex read planning envelope."""

        return self.run(["vortex-read-plan", str(dataset_uri)], check=check)

    def smoke_check(self, *, check: bool = True) -> PythonClientSmokeReport:
        """Run no-dataset commands that verify the Python client can reach ShardLoom."""

        binary_command = tuple(self._binary_parts())
        return PythonClientSmokeReport(
            binary_command=binary_command,
            python_package_version=__version__,
            platform=platform.platform(),
            status=self.status(check=check),
            python_capabilities=self.capabilities("python", check=check),
            deployment_capabilities=self.capabilities("deployment", check=check),
            input_adapters=self.input_adapters(check=check),
        )

    def binary_command(self) -> tuple[str, ...]:
        """Resolve the CLI binary command without running a ShardLoom command."""

        return tuple(self._binary_parts())

    def run(self, args: Sequence[CommandPart], *, check: bool = True) -> OutputEnvelope:
        """Invoke a ShardLoom CLI command with JSON output enabled."""

        command = self._command(args)
        try:
            completed = subprocess.run(
                command,
                cwd=self._cwd,
                env=self._effective_env(),
                text=True,
                capture_output=True,
                timeout=self._timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise ShardLoomBinaryNotFoundError(
                "ShardLoom CLI binary was not found while running "
                f"{command[0]!r}. Install the ShardLoom CLI package, put "
                "`shardloom` on PATH, or set SHARDLOOM_BIN to a valid binary."
            ) from exc
        envelope = self._parse_stdout(completed.stdout, command)
        if check and (completed.returncode != 0 or envelope.is_error):
            raise ShardLoomCommandError(
                command=command,
                returncode=completed.returncode,
                envelope=envelope,
                stderr=completed.stderr,
            )
        return envelope

    def _command(self, args: Sequence[CommandPart]) -> list[str]:
        command = self._binary_parts()
        command.extend(str(arg) for arg in args)
        self._append_json_format(command)
        return command

    def _binary_parts(self) -> list[str]:
        binary = self._resolved_binary()
        if isinstance(binary, (str, os.PathLike)):
            return [str(binary)]
        if not binary:
            raise ValueError("ShardLoom binary command cannot be empty")
        return [str(part) for part in binary]

    def _resolved_binary(self) -> Binary:
        if self._binary is not None:
            return self._binary

        effective_env = self._effective_env()
        env_binary = effective_env.get(ENV_BINARY)
        if env_binary:
            return self._resolve_configured_binary(env_binary, effective_env)

        if self._repo_root is not None:
            candidate = self._repo_binary_candidate()
            if candidate is not None:
                return candidate

        path_binary = shutil.which("shardloom", path=effective_env.get("PATH"))
        if path_binary is not None:
            return path_binary

        raise ShardLoomBinaryNotFoundError(
            "ShardLoom CLI binary could not be resolved. Install the "
            "ShardLoom CLI package, put `shardloom` on PATH, set "
            "SHARDLOOM_BIN to a valid binary, or set SHARDLOOM_REPO_ROOT to a "
            "checkout with target/release or target/debug binaries."
        )

    def _effective_env(self) -> Mapping[str, str]:
        if self._env is None:
            return os.environ
        effective_env = dict(os.environ)
        effective_env.update(self._env)
        return effective_env

    def _resolve_configured_binary(self, value: str, env: Mapping[str, str]) -> str:
        configured = value.strip()
        if configured == "":
            raise ShardLoomBinaryNotFoundError(f"{ENV_BINARY} must not be empty")
        if _looks_like_path(configured):
            candidate = Path(configured).expanduser()
            if not candidate.is_absolute():
                candidate = (self._cwd or Path.cwd()) / candidate
            if candidate.is_file():
                return str(candidate)
            raise ShardLoomBinaryNotFoundError(
                f"{ENV_BINARY} points to {configured!r}, but that file does "
                "not exist. Set SHARDLOOM_BIN to the compiled ShardLoom CLI "
                "binary or remove it to use PATH discovery."
            )
        resolved = shutil.which(configured, path=env.get("PATH"))
        if resolved is not None:
            return resolved
        raise ShardLoomBinaryNotFoundError(
            f"{ENV_BINARY}={configured!r} was not found on PATH. Install the "
            "ShardLoom CLI package, put the binary on PATH, or set "
            "SHARDLOOM_BIN to an absolute binary path."
        )

    def _repo_binary_candidate(self) -> Path | None:
        suffixes = (".exe", "") if os.name == "nt" else ("",)
        for profile in self._profile_order:
            for suffix in suffixes:
                candidate = self._repo_root / "target" / profile / f"shardloom{suffix}"
                if candidate.is_file():
                    return candidate
        return None

    @staticmethod
    def _append_json_format(command: list[str]) -> None:
        if "--format" not in command:
            command.extend(["--format", "json"])
            return
        index = command.index("--format")
        try:
            value = command[index + 1]
        except IndexError as exc:
            raise ValueError("--format requires a value") from exc
        if value != "json":
            raise ValueError("ShardLoom Python client requires --format json")

    @staticmethod
    def _parse_stdout(stdout: str, command: Sequence[str]) -> OutputEnvelope:
        first_line = stdout.splitlines()[0] if stdout else ""
        if not first_line:
            raise ShardLoomProtocolError(
                f"ShardLoom command emitted no JSON output: {' '.join(command)}"
            )
        try:
            payload = json.loads(first_line)
        except json.JSONDecodeError as exc:
            raise ShardLoomProtocolError(
                f"ShardLoom command emitted invalid JSON: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise ShardLoomProtocolError("ShardLoom JSON output envelope must be an object")
        try:
            return OutputEnvelope.from_json(payload)
        except ValueError as exc:
            raise ShardLoomProtocolError(str(exc)) from exc


def _required_field(envelope: OutputEnvelope, key: str) -> str:
    value = envelope.field(key)
    if value is None or value == "":
        raise ShardLoomProtocolError(
            f"ShardLoom command {envelope.command!r} did not emit required field {key!r}"
        )
    return value


LOCAL_VORTEX_FALLBACK_ATTEMPTED_FIELDS = (
    "local_count_native_io_fallback_attempted",
    "execution_certificate_fallback_attempted",
    "filtered_count_local_execution_fallback_attempted",
    "filter_local_execution_fallback_attempted",
    "project_local_execution_fallback_attempted",
    "filter_project_local_execution_fallback_attempted",
    "local_primitive_native_io_fallback_attempted",
    "local_primitive_execution_certificate_fallback_attempted",
)

LOCAL_VORTEX_CERTIFIED_FIELDS = (
    "local_primitive_native_io_certified",
    "local_primitive_execution_certificate_correctness_passed",
)

NO_WRITE_FALSE_FIELDS = (
    "fallback_attempted",
    "fallback_execution_allowed",
    "filesystem_probe",
    "network_probe",
    "catalog_probe",
    "adapter_probe",
    "runtime_execution",
    "query_execution",
    "external_engine_execution",
    "external_effects_executed",
    "data_read",
    "data_materialized",
    "read_io",
    "write_io",
    "object_store_io",
    "output_data_written",
    "manifest_written",
    "manifest_file_written",
    "draft_file_written",
    "commit_marker_written",
    "manifest_finalized",
    "manifest_committed",
    "commit_performed",
    "upstream_vortex_write_called",
    "recovery_action_executed",
    "rollback_executed",
    "cleanup_performed",
    "write_execution_allowed",
    "commit_execution_allowed",
    "payload_write_allowed",
    "marker_write_allowed",
    "finalization_execution_allowed",
    "snapshot_manifest_metadata_read_allowed",
    "catalog_resolution_allowed",
    "table_metadata_read_allowed",
    "catalog_io_allowed",
    "object_store_io_allowed",
    "data_io_allowed",
    "write_io_allowed",
    "external_table_format_dependency_allowed",
    "credential_resolution_allowed",
    "metadata_cache_runtime_allowed",
    "metadata_integration_claim_allowed",
    "performance_claim_allowed",
    "superiority_claim_allowed",
    "best_default_claim_allowed",
)

REPORT_ONLY_EXECUTION_VALUES = frozenset(
    {
        "not_performed",
        "local_commit_recovery_planning_only",
        "marker_write_or_not_performed",
        "commit_marker_write_or_not_performed",
        "payload_write_or_not_performed",
        "native_count_payload_write_or_not_performed",
    }
)

BLOCKER_FIELDS = (
    "claim_blocked",
    "runtime_promotions_blocked",
    "metadata_integration_claim_allowed",
    "performance_claim_allowed",
    "superiority_claim_allowed",
    "best_default_claim_allowed",
)


def _all_bool_fields(envelope: OutputEnvelope, keys: Sequence[str]) -> bool:
    return all(envelope.field_bool(key, False) is True for key in keys)


def _any_bool_field(envelope: OutputEnvelope, keys: Sequence[str]) -> bool:
    return any(
        envelope.field_bool(key, False) is True
        for key in keys
        if envelope.field(key) is not None
    )


def _compatibility_source_items(
    sources: Mapping[str, str | os.PathLike[str]] | None,
) -> tuple[tuple[str, str | os.PathLike[str]], ...]:
    if sources is None:
        return DEFAULT_COMPATIBILITY_SOURCE_SMOKE_INPUTS
    if not sources:
        raise ValueError("sources must not be empty")
    items = tuple((str(name), uri) for name, uri in sources.items())
    if any(name.strip() == "" for name, _ in items):
        raise ValueError("source names must not be empty")
    return items


def _workflow_remote_source_items(
    sources: Mapping[str, str | os.PathLike[str]] | None,
) -> tuple[tuple[str, str | os.PathLike[str]], ...]:
    if sources is None:
        return DEFAULT_WORKFLOW_READINESS_REMOTE_SOURCES
    if not sources:
        raise ValueError("remote_sources must not be empty")
    items = tuple((str(name), uri) for name, uri in sources.items())
    if any(name.strip() == "" for name, _ in items):
        raise ValueError("remote source names must not be empty")
    return items


def _signals_arg(signals: str | Sequence[str]) -> str:
    if isinstance(signals, str):
        value = signals
    else:
        values = [str(signal).strip() for signal in signals]
        if not values:
            raise ValueError("signals must not be empty")
        value = ",".join(values)
    if value.strip() == "":
        raise ValueError("signals must not be empty")
    if any(part.strip() == "" for part in value.split(",")):
        raise ValueError("signals must not contain empty tokens")
    return value


def _envelope_preserves_no_write(envelope: OutputEnvelope) -> bool:
    if envelope.fallback.attempted or envelope.fallback.allowed:
        return False
    return all(
        envelope.field_bool(key, False) is False
        for key in NO_WRITE_FALSE_FIELDS
        if envelope.field(key) is not None
    )


def _envelope_is_report_only_or_planned(envelope: OutputEnvelope) -> bool:
    execution = envelope.field("execution")
    if execution is not None and execution not in REPORT_ONLY_EXECUTION_VALUES:
        return False
    plan_only = envelope.field("plan_only")
    if plan_only is not None and envelope.field_bool("plan_only") is not True:
        return False
    return True


def _envelope_reports_blocked_or_incomplete(envelope: OutputEnvelope) -> bool:
    if envelope.is_error or envelope.has_error_diagnostics:
        return True
    for key in BLOCKER_FIELDS:
        if envelope.field(key) is None:
            continue
        value = envelope.field_bool(key)
        if key.endswith("_allowed"):
            if value is False:
                return True
        elif value is True:
            return True
    status = envelope.field("claim_evidence_status") or envelope.field("claim_gate_status")
    return status in {"needs_evidence", "evidence_missing", "blocked"}


def _columns_arg(columns: str | Sequence[str]) -> str:
    if isinstance(columns, str):
        value = columns
    else:
        values = [str(column) for column in columns]
        if not values:
            raise ValueError("columns must not be empty")
        value = ",".join(values)
    if value.strip() == "":
        raise ValueError("columns must not be empty")
    return value


def _append_resource_execution_args(
    args: list[str],
    *,
    option: str,
    enabled: bool,
    memory_gb: int | None,
    max_parallelism: int | None,
) -> None:
    if enabled:
        if memory_gb is None or max_parallelism is None:
            raise ValueError(
                f"{option} requires both memory_gb and max_parallelism"
            )
        args.extend(
            [
                option,
                str(_positive_int("memory_gb", memory_gb)),
                str(_positive_int("max_parallelism", max_parallelism)),
            ]
        )
        return
    if memory_gb is not None or max_parallelism is not None:
        raise ValueError(
            "memory_gb and max_parallelism require explicit local execution"
        )


def _positive_int(name: str, value: int) -> int:
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _looks_like_path(value: str) -> bool:
    path = Path(value)
    separators = [os.sep]
    if os.altsep is not None:
        separators.append(os.altsep)
    return path.is_absolute() or any(separator in value for separator in separators)


def _profile_order_from_env(env: Mapping[str, str]) -> tuple[str, ...]:
    raw = env.get(ENV_PROFILE_ORDER)
    if raw is None or raw.strip() == "":
        return DEFAULT_PROFILE_ORDER
    values = tuple(part.strip() for part in raw.split(",") if part.strip())
    return values or DEFAULT_PROFILE_ORDER


def _timeout_from_env(env: Mapping[str, str]) -> float | None:
    raw = env.get(ENV_TIMEOUT_SECONDS)
    if raw is None or raw.strip() == "":
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{ENV_TIMEOUT_SECONDS} must be a number of seconds") from exc
