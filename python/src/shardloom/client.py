"""Subprocess client for ShardLoom's CLI JSON protocol."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

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
class PreparedVortexArtifacts:
    """Prepared local Vortex artifacts emitted by a compatibility ingest/stage run."""

    prepare: OutputEnvelope

    @property
    def fact_vortex_path(self) -> str:
        """Return the prepared fact-table Vortex artifact path."""

        return _required_field(self.prepare, "prepared_artifact_fact_ref")

    @property
    def dim_vortex_path(self) -> str:
        """Return the prepared dimension-table Vortex artifact path."""

        return _required_field(self.prepare, "prepared_artifact_dim_ref")

    @property
    def artifact_ref(self) -> str:
        """Return the combined prepared artifact ref."""

        return _required_field(self.prepare, "prepared_artifact_ref")

    @property
    def artifact_digest(self) -> str:
        """Return the combined prepared artifact digest summary."""

        return _required_field(self.prepare, "prepared_artifact_digest")

    @property
    def cleanup_policy(self) -> str:
        """Return the caller-visible cleanup policy for prepared artifacts."""

        return _required_field(self.prepare, "prepared_artifact_cleanup_policy")

    @property
    def reuse_eligible(self) -> bool:
        """Whether the prepared artifact pair is eligible for native/prepared replay."""

        return self.prepare.field_bool("prepared_artifact_reuse_eligible", False) is True

    def run_prepared(
        self,
        client: "ShardLoomClient",
        scenario: str,
        *,
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run a scenario from the prepared Vortex artifacts."""

        return client.traditional_analytics_vortex_run(
            scenario,
            self.fact_vortex_path,
            self.dim_vortex_path,
            cdc_delta_vortex=cdc_delta_vortex,
            execution_mode="prepared_vortex",
            check=check,
        )


@dataclass(frozen=True, slots=True)
class GeneratedSourceWriteReport:
    """Typed view over a scoped local generated-source write smoke."""

    envelope: OutputEnvelope

    @property
    def output_path(self) -> str:
        """Return the local output path written by the smoke command."""

        return _required_field(self.envelope, "output_path")

    @property
    def generated_source_kind(self) -> str:
        """Return the generated-source kind."""

        return _required_field(self.envelope, "generated_source_kind")

    @property
    def generated_source_row_count(self) -> int:
        """Return the generated-source row count."""

        return self.envelope.field_int("generated_source_row_count", 0) or 0

    @property
    def generated_source_certificate_status(self) -> str:
        """Return the generated-source certificate status."""

        return _required_field(self.envelope, "generated_source_certificate_status")

    @property
    def output_native_io_certificate_status(self) -> str:
        """Return the local output Native I/O certificate status."""

        return _required_field(self.envelope, "output_native_io_certificate_status")

    @property
    def fallback_attempted(self) -> bool:
        """Whether the smoke command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the smoke command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def claim_gate_status(self) -> str:
        """Return the generated-output claim gate status."""

        return _required_field(self.envelope, "claim_gate_status")

    @property
    def generated_source_range_start(self) -> int | None:
        """Return the generated range start when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_start")

    @property
    def generated_source_range_end(self) -> int | None:
        """Return the generated range exclusive end when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_end")

    @property
    def generated_source_range_step(self) -> int | None:
        """Return the generated range step when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_step")

    @property
    def generated_source_range_column(self) -> str | None:
        """Return the generated range column name when present."""

        return self.envelope.field("generated_source_range_column")


@dataclass(frozen=True, slots=True)
class SqlLocalSourceSmokeReport:
    """Typed view over the scoped local CSV SQL projection/filter/limit smoke."""

    envelope: OutputEnvelope

    @property
    def result_jsonl(self) -> str:
        """Return the bounded inline JSONL result emitted by ShardLoom."""

        return _required_field(self.envelope, "result_jsonl")

    @property
    def output_path(self) -> str | None:
        """Return the local output path when the smoke wrote one."""

        value = self.envelope.field("output_path")
        return value or None

    @property
    def output_row_count(self) -> int:
        """Return the number of result rows emitted by the smoke."""

        return self.envelope.field_int("output_row_count", 0) or 0

    @property
    def selected_row_count(self) -> int:
        """Return the number of source rows selected before limit."""

        return self.envelope.field_int("selected_row_count", 0) or 0

    @property
    def output_io_performed(self) -> bool:
        """Whether the smoke wrote a local output file."""

        return self.envelope.field_bool("output_io_performed", False) is True

    @property
    def output_native_io_certificate_status(self) -> str | None:
        """Return the output Native I/O certificate status, when present."""

        return self.envelope.field("output_native_io_certificate_status")

    @property
    def fallback_attempted(self) -> bool:
        """Whether the smoke command attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the smoke command invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def claim_gate_status(self) -> str:
        """Return the scoped SQL smoke claim gate status."""

        return _required_field(self.envelope, "claim_gate_status")


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

        return _envelope_external_engine_invoked(self.envelope)


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
    def streaming_capability_matrix_report_id(self) -> str | None:
        """Return the GAR-0013 streaming capability matrix report identifier."""

        return self.envelope.field("streaming_capability_matrix_report_id")

    @property
    def streaming_capability_rows(self) -> tuple[str, ...]:
        """Return streaming capability matrix row identifiers."""

        return _csv_values(self.envelope.field("streaming_capability_matrix_row_order"))

    @property
    def streaming_capability_blocked_row_count(self) -> int:
        """Return blocked rows in the streaming capability matrix."""

        return self.envelope.field_int("streaming_capability_matrix_blocked_row_count", 0) or 0

    @property
    def streaming_capability_diagnostic_codes(self) -> tuple[str, ...]:
        """Return deterministic diagnostics emitted for blocked streaming matrix rows."""

        return _csv_values(self.envelope.field("streaming_capability_matrix_diagnostic_code_order"))

    @property
    def streaming_capability_no_fallback_no_external_engine(self) -> bool:
        """Whether every streaming matrix row reports no fallback and no external engine."""

        return (
            self.envelope.field_bool(
                "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_schema_version(self) -> str | None:
        """Return the live/hybrid fabric gate schema version."""

        return self.envelope.field("live_hybrid_fabric_gate_schema_version")

    @property
    def live_hybrid_fabric_gate_report_id(self) -> str | None:
        """Return the live/hybrid fabric gate report identifier."""

        return self.envelope.field("live_hybrid_fabric_gate_report_id")

    @property
    def live_hybrid_fabric_gate_rows(self) -> tuple[str, ...]:
        """Return live/hybrid fabric gate row identifiers."""

        return _csv_values(self.envelope.field("live_hybrid_fabric_gate_row_order"))

    @property
    def live_hybrid_fabric_gate_blocked_row_count(self) -> int:
        """Return blocked rows in the live/hybrid fabric gate."""

        return self.envelope.field_int("live_hybrid_fabric_gate_blocked_row_count", 0) or 0

    @property
    def live_hybrid_fabric_gate_report_only_row_count(self) -> int:
        """Return report-only rows in the live/hybrid fabric gate."""

        return (
            self.envelope.field_int(
                "live_hybrid_fabric_gate_report_only_row_count",
                0,
            )
            or 0
        )

    @property
    def live_hybrid_fabric_gate_fixture_smoke_row_count(self) -> int:
        """Return fixture-smoke rows in the live/hybrid fabric gate."""

        return (
            self.envelope.field_int(
                "live_hybrid_fabric_gate_fixture_smoke_row_count",
                0,
            )
            or 0
        )

    @property
    def live_hybrid_fabric_gate_claim_gate_status(self) -> str | None:
        """Return the live/hybrid fabric gate claim-gate status."""

        return self.envelope.field("live_hybrid_fabric_gate_claim_gate_status")

    @property
    def live_hybrid_freshness_claim_allowed(self) -> bool:
        """Whether production live/hybrid freshness claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_freshness_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_exactly_once_claim_allowed(self) -> bool:
        """Whether exactly-once live/hybrid claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_exactly_once_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_production_live_claim_allowed(self) -> bool:
        """Whether production live-engine claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_production_live_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_production_hybrid_claim_allowed(self) -> bool:
        """Whether production hybrid-engine claims are allowed."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_production_hybrid_claim_allowed",
                False,
            )
            is True
        )

    @property
    def live_hybrid_object_store_runtime_supported(self) -> bool:
        """Whether object-store runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_object_store_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_broker_runtime_supported(self) -> bool:
        """Whether broker runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_broker_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_state_store_runtime_supported(self) -> bool:
        """Whether durable state-store runtime is supported by the live/hybrid gate."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_state_store_runtime_supported",
                False,
            )
            is True
        )

    @property
    def live_hybrid_baseline_oracle_only(self) -> bool:
        """Whether external systems are baselines/oracles only."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_baseline_oracle_only",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_fallback_attempted(self) -> bool:
        """Whether the live/hybrid fabric gate attempted fallback."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_fallback_attempted",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_external_engine_invoked(self) -> bool:
        """Whether the live/hybrid fabric gate invoked an external engine."""

        return (
            self.envelope.field_bool(
                "live_hybrid_fabric_gate_external_engine_invoked",
                False,
            )
            is True
        )

    @property
    def live_hybrid_fabric_gate_no_fallback_no_external_engine(self) -> bool:
        """Whether the live/hybrid fabric gate preserved no-fallback evidence."""

        return (
            not self.live_hybrid_fabric_gate_fallback_attempted
            and not self.live_hybrid_fabric_gate_external_engine_invoked
        )

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

        return _envelope_external_engine_invoked(self.envelope)


@dataclass(frozen=True, slots=True)
class WorkloadCertificationDossier:
    """Typed view over a workload-scoped certification dossier."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic dossier scenario."""

        return self.envelope.field("scenario")

    @property
    def workload_id(self) -> str:
        """Return the workload identifier represented by this dossier."""

        return _required_field(self.envelope, "workload_id")

    @property
    def overall_status(self) -> str:
        """Return the cross-CG dossier status."""

        return _required_field(self.envelope, "overall_status")

    @property
    def certificate_refs(self) -> tuple[str, ...]:
        """Return available certificate references."""

        return _csv_values(self.envelope.field("certificate_refs"))

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        """Return stable blocker IDs preventing certification claims."""

        return _csv_values(self.envelope.field("blocker_ids"))

    @property
    def missing_evidence(self) -> tuple[str, ...]:
        """Return evidence surfaces not present for the workload."""

        return _csv_values(self.envelope.field("missing_evidence"))

    @property
    def blocked_evidence(self) -> tuple[str, ...]:
        """Return blocked evidence entries."""

        return _csv_values(self.envelope.field("blocked_evidence"))

    @property
    def unsupported_evidence(self) -> tuple[str, ...]:
        """Return unsupported evidence entries."""

        return _csv_values(self.envelope.field("unsupported_evidence"))

    @property
    def suggested_next_action(self) -> str | None:
        """Return the next action suggested by the dossier."""

        return self.envelope.field("suggested_next_action")

    @property
    def no_runtime(self) -> bool:
        """Whether the dossier avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the dossier declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the dossier performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class ClaimGateCloseoutReport:
    """Typed view over the P7 claim-gate and release-readiness closeout report."""

    envelope: OutputEnvelope

    @property
    def claim_gate_status(self) -> str:
        """Return the broad claim gate status."""

        return _required_field(self.envelope, "claim_gate_status")

    @property
    def release_readiness_status(self) -> str:
        """Return the release-readiness status."""

        return _required_field(self.envelope, "release_readiness_status")

    @property
    def p7_closeout_status(self) -> str:
        """Return the Priority 7 closeout status."""

        return _required_field(self.envelope, "p7_closeout_status")

    @property
    def allowed_claims(self) -> tuple[str, ...]:
        """Return report/local claims allowed by the closeout gate."""

        return _csv_values(self.envelope.field("allowed_claims"))

    @property
    def blocked_claims(self) -> tuple[str, ...]:
        """Return claims blocked by missing evidence."""

        return _csv_values(self.envelope.field("blocked_claims"))

    @property
    def out_of_scope_claims(self) -> tuple[str, ...]:
        """Return claims explicitly outside the current release scope."""

        return _csv_values(self.envelope.field("out_of_scope_claims"))

    @property
    def blocker_ids(self) -> tuple[str, ...]:
        """Return stable blocker IDs for blocked broad claims."""

        return _csv_values(self.envelope.field("blocker_ids"))

    @property
    def no_runtime(self) -> bool:
        """Whether the closeout report avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the closeout report declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the closeout report performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class EvidenceAwareOptimizerTraceReport:
    """Typed view over the report-only GAR-PERF-2B optimizer trace."""

    envelope: OutputEnvelope

    @property
    def optimizer_trace_id(self) -> str:
        """Return the stable optimizer trace identifier."""

        return _required_field(self.envelope, "optimizer_trace_id")

    @property
    def optimizer_registry_version(self) -> str:
        """Return the optimizer registry version."""

        return _required_field(self.envelope, "optimizer_registry_version")

    @property
    def optimizer_phase(self) -> str:
        """Return the optimizer phase represented by this report."""

        return _required_field(self.envelope, "optimizer_phase")

    @property
    def rule_order(self) -> tuple[str, ...]:
        """Return optimizer rule IDs in stable order."""

        return _csv_values(self.envelope.field("optimizer_rule_order"))

    @property
    def rule_status_vocabulary(self) -> tuple[str, ...]:
        """Return the stable rule status vocabulary."""

        return _csv_values(self.envelope.field("optimizer_rule_status_vocabulary"))

    @property
    def benchmark_trace_ref(self) -> str:
        """Return the benchmark trace reference for future timing rows."""

        return _required_field(self.envelope, "benchmark_optimizer_trace_ref")

    @property
    def claim_gate_status(self) -> str:
        """Return the optimizer trace claim-gate status."""

        return _required_field(self.envelope, "claim_gate_status")

    @property
    def no_runtime(self) -> bool:
        """Whether this report avoided runtime execution."""

        return self.envelope.field_bool("runtime_execution", True) is False

    @property
    def no_rewrite_applied(self) -> bool:
        """Whether this report applied no optimizer rewrites."""

        return (
            self.envelope.field_bool("optimizer_execution", True) is False
            and self.envelope.field_bool("plan_rewritten", True) is False
            and self.envelope.field("optimizer_rule_applied_count") == "0"
        )

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the trace preserves no fallback and no external engine execution."""

        return (
            self.envelope.field_bool("fallback_attempted", True) is False
            and self.envelope.field_bool("fallback_execution_allowed", True) is False
            and self.envelope.field_bool("external_engine_invoked", True) is False
            and (
                self.envelope.field_bool("all_no_fallback_no_external_engine", False)
                is True
            )
        )

    def rule_status(self, rule_id: str) -> str:
        """Return one optimizer rule status by ID."""

        normalized = rule_id.strip().lower().replace("-", "_")
        if normalized not in self.rule_order:
            raise KeyError(f"optimizer rule {rule_id!r} is not in the trace")
        return _required_field(self.envelope, f"optimizer_rule_{normalized}_status")

    def rule_applied(self, rule_id: str) -> bool:
        """Return whether one optimizer rule was applied."""

        normalized = rule_id.strip().lower().replace("-", "_")
        if normalized not in self.rule_order:
            raise KeyError(f"optimizer rule {rule_id!r} is not in the trace")
        return self.envelope.field_bool(f"optimizer_rule_{normalized}_applied", True) is True


@dataclass(frozen=True, slots=True)
class ComputeCapabilityRow:
    """One row in the report-only compute capability matrix."""

    row_id: str
    surface: str
    family: str
    support_status: str
    engine_mode: str
    execution_mode: str
    provider_kind: str
    semantic_profile: str
    materialization_decode_requirement: str
    memory_spill_requirement: str
    correctness_refs: tuple[str, ...]
    benchmark_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    vortex_native_claim_allowed: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class OperatorFamilyCoverageRow:
    """One operator-family coverage row in the compute matrix."""

    family_id: str
    support_status: str
    next_evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class NativeVortexAdmissionLane:
    """One scoped native Vortex provider-admission lane."""

    lane_id: str
    source_surface: str
    operator_surface: str
    sink_surface: str
    admission_status: str
    support_status: str
    execution_mode: str
    provider_kind: str
    provider_api_surface: tuple[str, ...]
    provider_crate: str
    provider_version: str
    feature_gate: str
    shardloom_admission_policy: str
    compute_row_ref: str
    benchmark_ref: str
    correctness_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    materialization_decode_refs: tuple[str, ...]
    policy_refs: tuple[str, ...]
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    residual_executor: str
    vortex_native_claim_allowed: bool
    fallback_attempted: bool
    external_engine_invoked: bool
    object_store_io: bool
    write_io: bool


@dataclass(frozen=True, slots=True)
class NativeUnsupportedCoverageRow:
    """One deterministic unsupported native coverage row in the compute matrix."""

    row_id: str
    category: str
    surface: str
    support_status: str
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    source_refs: tuple[str, ...]
    claim_gate_status: str
    execution_attempted: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class PredicateDtypeCoverageRow:
    """One predicate/DType/null/nested coverage row in the compute matrix."""

    row_id: str
    category: str
    family: str
    surface: str
    support_status: str
    runtime_surface: tuple[str, ...]
    statistics_required: tuple[str, ...]
    fixture_status: str
    correctness_refs: tuple[str, ...]
    benchmark_refs: tuple[str, ...]
    execution_certificate_refs: tuple[str, ...]
    native_io_refs: tuple[str, ...]
    materialization_decode_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    execution_attempted: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class MaterializationPolicyRow:
    """One shared materialization/decode policy row."""

    row_id: str
    operator_execution_class: str
    support_status: str
    data_decoded: bool
    data_materialized: bool
    stayed_encoded: bool
    materialization_boundary_required: bool
    materialization_boundary_emitted: bool
    materialized_temporary_path: bool
    encoded_native_claim_allowed: bool
    materialization_decode_refs: tuple[str, ...]
    policy_refs: tuple[str, ...]
    unsupported_diagnostic_code: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    runtime_execution: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class ComputeCapabilityMatrix:
    """Typed view over the report-only compute capability coverage matrix."""

    envelope: OutputEnvelope

    @property
    def matrix_status(self) -> str:
        """Return the matrix report status."""

        return _required_field(self.envelope, "matrix_status")

    @property
    def claim_grade_status(self) -> str:
        """Return whether the matrix allows claim-grade compute-engine claims."""

        return _required_field(self.envelope, "claim_grade_status")

    @property
    def rows(self) -> tuple[ComputeCapabilityRow, ...]:
        """Return matrix rows in declared order."""

        rows: list[ComputeCapabilityRow] = []
        for row_id in _csv_values(self.envelope.field("compute_row_order")):
            prefix = f"compute_row_{row_id}_"
            rows.append(
                ComputeCapabilityRow(
                    row_id=row_id,
                    surface=_required_field(self.envelope, f"{prefix}surface"),
                    family=_required_field(self.envelope, f"{prefix}family"),
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    engine_mode=_required_field(self.envelope, f"{prefix}engine_mode"),
                    execution_mode=_required_field(self.envelope, f"{prefix}execution_mode"),
                    provider_kind=_required_field(self.envelope, f"{prefix}provider_kind"),
                    semantic_profile=_required_field(self.envelope, f"{prefix}semantic_profile"),
                    materialization_decode_requirement=_required_field(
                        self.envelope,
                        f"{prefix}materialization_decode_requirement",
                    ),
                    memory_spill_requirement=_required_field(
                        self.envelope,
                        f"{prefix}memory_spill_requirement",
                    ),
                    correctness_refs=_csv_values(self.envelope.field(f"{prefix}correctness_refs")),
                    benchmark_refs=_csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                    execution_certificate_refs=_csv_values(
                        self.envelope.field(f"{prefix}execution_certificate_refs")
                    ),
                    native_io_refs=_csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                    unsupported_diagnostic_code=_required_field(
                        self.envelope,
                        f"{prefix}unsupported_diagnostic_code",
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    vortex_native_claim_allowed=self.envelope.field_bool(
                        f"{prefix}vortex_native_claim_allowed",
                        False,
                    )
                    is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_engine_invoked=self.envelope.field_bool(
                        f"{prefix}external_engine_invoked",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def operator_families(self) -> tuple[OperatorFamilyCoverageRow, ...]:
        """Return operator-family ladder rows in declared order."""

        rows: list[OperatorFamilyCoverageRow] = []
        for family_id in _csv_values(self.envelope.field("operator_family_order")):
            prefix = f"operator_family_{family_id}_"
            rows.append(
                OperatorFamilyCoverageRow(
                    family_id=family_id,
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    next_evidence=_csv_values(self.envelope.field(f"{prefix}next_evidence")),
                )
            )
        return tuple(rows)

    @property
    def native_vortex_admission_status(self) -> str:
        """Return the scoped native Vortex admission status."""

        return _required_field(self.envelope, "native_vortex_admission_status")

    @property
    def native_vortex_admission_lanes(self) -> tuple[NativeVortexAdmissionLane, ...]:
        """Return scoped native Vortex provider-admission lanes."""

        rows: list[NativeVortexAdmissionLane] = []
        for lane_id in _csv_values(self.envelope.field("native_vortex_admission_lane_order")):
            prefix = f"native_vortex_admission_lane_{lane_id}_"
            rows.append(
                NativeVortexAdmissionLane(
                    lane_id=lane_id,
                    source_surface=_required_field(self.envelope, f"{prefix}source_surface"),
                    operator_surface=_required_field(
                        self.envelope,
                        f"{prefix}operator_surface",
                    ),
                    sink_surface=_required_field(self.envelope, f"{prefix}sink_surface"),
                    admission_status=_required_field(
                        self.envelope,
                        f"{prefix}admission_status",
                    ),
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    execution_mode=_required_field(self.envelope, f"{prefix}execution_mode"),
                    provider_kind=_required_field(self.envelope, f"{prefix}provider_kind"),
                    provider_api_surface=_csv_values(
                        self.envelope.field(f"{prefix}provider_api_surface")
                    ),
                    provider_crate=_required_field(self.envelope, f"{prefix}provider_crate"),
                    provider_version=_required_field(self.envelope, f"{prefix}provider_version"),
                    feature_gate=_required_field(self.envelope, f"{prefix}feature_gate"),
                    shardloom_admission_policy=_required_field(
                        self.envelope,
                        f"{prefix}shardloom_admission_policy",
                    ),
                    compute_row_ref=_required_field(self.envelope, f"{prefix}compute_row_ref"),
                    benchmark_ref=_required_field(self.envelope, f"{prefix}benchmark_ref"),
                    correctness_refs=_csv_values(
                        self.envelope.field(f"{prefix}correctness_refs")
                    ),
                    execution_certificate_refs=_csv_values(
                        self.envelope.field(f"{prefix}execution_certificate_refs")
                    ),
                    native_io_refs=_csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                    materialization_decode_refs=_csv_values(
                        self.envelope.field(f"{prefix}materialization_decode_refs")
                    ),
                    policy_refs=_csv_values(self.envelope.field(f"{prefix}policy_refs")),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    claim_boundary=_required_field(self.envelope, f"{prefix}claim_boundary"),
                    residual_executor=_required_field(
                        self.envelope,
                        f"{prefix}residual_executor",
                    ),
                    vortex_native_claim_allowed=self.envelope.field_bool(
                        f"{prefix}vortex_native_claim_allowed",
                        False,
                    )
                    is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_engine_invoked=self.envelope.field_bool(
                        f"{prefix}external_engine_invoked",
                        True,
                    )
                    is True,
                    object_store_io=self.envelope.field_bool(
                        f"{prefix}object_store_io",
                        True,
                    )
                    is True,
                    write_io=self.envelope.field_bool(f"{prefix}write_io", True) is True,
                )
            )
        return tuple(rows)

    @property
    def native_unsupported_coverage_status(self) -> str:
        """Return native unsupported coverage status for the current matrix."""

        return _required_field(self.envelope, "native_unsupported_coverage_status")

    @property
    def native_unsupported_coverage_rows(self) -> tuple[NativeUnsupportedCoverageRow, ...]:
        """Return deterministic unsupported source/sink/operator/workload rows."""

        rows: list[NativeUnsupportedCoverageRow] = []
        for row_id in _csv_values(self.envelope.field("native_unsupported_coverage_row_order")):
            prefix = f"native_unsupported_coverage_row_{row_id}_"
            rows.append(
                NativeUnsupportedCoverageRow(
                    row_id=row_id,
                    category=_required_field(self.envelope, f"{prefix}category"),
                    surface=_required_field(self.envelope, f"{prefix}surface"),
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    unsupported_diagnostic_code=_required_field(
                        self.envelope,
                        f"{prefix}unsupported_diagnostic_code",
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    source_refs=_csv_values(self.envelope.field(f"{prefix}source_refs")),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    execution_attempted=self.envelope.field_bool(
                        f"{prefix}execution_attempted",
                        True,
                    )
                    is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_engine_invoked=self.envelope.field_bool(
                        f"{prefix}external_engine_invoked",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def native_unsupported_coverage_complete(self) -> bool:
        """Whether current source/sink/operator/workload coverage is explicit."""

        return (
            self.envelope.field_bool(
                "native_unsupported_coverage_current_matrix_complete",
                False,
            )
            is True
        )

    @property
    def predicate_dtype_coverage_rows(self) -> tuple[PredicateDtypeCoverageRow, ...]:
        """Return predicate, DType, null, nested, and statistics coverage rows."""

        rows: list[PredicateDtypeCoverageRow] = []
        for row_id in _csv_values(self.envelope.field("predicate_dtype_coverage_row_order")):
            prefix = f"predicate_dtype_coverage_row_{row_id}_"
            rows.append(
                PredicateDtypeCoverageRow(
                    row_id=row_id,
                    category=_required_field(self.envelope, f"{prefix}category"),
                    family=_required_field(self.envelope, f"{prefix}family"),
                    surface=_required_field(self.envelope, f"{prefix}surface"),
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    runtime_surface=_csv_values(self.envelope.field(f"{prefix}runtime_surface")),
                    statistics_required=_csv_values(
                        self.envelope.field(f"{prefix}statistics_required")
                    ),
                    fixture_status=_required_field(self.envelope, f"{prefix}fixture_status"),
                    correctness_refs=_csv_values(
                        self.envelope.field(f"{prefix}correctness_refs")
                    ),
                    benchmark_refs=_csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                    execution_certificate_refs=_csv_values(
                        self.envelope.field(f"{prefix}execution_certificate_refs")
                    ),
                    native_io_refs=_csv_values(self.envelope.field(f"{prefix}native_io_refs")),
                    materialization_decode_refs=_csv_values(
                        self.envelope.field(f"{prefix}materialization_decode_refs")
                    ),
                    unsupported_diagnostic_code=_required_field(
                        self.envelope,
                        f"{prefix}unsupported_diagnostic_code",
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    claim_boundary=_required_field(self.envelope, f"{prefix}claim_boundary"),
                    execution_attempted=self.envelope.field_bool(
                        f"{prefix}execution_attempted",
                        True,
                    )
                    is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_engine_invoked=self.envelope.field_bool(
                        f"{prefix}external_engine_invoked",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def predicate_dtype_coverage_complete(self) -> bool:
        """Whether current predicate/DType/null/nested coverage is explicit."""

        return (
            self.envelope.field_bool("predicate_dtype_coverage_current_matrix_complete", False)
            is True
        )

    @property
    def materialization_policy_report_ref(self) -> str:
        """Return the shared materialization/decode policy report ref."""

        return _required_field(self.envelope, "materialization_policy_report_ref")

    @property
    def materialization_policy_rows(self) -> tuple[MaterializationPolicyRow, ...]:
        """Return shared materialization/decode policy rows."""

        rows: list[MaterializationPolicyRow] = []
        for row_id in _csv_values(self.envelope.field("materialization_policy_row_order")):
            prefix = f"materialization_policy_row_{row_id}_"
            rows.append(
                MaterializationPolicyRow(
                    row_id=row_id,
                    operator_execution_class=_required_field(
                        self.envelope,
                        f"{prefix}operator_execution_class",
                    ),
                    support_status=_required_field(self.envelope, f"{prefix}support_status"),
                    data_decoded=self.envelope.field_bool(f"{prefix}data_decoded", True) is True,
                    data_materialized=self.envelope.field_bool(
                        f"{prefix}data_materialized",
                        True,
                    )
                    is True,
                    stayed_encoded=self.envelope.field_bool(
                        f"{prefix}stayed_encoded",
                        False,
                    )
                    is True,
                    materialization_boundary_required=self.envelope.field_bool(
                        f"{prefix}materialization_boundary_required",
                        False,
                    )
                    is True,
                    materialization_boundary_emitted=self.envelope.field_bool(
                        f"{prefix}materialization_boundary_emitted",
                        False,
                    )
                    is True,
                    materialized_temporary_path=self.envelope.field_bool(
                        f"{prefix}materialized_temporary_path",
                        False,
                    )
                    is True,
                    encoded_native_claim_allowed=self.envelope.field_bool(
                        f"{prefix}encoded_native_claim_allowed",
                        False,
                    )
                    is True,
                    materialization_decode_refs=_csv_values(
                        self.envelope.field(f"{prefix}materialization_decode_refs")
                    ),
                    policy_refs=_csv_values(self.envelope.field(f"{prefix}policy_refs")),
                    unsupported_diagnostic_code=_required_field(
                        self.envelope,
                        f"{prefix}unsupported_diagnostic_code",
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    claim_boundary=_required_field(self.envelope, f"{prefix}claim_boundary"),
                    runtime_execution=self.envelope.field_bool(
                        f"{prefix}runtime_execution",
                        True,
                    )
                    is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_engine_invoked=self.envelope.field_bool(
                        f"{prefix}external_engine_invoked",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def materialization_policy_all_rows_classified(self) -> bool:
        """Whether every materialization policy row has explicit decode/materialization flags."""

        return (
            self.envelope.field_bool("materialization_policy_all_rows_classified", False) is True
        )

    @property
    def no_runtime(self) -> bool:
        """Whether the matrix command avoided runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the matrix declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and self.envelope.field_bool("all_rows_fallback_attempted_false", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the matrix command performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class SemanticConformanceRow:
    """One semantic dimension row in the ShardLoomNative conformance suite."""

    row_id: str
    dimension: str
    operator_family: str
    fixture_status: str
    current_support: str
    assertion: str
    blocker_id: str
    required_future_evidence: tuple[str, ...]
    fixture_executed: bool
    passed: bool
    fallback_attempted: bool
    external_oracle_used: bool


@dataclass(frozen=True, slots=True)
class SemanticConformanceSuite:
    """Typed view over the P7.4 semantic conformance suite report."""

    envelope: OutputEnvelope

    @property
    def suite_status(self) -> str:
        """Return the semantic suite status."""

        return _required_field(self.envelope, "suite_status")

    @property
    def semantic_profile(self) -> str:
        """Return the named semantic profile under test."""

        return _required_field(self.envelope, "semantic_profile")

    @property
    def rows(self) -> tuple[SemanticConformanceRow, ...]:
        """Return semantic rows in declared order."""

        rows: list[SemanticConformanceRow] = []
        for row_id in _csv_values(self.envelope.field("row_order")):
            prefix = f"semantic_row_{row_id}_"
            rows.append(
                SemanticConformanceRow(
                    row_id=row_id,
                    dimension=_required_field(self.envelope, f"{prefix}dimension"),
                    operator_family=_required_field(
                        self.envelope,
                        f"{prefix}operator_family",
                    ),
                    fixture_status=_required_field(self.envelope, f"{prefix}fixture_status"),
                    current_support=_required_field(self.envelope, f"{prefix}current_support"),
                    assertion=_required_field(self.envelope, f"{prefix}assertion"),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    required_future_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_future_evidence")
                    ),
                    fixture_executed=self.envelope.field_bool(
                        f"{prefix}fixture_executed",
                        False,
                    )
                    is True,
                    passed=self.envelope.field_bool(f"{prefix}passed", False) is True,
                    fallback_attempted=self.envelope.field_bool(
                        f"{prefix}fallback_attempted",
                        True,
                    )
                    is True,
                    external_oracle_used=self.envelope.field_bool(
                        f"{prefix}external_oracle_used",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def executed_fixture_count(self) -> int:
        """Return the number of in-memory semantic fixtures executed."""

        return int(_required_field(self.envelope, "executed_fixture_count"))

    @property
    def passed_fixture_count(self) -> int:
        """Return the number of semantic fixtures that passed."""

        return int(_required_field(self.envelope, "passed_fixture_count"))

    @property
    def failed_fixture_count(self) -> int:
        """Return the number of semantic fixtures that failed."""

        return int(_required_field(self.envelope, "failed_fixture_count"))

    @property
    def no_runtime(self) -> bool:
        """Whether the suite avoided workload runtime execution."""

        return self.envelope.field_bool("no_runtime", False) is True

    @property
    def no_fallback(self) -> bool:
        """Whether the suite declares and preserves no fallback execution."""

        return (
            self.envelope.field_bool("no_fallback", False) is True
            and not self.envelope.fallback.attempted
            and not self.envelope.fallback.allowed
        )

    @property
    def no_effects(self) -> bool:
        """Whether the suite performed no external effects."""

        return self.envelope.field_bool("no_effects", False) is True


@dataclass(frozen=True, slots=True)
class ExecutionEvidenceSlot:
    """One evidence-slot status from a typed execution-result envelope."""

    kind: str
    status: str
    refs: tuple[str, ...]
    detail: str


@dataclass(frozen=True, slots=True)
class ExecutionResultEnvelopeView:
    """Typed view over an artifact-rich top-level execution result envelope."""

    envelope: OutputEnvelope

    @property
    def plan_id(self) -> str:
        """Return the top-level execution plan id."""

        return _required_field(self.envelope, "plan_id")

    @property
    def plan_kind(self) -> str:
        """Return the top-level execution plan kind."""

        return _required_field(self.envelope, "plan_kind")

    @property
    def execution_status(self) -> str:
        """Return the provider-neutral execution status."""

        return _required_field(self.envelope, "execution_status")

    @property
    def provider_api_surface(self) -> str | None:
        """Return the provider API surface when present."""

        value = self.envelope.field("provider_api_surface")
        return None if value in {None, "none"} else value

    @property
    def provider_version(self) -> str | None:
        """Return the provider version when present."""

        value = self.envelope.field("provider_version")
        return None if value in {None, "none"} else value

    @property
    def evidence_completeness_status(self) -> str:
        """Return the overall evidence completeness status."""

        return _required_field(self.envelope, "evidence_completeness_status")

    @property
    def result_refs(self) -> tuple[str, ...]:
        """Return result refs preserved in the execution envelope."""

        return _csv_values(self.envelope.field("result_refs"))

    @property
    def artifact_refs(self) -> tuple[str, ...]:
        """Return provider artifact refs preserved in the execution envelope."""

        return _csv_values(self.envelope.field("artifact_refs"))

    @property
    def inline_artifact_ids(self) -> tuple[str, ...]:
        """Return inline artifact ids preserved in the execution envelope."""

        return _csv_values(self.envelope.field("inline_artifact_ids"))

    @property
    def execution_certificate_refs(self) -> tuple[str, ...]:
        """Return execution certificate refs preserved in the envelope."""

        return _csv_values(self.envelope.field("execution_certificate_refs"))

    @property
    def native_io_certificate_refs(self) -> tuple[str, ...]:
        """Return Native I/O certificate refs preserved in the envelope."""

        return _csv_values(self.envelope.field("native_io_certificate_refs"))

    @property
    def representation_transitions(self) -> tuple[str, ...]:
        """Return representation-transition labels preserved in the envelope."""

        return _csv_values(self.envelope.field("representation_transitions"))

    @property
    def execution_mode_selection_fields(self) -> Mapping[str, str]:
        """Return the typed execution-mode selection report fields when present."""

        artifact = next(
            (
                artifact
                for artifact in self.envelope.artifacts
                if artifact.get("artifact_kind") == "execution_mode_selection_report"
            ),
            None,
        )
        if artifact is not None:
            fields = _artifact_payload_field_map(artifact)
            if fields:
                return fields
        keys = (
            "execution_mode_selection_schema_version",
            "requested_execution_mode",
            "selected_execution_mode",
            "execution_mode",
            "mode_selection_reason",
            "execution_mode_family",
            "source_format",
            "workload_constitution_id",
            "compatibility_import_included",
            "vortex_prepare_included",
            "vortex_write_reopen_included",
            "direct_transient_execution",
            "vortex_native_claim_allowed",
            "certification_requested",
            "result_sink_requested",
            "prepared_artifact_available",
            "native_vortex_provider_available",
            "mode_supported",
            "support_status",
            "unsupported_diagnostic_code",
            "blocker_id",
            "required_future_evidence",
            "claim_gate_status",
            "claim_gate_reason",
            "fallback_attempted",
            "external_engine_invoked",
        )
        return {
            key: value
            for key in keys
            if (value := self.envelope.field(key)) is not None
        }

    @property
    def compute_flow_evidence_fields(self) -> Mapping[str, str]:
        """Return typed compute-flow evidence fields when present."""

        artifact = next(
            (
                artifact
                for artifact in self.envelope.artifacts
                if artifact.get("artifact_kind") == "compute_flow_evidence"
            ),
            None,
        )
        if artifact is not None:
            fields = _artifact_payload_field_map(artifact)
            if fields:
                return fields
        return {}

    @property
    def facade_compatibility_matrix_fields(self) -> Mapping[str, str]:
        """Return typed GAR-0038 facade compatibility matrix fields when present."""

        artifact = next(
            (
                artifact
                for artifact in self.envelope.artifacts
                if artifact.get("artifact_kind") == "facade_compatibility_matrix"
            ),
            None,
        )
        if artifact is not None:
            fields = _artifact_payload_field_map(artifact)
            if fields:
                return fields
        keys = (
            "facade_compatibility_matrix_report_id",
            "facade_compatibility_matrix_gar_id",
            "facade_compatibility_matrix_support_status",
            "facade_compatibility_matrix_claim_gate_status",
            "facade_compatibility_matrix_row_order",
            "facade_executable_surface_count",
            "facade_report_only_surface_count",
            "facade_unsupported_surface_count",
            "facade_legacy_boundary_count",
            "facade_prohibited_surface_count",
            "facade_legacy_boundary_status",
            "facade_all_rows_no_fallback_no_external_engine",
        )
        return {
            key: value
            for key in keys
            if (value := self.envelope.field(key)) is not None
        }

    def _execution_mode_field(self, key: str) -> str | None:
        return self.execution_mode_selection_fields.get(key) or self.envelope.field(key)

    def _execution_mode_bool(self, key: str, default: bool = False) -> bool:
        value = self.execution_mode_selection_fields.get(key)
        if value is not None:
            if value.strip().lower() == "evidence_incomplete":
                return default
            return _string_bool_value(value, key)
        return self.envelope.field_bool(key, default) is True

    def _compute_flow_field(self, key: str) -> str | None:
        return self.compute_flow_evidence_fields.get(key) or self.envelope.field(key)

    def _compute_flow_bool(self, key: str, default: bool = False) -> bool:
        value = self.compute_flow_evidence_fields.get(key)
        if value is not None and value != "evidence_incomplete":
            return _string_bool_value(value, key)
        return self.envelope.field_bool(key, default) is True

    def _facade_matrix_field(self, key: str) -> str | None:
        fields = self.facade_compatibility_matrix_fields
        return (
            fields.get(key)
            or fields.get(f"facade_compatibility_matrix_{key}")
            or fields.get(f"facade_{key}")
            or self.envelope.field(f"facade_compatibility_matrix_{key}")
            or self.envelope.field(f"facade_{key}")
            or self.envelope.field(key)
        )

    def _facade_matrix_bool(self, key: str, default: bool = False) -> bool:
        value = self._facade_matrix_field(key)
        if value is None:
            return default
        return _string_bool_value(value, key)

    def _facade_matrix_int(self, key: str) -> int | None:
        value = self._facade_matrix_field(key)
        if value is None:
            return None
        return int(value)

    @property
    def requested_execution_mode(self) -> str | None:
        """Return the requested execution mode when the envelope reports one."""

        return self._execution_mode_field("requested_execution_mode")

    @property
    def selected_execution_mode(self) -> str | None:
        """Return the execution mode selected by ShardLoom."""

        return self._execution_mode_field("selected_execution_mode") or (
            self._execution_mode_field("execution_mode")
        )

    @property
    def mode_selection_reason(self) -> str | None:
        """Return the reported execution-mode selection reason."""

        return self._execution_mode_field("mode_selection_reason")

    @property
    def execution_mode_family(self) -> str | None:
        """Return the execution-mode family."""

        return self._execution_mode_field("execution_mode_family")

    @property
    def mode_supported(self) -> bool:
        """Whether the requested mode was admitted as supported."""

        return self._execution_mode_bool("mode_supported")

    @property
    def support_status(self) -> str | None:
        """Return the execution-mode admission support status."""

        return self._execution_mode_field("support_status")

    @property
    def claim_gate_status(self) -> str | None:
        """Return the workload/row claim-gate status."""

        return self._execution_mode_field("claim_gate_status")

    @property
    def claim_gate_reason(self) -> str | None:
        """Return the reason for the claim-gate status."""

        return self._execution_mode_field("claim_gate_reason")

    @property
    def unsupported_diagnostic_code(self) -> str | None:
        """Return the unsupported diagnostic for blocked mode admission."""

        return self._execution_mode_field("unsupported_diagnostic_code")

    @property
    def blocker_id(self) -> str | None:
        """Return the blocker identifier for unsupported mode admission."""

        return self._execution_mode_field("blocker_id")

    @property
    def required_future_evidence(self) -> str | None:
        """Return required future evidence for unsupported mode admission."""

        return self._execution_mode_field("required_future_evidence")

    @property
    def vortex_native_claim_allowed(self) -> bool:
        """Whether this envelope can satisfy the native Vortex claim gate."""

        return self._execution_mode_bool("vortex_native_claim_allowed")

    @property
    def compatibility_import_included(self) -> bool:
        """Whether compatibility import is included in the reported timing scope."""

        return self._execution_mode_bool("compatibility_import_included")

    @property
    def vortex_prepare_included(self) -> bool:
        """Whether Vortex preparation is included in the reported timing scope."""

        return self._execution_mode_bool("vortex_prepare_included")

    @property
    def vortex_write_reopen_included(self) -> bool:
        """Whether Vortex write/reopen is included in the reported timing scope."""

        return self._execution_mode_bool("vortex_write_reopen_included")

    @property
    def direct_transient_execution(self) -> bool:
        """Whether the execution used the direct transient compatibility mode."""

        return self._execution_mode_bool("direct_transient_execution")

    @property
    def result_sink_claim_gate_status(self) -> str | None:
        """Return result-sink-specific claim-gate status when present."""

        return self._compute_flow_field("result_sink_claim_gate_status")

    @property
    def result_sink_claim_gate_reason(self) -> str | None:
        """Return result-sink-specific claim-gate reason when present."""

        return self._compute_flow_field("result_sink_claim_gate_reason")

    @property
    def computed_result_sink_replay_verified(self) -> bool:
        """Whether a computed result sink was replay-verified."""

        return self._compute_flow_bool("computed_result_sink_replay_verified")

    @property
    def computed_result_sink_native_io_certificate_status(self) -> str | None:
        """Return the result-sink Native I/O certificate status when present."""

        return self._compute_flow_field("computed_result_sink_native_io_certificate_status")

    @property
    def operator_execution_class(self) -> str | None:
        """Return the prepared/native operator execution class when present."""

        return self._compute_flow_field("operator_execution_class")

    @property
    def operator_admission_status(self) -> str | None:
        """Return the prepared/native operator admission status when present."""

        return self._compute_flow_field("operator_admission_status")

    @property
    def operator_blocker_id(self) -> str | None:
        """Return the operator blocker id when prepared/native execution is not encoded-native."""

        return self._compute_flow_field("operator_blocker_id")

    @property
    def operator_blocker_reason(self) -> str | None:
        """Return the operator blocker reason when present."""

        return self._compute_flow_field("operator_blocker_reason")

    @property
    def operator_encoded_native_claim_allowed(self) -> bool:
        """Whether the row may claim encoded-native operator execution."""

        return self._compute_flow_bool("operator_encoded_native_claim_allowed")

    @property
    def operator_temporary_materialization_used(self) -> bool:
        """Whether prepared/native execution used a materialized temporary operator."""

        return self._compute_flow_bool("operator_temporary_materialization_used")

    @property
    def facade_compatibility_matrix_report_id(self) -> str | None:
        """Return the GAR-0038 facade compatibility matrix report id."""

        return self._facade_matrix_field("report_id")

    @property
    def facade_compatibility_matrix_gar_id(self) -> str | None:
        """Return the GAR id for the facade compatibility matrix."""

        return self._facade_matrix_field("gar_id")

    @property
    def facade_compatibility_matrix_support_status(self) -> str | None:
        """Return the facade matrix support status."""

        return self._facade_matrix_field("support_status")

    @property
    def facade_compatibility_matrix_claim_gate_status(self) -> str | None:
        """Return the facade matrix claim-gate status."""

        return self._facade_matrix_field("claim_gate_status")

    @property
    def facade_compatibility_matrix_row_order(self) -> tuple[str, ...]:
        """Return ordered surface names from the facade compatibility matrix."""

        return _csv_values(self._facade_matrix_field("row_order"))

    @property
    def facade_unsupported_surface_count(self) -> int | None:
        """Return the count of unsupported runtime surfaces in the facade matrix."""

        return self._facade_matrix_int("unsupported_surface_count")

    @property
    def facade_legacy_boundary_status(self) -> str | None:
        """Return the legacy facade boundary status."""

        return self._facade_matrix_field("legacy_boundary_status")

    @property
    def facade_all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether all facade matrix rows preserve no-fallback/no-external-engine policy."""

        return self._facade_matrix_bool("all_rows_no_fallback_no_external_engine")

    @property
    def evidence_slots(self) -> tuple[ExecutionEvidenceSlot, ...]:
        """Return explicit present/not-required/evidence-incomplete slot statuses."""

        artifact = next(
            (
                artifact
                for artifact in self.envelope.artifacts
                if artifact.get("artifact_kind") == "execution_evidence_slots"
            ),
            None,
        )
        if artifact is None:
            return ()
        fields = _artifact_payload_field_map(artifact)
        slots: list[ExecutionEvidenceSlot] = []
        for kind in _csv_values(fields.get("evidence_slot_order")):
            prefix = f"evidence_slot_{kind}_"
            slots.append(
                ExecutionEvidenceSlot(
                    kind=kind,
                    status=fields.get(f"{prefix}status", ""),
                    refs=_csv_values(fields.get(f"{prefix}refs")),
                    detail=fields.get(f"{prefix}detail", ""),
                )
            )
        return tuple(slots)

    @property
    def incomplete_evidence_slots(self) -> tuple[ExecutionEvidenceSlot, ...]:
        """Return evidence slots marked incomplete."""

        return tuple(
            slot for slot in self.evidence_slots if slot.status == "evidence_incomplete"
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether execution attempted fallback according to policy/envelope fields."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether execution invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)


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
    def execution_mode_vocabulary(self) -> tuple[str, ...]:
        """Return the shared execution-mode enum declared by the REST contract."""

        value = self.envelope.field("execution_mode_vocabulary", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def execution_mode_selection_schema_version(self) -> str | None:
        """Return the execution-mode selection report schema version."""

        return self.envelope.field("execution_mode_selection_schema_version")

    @property
    def execution_mode_selection_fields(self) -> tuple[str, ...]:
        """Return REST selection-report fields mirrored from CLI/Python."""

        value = self.envelope.field("execution_mode_selection_fields", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def rest_execution_mode_support_status(self) -> str | None:
        """Return the REST execution-mode support posture."""

        return self.envelope.field("rest_execution_mode_support_status")

    @property
    def unsupported_execution_mode_diagnostic_code(self) -> str | None:
        """Return the deterministic diagnostic for unsupported REST mode requests."""

        return self.envelope.field("unsupported_execution_mode_diagnostic_code")

    @property
    def rest_runtime_unsupported_schema_version(self) -> str | None:
        """Return the REST runtime unsupported gate schema version."""

        return self.envelope.field("rest_runtime_unsupported_schema_version")

    @property
    def rest_runtime_unsupported_report_id(self) -> str | None:
        """Return the REST runtime unsupported gate report identifier."""

        return self.envelope.field("rest_runtime_unsupported_report_id")

    @property
    def rest_runtime_unsupported_rows(self) -> tuple[str, ...]:
        """Return REST runtime unsupported gate row identifiers."""

        return _csv_values(self.envelope.field("rest_runtime_unsupported_row_order"))

    @property
    def rest_runtime_unsupported_blocked_row_count(self) -> int:
        """Return blocked rows in the REST runtime unsupported gate."""

        return self.envelope.field_int("rest_runtime_unsupported_blocked_row_count", 0) or 0

    @property
    def rest_runtime_unsupported_report_only_row_count(self) -> int:
        """Return report-only rows in the REST runtime unsupported gate."""

        return self.envelope.field_int("rest_runtime_unsupported_report_only_row_count", 0) or 0

    @property
    def rest_runtime_unsupported_diagnostic_codes(self) -> tuple[str, ...]:
        """Return diagnostics declared by the REST runtime unsupported gate."""

        return _csv_values(self.envelope.field("rest_runtime_unsupported_diagnostic_codes"))

    @property
    def rest_runtime_unsupported_claim_gate_status(self) -> str | None:
        """Return the REST runtime unsupported gate claim status."""

        return self.envelope.field("rest_runtime_unsupported_claim_gate_status")

    @property
    def rest_runtime_http_listener_supported(self) -> bool:
        """Whether the REST runtime gate supports an HTTP listener."""

        return self.envelope.field_bool("rest_runtime_http_listener_supported", False) is True

    @property
    def rest_runtime_remote_execution_supported(self) -> bool:
        """Whether the REST runtime gate supports remote execution."""

        return self.envelope.field_bool("rest_runtime_remote_execution_supported", False) is True

    @property
    def rest_runtime_flight_adbc_transport_supported(self) -> bool:
        """Whether the REST runtime gate supports Flight/ADBC transport."""

        return (
            self.envelope.field_bool(
                "rest_runtime_flight_adbc_transport_supported",
                False,
            )
            is True
        )

    @property
    def rest_runtime_external_broker_supported(self) -> bool:
        """Whether the REST runtime gate supports external brokers."""

        return self.envelope.field_bool("rest_runtime_external_broker_supported", False) is True

    @property
    def rest_runtime_dependency_expansion_allowed(self) -> bool:
        """Whether dependency-expanded server work is currently allowed."""

        return (
            self.envelope.field_bool(
                "rest_runtime_dependency_expansion_allowed",
                False,
            )
            is True
        )

    @property
    def rest_runtime_no_server_no_fallback_no_external_engine(self) -> bool:
        """Whether REST runtime posture preserves no-server/no-fallback boundaries."""

        return (
            self.envelope.field_bool("rest_runtime_server_started", True) is False
            and self.envelope.field_bool("rest_runtime_network_listener_opened", True) is False
            and self.envelope.field_bool("rest_runtime_external_engine_invoked", True) is False
            and self.envelope.field_bool("rest_runtime_fallback_attempted", True) is False
        )

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
class RestApiSecurityGovernance:
    """Typed view over the CG-23 security/governance/agent API contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic security/governance scenario."""

        return self.envelope.field("scenario")

    @property
    def governance_status(self) -> str | None:
        """Return the governance status."""

        return self.envelope.field("governance_status")

    @property
    def auth_postures(self) -> tuple[str, ...]:
        """Return declared auth posture entries."""

        value = self.envelope.field("auth_postures", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def api_scopes(self) -> tuple[str, ...]:
        """Return API scope posture entries."""

        value = self.envelope.field("api_scopes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def mcp_tools(self) -> tuple[str, ...]:
        """Return MCP tool posture entries."""

        value = self.envelope.field("mcp_tools", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def evidence_model_signals(self) -> tuple[str, ...]:
        """Return unified evidence-model signal names."""

        value = self.envelope.field("evidence_model_signals", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def credential_references_only(self) -> bool:
        """Whether credentials stay as references."""

        return self.envelope.field_bool("credential_references_only", False) is True

    @property
    def secrets_redacted(self) -> bool:
        """Whether secret material is redacted from output."""

        return self.envelope.field_bool("secrets_redacted", False) is True

    @property
    def raw_secret_values_present(self) -> bool:
        """Whether raw secret values were emitted."""

        return self.envelope.field_bool("raw_secret_values_present", False) is True

    @property
    def destructive_policy_required(self) -> bool:
        """Whether destructive operations require an explicit policy."""

        return self.envelope.field_bool("destructive_policy_required", False) is True

    @property
    def destructive_policy_present(self) -> bool:
        """Whether an explicit destructive-operation policy was present."""

        return self.envelope.field_bool("destructive_policy_present", False) is True

    @property
    def destructive_operations_allowed(self) -> bool:
        """Whether destructive operations are allowed."""

        return self.envelope.field_bool("destructive_operations_allowed", False) is True

    @property
    def mcp_dry_run_default(self) -> bool:
        """Whether MCP tools default to dry-run-safe behavior."""

        return self.envelope.field_bool("mcp_dry_run_default", False) is True

    @property
    def mcp_effectful_tools_allowed(self) -> bool:
        """Whether effectful MCP tools are allowed by default."""

        return self.envelope.field_bool("mcp_effectful_tools_allowed", False) is True

    @property
    def mcp_discovery_side_effect_free(self) -> bool:
        """Whether MCP discovery is side-effect-free."""

        return self.envelope.field_bool("mcp_discovery_side_effect_free", False) is True

    @property
    def opentelemetry_exporter_enabled(self) -> bool:
        """Whether OpenTelemetry export was enabled."""

        return self.envelope.field_bool("opentelemetry_exporter_enabled", False) is True

    @property
    def openlineage_facets_mapped(self) -> bool:
        """Whether OpenLineage facets are represented in the evidence model."""

        return self.envelope.field_bool("openlineage_facets_mapped", False) is True

    @property
    def problem_details_mapped(self) -> bool:
        """Whether problem-details errors are represented in the evidence model."""

        return self.envelope.field_bool("problem_details_mapped", False) is True

    @property
    def cloudevents_mapped(self) -> bool:
        """Whether CloudEvents are represented in the evidence model."""

        return self.envelope.field_bool("cloudevents_mapped", False) is True

    @property
    def certificate_refs_mapped(self) -> bool:
        """Whether certificate refs are represented in the evidence model."""

        return self.envelope.field_bool("certificate_refs_mapped", False) is True

    @property
    def credential_resolution(self) -> bool:
        """Whether credentials were resolved."""

        return self.envelope.field_bool("credential_resolution", False) is True

    @property
    def secret_resolution(self) -> bool:
        """Whether secret material was resolved."""

        return self.envelope.field_bool("secret_resolution", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether security/governance handling delegated execution."""

        return self.envelope.field_bool("execution_delegated", False) is True


@dataclass(frozen=True, slots=True)
class RestApiDataPlane:
    """Typed view over the CG-23 data-plane and standards boundary contract."""

    envelope: OutputEnvelope

    @property
    def scenario(self) -> str | None:
        """Return the deterministic data-plane scenario."""

        return self.envelope.field("scenario")

    @property
    def data_plane_status(self) -> str | None:
        """Return the data-plane status."""

        return self.envelope.field("data_plane_status")

    @property
    def transfer_modes(self) -> tuple[str, ...]:
        """Return result transfer mode entries."""

        value = self.envelope.field("transfer_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def standards_names(self) -> tuple[str, ...]:
        """Return classified standards names."""

        value = self.envelope.field("standards_names", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def preferred_large_payload_modes(self) -> tuple[str, ...]:
        """Return preferred large-payload result policies."""

        value = self.envelope.field("preferred_large_payload_modes", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def large_payload_threshold_bytes(self) -> int:
        """Return the large-payload threshold."""

        return self.envelope.field_int("large_payload_threshold_bytes", 0) or 0

    @property
    def rest_control_plane_sufficient_for_local_use(self) -> bool:
        """Whether REST remains sufficient for local use."""

        return (
            self.envelope.field_bool("rest_control_plane_sufficient_for_local_use", False)
            is True
        )

    @property
    def flight_adbc_required_for_basic_local_use(self) -> bool:
        """Whether Flight/ADBC is required for basic local use."""

        return (
            self.envelope.field_bool("flight_adbc_required_for_basic_local_use", False)
            is True
        )

    @property
    def flight_ticket_requested(self) -> bool:
        """Whether a Flight ticket was requested."""

        return self.envelope.field_bool("flight_ticket_requested", False) is True

    @property
    def flight_ticket_supported(self) -> bool:
        """Whether Flight ticket delivery is currently supported."""

        return self.envelope.field_bool("flight_ticket_supported", False) is True

    @property
    def adbc_endpoint_requested(self) -> bool:
        """Whether an ADBC endpoint was requested."""

        return self.envelope.field_bool("adbc_endpoint_requested", False) is True

    @property
    def adbc_endpoint_supported(self) -> bool:
        """Whether ADBC endpoint delivery is currently supported."""

        return self.envelope.field_bool("adbc_endpoint_supported", False) is True

    @property
    def decoded_columnar_boundary_declared(self) -> bool:
        """Whether decoded-columnar boundaries are explicitly declared."""

        return self.envelope.field_bool("decoded_columnar_boundary_declared", False) is True

    @property
    def materialization_declared(self) -> bool:
        """Whether transfer materialization is declared."""

        return self.envelope.field_bool("materialization_declared", False) is True

    @property
    def result_policy_declared(self) -> bool:
        """Whether result policy is declared."""

        return self.envelope.field_bool("result_policy_declared", False) is True

    @property
    def standards_matrix_count(self) -> int:
        """Return the standards matrix row count."""

        return self.envelope.field_int("standards_matrix_count", 0) or 0

    @property
    def flight_server_started(self) -> bool:
        """Whether a Flight server was started."""

        return self.envelope.field_bool("flight_server_started", False) is True

    @property
    def adbc_endpoint_opened(self) -> bool:
        """Whether an ADBC endpoint was opened."""

        return self.envelope.field_bool("adbc_endpoint_opened", False) is True

    @property
    def broker_io(self) -> bool:
        """Whether broker I/O was performed."""

        return self.envelope.field_bool("broker_io", False) is True

    @property
    def object_store_io(self) -> bool:
        """Whether object-store I/O was performed."""

        return self.envelope.field_bool("object_store_io", False) is True

    @property
    def catalog_probe(self) -> bool:
        """Whether catalog probing was performed."""

        return self.envelope.field_bool("catalog_probe", False) is True

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def execution_delegated(self) -> bool:
        """Whether data-plane handling delegated execution."""

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

        return _envelope_external_engine_invoked(self.envelope)

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

        return _envelope_external_engine_invoked(self.envelope)

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

    def rest_api_security_governance(
        self,
        scenario: str = "safe-local-default",
        *,
        check: bool = True,
    ) -> RestApiSecurityGovernance:
        """Return a CG-23 security/governance/agent contract envelope."""

        return RestApiSecurityGovernance(
            self.run(["rest-api-security-governance", scenario], check=check)
        )

    def rest_api_data_plane(
        self,
        scenario: str = "artifact-reference-default",
        *,
        check: bool = True,
    ) -> RestApiDataPlane:
        """Return a CG-23 data-plane/standards boundary contract envelope."""

        return RestApiDataPlane(self.run(["rest-api-data-plane", scenario], check=check))

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

    def workload_certification_dossier(
        self,
        scenario: str = "local-vortex-count",
        *,
        check: bool = True,
    ) -> WorkloadCertificationDossier:
        """Return a workload-scoped cross-CG certification dossier."""

        return WorkloadCertificationDossier(
            self.run(["workload-certification-dossier", scenario], check=check)
        )

    def claim_gate_closeout(self, *, check: bool = True) -> ClaimGateCloseoutReport:
        """Return the P7 claim-gate and release-readiness closeout report."""

        return ClaimGateCloseoutReport(self.run(["claim-gate-closeout"], check=check))

    def compute_capability_matrix(self, *, check: bool = True) -> ComputeCapabilityMatrix:
        """Return the P7.4 report-only compute capability coverage matrix."""

        return ComputeCapabilityMatrix(self.run(["compute-capability-matrix"], check=check))

    def semantic_conformance_suite(self, *, check: bool = True) -> SemanticConformanceSuite:
        """Return the P7.4 ShardLoomNative semantic conformance suite report."""

        return SemanticConformanceSuite(self.run(["semantic-conformance-suite"], check=check))

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

    def optimizer_plan(self, *, check: bool = True) -> EvidenceAwareOptimizerTraceReport:
        """Return the report-only evidence-aware optimizer trace."""

        return EvidenceAwareOptimizerTraceReport(
            self.run(["optimizer-plan"], check=check)
        )

    def estimate(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only estimate envelope for an operation summary."""

        return self.run(["estimate", operation], check=check)

    def workflow_unsupported_plan(
        self,
        operation: str,
        workflow_summary: str,
        target_ref: str | os.PathLike[str] | None = None,
        *,
        check: bool = False,
    ) -> OutputEnvelope:
        """Return a report-only unsupported workflow-operation envelope."""

        command = ["workflow-unsupported-plan", operation, workflow_summary]
        if target_ref is not None:
            command.append(str(target_ref))
        return self.run(command, check=check)

    def generated_source_user_rows_smoke(
        self,
        output_path: str | os.PathLike[str],
        schema_arg: str,
        rows_arg: str,
        *,
        source_kind: str = "user_rows",
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Run the scoped local user-row generated-output smoke command."""

        command: list[CommandPart] = [
            "generated-source-user-rows-smoke",
            str(output_path),
            schema_arg,
            rows_arg,
            "--source-kind",
            source_kind,
            "--output-format",
            output_format,
        ]
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def generated_source_range_smoke(
        self,
        output_path: str | os.PathLike[str],
        start: int,
        end: int,
        *,
        step: int = 1,
        column: str = "value",
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Run the scoped local engine-native range generated-output smoke command."""

        command: list[CommandPart] = [
            "generated-source-range-smoke",
            str(output_path),
            str(start),
            str(end),
            "--step",
            str(step),
            "--column",
            str(column),
            "--output-format",
            output_format,
        ]
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def generated_source_sql_smoke(
        self,
        output_path: str | os.PathLike[str],
        statement: str,
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Run the scoped local source-free SQL generated-output smoke command."""

        command: list[CommandPart] = [
            "generated-source-sql-smoke",
            str(output_path),
            statement,
            "--output-format",
            output_format,
        ]
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def sql_local_source_smoke(
        self,
        statement: str,
        *,
        output_path: str | os.PathLike[str] | None = None,
        output_format: str = "inline-jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport:
        """Run the scoped local CSV SQL projection/filter/limit smoke command."""

        command: list[CommandPart] = [
            "sql-local-source-smoke",
            statement,
            "--output-format",
            output_format,
        ]
        if output_path is not None:
            command.extend(["--output", str(output_path)])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return SqlLocalSourceSmokeReport(self.run(command, check=check))

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
        verify_native_replay: bool = False,
        write_result_vortex: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        execution_mode: str | None = None,
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
        if verify_native_replay:
            args.append("--verify-native-replay")
        if write_result_vortex:
            args.append("--write-result-vortex")
        if memory_gb is not None:
            args.extend(["--memory-gb", str(memory_gb)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        if execution_mode is not None:
            args.extend(["--execution-mode", execution_mode])
        return self.run(args, check=check)

    def traditional_analytics_vortex_run(
        self,
        scenario: str,
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        *,
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        execution_mode: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit native Vortex traditional analytics smoke command."""

        args = [
            "traditional-analytics-vortex-run",
            scenario,
            str(fact_vortex),
            str(dim_vortex),
        ]
        if cdc_delta_vortex is not None:
            args.extend(["--cdc-delta-vortex", str(cdc_delta_vortex)])
        if workspace is not None:
            args.extend(["--workspace", str(workspace)])
        if write_result_vortex:
            args.append("--write-result-vortex")
        if execution_mode is not None:
            args.extend(["--execution-mode", execution_mode])
        return self.run(args, check=check)

    def prepare_traditional_analytics_vortex_artifacts(
        self,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str],
        input_format: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PreparedVortexArtifacts:
        """Prepare reusable local Vortex artifacts through the certified ingest/stage path."""

        envelope = self.traditional_analytics_run(
            "csv/file ingest",
            fact_input,
            dim_input,
            workspace=workspace,
            input_format=input_format,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            execution_mode="compatibility_import_certified",
            check=check,
        )
        return PreparedVortexArtifacts(prepare=envelope)

    def live_etl_smoke(
        self,
        scenario: str,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        input_format: str = "csv",
        workspace: str | os.PathLike[str] | None = None,
        compatibility_output_format: str | None = None,
        verify_native_replay: bool = False,
        write_result_vortex: bool = False,
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
                verify_native_replay=verify_native_replay,
                write_result_vortex=write_result_vortex,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        if verify_native_replay:
            raise ValueError(
                "verify_native_replay is only supported for compatibility-file live ETL smoke runs"
            )
        if write_result_vortex and workspace is None:
            raise ValueError(
                "write_result_vortex for existing Vortex inputs requires workspace"
            )
        return self.traditional_analytics_vortex_run(
            scenario,
            fact_input,
            dim_input,
            workspace=workspace,
            write_result_vortex=write_result_vortex,
            execution_mode="native_vortex",
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
        write_result_vortex: bool = False,
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
            write_result_vortex=write_result_vortex,
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
                workspace=str(Path(workspace) / "native_replay_result_sink")
                if write_result_vortex
                else None,
                write_result_vortex=write_result_vortex,
                execution_mode="native_vortex",
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

    def object_store_runtime_gate(self, *, check: bool = True) -> OutputEnvelope:
        """Return the CG-10 object-store/distributed runtime promotion gate."""

        return self.run(["cg10-object-store-runtime-gate"], check=check)

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
        self,
        dataset_uri: str | os.PathLike[str],
        *,
        source_format: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a side-effect-free universal input plan for a dataset URI."""

        command: list[CommandPart] = ["input-plan", str(dataset_uri)]
        if source_format is not None:
            command.extend(["--source-format", str(source_format)])
        return self.run(command, check=check)

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


def _csv_values(value: str | None) -> tuple[str, ...]:
    if value is None or value == "" or value == "none":
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


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


def _envelope_external_engine_invoked(envelope: OutputEnvelope) -> bool:
    typed_policy = _typed_payload_field_bool(envelope.policy, "external_engine_invoked", None)
    if typed_policy is not None:
        return typed_policy is True
    return envelope.field_bool("external_engine_invoked", False) is True


def _artifact_payload_field_map(artifact: Mapping[str, Any]) -> dict[str, str]:
    payload = artifact.get("payload")
    if not isinstance(payload, Mapping):
        return {}
    return _typed_payload_field_map(payload)


def _typed_payload_field_map(payload: Mapping[str, Any]) -> dict[str, str]:
    fields = payload.get("fields")
    if not isinstance(fields, list):
        return {}
    out: dict[str, str] = {}
    for field in fields:
        if isinstance(field, Mapping):
            out[str(field.get("key", ""))] = str(field.get("value", ""))
    return out


def _typed_payload_field_bool(
    payload: Mapping[str, Any],
    key: str,
    default: bool | None = None,
) -> bool | None:
    value = _typed_payload_field_map(payload).get(key)
    if value is None:
        return default
    return _string_bool_value(value, key)


def _string_bool_value(value: str, key: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise ValueError(f"typed payload field {key!r} is not a boolean value: {value!r}")


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
