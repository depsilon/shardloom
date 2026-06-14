"""Subprocess client for ShardLoom's CLI JSON protocol."""

from __future__ import annotations

import csv
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence, Union

from ._compat import dataclass
from ._version import __version__
from .errors import (
    ShardLoomBinaryNotFoundError,
    ShardLoomCommandError,
    ShardLoomProtocolError,
)
from .models import ClaimSummary, Diagnostic, EvidenceSummary, OutputEnvelope

CommandPart = Union[str, os.PathLike[str]]
Binary = Union[CommandPart, Sequence[CommandPart]]
FanoutOutputs = Union[Mapping[str, CommandPart], Sequence[tuple[str, CommandPart]]]
DEFAULT_PROFILE_ORDER = ("release", "debug")
ETL_INPUT_FORMATS = frozenset(
    {"csv", "jsonl", "ndjson", "parquet", "arrow-ipc", "arrow_ipc", "avro", "orc", "vortex"}
)
DECIMAL_CAST_TYPED_DECIMAL_OUTPUT_BOUNDARY = (
    "jsonl_exact_decimal_string_csv_exact_decimal_text_"
    "parquet_arrow_avro_vortex_typed_decimal_orc_blocked"
)
COMPLEX_PROJECTION_TYPED_NESTED_OUTPUT_BOUNDARY = (
    "typed_nested_compatibility_sink_with_result_jsonl_evidence"
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
_URI_WITH_AUTHORITY_RE = re.compile(
    r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://(?P<body>[^\s'\"<>)\],;]+)"
)


def _split_field_list(value: str | None) -> tuple[str, ...]:
    """Split comma-separated evidence values while preserving parenthesized dtype args."""

    if not value or value == "not_applicable":
        return ()
    parts: list[str] = []
    start = 0
    depth = 0
    for index, char in enumerate(value):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            part = value[start:index]
            if part:
                parts.append(part)
            start = index + 1
    tail = value[start:]
    if tail:
        parts.append(tail)
    return tuple(parts)


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

        return _required_field_any(
            self.prepare,
            "prepared_artifact_fact_ref",
            "prepare_batch_fact_vortex_path",
        )

    @property
    def dim_vortex_path(self) -> str:
        """Return the prepared dimension-table Vortex artifact path."""

        return _required_field_any(
            self.prepare,
            "prepared_artifact_dim_ref",
            "prepare_batch_dim_vortex_path",
        )

    @property
    def artifact_ref(self) -> str:
        """Return the combined prepared artifact ref."""

        value = self.prepare.field("prepared_artifact_ref")
        if value:
            return value
        fact = self.prepare.field("prepare_batch_fact_vortex_path")
        dim = self.prepare.field("prepare_batch_dim_vortex_path")
        cdc = self.prepare.field("prepare_batch_cdc_delta_vortex_path")
        if fact and dim:
            refs = [f"fact={fact}", f"dim={dim}"]
            if cdc:
                refs.append(f"cdc={cdc}")
            return ",".join(refs)
        return _required_field(self.prepare, "prepared_artifact_ref")

    @property
    def artifact_digest(self) -> str:
        """Return the combined prepared artifact digest summary."""

        value = self.prepare.field("prepared_artifact_digest")
        if value:
            return value
        fact = self.prepare.field("prepare_batch_fact_vortex_digest")
        dim = self.prepare.field("prepare_batch_dim_vortex_digest")
        cdc = self.prepare.field("prepare_batch_cdc_delta_vortex_digest")
        if fact and dim:
            digests = [f"fact={fact}", f"dim={dim}"]
            if cdc:
                digests.append(f"cdc={cdc}")
            return ",".join(digests)
        return _required_field(self.prepare, "prepared_artifact_digest")

    @property
    def cdc_delta_vortex_path(self) -> str | None:
        """Return the prepared CDC delta Vortex artifact path, when emitted."""

        value = self.prepare.field("cdc_delta_vortex_path") or self.prepare.field(
            "prepare_batch_cdc_delta_vortex_path"
        )
        return value or None

    @property
    def prepared_state_id(self) -> str:
        """Return the prepared-state identifier from the ingest/stage report."""

        return _required_field_any(
            self.prepare,
            "prepared_state_id",
            "prepare_batch_prepared_state_id",
        )

    @property
    def prepared_state_digest(self) -> str:
        """Return the prepared-state digest from the ingest/stage report."""

        return _required_field_any(
            self.prepare,
            "prepared_state_digest",
            "prepare_batch_prepared_state_digest",
        )

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether this artifact handle came from a manifest-backed reuse hit."""

        explicit = self.prepare.field("prepared_state_reuse_hit")
        if explicit is not None:
            return self.prepare.field_bool("prepared_state_reuse_hit", False) is True
        return self.prepare.field_bool("prepare_batch_prepared_state_reuse_hit", False) is True

    @property
    def prepared_state_reuse_reason(self) -> str | None:
        """Return the prepared-state reuse or invalidation reason."""

        return self.prepare.field("prepared_state_reuse_reason") or self.prepare.field(
            "prepare_batch_prepared_state_reuse_reason"
        )

    @property
    def prepared_state_reuse_manifest_digest(self) -> str | None:
        """Return the reuse manifest digest when workspace reuse was evaluated."""

        return self.prepare.field("prepared_state_reuse_manifest_digest") or self.prepare.field(
            "prepare_batch_prepared_state_reuse_manifest_digest"
        )

    @property
    def prepared_state_invalidation_reason(self) -> str | None:
        """Return why a prepared-state reuse candidate was invalidated."""

        return self.prepare.field("invalidation_reason")

    @property
    def source_state_id(self) -> str:
        """Return the SourceState identifier from the ingest/stage report."""

        return _required_field_any(
            self.prepare,
            "source_state_id",
            "prepare_batch_source_state_id",
        )

    @property
    def source_state_digest(self) -> str:
        """Return the SourceState digest from the ingest/stage report."""

        return _required_field_any(
            self.prepare,
            "source_state_digest",
            "prepare_batch_source_state_digest",
        )

    @property
    def source_state_columnar_preserved(self) -> bool:
        """Whether preparation preserved columnar SourceState evidence."""

        return (
            self.prepare.field_bool("source_state_columnar_preserved", False) is True
            or self.prepare.field_bool(
                "prepare_batch_source_state_columnar_preserved", False
            )
            is True
        )

    @property
    def source_state_record_batch_count(self) -> int:
        """Return the number of preserved SourceState record batches."""

        return (
            self.prepare.field_int("source_state_record_batch_count")
            or self.prepare.field_int("prepare_batch_source_state_record_batch_count")
            or 0
        )

    @property
    def vortex_array_build_provider_kind(self) -> str | None:
        """Return the Vortex array-build provider kind, when emitted."""

        return self.prepare.field(
            "vortex_array_build_provider_kind"
        ) or self.prepare.field("prepare_batch_vortex_array_build_provider_kind")

    @property
    def vortex_array_build_provider_surface(self) -> str | None:
        """Return the Vortex array-build provider API surface, when emitted."""

        return self.prepare.field(
            "vortex_array_build_provider_surface"
        ) or self.prepare.field("prepare_batch_vortex_array_build_provider_surface")

    @property
    def vortex_array_build_strategy(self) -> str | None:
        """Return the Vortex array-build strategy, when emitted."""

        return self.prepare.field("vortex_array_build_strategy") or self.prepare.field(
            "prepare_batch_vortex_array_build_strategy"
        )

    @property
    def vortex_array_build_input_layout(self) -> str | None:
        """Return the Vortex array-build input layout, when emitted."""

        return self.prepare.field(
            "vortex_array_build_input_layout"
        ) or self.prepare.field("prepare_batch_vortex_array_build_input_layout")

    @property
    def vortex_array_build_record_batch_count(self) -> int:
        """Return the number of record batches handed to the Vortex provider."""

        return (
            self.prepare.field_int("vortex_array_build_record_batch_count")
            or self.prepare.field_int("prepare_batch_vortex_array_build_record_batch_count")
            or 0
        )

    @property
    def vortex_array_build_manual_scalar_copy_avoided(self) -> bool:
        """Whether the Vortex array build avoided the manual scalar-copy path."""

        return (
            self.prepare.field_bool(
                "vortex_array_build_manual_scalar_copy_avoided", False
            )
            is True
            or self.prepare.field_bool(
                "prepare_batch_vortex_array_build_manual_scalar_copy_avoided", False
            )
            is True
        )

    @property
    def vortex_preparation_spine_status(self) -> str | None:
        """Return the local Vortex preparation-spine status, when emitted."""

        return self.prepare.field("vortex_preparation_spine_status") or self.prepare.field(
            "prepare_batch_vortex_preparation_spine_status"
        )

    @property
    def vortex_preparation_spine_vortex_first_decision(self) -> str | None:
        """Return the Vortex-first provider decision for preparation."""

        return self.prepare.field(
            "vortex_preparation_spine_vortex_first_decision"
        ) or self.prepare.field(
            "prepare_batch_vortex_preparation_spine_vortex_first_decision"
        )

    @property
    def vortex_preparation_spine_provider_kind(self) -> str | None:
        """Return the admitted preparation-spine provider kind."""

        return self.prepare.field(
            "vortex_preparation_spine_provider_kind"
        ) or self.prepare.field("prepare_batch_vortex_preparation_spine_provider_kind")

    @property
    def vortex_preparation_spine_provider_api_surface(self) -> str | None:
        """Return the provider API surface checked for preparation."""

        return self.prepare.field(
            "vortex_preparation_spine_provider_api_surface"
        ) or self.prepare.field(
            "prepare_batch_vortex_preparation_spine_provider_api_surface"
        )

    @property
    def vortex_preparation_spine_source_split_count(self) -> int:
        """Return the number of source splits recorded by the preparation spine."""

        return (
            self.prepare.field_int("vortex_preparation_spine_source_split_count")
            or self.prepare.field_int(
                "prepare_batch_vortex_preparation_spine_source_split_count"
            )
            or 0
        )

    @property
    def vortex_preparation_spine_source_split_refs(self) -> tuple[str, ...]:
        """Return source split refs recorded by the preparation spine."""

        value = self.prepare.field(
            "vortex_preparation_spine_source_split_refs"
        ) or self.prepare.field("prepare_batch_vortex_preparation_spine_source_split_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_native_io_certificate_status(self) -> str | None:
        """Return the Native I/O certificate posture for the preparation spine."""

        return self.prepare.field(
            "vortex_preparation_spine_native_io_certificate_status"
        ) or self.prepare.field(
            "prepare_batch_vortex_preparation_spine_native_io_certificate_status"
        )

    @property
    def cleanup_policy(self) -> str:
        """Return the caller-visible cleanup policy for prepared artifacts."""

        return (
            self.prepare.field("prepared_artifact_cleanup_policy")
            or self.prepare.field("prepare_batch_prepared_artifact_cleanup_policy")
            or "caller_owned_workspace_cleanup"
        )

    @property
    def reuse_eligible(self) -> bool:
        """Whether the prepared artifact pair is eligible for native/prepared replay."""

        return (
            self.prepare.field_bool("prepared_artifact_reuse_eligible", False) is True
            or self.prepare.field_bool(
                "prepare_batch_prepared_artifact_reuse_eligible", False
            )
            is True
            or self.prepare.field_bool("prepare_batch_prepared_state_reused", False)
            is True
        )

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

    def run_batch(
        self,
        client: "ShardLoomClient",
        scenarios: str | Sequence[str],
        *,
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run multiple scenarios from these prepared Vortex artifacts."""

        return client.traditional_analytics_vortex_batch_run(
            scenarios,
            self.fact_vortex_path,
            self.dim_vortex_path,
            cdc_delta_vortex=cdc_delta_vortex or self.cdc_delta_vortex_path,
            workspace=workspace,
            write_result_vortex=write_result_vortex,
            execution_mode="prepared_vortex",
            evidence_level=evidence_level,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class PreparedVortexBatchResult:
    """Prepare-once compatibility import plus prepared/native batch replay result."""

    prepare: OutputEnvelope
    batch: OutputEnvelope

    @property
    def artifacts(self) -> PreparedVortexArtifacts:
        """Return typed prepared-artifact accessors for the preparation step."""

        return PreparedVortexArtifacts(self.prepare)

    @property
    def scenario_order(self) -> tuple[str, ...]:
        """Return the batch scenario order reported by the runtime."""

        return _csv_values(self.batch.field("scenario_order"))

    @property
    def source_state_digest(self) -> str:
        """Return the batch SourceState digest."""

        return _required_field(self.batch, "source_state_digest")

    @property
    def source_state_reuse_status(self) -> str:
        """Return the batch SourceState reuse status."""

        return _required_field(self.batch, "source_state_reuse_status")

    @property
    def source_state_reused(self) -> bool:
        """Whether the batch reported SourceState reuse."""

        return self.batch.field_bool("source_state_reused", False) is True

    @property
    def source_state_recompute_avoided_count(self) -> int:
        """Return the reported recompute-avoided count for batch SourceState reuse."""

        return self.batch.field_int("source_state_recompute_avoided_count", 0) or 0

    @property
    def session_route_used(self) -> bool:
        """Whether this batch result used the caller-owned session route."""

        return self.batch.field_bool("session_route_used", False) is True

    @property
    def process_spawn_count(self) -> int | None:
        """Return the route-invocation process spawn count when reported."""

        return self.batch.field_int("process_spawn_count")

    @property
    def prepared_artifacts_reuse_eligible(self) -> bool:
        """Whether the preparation step marked artifacts as reuse eligible."""

        return self.artifacts.reuse_eligible

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether the batch route reused a manifest-backed prepared state."""

        explicit = self.batch.field("prepared_state_reuse_hit")
        if explicit is not None:
            return self.batch.field_bool("prepared_state_reuse_hit", False) is True
        return (
            self.batch.field_bool("prepare_batch_prepared_state_reuse_hit", False) is True
            or self.artifacts.prepared_state_reuse_hit
        )

    @property
    def prepared_state_reuse_reason(self) -> str | None:
        """Return the batch prepared-state reuse reason."""

        return (
            self.batch.field("prepared_state_reuse_reason")
            or self.batch.field("prepare_batch_prepared_state_reuse_reason")
            or self.artifacts.prepared_state_reuse_reason
        )

    @property
    def prepared_state_reuse_manifest_digest(self) -> str | None:
        """Return the workspace reuse manifest digest for the batch route."""

        return (
            self.batch.field("prepared_state_reuse_manifest_digest")
            or self.batch.field("prepare_batch_prepared_state_reuse_manifest_digest")
            or self.artifacts.prepared_state_reuse_manifest_digest
        )

    @property
    def prepared_state_invalidation_reason(self) -> str | None:
        """Return the invalidation reason when prepared-state reuse missed or blocked."""

        return (
            self.batch.field("invalidation_reason")
            or self.artifacts.prepared_state_invalidation_reason
        )

    @property
    def selected_evidence_level(self) -> str:
        """Return the selected runtime evidence level for the batch."""

        return _required_field(self.batch, "selected_evidence_level")

    @property
    def lifecycle_status(self) -> str:
        """Return the prepared/native Vortex lifecycle status for the combined route."""

        return _required_field(self.batch, "prepare_batch_lifecycle_status")

    @property
    def lifecycle_output_status(self) -> str:
        """Return the result-output posture for the combined lifecycle."""

        return _required_field(self.batch, "prepare_batch_lifecycle_output_status")

    @property
    def lifecycle_complete_with_output_replay(self) -> bool:
        """Whether the route prepared, scanned, wrote, and replay-verified output."""

        return (
            self.lifecycle_status
            == "prepared_vortex_lifecycle_complete_with_output_replay"
        )

    @property
    def lifecycle_no_standalone_lane(self) -> bool:
        """Whether lifecycle evidence stayed inside the prepare/batch route."""

        return (
            self.batch.field_bool("prepare_batch_lifecycle_no_standalone_lane", False)
            is True
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether either step reported attempted fallback execution."""

        return self.prepare.fallback.attempted or self.batch.fallback.attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether either step reported external engine invocation."""

        return _envelope_external_engine_invoked(self.prepare) or _envelope_external_engine_invoked(
            self.batch
        )


@dataclass(frozen=True, slots=True)
class VortexIngestSmokeReport:
    """Typed view over a scoped local `vortex_ingest` prepare-once smoke."""

    envelope: OutputEnvelope

    @property
    def source_path(self) -> str:
        """Return the local source path admitted by UniversalIngress."""

        return _required_field(self.envelope, "source_path")

    @property
    def target_vortex_path(self) -> str:
        """Return the local Vortex artifact path written by the smoke."""

        return _required_field(self.envelope, "target_vortex_path")

    @property
    def workspace_path_safety_status(self) -> str | None:
        """Return the enforced workspace path safety status for the Vortex output."""

        return self.envelope.field("vortex_ingest_output_workspace_path_safety_status")

    @property
    def output_commit_status(self) -> str | None:
        """Return the local output commit status for the Vortex artifact."""

        return self.envelope.field("vortex_ingest_output_commit_status")

    @property
    def source_format(self) -> str:
        """Return the local source format."""

        return _required_field(self.envelope, "source_format")

    @property
    def source_adapter_id(self) -> str | None:
        """Return the selected local input adapter identifier."""

        return self.envelope.field("source_adapter_id")

    @property
    def source_adapter_registry_entry_id(self) -> str | None:
        """Return the selected local input adapter registry row."""

        return self.envelope.field("source_adapter_registry_entry_id")

    @property
    def source_adapter_feature_gate(self) -> str | None:
        """Return the feature gate required by the selected source adapter."""

        return self.envelope.field("source_adapter_feature_gate")

    @property
    def source_adapter_boundary(self) -> str | None:
        """Return the read/ingest boundary used by the selected source adapter."""

        return self.envelope.field("source_adapter_boundary")

    @property
    def vortex_ingest_status(self) -> str:
        """Return the `vortex_ingest` route status."""

        return _required_field(self.envelope, "vortex_ingest_status")

    @property
    def prepared_state_id(self) -> str:
        """Return the prepared-state identifier."""

        return _required_field(self.envelope, "prepared_state_id")

    @property
    def prepared_state_digest(self) -> str:
        """Return the prepared-state digest."""

        return _required_field(self.envelope, "prepared_state_digest")

    @property
    def vortex_artifact_digest(self) -> str:
        """Return the local Vortex artifact digest."""

        return _required_field(self.envelope, "vortex_artifact_digest")

    @property
    def input_row_count(self) -> int:
        """Return the input row count written into the prepared artifact."""

        return self.envelope.field_int("input_row_count", 0) or 0

    @property
    def writer_row_count(self) -> int:
        """Return the upstream Vortex writer row count."""

        return self.envelope.field_int("writer_row_count", 0) or 0

    @property
    def reopen_row_count(self) -> int:
        """Return the row count observed after reopening/scanning the artifact."""

        return self.envelope.field_int("reopen_row_count", 0) or 0

    @property
    def reopen_verification_status(self) -> str | None:
        """Return the Vortex reopen verification status."""

        return self.envelope.field("reopen_verification_status")

    @property
    def certification_level(self) -> str | None:
        """Return the requested ingest certification depth."""

        return self.envelope.field("certification_level")

    @property
    def certification_status(self) -> str | None:
        """Return the certification status for the prepare-once route."""

        return self.envelope.field("certification_status")

    @property
    def source_state_materialization_layout(self) -> str | None:
        """Return the SourceState materialization layout."""

        return self.envelope.field("source_state_materialization_layout")

    @property
    def source_state_parse_normalization(self) -> str | None:
        """Return the SourceState parse/normalization path."""

        return self.envelope.field("source_state_parse_normalization")

    @property
    def source_state_columnar_preserved(self) -> bool:
        """Whether the prepare-once route preserved a columnar SourceState."""

        return self.envelope.field_bool("source_state_columnar_preserved", False) is True

    @property
    def source_state_record_batch_count(self) -> int:
        """Return the number of columnar record batches in SourceState."""

        return self.envelope.field_int("source_state_record_batch_count", 0) or 0

    @property
    def source_to_columnar_millis(self) -> int:
        """Return source-to-columnar preparation time in milliseconds."""

        return self.envelope.field_int("source_to_columnar_millis", 0) or 0

    @property
    def vortex_array_build_millis(self) -> int:
        """Return Vortex array-build time in milliseconds."""

        return self.envelope.field_int("vortex_array_build_millis", 0) or 0

    @property
    def vortex_array_build_provider_kind(self) -> str | None:
        """Return the Vortex array-build provider kind."""

        return self.envelope.field("vortex_array_build_provider_kind")

    @property
    def vortex_array_build_provider_surface(self) -> str | None:
        """Return the Vortex array-build provider API surface."""

        return self.envelope.field("vortex_array_build_provider_surface")

    @property
    def vortex_array_build_strategy(self) -> str | None:
        """Return the Vortex array-build strategy."""

        return self.envelope.field("vortex_array_build_strategy")

    @property
    def vortex_array_build_input_layout(self) -> str | None:
        """Return the Vortex array-build input layout."""

        return self.envelope.field("vortex_array_build_input_layout")

    @property
    def vortex_array_build_record_batch_count(self) -> int:
        """Return the number of record batches handed to the Vortex provider."""

        return self.envelope.field_int("vortex_array_build_record_batch_count", 0) or 0

    @property
    def vortex_array_build_manual_scalar_copy_avoided(self) -> bool:
        """Whether the Vortex array build avoided the manual scalar-copy path."""

        return (
            self.envelope.field_bool(
                "vortex_array_build_manual_scalar_copy_avoided", False
            )
            is True
        )

    @property
    def vortex_preparation_spine_status(self) -> str | None:
        """Return the local Vortex preparation-spine status."""

        return self.envelope.field("vortex_preparation_spine_status")

    @property
    def vortex_preparation_spine_vortex_first_decision(self) -> str | None:
        """Return the Vortex-first provider decision for preparation."""

        return self.envelope.field("vortex_preparation_spine_vortex_first_decision")

    @property
    def vortex_preparation_spine_provider_kind(self) -> str | None:
        """Return the admitted preparation-spine provider kind."""

        return self.envelope.field("vortex_preparation_spine_provider_kind")

    @property
    def vortex_preparation_spine_provider_api_surface(self) -> str | None:
        """Return the provider API surface checked for preparation."""

        return self.envelope.field("vortex_preparation_spine_provider_api_surface")

    @property
    def vortex_preparation_spine_source_split_count(self) -> int:
        """Return the number of source splits recorded by the preparation spine."""

        return self.envelope.field_int(
            "vortex_preparation_spine_source_split_count", 0
        ) or 0

    @property
    def vortex_preparation_spine_source_split_refs(self) -> tuple[str, ...]:
        """Return source split refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_split_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_source_byte_range_refs(self) -> tuple[str, ...]:
        """Return source byte-range refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_byte_range_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_source_row_range_refs(self) -> tuple[str, ...]:
        """Return source row-range refs recorded by the preparation spine."""

        value = self.envelope.field("vortex_preparation_spine_source_row_range_refs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_preparation_spine_native_io_certificate_status(self) -> str | None:
        """Return the Native I/O certificate posture for the preparation spine."""

        return self.envelope.field(
            "vortex_preparation_spine_native_io_certificate_status"
        )

    @property
    def vortex_scout_ingress_status(self) -> str | None:
        """Return scoped scout ingress and triage status."""

        return self.envelope.field("vortex_scout_ingress_status")

    @property
    def vortex_scout_ingress_anomaly_count(self) -> int:
        """Return the number of source anomalies reported by scout ingress."""

        return self.envelope.field_int("vortex_scout_ingress_anomaly_count", 0) or 0

    @property
    def vortex_scout_ingress_anomaly_families(self) -> tuple[str, ...]:
        """Return anomaly families reported by scout ingress."""

        value = self.envelope.field("vortex_scout_ingress_anomaly_families")
        return () if value in {None, "", "none"} else tuple(value.split(","))

    @property
    def vortex_scout_ingress_schema_drift_status(self) -> str | None:
        """Return schema-drift status reported by scout ingress."""

        return self.envelope.field("vortex_scout_ingress_schema_drift_status")

    @property
    def vortex_scout_ingress_unsupported_shape_status(self) -> str | None:
        """Return unsupported-shape status reported by scout ingress."""

        return self.envelope.field("vortex_scout_ingress_unsupported_shape_status")

    @property
    def vortex_scout_ingress_quarantine_required(self) -> bool:
        """Whether scout ingress requires quarantine planning."""

        return (
            self.envelope.field_bool(
                "vortex_scout_ingress_quarantine_required",
                False,
            )
            is True
        )

    @property
    def vortex_scout_ingress_quarantine_output_plan_status(self) -> str | None:
        """Return quarantine-output planning status."""

        return self.envelope.field("vortex_scout_ingress_quarantine_output_plan_status")

    @property
    def vortex_scout_ingress_unsupported_diagnostic_code(self) -> str | None:
        """Return the scout diagnostic code for blocked source admission."""

        return self.envelope.field("vortex_scout_ingress_unsupported_diagnostic_code")

    @property
    def vortex_scout_ingress_no_standalone_lane_status(self) -> str | None:
        """Return whether scout ingress stayed in the vortex_ingest route."""

        return self.envelope.field("vortex_scout_ingress_no_standalone_lane_status")

    @property
    def vortex_layout_write_advisor_status(self) -> str | None:
        """Return scoped layout/write advisor status."""

        return self.envelope.field("vortex_layout_write_advisor_status")

    @property
    def vortex_layout_write_advisor_strategy_admitted(self) -> bool:
        """Whether the layout/write advisor admitted the local strategy."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_strategy_admitted",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_runtime_decision_applied(self) -> bool:
        """Whether the admitted layout/write decision governed the writer path."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_runtime_decision_applied",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_selected_strategy(self) -> str | None:
        """Return the writer-validated layout/write strategy selection."""

        return self.envelope.field("vortex_layout_write_advisor_selected_strategy")

    @property
    def vortex_layout_write_advisor_strategy_decision_digest(self) -> str | None:
        """Return the stable digest for the writer-validated strategy decision."""

        return self.envelope.field(
            "vortex_layout_write_advisor_strategy_decision_digest"
        )

    @property
    def vortex_layout_write_advisor_provider_admitted(self) -> bool:
        """Whether the selected writer provider was admitted for the route."""

        return (
            self.envelope.field_bool(
                "vortex_layout_write_advisor_provider_admitted",
                False,
            )
            is True
        )

    @property
    def vortex_layout_write_advisor_blocker(self) -> str | None:
        """Return the layout/write runtime blocker, or ``none`` when applied."""

        return self.envelope.field("vortex_layout_write_advisor_blocker")

    @property
    def vortex_layout_write_advisor_layout_strategy(self) -> str | None:
        """Return the admitted or blocked layout strategy."""

        return self.envelope.field("vortex_layout_write_advisor_layout_strategy")

    @property
    def vortex_layout_write_advisor_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether layout/write advisor evidence stayed in vortex_ingest."""

        return self.envelope.field(
            "vortex_layout_write_advisor_no_standalone_lane_status"
        )

    @property
    def vortex_copy_budget_status(self) -> str | None:
        """Return scoped copy-budget and buffer-lifecycle status."""

        return self.envelope.field("vortex_copy_budget_status")

    @property
    def vortex_copy_budget_measurement_status(self) -> str | None:
        """Return copy-budget measurement completeness status."""

        return self.envelope.field("vortex_copy_budget_measurement_status")

    @property
    def vortex_copy_budget_buffer_reuse_status(self) -> str | None:
        """Return buffer-reuse admission or blocker status."""

        return self.envelope.field("vortex_copy_budget_buffer_reuse_status")

    @property
    def vortex_copy_budget_unsafe_lifetime_shortcut_status(self) -> str | None:
        """Return unsafe-lifetime shortcut policy status."""

        return self.envelope.field("vortex_copy_budget_unsafe_lifetime_shortcut_status")

    @property
    def vortex_copy_budget_no_standalone_lane_status(self) -> str | None:
        """Return whether copy-budget evidence stayed in vortex_ingest."""

        return self.envelope.field("vortex_copy_budget_no_standalone_lane_status")

    @property
    def vortex_capillary_preparation_status(self) -> str | None:
        """Return scoped capillary cold-preparation status."""

        return self.envelope.field("vortex_capillary_preparation_status")

    @property
    def vortex_capillary_preparation_activation_result(self) -> str | None:
        """Return whether capillary task planning was activated or skipped."""

        return self.envelope.field("vortex_capillary_preparation_activation_result")

    @property
    def vortex_capillary_preparation_activation_reason(self) -> str | None:
        """Return the deterministic capillary activation or skip reason."""

        return self.envelope.field("vortex_capillary_preparation_activation_reason")

    @property
    def vortex_capillary_preparation_activation_observed_split_count(self) -> int:
        """Return the split count observed by the capillary activation gate."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_activation_observed_split_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_task_count(self) -> int:
        """Return the number of cold-preparation capillary tasks."""

        return self.envelope.field_int("vortex_capillary_preparation_task_count", 0) or 0

    @property
    def vortex_capillary_preparation_task_roles(self) -> tuple[str, ...]:
        """Return the reported capillary task role sequence."""

        value = self.envelope.field("vortex_capillary_preparation_task_roles")
        return () if value in {None, "", "none"} else tuple(value.split(","))

    @property
    def vortex_capillary_preparation_execution_window_count(self) -> int:
        """Return the number of applied capillary execution windows."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_execution_window_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_execution_window_ids(self) -> tuple[str, ...]:
        """Return applied capillary execution-window IDs."""

        value = self.envelope.field("vortex_capillary_preparation_execution_window_ids")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_capillary_preparation_scheduler_applied(self) -> bool:
        """Whether capillary execution windows shaped the cold-preparation route."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_scheduler_applied", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_scheduler_application_reason(self) -> str | None:
        """Return the capillary scheduler admission or block reason."""

        return self.envelope.field(
            "vortex_capillary_preparation_scheduler_application_reason"
        )

    @property
    def vortex_capillary_preparation_prewrite_status(self) -> str | None:
        """Return whether capillary control gated the local pre-write route."""

        return self.envelope.field("vortex_capillary_preparation_prewrite_status")

    @property
    def vortex_capillary_preparation_prewrite_scheduler_applied(self) -> bool:
        """Whether the capillary scheduler was applied before local array build."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_scheduler_applied", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_prewrite_execution_window_count(self) -> int:
        """Return the number of pre-write capillary execution windows."""

        return (
            self.envelope.field_int(
                "vortex_capillary_preparation_prewrite_execution_window_count", 0
            )
            or 0
        )

    @property
    def vortex_capillary_preparation_prewrite_execution_window_ids(
        self,
    ) -> tuple[str, ...]:
        """Return pre-write capillary execution-window IDs."""

        value = self.envelope.field(
            "vortex_capillary_preparation_prewrite_execution_window_ids"
        )
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_capillary_preparation_prewrite_array_build_gate_status(
        self,
    ) -> str | None:
        """Return the pre-write capillary gate status for local array build."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_array_build_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_write_gate_status(self) -> str | None:
        """Return the pre-write capillary gate status for Vortex write."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_write_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_reopen_gate_status(self) -> str | None:
        """Return the pre-write capillary gate status for reopen verification."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_reopen_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_sink_evidence_gate_status(
        self,
    ) -> str | None:
        """Return the pre-write capillary gate status for sink evidence."""

        return self.envelope.field(
            "vortex_capillary_preparation_prewrite_sink_evidence_gate_status"
        )

    @property
    def vortex_capillary_preparation_prewrite_fallback_attempted(self) -> bool:
        """Whether pre-write capillary control attempted fallback execution."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_fallback_attempted", False
            )
            is True
        )

    @property
    def vortex_capillary_preparation_prewrite_external_engine_invoked(self) -> bool:
        """Whether pre-write capillary control invoked an external engine."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_prewrite_external_engine_invoked",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_native_io_certificate_status(
        self,
    ) -> str | None:
        """Return the Native I/O certificate posture for capillary preparation."""

        return self.envelope.field(
            "vortex_capillary_preparation_native_io_certificate_status"
        )

    @property
    def vortex_capillary_preparation_pulseweave_status(self) -> str | None:
        """Return PulseWeave status for capillary cold-preparation control."""

        return self.envelope.field("vortex_capillary_preparation_pulseweave_status")

    @property
    def vortex_capillary_preparation_pulseweave_runtime_decision_applied(
        self,
    ) -> bool:
        """Whether PulseWeave applied to capillary cold-preparation tasks."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_pulseweave_runtime_decision_applied",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_pulseweave_decision_digest(
        self,
    ) -> str | None:
        """Return the PulseWeave decision digest for capillary preparation."""

        return self.envelope.field(
            "vortex_capillary_preparation_pulseweave_decision_digest"
        )

    @property
    def vortex_capillary_preparation_proofbound_claim_allowed(self) -> bool:
        """Whether ProofBound admitted automatic capillary PulseWeave control."""

        return (
            self.envelope.field_bool(
                "vortex_capillary_preparation_proofbound_claim_allowed",
                False,
            )
            is True
        )

    @property
    def vortex_capillary_preparation_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether capillary evidence stayed in the vortex_ingest route."""

        return self.envelope.field(
            "vortex_capillary_preparation_no_standalone_lane_status"
        )

    @property
    def vortex_differential_preparation_status(self) -> str | None:
        """Return scoped differential-preparation overlay status when requested."""

        return self.envelope.field("vortex_differential_preparation_status")

    @property
    def vortex_differential_preparation_update_mode(self) -> str | None:
        """Return the requested differential-preparation update mode."""

        return self.envelope.field("vortex_differential_preparation_update_mode")

    @property
    def vortex_differential_preparation_delta_row_count(self) -> int:
        """Return the number of delta rows admitted into the overlay manifest."""

        return self.envelope.field_int(
            "vortex_differential_preparation_delta_row_count", 0
        ) or 0

    @property
    def vortex_differential_preparation_overlay_applied(self) -> bool:
        """Whether the differential-preparation overlay was applied."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_overlay_applied", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_base_reprepare_performed(self) -> bool:
        """Whether the differential path rewrote the base prepared artifact."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_base_reprepare_performed", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_delta_artifact_written(self) -> bool:
        """Whether the differential path wrote a delta-only Vortex artifact."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_delta_artifact_written", False
            )
            is True
        )

    @property
    def vortex_differential_preparation_native_io_certificate_status(
        self,
    ) -> str | None:
        """Return the Native I/O certificate posture for differential preparation."""

        return self.envelope.field(
            "vortex_differential_preparation_native_io_certificate_status"
        )

    @property
    def vortex_differential_preparation_no_standalone_lane_status(
        self,
    ) -> str | None:
        """Return whether differential evidence stayed in the vortex_ingest route."""

        return self.envelope.field(
            "vortex_differential_preparation_no_standalone_lane_status"
        )

    @property
    def vortex_differential_preparation_refinement_status(self) -> str | None:
        """Return automatic differential refinement admission status."""

        return self.envelope.field("vortex_differential_preparation_refinement_status")

    @property
    def vortex_differential_preparation_refinement_mode(self) -> str | None:
        """Return the differential refinement mode."""

        return self.envelope.field("vortex_differential_preparation_refinement_mode")

    @property
    def vortex_differential_preparation_automatic_detection_status(
        self,
    ) -> str | None:
        """Return automatic append-only detection status."""

        return self.envelope.field(
            "vortex_differential_preparation_automatic_detection_status"
        )

    @property
    def vortex_differential_preparation_blocker_id(self) -> str | None:
        """Return the deterministic differential/refinement blocker, when any."""

        return self.envelope.field("vortex_differential_preparation_blocker_id")

    @property
    def vortex_differential_preparation_refinement_manifest_path(
        self,
    ) -> str | None:
        """Return the automatic refinement manifest path, when emitted."""

        return self.envelope.field(
            "vortex_differential_preparation_refinement_manifest_path"
        )

    @property
    def vortex_differential_preparation_refinement_manifest_digest(
        self,
    ) -> str | None:
        """Return the automatic refinement manifest digest, when emitted."""

        return self.envelope.field(
            "vortex_differential_preparation_refinement_manifest_digest"
        )

    @property
    def vortex_differential_preparation_refinement_manifest_written(self) -> bool:
        """Whether the automatic refinement manifest was written."""

        return (
            self.envelope.field_bool(
                "vortex_differential_preparation_refinement_manifest_written",
                False,
            )
            is True
        )

    @property
    def vortex_differential_preparation_refined_prepared_state_id(
        self,
    ) -> str | None:
        """Return the logical refined prepared-state id."""

        return self.envelope.field(
            "vortex_differential_preparation_refined_prepared_state_id"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_family(
        self,
    ) -> str | None:
        """Return the first admitted overlay consumer family."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_family"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_status(
        self,
    ) -> str | None:
        """Return the overlay consumer admission status."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_status"
        )

    @property
    def vortex_differential_preparation_overlay_consumer_correctness_digest(
        self,
    ) -> str | None:
        """Return the overlay consumer correctness digest."""

        return self.envelope.field(
            "vortex_differential_preparation_overlay_consumer_correctness_digest"
        )

    @property
    def source_io_performed(self) -> bool:
        """Whether source I/O was performed by the smoke."""

        return self.envelope.field_bool("source_io_performed", False) is True

    @property
    def prepared_state_created(self) -> bool:
        """Whether this smoke created a local `VortexPreparedState`."""

        return self.envelope.field_bool("prepared_state_created", False) is True

    @property
    def prepared_state_reused(self) -> bool:
        """Whether this smoke reused a local `VortexPreparedState`."""

        return self.envelope.field_bool("prepared_state_reused", False) is True

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether artifact-adjacent prepared-state reuse hit for this smoke."""

        return self.envelope.field_bool("prepared_state_reuse_hit", False) is True

    @property
    def prepared_state_reuse_reason(self) -> str | None:
        """Return the prepared-state reuse hit, miss, or invalidation reason."""

        return self.envelope.field("prepared_state_reuse_reason")

    @property
    def prepared_state_reuse_manifest_digest(self) -> str | None:
        """Return the prepared-state reuse manifest digest when emitted."""

        return self.envelope.field("prepared_state_reuse_manifest_digest")

    @property
    def prepared_state_invalidation_reason(self) -> str | None:
        """Return the fail-closed prepared-state invalidation reason."""

        return self.envelope.field("prepared_state_invalidation_reason") or self.envelope.field(
            "invalidation_reason"
        )

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
        """Return the route claim-gate status."""

        return _required_field(self.envelope, "claim_gate_status")


@dataclass(frozen=True, slots=True)
class GeneratedSourceWriteReport:
    """Typed view over a scoped local generated-source write smoke."""

    envelope: OutputEnvelope

    @property
    def output_path(self) -> str:
        """Return the local output path written by the smoke command."""

        return _required_field(self.envelope, "output_path")

    @property
    def output_format(self) -> str | None:
        """Return the local output sink format when present."""

        return self.envelope.field("output_format")

    @property
    def workspace_path_safety_status(self) -> str | None:
        """Return the enforced workspace path safety status for the output."""

        return self.envelope.field("output_workspace_path_safety_status")

    @property
    def output_commit_mode(self) -> str | None:
        """Return the local output commit mode."""

        return self.envelope.field("output_commit_mode")

    @property
    def output_commit_status(self) -> str | None:
        """Return the local output commit status."""

        return self.envelope.field("output_commit_status")

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
    def vortex_output_runtime_execution(self) -> bool:
        """Whether this generated-source write used the local Vortex sink."""

        return self.envelope.field_bool("vortex_output_runtime_execution", False) is True

    @property
    def vortex_output_reopen_verified(self) -> bool:
        """Whether the generated Vortex output was reopened for row-count proof."""

        return self.envelope.field_bool("vortex_output_reopen_verified", False) is True

    @property
    def vortex_artifact_digest(self) -> str | None:
        """Return the generated Vortex artifact digest when present."""

        value = self.envelope.field("vortex_artifact_digest")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def vortex_output_row_count(self) -> int | None:
        """Return the generated Vortex output row count when present."""

        return self.envelope.field_int("vortex_output_row_count")

    @property
    def prepared_state_created(self) -> bool:
        """Whether this generated-source write created a local Vortex prepared state."""

        return self.envelope.field_bool("prepared_state_created", False) is True

    @property
    def prepared_state_reused(self) -> bool:
        """Whether this generated-source write reused a prepared state."""

        return self.envelope.field_bool("prepared_state_reused", False) is True

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether prepared-state reuse hit for this generated-source write."""

        return self.envelope.field_bool("prepared_state_reuse_hit", False) is True

    @property
    def prepared_state_reuse_scope(self) -> str | None:
        """Return the prepared-state reuse scope for generated-source output."""

        return self.envelope.field("prepared_state_reuse_scope")

    @property
    def prepared_state_reuse_reason(self) -> str | None:
        """Return the prepared-state reuse hit, miss, or boundary reason."""

        return self.envelope.field("prepared_state_reuse_reason")

    @property
    def prepared_state_reuse_manifest_digest(self) -> str | None:
        """Return the prepared-state reuse manifest digest when emitted."""

        value = self.envelope.field("prepared_state_reuse_manifest_digest")
        if value in {None, "", "none", "not_applicable"}:
            return None
        return value

    @property
    def prepared_state_invalidation_reason(self) -> str | None:
        """Return the prepared-state invalidation or boundary reason."""

        return self.envelope.field("prepared_state_invalidation_reason") or self.envelope.field(
            "invalidation_reason"
        )

    @property
    def upstream_vortex_write_called(self) -> bool:
        """Whether the scoped upstream Vortex writer boundary was invoked."""

        return self.envelope.field_bool("upstream_vortex_write_called", False) is True

    @property
    def upstream_vortex_scan_called(self) -> bool:
        """Whether the scoped upstream Vortex reopen/scan proof was invoked."""

        return self.envelope.field_bool("upstream_vortex_scan_called", False) is True

    @property
    def output_route(self) -> str | None:
        """Return the generated-source local sink route label."""

        return self.envelope.field("output_route")

    @property
    def result_replay_verified(self) -> bool:
        """Whether generated output artifacts were replay/digest verified."""

        return self.envelope.field_bool("result_replay_verified", False) is True

    @property
    def output_replay_status(self) -> str | None:
        """Return generated output replay status."""

        return self.envelope.field("output_replay_status")

    @property
    def output_replay_millis(self) -> int | None:
        """Return generated output replay verification time in milliseconds."""

        return self.envelope.field_int("output_replay_millis")

    @property
    def output_fidelity_report_status(self) -> str | None:
        """Return the generated output fidelity-report status."""

        return self.envelope.field("output_fidelity_report_status")

    @property
    def output_fidelity_loss(self) -> tuple[str, ...]:
        """Return `format:loss` entries for generated output fidelity limits."""

        return _csv_values(self.envelope.field("output_fidelity_loss"))

    @property
    def sink_artifact_count(self) -> int:
        """Return the number of generated local sink artifacts."""

        return self.envelope.field_int("sink_artifact_count", 0) or 0

    @property
    def sink_artifact_ref(self) -> str | None:
        """Return the primary or labeled generated local sink artifact ref."""

        value = self.envelope.field("sink_artifact_ref")
        if value in {None, "", "not_requested", "not_applicable"}:
            return None
        return value

    @property
    def sink_artifact_refs(self) -> tuple[str, ...]:
        """Return `format:path` refs for generated local sink artifacts."""

        return _csv_reference_values(self.envelope.field("sink_artifact_refs"))

    @property
    def sink_artifact_digest(self) -> str | None:
        """Return the primary or labeled generated local sink artifact digest."""

        value = self.envelope.field("sink_artifact_digest")
        if value in {None, "", "not_requested", "not_applicable"}:
            return None
        return value

    @property
    def sink_artifact_digests(self) -> tuple[str, ...]:
        """Return `format:digest` entries for generated local sink artifacts."""

        return _csv_reference_values(self.envelope.field("sink_artifact_digests"))

    @property
    def sink_artifact_manifest_status(self) -> str | None:
        """Return replay-backed manifest status for generated local sink artifacts."""

        return self.envelope.field("sink_artifact_manifest_status")

    @property
    def output_fanout_performed(self) -> bool:
        """Whether the generated-source smoke wrote fanout outputs."""

        return self.envelope.field_bool("output_fanout_performed", False) is True

    @property
    def result_reuse_for_fanout(self) -> bool:
        """Whether one computed generated result was reused for fanout writes."""

        return self.envelope.field_bool("result_reuse_for_fanout", False) is True

    @property
    def fanout_result_reuse_hit(self) -> bool:
        """Whether generated-source fanout reused the computed primary result."""

        return self.envelope.field_bool("fanout_result_reuse_hit", False) is True

    @property
    def fanout_output_count(self) -> int:
        """Return the number of generated-source fanout outputs."""

        return self.envelope.field_int("fanout_output_count", 0) or 0

    @property
    def fanout_output_formats(self) -> tuple[str, ...]:
        """Return generated-source fanout output formats."""

        return _csv_values(self.envelope.field("fanout_output_formats"))

    @property
    def fanout_output_paths(self) -> tuple[str, ...]:
        """Return generated-source fanout output paths."""

        return _csv_values(self.envelope.field("fanout_output_paths"))

    @property
    def fanout_output_digests(self) -> tuple[str, ...]:
        """Return `format:digest` entries for generated-source fanout outputs."""

        return _csv_values(self.envelope.field("fanout_output_digests"))

    @property
    def fanout_output_workspace_path_safety_statuses(self) -> tuple[str, ...]:
        """Return `format:accepted` entries for generated fanout path safety."""

        return _csv_values(
            self.envelope.field("fanout_output_workspace_path_safety_statuses")
        )

    @property
    def fanout_output_commit_modes(self) -> tuple[str, ...]:
        """Return `format:commit_mode` entries for generated fanout outputs."""

        return _csv_values(self.envelope.field("fanout_output_commit_modes"))

    @property
    def fanout_output_native_io_certificate_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for generated fanout output certificates."""

        return _csv_values(
            self.envelope.field("fanout_output_native_io_certificate_statuses")
        )

    @property
    def fanout_output_replay_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for generated fanout replay verification."""

        return _csv_values(self.envelope.field("fanout_output_replay_statuses"))

    @property
    def fanout_output_fidelity_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for generated fanout fidelity reports."""

        return _csv_values(self.envelope.field("fanout_output_fidelity_statuses"))

    @property
    def fanout_output_fidelity_loss(self) -> tuple[str, ...]:
        """Return `format:loss` entries for generated fanout fidelity limits."""

        return _csv_values(self.envelope.field("fanout_output_fidelity_loss"))

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
    def evidence_summary(self) -> EvidenceSummary:
        """Return the compact evidence summary for this generated-source write."""

        return self.envelope.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return the compact claim summary for this generated-source write."""

        return self.envelope.claim_summary

    @property
    def generated_source_range_start(self) -> int | None:
        """Return the generated range start when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_start")

    @property
    def generated_source_range_end(self) -> int | None:
        """Return the generated range end when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_end")

    @property
    def generated_source_range_step(self) -> int | None:
        """Return the generated range step when this report is for a range source."""

        return self.envelope.field_int("generated_source_range_step")

    @property
    def generated_source_range_column(self) -> str | None:
        """Return the generated range column name when present."""

        return self.envelope.field("generated_source_range_column")

    @property
    def generated_source_sql_generator_function(self) -> str | None:
        """Return the SQL generator function name when present."""

        return self.envelope.field("generated_source_sql_generator_function")

    @property
    def generated_source_range_end_inclusive(self) -> bool | None:
        """Return whether the generated range end is inclusive when present."""

        return self.envelope.field_bool("generated_source_range_end_inclusive")

    @property
    def sql_source_free_filter_runtime_execution(self) -> bool:
        """Whether scoped SQL source-free filtering executed in the smoke command."""

        return (
            self.envelope.field_bool(
                "sql_source_free_filter_runtime_execution",
                False,
            )
            is True
        )

    @property
    def sql_source_free_filter_source_column(self) -> str | None:
        """Return the generated SQL filter source column when present."""

        return self.envelope.field("sql_source_free_filter_source_column")

    @property
    def sql_source_free_filter_predicate(self) -> str | None:
        """Return the generated SQL filter predicate evidence label when present."""

        return self.envelope.field("sql_source_free_filter_predicate")

    @property
    def sql_source_free_filter_selected_row_count(self) -> int | None:
        """Return the generated SQL filter row count before LIMIT when present."""

        return self.envelope.field_int("sql_source_free_filter_selected_row_count")

    @property
    def sql_source_free_limit_runtime_execution(self) -> bool:
        """Whether scoped SQL source-free LIMIT executed in the smoke command."""

        return (
            self.envelope.field_bool(
                "sql_source_free_limit_runtime_execution",
                False,
            )
            is True
        )

    @property
    def sql_source_free_limit_count(self) -> int | None:
        """Return the generated SQL LIMIT count when present."""

        return self.envelope.field_int("sql_source_free_limit_count")

    @property
    def sql_source_free_order_by_runtime_execution(self) -> bool:
        """Whether scoped SQL source-free ORDER BY executed in the smoke command."""

        return (
            self.envelope.field_bool(
                "sql_source_free_order_by_runtime_execution",
                False,
            )
            is True
        )

    @property
    def sql_source_free_top_n_runtime_execution(self) -> bool:
        """Whether scoped SQL source-free ORDER BY plus LIMIT executed as top-N."""

        return (
            self.envelope.field_bool(
                "sql_source_free_top_n_runtime_execution",
                False,
            )
            is True
        )

    @property
    def sql_source_free_sort_operator_family(self) -> str | None:
        """Return the source-free range sort operator family when present."""

        return self.envelope.field("sql_source_free_sort_operator_family")

    @property
    def sql_source_free_sort_keys(self) -> tuple[str, ...]:
        """Return source-free range ORDER BY keys when present."""

        return _csv_values(self.envelope.field("sql_source_free_sort_keys"))

    @property
    def sql_source_free_sort_direction(self) -> tuple[str, ...]:
        """Return source-free range ORDER BY directions when present."""

        return _csv_values(self.envelope.field("sql_source_free_sort_direction"))

    @property
    def sql_source_free_sort_input_row_count(self) -> int | None:
        """Return source-free range sort input row count when present."""

        return self.envelope.field_int("sql_source_free_sort_input_row_count")

    @property
    def sql_source_free_top_n_limit(self) -> int | None:
        """Return source-free range top-N limit when present."""

        return self.envelope.field_int("sql_source_free_top_n_limit")

    @property
    def sql_source_free_projection_runtime_execution(self) -> bool:
        """Whether scoped SQL source-free projection executed in the smoke command."""

        return (
            self.envelope.field_bool(
                "sql_source_free_projection_runtime_execution",
                False,
            )
            is True
        )

    @property
    def sql_source_free_projection_source_column(self) -> str | None:
        """Return the generated SQL projection source column when present."""

        return self.envelope.field("sql_source_free_projection_source_column")

    @property
    def sql_source_free_projection_columns(self) -> tuple[str, ...]:
        """Return generated SQL projection output columns when present."""

        return _csv_values(self.envelope.field("sql_source_free_projection_columns"))

    @property
    def sql_source_free_projection_expressions(self) -> tuple[str, ...]:
        """Return generated SQL projection expressions when present."""

        return _csv_values(self.envelope.field("sql_source_free_projection_expressions"))


@dataclass(frozen=True, slots=True)
class SqlLocalSourceSmokeReport:
    """Typed view over scoped local-source SQL smoke reports."""

    envelope: OutputEnvelope

    @property
    def status(self) -> str:
        """Return the command status, including deterministic unsupported/error statuses."""

        return self.envelope.status

    @property
    def is_error(self) -> bool:
        """Whether the command status is an error or unsupported runtime diagnostic."""

        return self.envelope.is_error

    @property
    def has_error_diagnostics(self) -> bool:
        """Whether the runtime returned error/fatal diagnostics."""

        return self.envelope.has_error_diagnostics

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return deterministic runtime diagnostics emitted by the SQL smoke command."""

        return self.envelope.diagnostics

    @property
    def unsupported_reasons(self) -> tuple[str, ...]:
        """Return stable unsupported diagnostic reasons/messages for Python callers."""

        reasons: list[str] = []
        for diagnostic in self.diagnostics:
            if diagnostic.reason:
                reasons.append(diagnostic.reason)
            elif diagnostic.message:
                reasons.append(diagnostic.message)
        if not reasons and self.envelope.is_error and self.envelope.human_text:
            reasons.append(self.envelope.human_text)
        return tuple(dict.fromkeys(reasons))

    @property
    def result_jsonl(self) -> str:
        """Return the bounded inline JSONL result emitted by ShardLoom."""

        return _required_field(self.envelope, "result_jsonl")

    @property
    def result_rows(self) -> tuple[Mapping[str, Any], ...]:
        """Return bounded inline JSONL rows as Python mappings."""

        return _jsonl_object_rows(self.result_jsonl, field_name="result_jsonl")

    @property
    def first_result_row(self) -> Mapping[str, Any] | None:
        """Return the first bounded result row, if one was emitted."""

        rows = self.result_rows
        return rows[0] if rows else None

    @property
    def source_state_id(self) -> str | None:
        """Return the local SourceState identifier emitted by the smoke."""

        value = self.envelope.field("source_state_id")
        if value in {None, "", "not_applicable", "none"}:
            return None
        return value

    @property
    def source_state_digest(self) -> str | None:
        """Return the local SourceState digest emitted by the smoke."""

        value = self.envelope.field("source_state_digest")
        if value in {None, "", "not_applicable", "none"}:
            return None
        return value

    @property
    def source_state_contract_schema_version(self) -> str | None:
        """Return the local SourceState contract schema version."""

        return self.envelope.field("source_state_contract_schema_version")

    @property
    def source_schema_digest(self) -> str | None:
        """Return the local source-schema digest when emitted."""

        return self.envelope.field("source_schema_digest")

    @property
    def plan_digest(self) -> str | None:
        """Return the SQL local-source plan digest when emitted."""

        return self.envelope.field("plan_digest")

    @property
    def execution_certificate_ref(self) -> str | None:
        """Return the execution certificate reference when emitted."""

        return self.envelope.field("execution_certificate_ref")

    @property
    def source_state_read_plan(self) -> str | None:
        """Return the local SourceState read-plan status."""

        return self.envelope.field("source_state_read_plan")

    @property
    def source_state_projection_pushdown_status(self) -> str | None:
        """Return reader projection pushdown status for the local SourceState."""

        return self.envelope.field("source_state_projection_pushdown_status")

    @property
    def user_surface_runtime_scope(self) -> str | None:
        """Return whether SQL/Python compute used the common user-surface runtime."""

        return self.envelope.field("user_surface_runtime_scope")

    @property
    def format_specific_boundary_scope(self) -> str | None:
        """Return where format-specific behavior is allowed for this report."""

        return self.envelope.field("format_specific_boundary_scope")

    @property
    def format_specific_compute_path(self) -> bool:
        """Whether this report used a format-specific compute path."""

        return self.envelope.field_bool("format_specific_compute_path", False) is True

    @property
    def source_state_materialization_layout(self) -> str | None:
        """Return the local SourceState materialization layout."""

        return self.envelope.field("source_state_materialization_layout")

    @property
    def source_state_parse_normalization(self) -> str | None:
        """Return the local SourceState parse/normalization route."""

        return self.envelope.field("source_state_parse_normalization")

    @property
    def source_state_columnar_preserved(self) -> bool:
        """Whether the local SourceState preserved a columnar adapter boundary."""

        return self.envelope.field_bool("source_state_columnar_preserved", False) is True

    @property
    def source_state_record_batch_count(self) -> int:
        """Return the preserved local SourceState record-batch count."""

        return self.envelope.field_int("source_state_record_batch_count", 0) or 0

    @property
    def source_to_columnar_millis(self) -> int:
        """Return source-to-columnar adapter time in milliseconds."""

        return self.envelope.field_int("source_to_columnar_millis", 0) or 0

    @property
    def source_state_runtime_consumption_layout(self) -> str | None:
        """Return the runtime layout that consumed the local SourceState."""

        return self.envelope.field("source_state_runtime_consumption_layout")

    @property
    def source_state_scalar_runtime_materialization_required(self) -> bool:
        """Whether the local SQL runtime still materialized scalar rows."""

        return (
            self.envelope.field_bool(
                "source_state_scalar_runtime_materialization_required", False
            )
            is True
        )

    @property
    def source_state_materialized_columns(self) -> tuple[str, ...]:
        """Return SourceState columns materialized by the local runtime."""

        return _csv_values(self.envelope.field("source_state_materialized_columns"))

    @property
    def source_state_reader_projection_columns(self) -> tuple[str, ...]:
        """Return columns requested from the local reader before scalar materialization."""

        return _csv_values(self.envelope.field("source_state_reader_projection_columns"))

    @property
    def output_path(self) -> str | None:
        """Return the local output path when the smoke wrote one."""

        value = self.envelope.field("output_path")
        return value or None

    @property
    def output_format(self) -> str | None:
        """Return the local output sink format when present."""

        value = self.envelope.field("output_format")
        return value or None

    @property
    def workspace_path_safety_status(self) -> str | None:
        """Return the enforced workspace path safety status for the primary output."""

        return self.envelope.field("output_workspace_path_safety_status")

    @property
    def output_commit_mode(self) -> str | None:
        """Return the primary local output commit mode."""

        return self.envelope.field("output_commit_mode")

    @property
    def output_commit_status(self) -> str | None:
        """Return the primary local output commit status."""

        return self.envelope.field("output_commit_status")

    @property
    def output_row_count(self) -> int:
        """Return the number of result rows emitted by the smoke."""

        return self.envelope.field_int("output_row_count", 0) or 0

    @property
    def selected_row_count(self) -> int:
        """Return the number of source rows selected before limit."""

        return self.envelope.field_int("selected_row_count", 0) or 0

    @property
    def distinct_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed row-level SELECT DISTINCT projection."""

        return (
            self.envelope.field_bool("distinct_projection_runtime_execution", False)
            is True
        )

    @property
    def distinct_projection_output_columns(self) -> tuple[str, ...]:
        """Return output columns covered by row-level DISTINCT projection evidence."""

        value = self.envelope.field("distinct_projection_output_columns")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def distinct_projection_input_row_count(self) -> int:
        """Return row count considered by row-level DISTINCT before deduplication."""

        return self.envelope.field_int("distinct_projection_input_row_count", 0) or 0

    @property
    def distinct_projection_output_row_count(self) -> int:
        """Return row count emitted after row-level DISTINCT deduplication."""

        return self.envelope.field_int("distinct_projection_output_row_count", 0) or 0

    @property
    def distinct_projection_limit_applied_after_deduplication(self) -> bool:
        """Whether LIMIT was applied after row-level DISTINCT deduplication."""

        return (
            self.envelope.field_bool(
                "distinct_projection_limit_applied_after_deduplication", False
            )
            is True
        )

    @property
    def distinct_projection_null_semantics(self) -> str | None:
        """Return row-level DISTINCT null-semantics evidence."""

        value = self.envelope.field("distinct_projection_null_semantics")
        if value == "not_applicable":
            return None
        return value

    @property
    def sql_set_operation_runtime_execution(self) -> bool:
        """Whether this smoke executed scoped SQL set-operation composition."""

        return (
            self.envelope.field_bool("sql_set_operation_runtime_execution", False)
            is True
        )

    @property
    def sql_set_operation_mode(self) -> str | None:
        """Return the scoped SQL set-operation mode, when present."""

        return self.envelope.field("sql_set_operation_mode")

    @property
    def sql_set_operator(self) -> str | None:
        """Return the SQL set operator, when present."""

        return self.envelope.field("sql_set_operator")

    @property
    def sql_set_operation_branch_count(self) -> int:
        """Return the number of SELECT branches composed by the set operation."""

        return self.envelope.field_int("sql_set_operation_branch_count", 0) or 0

    @property
    def sql_set_operation_input_row_count(self) -> int:
        """Return rows entering SQL set-operation composition after branch filters."""

        return self.envelope.field_int("sql_set_operation_input_row_count", 0) or 0

    @property
    def sql_set_operation_candidate_row_count(self) -> int:
        """Return rows remaining after set-operation distinct semantics before LIMIT."""

        return self.envelope.field_int("sql_set_operation_candidate_row_count", 0) or 0

    @property
    def sql_set_operation_output_row_count(self) -> int:
        """Return rows emitted by the SQL set-operation result."""

        return self.envelope.field_int("sql_set_operation_output_row_count", 0) or 0

    @property
    def sql_set_operation_null_semantics(self) -> str | None:
        """Return scoped SQL set-operation null-semantics evidence."""

        return self.envelope.field("sql_set_operation_null_semantics")

    @property
    def sql_union_runtime_execution(self) -> bool:
        """Whether this smoke executed scoped SQL UNION composition."""

        return self.envelope.field_bool("sql_union_runtime_execution", False) is True

    @property
    def sql_union_mode(self) -> str | None:
        """Return the scoped SQL UNION mode, when present."""

        return self.envelope.field("sql_union_mode")

    @property
    def sql_union_branch_count(self) -> int:
        """Return the number of SELECT branches composed by SQL UNION."""

        return self.envelope.field_int("sql_union_branch_count", 0) or 0

    @property
    def sql_union_input_row_count(self) -> int:
        """Return rows entering SQL UNION composition after branch filters."""

        return self.envelope.field_int("sql_union_input_row_count", 0) or 0

    @property
    def sql_union_distinct_input_row_count(self) -> int:
        """Return rows remaining after UNION DISTINCT deduplication before LIMIT."""

        return self.envelope.field_int("sql_union_distinct_input_row_count", 0) or 0

    @property
    def sql_union_output_row_count(self) -> int:
        """Return rows emitted by the SQL UNION result."""

        return self.envelope.field_int("sql_union_output_row_count", 0) or 0

    @property
    def sql_union_null_semantics(self) -> str | None:
        """Return scoped SQL UNION null-semantics evidence."""

        return self.envelope.field("sql_union_null_semantics")

    @property
    def computed_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted computed projection path."""

        return (
            self.envelope.field_bool("computed_projection_runtime_execution", False)
            is True
        )

    @property
    def computed_projection_top_n_runtime_execution(self) -> bool:
        """Whether this smoke ordered a computed projection path through top-N."""

        return (
            self.envelope.field_bool(
                "computed_projection_top_n_runtime_execution", False
            )
            is True
        )

    @property
    def computed_projection_operator_family(self) -> str | None:
        """Return the computed projection operator family emitted by the smoke."""

        return self.envelope.field("computed_projection_operator_family")

    @property
    def window_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted window projection path."""

        return self.envelope.field_bool("window_runtime_execution", False) is True

    @property
    def window_operator_family(self) -> str | None:
        """Return the admitted window operator family emitted by the smoke."""

        return self.envelope.field("window_operator_family")

    @property
    def window_function(self) -> tuple[str, ...]:
        """Return admitted window function names emitted by the smoke."""

        return _csv_values(self.envelope.field("window_function"))

    @property
    def window_partition_columns(self) -> tuple[str, ...]:
        """Return window PARTITION BY columns emitted by the smoke."""

        value = self.envelope.field("window_partition_columns")
        if value in {None, "", "not_applicable"}:
            return ()
        return tuple(
            part.strip()
            for group in value.split(";")
            for part in group.split(",")
            if part.strip()
        )

    @property
    def window_order_by_columns(self) -> tuple[str, ...]:
        """Return window ORDER BY columns emitted by the smoke."""

        value = self.envelope.field("window_order_by_columns")
        if value in {None, "", "not_applicable"}:
            return ()
        return tuple(part for group in value.split(";") for part in group.split(",") if part)

    @property
    def window_order_by_directions(self) -> tuple[str, ...]:
        """Return window ORDER BY directions emitted by the smoke."""

        value = self.envelope.field("window_order_by_directions")
        if value in {None, "", "not_applicable"}:
            return ()
        return tuple(part for group in value.split(";") for part in group.split(",") if part)

    @property
    def window_output_columns(self) -> tuple[str, ...]:
        """Return window projection output columns emitted by the smoke."""

        return _csv_values(self.envelope.field("window_output_columns"))

    @property
    def window_value_columns(self) -> tuple[str, ...]:
        """Return LAG/LEAD value columns emitted by the smoke."""

        return tuple(
            value
            for value in _csv_values(self.envelope.field("window_value_columns"))
            if value != "none"
        )

    @property
    def window_offset_rows(self) -> tuple[int, ...]:
        """Return LAG/LEAD offsets emitted by the smoke."""

        return tuple(
            int(value)
            for value in _csv_values(self.envelope.field("window_offset_rows"))
            if value not in {"none", "not_applicable"}
        )

    @property
    def window_bucket_counts(self) -> tuple[int, ...]:
        """Return NTILE bucket counts emitted by the smoke."""

        return tuple(
            int(value)
            for value in _csv_values(self.envelope.field("window_bucket_counts"))
            if value not in {"none", "not_applicable"}
        )

    @property
    def window_row_number_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted ROW_NUMBER window semantics."""

        return (
            self.envelope.field_bool("window_row_number_runtime_execution", False)
            is True
        )

    @property
    def window_rank_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted RANK window semantics."""

        return self.envelope.field_bool("window_rank_runtime_execution", False) is True

    @property
    def window_dense_rank_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted DENSE_RANK window semantics."""

        return (
            self.envelope.field_bool("window_dense_rank_runtime_execution", False)
            is True
        )

    @property
    def window_lag_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted LAG window semantics."""

        return self.envelope.field_bool("window_lag_runtime_execution", False) is True

    @property
    def window_lead_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted LEAD window semantics."""

        return self.envelope.field_bool("window_lead_runtime_execution", False) is True

    @property
    def window_ntile_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted NTILE window semantics."""

        return self.envelope.field_bool("window_ntile_runtime_execution", False) is True

    @property
    def window_percent_rank_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted PERCENT_RANK window semantics."""

        return (
            self.envelope.field_bool("window_percent_rank_runtime_execution", False)
            is True
        )

    @property
    def window_cume_dist_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted CUME_DIST window semantics."""

        return self.envelope.field_bool("window_cume_dist_runtime_execution", False) is True

    @property
    def predicate_operator_family(self) -> str | None:
        """Return the predicate operator family emitted by the smoke."""

        return self.envelope.field("predicate_operator_family")

    @property
    def filter_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted filter predicate path."""

        return self.envelope.field_bool("filter_runtime_execution", False) is True

    @property
    def null_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted IS NULL / IS NOT NULL predicate."""

        return self.envelope.field_bool("null_predicate_runtime_execution", False) is True

    @property
    def null_predicate_operator(self) -> tuple[str, ...]:
        """Return null predicate operators emitted by the smoke."""

        value = self.envelope.field("null_predicate_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def null_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source columns used by admitted null predicates."""

        value = self.envelope.field("null_predicate_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def null_predicate_null_semantics(self) -> str | None:
        """Return the null-semantics contract for admitted null predicates."""

        return self.envelope.field("null_predicate_null_semantics")

    @property
    def boolean_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted boolean predicate."""

        return self.envelope.field_bool("boolean_predicate_runtime_execution", False) is True

    @property
    def boolean_predicate_operator(self) -> tuple[str, ...]:
        """Return boolean predicate operators emitted by the smoke."""

        value = self.envelope.field("boolean_predicate_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def boolean_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source columns used by admitted boolean predicates."""

        value = self.envelope.field("boolean_predicate_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def boolean_predicate_null_semantics(self) -> str | None:
        """Return the null-semantics contract for admitted boolean predicates."""

        return self.envelope.field("boolean_predicate_null_semantics")

    @property
    def logical_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted logical predicate path."""

        return self.envelope.field_bool("logical_predicate_runtime_execution", False) is True

    @property
    def logical_predicate_operator(self) -> str | None:
        """Return the admitted logical predicate operator, when present."""

        return self.envelope.field("logical_predicate_operator")

    @property
    def logical_predicate_leaf_count(self) -> int:
        """Return the number of predicate leaves in the logical predicate tree."""

        return self.envelope.field_int("logical_predicate_leaf_count", 0) or 0

    @property
    def in_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted IN predicate path."""

        return self.envelope.field_bool("in_predicate_runtime_execution", False) is True

    @property
    def in_list_value_count(self) -> int:
        """Return the number of admitted literal values in the IN predicate tree."""

        return self.envelope.field_int("in_list_value_count", 0) or 0

    @property
    def in_list_null_value_count(self) -> int:
        """Return the number of NULL literals in the admitted IN predicate tree."""

        return self.envelope.field_int("in_list_null_value_count", 0) or 0

    @property
    def row_value_in_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted row-value IN predicate path."""

        return (
            self.envelope.field_bool(
                "row_value_in_predicate_runtime_execution", False
            )
            is True
        )

    @property
    def row_value_in_source_columns(self) -> tuple[str, ...]:
        """Return source columns used by admitted row-value IN predicates."""

        value = self.envelope.field("row_value_in_source_columns", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def row_value_in_column_groups(self) -> tuple[str, ...]:
        """Return grouped source-column sets used by row-value IN predicates."""

        value = self.envelope.field("row_value_in_column_groups", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def row_value_in_column_count(self) -> int:
        """Return the source-column arity across admitted row-value IN predicates."""

        return self.envelope.field_int("row_value_in_column_count", 0) or 0

    @property
    def row_value_in_tuple_count(self) -> int:
        """Return admitted tuple count for row-value literal or source-backed IN predicates."""

        return self.envelope.field_int("row_value_in_tuple_count", 0) or 0

    @property
    def row_value_in_null_value_count(self) -> int:
        """Return NULL scalar count in row-value literal or source-backed IN tuples."""

        return self.envelope.field_int("row_value_in_null_value_count", 0) or 0

    @property
    def row_value_in_null_semantics(self) -> str | None:
        """Return the row-value IN null semantics evidence label."""

        return self.envelope.field("row_value_in_null_semantics")

    @property
    def in_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted IN-subquery predicate path."""

        return self.envelope.field_bool("in_subquery_runtime_execution", False) is True

    @property
    def in_subquery_source_columns(self) -> tuple[str, ...]:
        """Return subquery source columns used by admitted IN-subquery predicates."""

        value = self.envelope.field("in_subquery_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def in_subquery_source_formats(self) -> tuple[str, ...]:
        """Return subquery source formats used by admitted IN-subquery predicates."""

        value = self.envelope.field("in_subquery_source_format", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def in_subquery_materialized_value_count(self) -> int:
        """Return bounded materialized scalar or tuple count for IN subqueries."""

        return self.envelope.field_int("in_subquery_materialized_value_count", 0) or 0

    @property
    def in_subquery_materialized_null_value_count(self) -> int:
        """Return the bounded materialized NULL count for IN-subquery values."""

        return (
            self.envelope.field_int("in_subquery_materialized_null_value_count", 0)
            or 0
        )

    @property
    def in_subquery_filter_runtime_execution(self) -> bool:
        """Whether an admitted IN-subquery evaluated its own WHERE predicate."""

        return (
            self.envelope.field_bool("in_subquery_filter_runtime_execution", False)
            is True
        )

    @property
    def in_subquery_order_by_runtime_execution(self) -> bool:
        """Whether an admitted IN-subquery applied ORDER BY before materialization."""

        return (
            self.envelope.field_bool("in_subquery_order_by_runtime_execution", False)
            is True
        )

    @property
    def in_subquery_limit_runtime_execution(self) -> bool:
        """Whether an admitted IN-subquery applied LIMIT before materialization."""

        return (
            self.envelope.field_bool("in_subquery_limit_runtime_execution", False)
            is True
        )

    @property
    def in_subquery_input_row_count(self) -> int:
        """Return the number of source rows read by admitted IN-subqueries."""

        return self.envelope.field_int("in_subquery_input_row_count", 0) or 0

    @property
    def in_subquery_filtered_row_count(self) -> int:
        """Return the number of IN-subquery rows selected before ORDER BY/LIMIT."""

        return self.envelope.field_int("in_subquery_filtered_row_count", 0) or 0

    @property
    def in_subquery_materialization_bound(self) -> int:
        """Return the deterministic materialization bound for IN-subquery values."""

        return self.envelope.field_int("in_subquery_materialization_bound", 0) or 0

    @property
    def having_in_subquery_runtime_execution(self) -> bool:
        """Whether HAVING used an admitted IN-subquery predicate."""

        return (
            self.envelope.field_bool("having_in_subquery_runtime_execution", False)
            is True
        )

    @property
    def projected_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted projected source subquery."""

        return (
            self.envelope.field_bool("projected_subquery_runtime_execution", False)
            is True
        )

    @property
    def projected_subquery_statement_kinds(self) -> tuple[str, ...]:
        """Return statement kinds used by admitted projected source subqueries."""

        value = self.envelope.field("projected_subquery_statement_kind", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def projected_subquery_output_column_counts(self) -> tuple[int, ...]:
        """Return output-column counts for admitted projected source subqueries."""

        value = self.envelope.field("projected_subquery_output_column_count", "")
        if not value or value == "not_applicable":
            return ()
        counts: list[int] = []
        for part in value.split(","):
            if part:
                counts.append(int(part))
        return tuple(counts)

    @property
    def projected_subquery_join_runtime_execution(self) -> bool:
        """Whether an admitted projected source subquery used a join."""

        return (
            self.envelope.field_bool(
                "projected_subquery_join_runtime_execution", False
            )
            is True
        )

    @property
    def projected_subquery_group_by_runtime_execution(self) -> bool:
        """Whether an admitted projected source subquery used GROUP BY."""

        return (
            self.envelope.field_bool(
                "projected_subquery_group_by_runtime_execution", False
            )
            is True
        )

    @property
    def projected_subquery_having_runtime_execution(self) -> bool:
        """Whether an admitted projected source subquery used HAVING."""

        return (
            self.envelope.field_bool(
                "projected_subquery_having_runtime_execution", False
            )
            is True
        )

    @property
    def correlated_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted correlated source subquery."""

        return (
            self.envelope.field_bool("correlated_subquery_runtime_execution", False)
            is True
        )

    @property
    def correlated_subquery_outer_aliases(self) -> tuple[str, ...]:
        """Return outer-row aliases used by admitted correlated subqueries."""

        value = self.envelope.field("correlated_subquery_outer_alias", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def correlated_subquery_outer_columns(self) -> tuple[str, ...]:
        """Return outer-row columns referenced by admitted correlated subqueries."""

        value = self.envelope.field("correlated_subquery_outer_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def correlated_subquery_evaluation_strategy(self) -> str | None:
        """Return the correlated-subquery evaluation strategy evidence label."""

        return self.envelope.field("correlated_subquery_evaluation_strategy")

    @property
    def correlated_subquery_outer_row_evaluation_count(self) -> int:
        """Return the number of outer rows evaluated by correlated subqueries."""

        return (
            self.envelope.field_int(
                "correlated_subquery_outer_row_evaluation_count", 0
            )
            or 0
        )

    @property
    def source_qualified_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed a source-qualified local subquery path."""

        return (
            self.envelope.field_bool(
                "source_qualified_subquery_runtime_execution", False
            )
            is True
        )

    @property
    def source_qualified_subquery_source_qualifiers(self) -> tuple[str, ...]:
        """Return source qualifiers used by admitted local subqueries."""

        value = self.envelope.field("source_qualified_subquery_source_qualifier", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def source_qualified_subquery_operator_families(self) -> tuple[str, ...]:
        """Return operator families for source-qualified local subqueries."""

        value = self.envelope.field("source_qualified_subquery_operator_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def source_qualified_subquery_source_columns(self) -> tuple[str, ...]:
        """Return normalized source columns for source-qualified local subqueries."""

        value = self.envelope.field("source_qualified_subquery_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def quantified_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted ANY/ALL-subquery predicate path."""

        return (
            self.envelope.field_bool("quantified_subquery_runtime_execution", False)
            is True
        )

    @property
    def quantified_subquery_quantifiers(self) -> tuple[str, ...]:
        """Return quantified subquery quantifiers used by admitted predicates."""

        value = self.envelope.field("quantified_subquery_quantifier", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def quantified_subquery_comparison_operators(self) -> tuple[str, ...]:
        """Return comparison operators used by admitted quantified subqueries."""

        value = self.envelope.field("quantified_subquery_comparison_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def quantified_subquery_source_columns(self) -> tuple[str, ...]:
        """Return selected source columns used by admitted quantified subqueries."""

        value = self.envelope.field("quantified_subquery_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def quantified_subquery_source_formats(self) -> tuple[str, ...]:
        """Return subquery source formats used by admitted quantified subqueries."""

        value = self.envelope.field("quantified_subquery_source_format", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def quantified_subquery_filter_runtime_execution(self) -> bool:
        """Whether an admitted quantified subquery evaluated its own WHERE predicate."""

        return (
            self.envelope.field_bool(
                "quantified_subquery_filter_runtime_execution", False
            )
            is True
        )

    @property
    def quantified_subquery_order_by_runtime_execution(self) -> bool:
        """Whether an admitted quantified subquery applied ORDER BY."""

        return (
            self.envelope.field_bool(
                "quantified_subquery_order_by_runtime_execution", False
            )
            is True
        )

    @property
    def quantified_subquery_limit_runtime_execution(self) -> bool:
        """Whether an admitted quantified subquery applied LIMIT."""

        return (
            self.envelope.field_bool(
                "quantified_subquery_limit_runtime_execution", False
            )
            is True
        )

    @property
    def quantified_subquery_input_row_count(self) -> int:
        """Return the number of source rows read by admitted quantified subqueries."""

        return self.envelope.field_int("quantified_subquery_input_row_count", 0) or 0

    @property
    def quantified_subquery_filtered_row_count(self) -> int:
        """Return quantified subquery rows selected before ORDER BY/LIMIT."""

        return self.envelope.field_int("quantified_subquery_filtered_row_count", 0) or 0

    @property
    def quantified_subquery_materialization_bound(self) -> int:
        """Return the deterministic materialization bound for quantified subqueries."""

        return self.envelope.field_int("quantified_subquery_materialization_bound", 0) or 0

    @property
    def quantified_subquery_materialized_value_count(self) -> int:
        """Return bounded materialized scalar count for quantified subqueries."""

        return (
            self.envelope.field_int("quantified_subquery_materialized_value_count", 0)
            or 0
        )

    @property
    def quantified_subquery_materialized_null_value_count(self) -> int:
        """Return bounded materialized NULL count for quantified subquery values."""

        return (
            self.envelope.field_int(
                "quantified_subquery_materialized_null_value_count", 0
            )
            or 0
        )

    @property
    def quantified_subquery_null_semantics(self) -> str | None:
        """Return the quantified subquery null-semantics evidence label."""

        return self.envelope.field("quantified_subquery_null_semantics")

    @property
    def having_quantified_subquery_runtime_execution(self) -> bool:
        """Whether HAVING used an admitted quantified subquery predicate."""

        return (
            self.envelope.field_bool(
                "having_quantified_subquery_runtime_execution", False
            )
            is True
        )

    @property
    def exists_subquery_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted EXISTS-subquery predicate path."""

        return (
            self.envelope.field_bool("exists_subquery_runtime_execution", False)
            is True
        )

    @property
    def exists_subquery_projection_kind(self) -> tuple[str, ...]:
        """Return projection kinds used by admitted EXISTS-subquery predicates."""

        value = self.envelope.field("exists_subquery_projection_kind", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def exists_subquery_source_columns(self) -> tuple[str, ...]:
        """Return selected source columns used by admitted EXISTS-subquery predicates."""

        value = self.envelope.field("exists_subquery_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def exists_subquery_source_formats(self) -> tuple[str, ...]:
        """Return subquery source formats used by admitted EXISTS-subquery predicates."""

        value = self.envelope.field("exists_subquery_source_format", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def exists_subquery_filter_runtime_execution(self) -> bool:
        """Whether an admitted EXISTS-subquery evaluated its own WHERE predicate."""

        return (
            self.envelope.field_bool("exists_subquery_filter_runtime_execution", False)
            is True
        )

    @property
    def exists_subquery_order_by_runtime_execution(self) -> bool:
        """Whether an admitted EXISTS-subquery applied ORDER BY before presence testing."""

        return (
            self.envelope.field_bool("exists_subquery_order_by_runtime_execution", False)
            is True
        )

    @property
    def exists_subquery_limit_runtime_execution(self) -> bool:
        """Whether an admitted EXISTS-subquery applied LIMIT before presence testing."""

        return (
            self.envelope.field_bool("exists_subquery_limit_runtime_execution", False)
            is True
        )

    @property
    def exists_subquery_input_row_count(self) -> int:
        """Return the number of source rows read by admitted EXISTS-subqueries."""

        return self.envelope.field_int("exists_subquery_input_row_count", 0) or 0

    @property
    def exists_subquery_filtered_row_count(self) -> int:
        """Return EXISTS-subquery rows selected before ORDER BY/LIMIT."""

        return self.envelope.field_int("exists_subquery_filtered_row_count", 0) or 0

    @property
    def exists_subquery_bounded_row_count(self) -> int:
        """Return EXISTS-subquery rows remaining after ORDER BY/LIMIT."""

        return self.envelope.field_int("exists_subquery_bounded_row_count", 0) or 0

    @property
    def exists_subquery_scan_bound(self) -> int:
        """Return the deterministic source-row scan bound for EXISTS subqueries."""

        return self.envelope.field_int("exists_subquery_scan_bound", 0) or 0

    @property
    def exists_subquery_result(self) -> tuple[bool, ...]:
        """Return the two-valued presence result for admitted EXISTS subqueries."""

        value = self.envelope.field("exists_subquery_result", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part == "true" for part in value.split(",") if part)

    @property
    def exists_subquery_null_semantics(self) -> str | None:
        """Return the EXISTS null-semantics evidence label."""

        return self.envelope.field("exists_subquery_null_semantics")

    @property
    def having_exists_subquery_runtime_execution(self) -> bool:
        """Whether HAVING used an admitted EXISTS-subquery predicate."""

        return (
            self.envelope.field_bool("having_exists_subquery_runtime_execution", False)
            is True
        )

    @property
    def in_predicate_null_semantics(self) -> str | None:
        """Return the null-semantics contract for admitted IN predicates."""

        return self.envelope.field("in_predicate_null_semantics")

    @property
    def date_extract_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted Date32 extract predicate path."""

        return self.envelope.field_bool("date_extract_runtime_execution", False) is True

    @property
    def date_extract_operator(self) -> tuple[str, ...]:
        """Return Date32 extract operators emitted by the smoke."""

        value = self.envelope.field("date_extract_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_extract_source_columns(self) -> tuple[str, ...]:
        """Return Date32 extract source columns emitted by the smoke."""

        value = self.envelope.field("date_extract_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_literal_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTC timestamp literal path."""

        return self.envelope.field_bool("timestamp_literal_runtime_execution", False) is True

    @property
    def timestamp_extract_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTC timestamp extract predicate path."""

        return self.envelope.field_bool("timestamp_extract_runtime_execution", False) is True

    @property
    def timestamp_extract_operator(self) -> tuple[str, ...]:
        """Return UTC timestamp extract operators emitted by the smoke."""

        value = self.envelope.field("timestamp_extract_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_extract_source_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp extract source columns emitted by the smoke."""

        value = self.envelope.field("timestamp_extract_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted Date32 day-arithmetic predicate path."""

        return self.envelope.field_bool("date_arithmetic_runtime_execution", False) is True

    @property
    def date_arithmetic_operator(self) -> tuple[str, ...]:
        """Return Date32 day-arithmetic operators emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_days(self) -> tuple[str, ...]:
        """Return Date32 day-arithmetic offsets emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_days", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_source_columns(self) -> tuple[str, ...]:
        """Return Date32 day-arithmetic source columns emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTC timestamp arithmetic predicate."""

        return self.envelope.field_bool("timestamp_arithmetic_runtime_execution", False) is True

    @property
    def timestamp_arithmetic_operator(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic operators emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_seconds(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic second offsets emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_seconds", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_source_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic source columns emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted string predicate leaf."""

        return self.envelope.field_bool("string_predicate_runtime_execution", False) is True

    @property
    def string_predicate_operator(self) -> tuple[str, ...]:
        """Return string predicate operators emitted by the smoke."""

        value = self.envelope.field("string_predicate_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_predicate_like_escape_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted LIKE ESCAPE predicate."""

        return (
            self.envelope.field_bool(
                "string_predicate_like_escape_runtime_execution", False
            )
            is True
        )

    @property
    def string_predicate_like_escape_character(self) -> tuple[str, ...]:
        """Return LIKE ESCAPE characters emitted by the smoke."""

        return _csv_present_values(
            self.envelope.field("string_predicate_like_escape_character")
        )

    @property
    def string_transform_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 transform predicate."""

        return self.envelope.field_bool("string_transform_runtime_execution", False) is True

    @property
    def string_transform_operator(self) -> tuple[str, ...]:
        """Return UTF-8 transform operators emitted by the smoke."""

        value = self.envelope.field("string_transform_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_transform_source_columns(self) -> tuple[str, ...]:
        """Return UTF-8 transform source columns emitted by the smoke."""

        value = self.envelope.field("string_transform_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 string function predicate."""

        return self.envelope.field_bool("string_function_runtime_execution", False) is True

    @property
    def string_function_operator(self) -> tuple[str, ...]:
        """Return UTF-8 string function predicate operators emitted by the smoke."""

        value = self.envelope.field("string_function_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by string function predicates."""

        value = self.envelope.field("string_function_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_literal_counts(self) -> tuple[int, ...]:
        """Return string literal counts used by string function predicates."""

        value = self.envelope.field("string_function_literal_count", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(int(part) for part in value.split(",") if part)

    @property
    def string_function_rhs_dtypes(self) -> tuple[str, ...]:
        """Return string function predicate right-hand literal dtypes emitted by the smoke."""

        value = self.envelope.field("string_function_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_length_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 length predicate."""

        return self.envelope.field_bool("string_length_runtime_execution", False) is True

    @property
    def string_length_source_columns(self) -> tuple[str, ...]:
        """Return UTF-8 length predicate source columns emitted by the smoke."""

        value = self.envelope.field("string_length_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_length_rhs_dtypes(self) -> tuple[str, ...]:
        """Return UTF-8 length predicate literal dtypes emitted by the smoke."""

        value = self.envelope.field("string_length_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric arithmetic predicate."""

        return self.envelope.field_bool("numeric_arithmetic_runtime_execution", False) is True

    @property
    def numeric_arithmetic_operator(self) -> tuple[str, ...]:
        """Return numeric arithmetic predicate operators emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_source_columns(self) -> tuple[str, ...]:
        """Return numeric arithmetic predicate source columns emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_rhs_dtypes(self) -> tuple[str, ...]:
        """Return numeric arithmetic predicate literal dtypes emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_abs_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric ABS predicate."""

        return self.envelope.field_bool("numeric_abs_runtime_execution", False) is True

    @property
    def numeric_abs_source_columns(self) -> tuple[str, ...]:
        """Return numeric ABS predicate source columns emitted by the smoke."""

        value = self.envelope.field("numeric_abs_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_abs_rhs_dtypes(self) -> tuple[str, ...]:
        """Return numeric ABS predicate literal dtypes emitted by the smoke."""

        value = self.envelope.field("numeric_abs_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric rounding predicate."""

        return self.envelope.field_bool("numeric_rounding_runtime_execution", False) is True

    @property
    def numeric_rounding_operators(self) -> tuple[str, ...]:
        """Return numeric rounding predicate operators emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_source_columns(self) -> tuple[str, ...]:
        """Return numeric rounding predicate source columns emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_rhs_dtypes(self) -> tuple[str, ...]:
        """Return numeric rounding predicate literal dtypes emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted generic expression predicate."""

        return (
            self.envelope.field_bool(
                "generic_expression_predicate_runtime_execution", False
            )
            is True
        )

    @property
    def generic_expression_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by generic expression predicates."""

        value = self.envelope.field("generic_expression_predicate_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_predicate_operator_families(self) -> tuple[str, ...]:
        """Return operator-family groups used by generic expression predicates."""

        value = self.envelope.field("generic_expression_predicate_operator_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_predicate_binary_operator_count(self) -> int:
        """Return the number of binary operators in generic expression predicates."""

        return (
            self.envelope.field_int(
                "generic_expression_predicate_binary_operator_count", 0
            )
            or 0
        )

    @property
    def generic_expression_predicate_comparison_operators(self) -> tuple[str, ...]:
        """Return comparison operators used by generic expression predicates."""

        value = self.envelope.field(
            "generic_expression_predicate_comparison_operator", ""
        )
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric arithmetic projection."""

        return (
            self.envelope.field_bool(
                "numeric_arithmetic_projection_runtime_execution", False
            )
            is True
        )

    @property
    def complex_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed admitted scoped complex projection values."""

        return self.envelope.field_bool("complex_projection_runtime_execution", False) is True

    @property
    def complex_projection_columns(self) -> tuple[str, ...]:
        """Return complex projection output columns emitted by the smoke."""

        value = self.envelope.field("complex_projection_columns", "")
        return _split_field_list(value)

    @property
    def complex_projection_kinds(self) -> tuple[str, ...]:
        """Return scoped complex projection kinds emitted by the smoke."""

        value = self.envelope.field("complex_projection_kind", "")
        return _split_field_list(value)

    @property
    def complex_projection_output_dtypes(self) -> tuple[str, ...]:
        """Return complex projection logical output dtypes emitted by the smoke."""

        value = self.envelope.field("complex_projection_output_dtype", "")
        return _split_field_list(value)

    @property
    def complex_projection_source_columns(self) -> tuple[str, ...]:
        """Return source columns used inside scoped complex projections."""

        value = self.envelope.field("complex_projection_source_column", "")
        return _split_field_list(value)

    @property
    def complex_projection_output_boundary(self) -> str | None:
        """Return the result/sink boundary for scoped complex projections."""

        value = self.envelope.field("complex_projection_output_boundary")
        if value in {None, "", "not_applicable", "none"}:
            return None
        return value

    @property
    def complex_projection_typed_nested_sink_formats(self) -> tuple[str, ...]:
        """Return local formats that preserve inferable nested projection columns."""

        if (
            self.complex_projection_output_boundary
            != COMPLEX_PROJECTION_TYPED_NESTED_OUTPUT_BOUNDARY
        ):
            return ()
        return ("parquet", "arrow_ipc", "avro", "vortex")

    @property
    def complex_projection_blocked_typed_nested_sink_formats(self) -> tuple[str, ...]:
        """Return local formats that still block typed nested projection preservation."""

        if (
            self.complex_projection_output_boundary
            != COMPLEX_PROJECTION_TYPED_NESTED_OUTPUT_BOUNDARY
        ):
            return ()
        return ("orc",)

    @property
    def typed_nested_child_schema_evidence_status(self) -> str | None:
        """Return the typed nested child-schema evidence posture for SQL sinks."""

        value = self.envelope.field("typed_nested_child_schema_evidence_status")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def typed_nested_child_schema_blocker(self) -> str | None:
        """Return the typed nested child-schema blocker, when present."""

        value = self.envelope.field("typed_nested_child_schema_blocker")
        if value in {None, "", "none", "not_applicable"}:
            return None
        return value

    @property
    def typed_nested_child_schema_blocked_sink_formats(self) -> tuple[str, ...]:
        """Return sinks blocked by missing typed nested child-schema evidence."""

        value = self.envelope.field("typed_nested_child_schema_blocked_sink_formats", "")
        if not value or value in {"none", "not_applicable"}:
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def cast_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted cast predicate."""

        return self.envelope.field_bool("cast_runtime_execution", False) is True

    @property
    def cast_source_columns(self) -> tuple[str, ...]:
        """Return cast predicate source columns emitted by the smoke."""

        value = self.envelope.field("cast_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def cast_target_dtypes(self) -> tuple[str, ...]:
        """Return cast predicate target dtypes emitted by the smoke."""

        value = self.envelope.field("cast_target_dtype", "")
        return _split_field_list(value)

    @property
    def cast_modes(self) -> tuple[str, ...]:
        """Return cast predicate modes emitted by the smoke."""

        value = self.envelope.field("cast_mode", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def cast_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted cast projection."""

        return self.envelope.field_bool("cast_projection_runtime_execution", False) is True

    @property
    def cast_projection_source_columns(self) -> tuple[str, ...]:
        """Return cast projection source columns emitted by the smoke."""

        value = self.envelope.field("cast_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def cast_projection_output_columns(self) -> tuple[str, ...]:
        """Return cast projection output columns emitted by the smoke."""

        value = self.envelope.field("cast_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def cast_projection_target_dtypes(self) -> tuple[str, ...]:
        """Return cast projection target dtypes emitted by the smoke."""

        value = self.envelope.field("cast_projection_target_dtype", "")
        return _split_field_list(value)

    @property
    def cast_projection_modes(self) -> tuple[str, ...]:
        """Return cast projection modes emitted by the smoke."""

        value = self.envelope.field("cast_projection_mode", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def decimal_cast_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted scoped decimal cast."""

        return self.envelope.field_bool("decimal_cast_runtime_execution", False) is True

    @property
    def decimal_cast_source_columns(self) -> tuple[str, ...]:
        """Return source columns used by admitted scoped decimal casts."""

        value = self.envelope.field("decimal_cast_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def decimal_cast_output_columns(self) -> tuple[str, ...]:
        """Return decimal cast projection output columns emitted by the smoke."""

        value = self.envelope.field("decimal_cast_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def decimal_cast_target_dtypes(self) -> tuple[str, ...]:
        """Return normalized decimal cast target dtypes emitted by the smoke."""

        value = self.envelope.field("decimal_cast_target_dtype", "")
        return _split_field_list(value)

    @property
    def decimal_cast_precisions(self) -> tuple[int, ...]:
        """Return decimal cast precisions emitted by the smoke."""

        value = self.envelope.field("decimal_cast_precision", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(int(part) for part in value.split(",") if part)

    @property
    def decimal_cast_scales(self) -> tuple[int, ...]:
        """Return decimal cast scales emitted by the smoke."""

        value = self.envelope.field("decimal_cast_scale", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(int(part) for part in value.split(",") if part)

    @property
    def decimal_cast_modes(self) -> tuple[str, ...]:
        """Return decimal cast modes emitted by the smoke."""

        value = self.envelope.field("decimal_cast_mode", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def decimal_cast_output_boundary(self) -> str | None:
        """Return the exact-result boundary for admitted scoped decimal casts."""

        value = self.envelope.field("decimal_cast_output_boundary")
        if value == "not_applicable":
            return None
        return value

    @property
    def decimal_cast_typed_decimal_sink_formats(self) -> tuple[str, ...]:
        """Return local formats that preserve scoped decimal casts as typed decimal output."""

        if self.decimal_cast_output_boundary != DECIMAL_CAST_TYPED_DECIMAL_OUTPUT_BOUNDARY:
            return ()
        return ("parquet", "arrow_ipc", "avro", "vortex")

    @property
    def decimal_cast_blocked_typed_decimal_sink_formats(self) -> tuple[str, ...]:
        """Return local formats that still block scoped typed decimal output preservation."""

        if self.decimal_cast_output_boundary != DECIMAL_CAST_TYPED_DECIMAL_OUTPUT_BOUNDARY:
            return ()
        return ("orc",)

    @property
    def null_coalesce_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted null coalesce projection."""

        return (
            self.envelope.field_bool(
                "null_coalesce_projection_runtime_execution", False
            )
            is True
        )

    @property
    def null_coalesce_projection_source_columns(self) -> tuple[str, ...]:
        """Return null coalesce projection source columns emitted by the smoke."""

        value = self.envelope.field("null_coalesce_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def null_coalesce_projection_output_columns(self) -> tuple[str, ...]:
        """Return null coalesce projection output columns emitted by the smoke."""

        value = self.envelope.field("null_coalesce_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def null_coalesce_projection_fallback_dtypes(self) -> tuple[str, ...]:
        """Return null coalesce projection fallback dtypes emitted by the smoke."""

        value = self.envelope.field("null_coalesce_projection_fallback_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def nullif_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted NULLIF projection."""

        return (
            self.envelope.field_bool("nullif_projection_runtime_execution", False)
            is True
        )

    @property
    def nullif_projection_source_columns(self) -> tuple[str, ...]:
        """Return NULLIF projection source columns emitted by the smoke."""

        value = self.envelope.field("nullif_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def nullif_projection_output_columns(self) -> tuple[str, ...]:
        """Return NULLIF projection output columns emitted by the smoke."""

        value = self.envelope.field("nullif_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def nullif_projection_sentinel_dtypes(self) -> tuple[str, ...]:
        """Return NULLIF projection sentinel dtypes emitted by the smoke."""

        value = self.envelope.field("nullif_projection_sentinel_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def conditional_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted conditional projection."""

        return (
            self.envelope.field_bool("conditional_projection_runtime_execution", False)
            is True
        )

    @property
    def conditional_projection_predicate_families(self) -> tuple[str, ...]:
        """Return conditional projection predicate families emitted by the smoke."""

        value = self.envelope.field("conditional_projection_predicate_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def conditional_projection_source_columns(self) -> tuple[str, ...]:
        """Return conditional projection source columns emitted by the smoke."""

        value = self.envelope.field("conditional_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def conditional_projection_output_columns(self) -> tuple[str, ...]:
        """Return conditional projection output columns emitted by the smoke."""

        value = self.envelope.field("conditional_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def conditional_projection_then_dtypes(self) -> tuple[str, ...]:
        """Return conditional projection THEN branch dtypes emitted by the smoke."""

        value = self.envelope.field("conditional_projection_then_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def conditional_projection_else_dtypes(self) -> tuple[str, ...]:
        """Return conditional projection ELSE branch dtypes emitted by the smoke."""

        value = self.envelope.field("conditional_projection_else_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def predicate_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted predicate projection."""

        return (
            self.envelope.field_bool("predicate_projection_runtime_execution", False)
            is True
        )

    @property
    def predicate_projection_predicate_families(self) -> tuple[str, ...]:
        """Return predicate projection predicate families emitted by the smoke."""

        value = self.envelope.field("predicate_projection_predicate_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def predicate_projection_source_columns(self) -> tuple[str, ...]:
        """Return predicate projection source columns emitted by the smoke."""

        value = self.envelope.field("predicate_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def predicate_projection_output_columns(self) -> tuple[str, ...]:
        """Return predicate projection output columns emitted by the smoke."""

        value = self.envelope.field("predicate_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def predicate_projection_null_semantics(self) -> tuple[str, ...]:
        """Return predicate projection null-semantics labels emitted by the smoke."""

        value = self.envelope.field("predicate_projection_null_semantics", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_projection_operator(self) -> tuple[str, ...]:
        """Return numeric arithmetic projection operators emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_projection_source_columns(self) -> tuple[str, ...]:
        """Return numeric arithmetic projection source columns emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_projection_output_columns(self) -> tuple[str, ...]:
        """Return numeric arithmetic projection output columns emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_arithmetic_projection_rhs_dtypes(self) -> tuple[str, ...]:
        """Return numeric arithmetic projection literal dtypes emitted by the smoke."""

        value = self.envelope.field("numeric_arithmetic_projection_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_abs_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric ABS projection."""

        return (
            self.envelope.field_bool("numeric_abs_projection_runtime_execution", False)
            is True
        )

    @property
    def numeric_abs_projection_source_columns(self) -> tuple[str, ...]:
        """Return numeric ABS projection source columns emitted by the smoke."""

        value = self.envelope.field("numeric_abs_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_abs_projection_output_columns(self) -> tuple[str, ...]:
        """Return numeric ABS projection output columns emitted by the smoke."""

        value = self.envelope.field("numeric_abs_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted numeric rounding projection."""

        return (
            self.envelope.field_bool(
                "numeric_rounding_projection_runtime_execution", False
            )
            is True
        )

    @property
    def numeric_rounding_projection_operators(self) -> tuple[str, ...]:
        """Return numeric rounding projection operators emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_projection_source_columns(self) -> tuple[str, ...]:
        """Return numeric rounding projection source columns emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def numeric_rounding_projection_output_columns(self) -> tuple[str, ...]:
        """Return numeric rounding projection output columns emitted by the smoke."""

        value = self.envelope.field("numeric_rounding_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted generic expression projection."""

        return (
            self.envelope.field_bool(
                "generic_expression_projection_runtime_execution", False
            )
            is True
        )

    @property
    def generic_expression_projection_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by generic expression projections."""

        value = self.envelope.field("generic_expression_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_projection_output_columns(self) -> tuple[str, ...]:
        """Return generic expression projection output columns emitted by the smoke."""

        value = self.envelope.field("generic_expression_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_projection_operator_families(self) -> tuple[str, ...]:
        """Return operator-family groups used by generic expression projections."""

        value = self.envelope.field("generic_expression_projection_operator_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def generic_expression_projection_binary_operator_count(self) -> int:
        """Return the number of binary operators in generic expression projections."""

        return (
            self.envelope.field_int(
                "generic_expression_projection_binary_operator_count", 0
            )
            or 0
        )

    @property
    def date_arithmetic_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted Date32 day arithmetic projection."""

        return (
            self.envelope.field_bool(
                "date_arithmetic_projection_runtime_execution", False
            )
            is True
        )

    @property
    def date_arithmetic_projection_operator(self) -> tuple[str, ...]:
        """Return Date32 day arithmetic projection operators emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_projection_days(self) -> tuple[str, ...]:
        """Return Date32 day arithmetic projection day counts emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_projection_days", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_projection_source_columns(self) -> tuple[str, ...]:
        """Return Date32 day arithmetic projection source columns emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_arithmetic_projection_output_columns(self) -> tuple[str, ...]:
        """Return Date32 day arithmetic projection output columns emitted by the smoke."""

        value = self.envelope.field("date_arithmetic_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTC timestamp arithmetic projection."""

        return (
            self.envelope.field_bool(
                "timestamp_arithmetic_projection_runtime_execution", False
            )
            is True
        )

    @property
    def timestamp_arithmetic_projection_operator(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic projection operators emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_projection_seconds(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic projection second counts emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_projection_seconds", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_projection_source_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic projection source columns emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_arithmetic_projection_output_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp arithmetic projection output columns emitted by the smoke."""

        value = self.envelope.field("timestamp_arithmetic_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_transform_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 transform projection."""

        return (
            self.envelope.field_bool("string_transform_projection_runtime_execution", False)
            is True
        )

    @property
    def string_transform_projection_operator(self) -> tuple[str, ...]:
        """Return UTF-8 transform projection operators emitted by the smoke."""

        value = self.envelope.field("string_transform_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_transform_projection_source_columns(self) -> tuple[str, ...]:
        """Return UTF-8 transform projection source columns emitted by the smoke."""

        value = self.envelope.field("string_transform_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_transform_projection_output_columns(self) -> tuple[str, ...]:
        """Return UTF-8 transform projection output columns emitted by the smoke."""

        value = self.envelope.field("string_transform_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_length_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 length projection."""

        return (
            self.envelope.field_bool("string_length_projection_runtime_execution", False)
            is True
        )

    @property
    def string_length_projection_source_columns(self) -> tuple[str, ...]:
        """Return UTF-8 length projection source columns emitted by the smoke."""

        value = self.envelope.field("string_length_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_length_projection_output_columns(self) -> tuple[str, ...]:
        """Return UTF-8 length projection output columns emitted by the smoke."""

        value = self.envelope.field("string_length_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTF-8 string function projection."""

        return (
            self.envelope.field_bool("string_function_projection_runtime_execution", False)
            is True
        )

    @property
    def string_function_projection_operator(self) -> tuple[str, ...]:
        """Return UTF-8 string function projection operators emitted by the smoke."""

        value = self.envelope.field("string_function_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_projection_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by string function projections."""

        value = self.envelope.field("string_function_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_projection_output_columns(self) -> tuple[str, ...]:
        """Return UTF-8 string function projection output columns emitted by the smoke."""

        value = self.envelope.field("string_function_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def string_function_projection_literal_counts(self) -> tuple[int, ...]:
        """Return string literal counts used by string function projections."""

        value = self.envelope.field("string_function_projection_literal_count", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(int(part) for part in value.split(",") if part)

    @property
    def binary_helper_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted binary helper projection."""

        return (
            self.envelope.field_bool("binary_helper_projection_runtime_execution", False)
            is True
        )

    @property
    def binary_helper_projection_operator(self) -> tuple[str, ...]:
        """Return binary helper projection operators emitted by the smoke."""

        value = self.envelope.field("binary_helper_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_projection_source_columns(self) -> tuple[str, ...]:
        """Return binary helper projection source columns emitted by the smoke."""

        value = self.envelope.field("binary_helper_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_projection_output_columns(self) -> tuple[str, ...]:
        """Return binary helper projection output columns emitted by the smoke."""

        value = self.envelope.field("binary_helper_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_projection_output_dtype(self) -> str | None:
        """Return the admitted output dtype for binary helper projections."""

        value = self.envelope.field("binary_helper_projection_output_dtype")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def binary_helper_projection_null_semantics(self) -> str | None:
        """Return null-semantics evidence for binary helper projections."""

        value = self.envelope.field("binary_helper_projection_null_semantics")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def binary_byte_length_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted binary byte-length projection."""

        return (
            self.envelope.field_bool(
                "binary_byte_length_projection_runtime_execution", False
            )
            is True
        )

    @property
    def binary_byte_length_projection_argument_family(self) -> tuple[str, ...]:
        """Return binary expression families used by byte-length projections."""

        value = self.envelope.field("binary_byte_length_projection_argument_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_projection_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by byte-length projections."""

        value = self.envelope.field("binary_byte_length_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_projection_output_columns(self) -> tuple[str, ...]:
        """Return byte-length projection output columns emitted by the smoke."""

        value = self.envelope.field("binary_byte_length_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_projection_output_dtype(self) -> str | None:
        """Return the output dtype for byte-length projections."""

        value = self.envelope.field("binary_byte_length_projection_output_dtype")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def binary_byte_length_projection_null_semantics(self) -> str | None:
        """Return null-semantics evidence for byte-length projections."""

        value = self.envelope.field("binary_byte_length_projection_null_semantics")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def binary_helper_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted binary helper predicate."""

        return (
            self.envelope.field_bool("binary_helper_predicate_runtime_execution", False)
            is True
        )

    @property
    def binary_helper_predicate_operator(self) -> tuple[str, ...]:
        """Return binary helper predicate operators emitted by the smoke."""

        value = self.envelope.field("binary_helper_predicate_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_predicate_comparison_operator(self) -> tuple[str, ...]:
        """Return comparison operators used by binary helper predicates."""

        value = self.envelope.field("binary_helper_predicate_comparison_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source columns decoded by binary helper predicates."""

        value = self.envelope.field("binary_helper_predicate_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_predicate_literal_hex_values(self) -> tuple[str, ...]:
        """Return hex payloads used by binary helper predicate literals."""

        value = self.envelope.field("binary_helper_predicate_literal_hex_value", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_helper_predicate_null_semantics(self) -> str | None:
        """Return null-semantics evidence for binary helper predicates."""

        value = self.envelope.field("binary_helper_predicate_null_semantics")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def binary_byte_length_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted binary byte-length predicate."""

        return (
            self.envelope.field_bool(
                "binary_byte_length_predicate_runtime_execution", False
            )
            is True
        )

    @property
    def binary_byte_length_predicate_argument_family(self) -> tuple[str, ...]:
        """Return binary expression families used by byte-length predicates."""

        value = self.envelope.field("binary_byte_length_predicate_argument_family", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_predicate_comparison_operator(self) -> tuple[str, ...]:
        """Return comparison operators used by byte-length predicates."""

        value = self.envelope.field(
            "binary_byte_length_predicate_comparison_operator", ""
        )
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source-column groups used by byte-length predicates."""

        value = self.envelope.field("binary_byte_length_predicate_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_predicate_rhs_dtypes(self) -> tuple[str, ...]:
        """Return RHS dtypes used by byte-length predicates."""

        value = self.envelope.field("binary_byte_length_predicate_rhs_dtype", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def binary_byte_length_predicate_null_semantics(self) -> str | None:
        """Return null-semantics evidence for byte-length predicates."""

        value = self.envelope.field("binary_byte_length_predicate_null_semantics")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def date_extract_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted Date32 extract projection."""

        return (
            self.envelope.field_bool("date_extract_projection_runtime_execution", False)
            is True
        )

    @property
    def date_extract_projection_operator(self) -> tuple[str, ...]:
        """Return Date32 extract projection operators emitted by the smoke."""

        value = self.envelope.field("date_extract_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_extract_projection_source_columns(self) -> tuple[str, ...]:
        """Return Date32 extract projection source columns emitted by the smoke."""

        value = self.envelope.field("date_extract_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def date_extract_projection_output_columns(self) -> tuple[str, ...]:
        """Return Date32 extract projection output columns emitted by the smoke."""

        value = self.envelope.field("date_extract_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_extract_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted UTC timestamp extract projection."""

        return (
            self.envelope.field_bool(
                "timestamp_extract_projection_runtime_execution", False
            )
            is True
        )

    @property
    def timestamp_extract_projection_operator(self) -> tuple[str, ...]:
        """Return UTC timestamp extract projection operators emitted by the smoke."""

        value = self.envelope.field("timestamp_extract_projection_operator", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_extract_projection_source_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp extract projection source columns emitted by the smoke."""

        value = self.envelope.field("timestamp_extract_projection_source_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def timestamp_extract_projection_output_columns(self) -> tuple[str, ...]:
        """Return UTC timestamp extract projection output columns emitted by the smoke."""

        value = self.envelope.field("timestamp_extract_projection_output_column", "")
        if not value or value == "not_applicable":
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def output_io_performed(self) -> bool:
        """Whether the smoke wrote a local output file."""

        return self.envelope.field_bool("output_io_performed", False) is True

    @property
    def output_native_io_certificate_status(self) -> str | None:
        """Return the output Native I/O certificate status, when present."""

        return self.envelope.field("output_native_io_certificate_status")

    @property
    def result_replay_verified(self) -> bool:
        """Whether admitted local output artifacts were replay/digest verified."""

        return self.envelope.field_bool("result_replay_verified", False) is True

    @property
    def output_replay_status(self) -> str | None:
        """Return the local output replay status."""

        return self.envelope.field("output_replay_status")

    @property
    def output_replay_millis(self) -> int | None:
        """Return local output replay verification time in milliseconds."""

        return self.envelope.field_int("output_replay_millis")

    @property
    def output_fidelity_report_status(self) -> str | None:
        """Return the scoped local output fidelity-report status."""

        return self.envelope.field("output_fidelity_report_status")

    @property
    def output_fidelity_loss(self) -> tuple[str, ...]:
        """Return `format:loss` entries for local output fidelity limits."""

        return _csv_values(self.envelope.field("output_fidelity_loss"))

    @property
    def result_batch_state_status(self) -> str | None:
        """Return the local SQL result batch-state status."""

        return self.envelope.field("result_batch_state_status")

    @property
    def result_batch_state_digest(self) -> str | None:
        """Return the local SQL result batch-state digest."""

        return self.envelope.field("result_batch_state_digest")

    @property
    def result_batch_state_layout(self) -> str | None:
        """Return the local SQL result batch-state layout label."""

        return self.envelope.field("result_batch_state_layout")

    @property
    def result_batch_state_row_count(self) -> int | None:
        """Return the local SQL result batch-state row count."""

        return self.envelope.field_int("result_batch_state_row_count")

    @property
    def result_batch_state_column_count(self) -> int | None:
        """Return the local SQL result batch-state column count."""

        return self.envelope.field_int("result_batch_state_column_count")

    @property
    def result_batch_state_materialization_required(self) -> str | None:
        """Return why terminal materialization remains required, when reported."""

        return self.envelope.field("result_batch_state_materialization_required")

    @property
    def result_batch_state_decode_required(self) -> bool:
        """Whether the local SQL result batch-state required decode."""

        return self.envelope.field_bool("result_batch_state_decode_required", False) is True

    @property
    def result_batch_state_build_millis(self) -> int | None:
        """Return result batch-state construction time in milliseconds."""

        return self.envelope.field_int("result_batch_state_build_millis")

    @property
    def output_plan_materialization_required(self) -> str | None:
        """Return the sink-driven OutputPlan materialization requirement."""

        return self.envelope.field("output_plan_materialization_required")

    @property
    def output_plan_required_columns(self) -> tuple[str, ...]:
        """Return result columns required by requested local sinks."""

        return _csv_output_plan_required_columns(
            self.envelope.field("output_plan_required_columns")
        )

    @property
    def output_plan_ordering_required(self) -> str | None:
        """Return whether requested sinks require ordering."""

        return self.envelope.field("output_plan_ordering_required")

    @property
    def output_plan_statistics_required(self) -> str | None:
        """Return sink statistics/replay requirements."""

        return self.envelope.field("output_plan_statistics_required")

    @property
    def output_plan_text_materialization_boundary(self) -> str | None:
        """Return the terminal text-materialization boundary, when any."""

        return self.envelope.field("output_plan_text_materialization_boundary")

    @property
    def output_plan_conversion_blocker(self) -> str | None:
        """Return the deterministic OutputPlan conversion blocker, if any."""

        return self.envelope.field("output_plan_conversion_blocker")

    @property
    def output_plan_type_nullability_support(self) -> str | None:
        """Return sink type/nullability support posture."""

        return self.envelope.field("output_plan_type_nullability_support")

    @property
    def output_plan_dictionary_required(self) -> str | None:
        """Return sink dictionary requirement posture."""

        return self.envelope.field("output_plan_dictionary_required")

    @property
    def output_plan_compression_encoding_posture(self) -> str | None:
        """Return sink compression/encoding posture."""

        return self.envelope.field("output_plan_compression_encoding_posture")

    @property
    def output_plan_replay_depth(self) -> str | None:
        """Return sink replay depth required by the OutputPlan."""

        return self.envelope.field("output_plan_replay_depth")

    @property
    def output_layout_write_advisor_status(self) -> str | None:
        """Return output layout/write advisor status."""

        return self.envelope.field("output_layout_write_advisor_status")

    @property
    def output_layout_write_advisor_selected_strategy(self) -> str | None:
        """Return the selected output layout/write advisor strategy."""

        return self.envelope.field("output_layout_write_advisor_selected_strategy")

    @property
    def output_layout_write_advisor_runtime_decision_applied(self) -> bool:
        """Whether the output layout/write advisor applied a runtime writer decision."""

        return (
            self.envelope.field_bool(
                "output_layout_write_advisor_runtime_decision_applied", False
            )
            is True
        )

    @property
    def output_metadata_preservation_map(self) -> str | None:
        """Return per-sink metadata preservation accounting."""

        return self.envelope.field("output_metadata_preservation_map")

    @property
    def output_metadata_loss(self) -> str | None:
        """Return per-sink metadata loss accounting."""

        return self.envelope.field("output_metadata_loss")

    @property
    def fanout_conversion_dag_status(self) -> str | None:
        """Return the shared fanout conversion DAG status."""

        return self.envelope.field("fanout_conversion_dag_status")

    @property
    def fanout_shared_stage_count(self) -> int | None:
        """Return the number of shared conversion DAG stages."""

        return self.envelope.field_int("fanout_shared_stage_count")

    @property
    def fanout_terminal_sink_count(self) -> int | None:
        """Return the number of terminal sinks in the conversion DAG."""

        return self.envelope.field_int("fanout_terminal_sink_count")

    @property
    def fanout_shared_conversion_millis(self) -> int | None:
        """Return shared fanout conversion time in milliseconds."""

        return self.envelope.field_int("fanout_shared_conversion_millis")

    @property
    def fanout_terminal_conversion_millis(self) -> int | None:
        """Return terminal sink conversion time in milliseconds."""

        return self.envelope.field_int("fanout_terminal_conversion_millis")

    @property
    def fanout_duplicate_conversion_avoided(self) -> bool:
        """Whether the shared DAG avoided duplicate conversion work."""

        return self.envelope.field_bool("fanout_duplicate_conversion_avoided", False) is True

    @property
    def output_capillary_status(self) -> str | None:
        """Return output capillary scheduling status."""

        return self.envelope.field("output_capillary_status")

    @property
    def output_capillary_task_roles(self) -> str | None:
        """Return typed output capillary task roles."""

        return self.envelope.field("output_capillary_task_roles")

    @property
    def output_capillary_window_count(self) -> int | None:
        """Return the number of output capillary execution windows."""

        return self.envelope.field_int("output_capillary_window_count")

    @property
    def output_sink_pressure_status(self) -> str | None:
        """Return output sink-pressure control status."""

        return self.envelope.field("output_sink_pressure_status")

    @property
    def output_memory_pressure_status(self) -> str | None:
        """Return output memory-pressure control status."""

        return self.envelope.field("output_memory_pressure_status")

    @property
    def pulseweave_output_policy_applied(self) -> bool:
        """Whether PulseWeave output policy was applied."""

        return self.envelope.field_bool("pulseweave_output_policy_applied", False) is True

    @property
    def output_conversion_millis(self) -> int | None:
        """Return aggregate local output conversion time in milliseconds."""

        return self.envelope.field_int("output_conversion_millis")

    @property
    def sink_artifact_conversion_millis(self) -> str | None:
        """Return primary or labeled local sink conversion timing."""

        return self.envelope.field("sink_artifact_conversion_millis")

    @property
    def sink_artifact_count(self) -> int:
        """Return the number of local SQL sink artifacts."""

        return self.envelope.field_int("sink_artifact_count", 0) or 0

    @property
    def sink_artifact_ref(self) -> str | None:
        """Return the primary or labeled local SQL sink artifact ref."""

        value = self.envelope.field("sink_artifact_ref")
        if value in {None, "", "not_requested", "not_applicable"}:
            return None
        return value

    @property
    def sink_artifact_refs(self) -> tuple[str, ...]:
        """Return `format:path` refs for local SQL sink artifacts."""

        return _csv_reference_values(self.envelope.field("sink_artifact_refs"))

    @property
    def sink_artifact_digest(self) -> str | None:
        """Return the primary or labeled local SQL sink artifact digest."""

        value = self.envelope.field("sink_artifact_digest")
        if value in {None, "", "not_requested", "not_applicable"}:
            return None
        return value

    @property
    def sink_artifact_digests(self) -> tuple[str, ...]:
        """Return `format:digest` entries for local SQL sink artifacts."""

        return _csv_reference_values(self.envelope.field("sink_artifact_digests"))

    @property
    def sink_artifact_manifest_status(self) -> str | None:
        """Return replay-backed manifest status for local SQL sink artifacts."""

        return self.envelope.field("sink_artifact_manifest_status")

    @property
    def vortex_output_runtime_execution(self) -> bool:
        """Whether this local-source write used the scoped local Vortex sink."""

        return self.envelope.field_bool("vortex_output_runtime_execution", False) is True

    @property
    def vortex_output_reopen_verified(self) -> bool:
        """Whether any local-source Vortex output was reopened for row-count proof."""

        return self.envelope.field_bool("vortex_output_reopen_verified", False) is True

    @property
    def vortex_artifact_digest(self) -> str | None:
        """Return the first local-source Vortex artifact digest when present."""

        value = self.envelope.field("vortex_artifact_digest")
        if value in {None, "", "not_applicable"}:
            return None
        return value

    @property
    def vortex_output_row_count(self) -> int | None:
        """Return the first local-source Vortex output row count when present."""

        return self.envelope.field_int("vortex_output_row_count")

    @property
    def upstream_vortex_write_called(self) -> bool:
        """Whether the scoped upstream Vortex writer boundary was invoked."""

        return self.envelope.field_bool("upstream_vortex_write_called", False) is True

    @property
    def upstream_vortex_scan_called(self) -> bool:
        """Whether the scoped upstream Vortex reopen/scan proof was invoked."""

        return self.envelope.field_bool("upstream_vortex_scan_called", False) is True

    @property
    def output_fanout_performed(self) -> bool:
        """Whether the smoke wrote more than one local fanout output."""

        return self.envelope.field_bool("output_fanout_performed", False) is True

    @property
    def fanout_output_count(self) -> int:
        """Return the number of local fanout outputs written by the smoke."""

        return self.envelope.field_int("fanout_output_count", 0) or 0

    @property
    def fanout_output_formats(self) -> tuple[str, ...]:
        """Return the fanout output formats, excluding the primary output."""

        return _csv_values(self.envelope.field("fanout_output_formats"))

    @property
    def fanout_output_paths(self) -> tuple[str, ...]:
        """Return the fanout output paths, excluding the primary output."""

        return _csv_values(self.envelope.field("fanout_output_paths"))

    @property
    def fanout_output_digests(self) -> tuple[str, ...]:
        """Return `format:digest` entries for fanout outputs."""

        return _csv_values(self.envelope.field("fanout_output_digests"))

    @property
    def fanout_output_workspace_path_safety_statuses(self) -> tuple[str, ...]:
        """Return `format:accepted` entries for fanout workspace path safety."""

        return _csv_values(
            self.envelope.field("fanout_output_workspace_path_safety_statuses")
        )

    @property
    def fanout_output_commit_modes(self) -> tuple[str, ...]:
        """Return `format:commit_mode` entries for fanout outputs."""

        return _csv_values(self.envelope.field("fanout_output_commit_modes"))

    @property
    def fanout_output_native_io_certificate_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for fanout output certificates."""

        return _csv_values(
            self.envelope.field("fanout_output_native_io_certificate_statuses")
        )

    @property
    def fanout_output_replay_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for fanout replay verification."""

        return _csv_values(self.envelope.field("fanout_output_replay_statuses"))

    @property
    def fanout_output_conversion_millis(self) -> int | None:
        """Return aggregate local fanout conversion time in milliseconds."""

        return self.envelope.field_int("fanout_output_conversion_millis")

    @property
    def fanout_output_fidelity_statuses(self) -> tuple[str, ...]:
        """Return `format:status` entries for fanout fidelity reports."""

        return _csv_values(self.envelope.field("fanout_output_fidelity_statuses"))

    @property
    def fanout_output_fidelity_loss(self) -> tuple[str, ...]:
        """Return `format:loss` entries for fanout fidelity limits."""

        return _csv_values(self.envelope.field("fanout_output_fidelity_loss"))

    @property
    def fanout_result_reuse_hit(self) -> bool:
        """Whether the smoke reused the computed result for local fanout outputs."""

        return self.envelope.field_bool("fanout_result_reuse_hit", False) is True

    @property
    def aggregate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted aggregate path."""

        return self.envelope.field_bool("aggregate_runtime_execution", False) is True

    @property
    def aggregate_operator_family(self) -> str | None:
        """Return the aggregate operator family label when present."""

        return self.envelope.field("aggregate_operator_family")

    @property
    def aggregate_functions(self) -> tuple[str, ...]:
        """Return aggregate function labels emitted by the smoke."""

        value = self.envelope.field("aggregate_functions", "")
        if not value:
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def aggregate_output_columns(self) -> tuple[str, ...]:
        """Return aggregate output column names emitted by the smoke."""

        value = self.envelope.field("aggregate_output_columns")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def aggregate_alias_runtime_execution(self) -> bool:
        """Whether this smoke executed aggregate output aliases."""

        return self.envelope.field_bool("aggregate_alias_runtime_execution", False) is True

    @property
    def aggregate_aliases(self) -> tuple[str, ...]:
        """Return explicit aggregate aliases emitted by the smoke."""

        value = self.envelope.field("aggregate_aliases")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def distinct_aggregate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted DISTINCT aggregate."""

        return (
            self.envelope.field_bool("distinct_aggregate_runtime_execution", False)
            is True
        )

    @property
    def distinct_aggregate_functions(self) -> tuple[str, ...]:
        """Return DISTINCT aggregate function labels emitted by the smoke."""

        value = self.envelope.field("distinct_aggregate_function")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def distinct_aggregate_columns(self) -> tuple[str, ...]:
        """Return DISTINCT aggregate source columns emitted by the smoke."""

        value = self.envelope.field("distinct_aggregate_column")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def distinct_aggregate_null_semantics(self) -> str | None:
        """Return DISTINCT aggregate null-semantics evidence."""

        value = self.envelope.field("distinct_aggregate_null_semantics")
        if value == "not_applicable":
            return None
        return value

    @property
    def group_by_runtime_execution(self) -> bool:
        """Whether this smoke executed the admitted grouped aggregate path."""

        return self.envelope.field_bool("group_by_runtime_execution", False) is True

    @property
    def group_by_columns(self) -> tuple[str, ...]:
        """Return group-by columns emitted by the smoke."""

        value = self.envelope.field("group_by_columns", "")
        if not value:
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def group_by_key_arity(self) -> int:
        """Return the number of columns in the grouped aggregate key."""

        return self.envelope.field_int("group_by_key_arity", 0) or 0

    @property
    def group_by_multi_key_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted multi-key grouped aggregate path."""

        return self.envelope.field_bool("group_by_multi_key_runtime_execution", False) is True

    @property
    def group_by_group_count(self) -> int:
        """Return the number of groups emitted before the result limit."""

        return self.envelope.field_int("group_by_group_count", 0) or 0

    @property
    def having_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted post-aggregate HAVING predicate."""

        return self.envelope.field_bool("having_runtime_execution", False) is True

    @property
    def having_operator_family(self) -> str | None:
        """Return the HAVING predicate operator family when present."""

        value = self.envelope.field("having_operator_family")
        if value == "not_applicable":
            return None
        return value

    @property
    def having_source_columns(self) -> tuple[str, ...]:
        """Return aggregate output columns referenced by HAVING."""

        value = self.envelope.field("having_source_column")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def having_aggregate_runtime_execution(self) -> bool:
        """Whether HAVING evaluated an unprojected aggregate function."""

        return (
            self.envelope.field_bool("having_aggregate_runtime_execution", False)
            is True
        )

    @property
    def having_aggregate_functions(self) -> tuple[str, ...]:
        """Return unprojected aggregate functions evaluated for HAVING."""

        value = self.envelope.field("having_aggregate_function", "not_applicable")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def having_aggregate_output_columns(self) -> tuple[str, ...]:
        """Return hidden aggregate columns used only for HAVING evaluation."""

        value = self.envelope.field("having_aggregate_output_column", "not_applicable")
        if value == "not_applicable":
            return ()
        return _csv_values(value)

    @property
    def having_input_row_count(self) -> int:
        """Return aggregate rows evaluated by HAVING before the result limit."""

        return self.envelope.field_int("having_input_row_count", 0) or 0

    @property
    def having_selected_row_count(self) -> int:
        """Return aggregate rows retained by HAVING before the result limit."""

        return self.envelope.field_int("having_selected_row_count", 0) or 0

    @property
    def order_by_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted ORDER BY path."""

        return self.envelope.field_bool("order_by_runtime_execution", False) is True

    @property
    def top_n_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted top-N path."""

        return self.envelope.field_bool("top_n_runtime_execution", False) is True

    @property
    def sort_keys(self) -> tuple[str, ...]:
        """Return sort key columns emitted by the smoke."""

        value = self.envelope.field("sort_keys", "")
        if not value:
            return ()
        return tuple(part for part in value.split(",") if part)

    @property
    def sort_direction(self) -> str | None:
        """Return the sort direction emitted by the smoke."""

        return self.envelope.field("sort_direction")

    @property
    def sort_null_ordering(self) -> str | None:
        """Return the scoped null-ordering policy emitted by the smoke."""

        return self.envelope.field("sort_null_ordering")

    @property
    def top_n_limit(self) -> int:
        """Return the top-N limit emitted by the smoke."""

        return self.envelope.field_int("top_n_limit", 0) or 0

    @property
    def join_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted join path."""

        return self.envelope.field_bool("join_runtime_execution", False) is True

    @property
    def join_type(self) -> str | None:
        """Return the scoped join type emitted by the smoke."""

        return self.envelope.field("join_type")

    @property
    def join_on_predicate_runtime_execution(self) -> bool:
        """Whether this smoke executed a non-equi/expression ON predicate."""

        return (
            self.envelope.field_bool("join_on_predicate_runtime_execution", False)
            is True
        )

    @property
    def join_on_predicate_operator_family(self) -> str | None:
        """Return the expression ON predicate operator family emitted by the smoke."""

        return self.envelope.field("join_on_predicate_operator_family")

    @property
    def join_on_predicate_source_columns(self) -> tuple[str, ...]:
        """Return source columns referenced by the expression ON predicate."""

        return _csv_values(self.envelope.field("join_on_predicate_source_column", ""))

    @property
    def join_left_key(self) -> str | None:
        """Return the left join key emitted by the smoke."""

        return self.envelope.field("join_left_key")

    @property
    def join_right_key(self) -> str | None:
        """Return the right join key emitted by the smoke."""

        return self.envelope.field("join_right_key")

    @property
    def join_left_keys(self) -> tuple[str, ...]:
        """Return all left join keys emitted by the smoke."""

        return _csv_values(self.envelope.field("join_left_keys"))

    @property
    def join_right_keys(self) -> tuple[str, ...]:
        """Return all right join keys emitted by the smoke."""

        return _csv_values(self.envelope.field("join_right_keys"))

    @property
    def join_key_arity(self) -> int:
        """Return the number of equi-join key pairs emitted by the smoke."""

        return self.envelope.field_int("join_key_arity", 0) or 0

    @property
    def join_multi_key_runtime_execution(self) -> bool:
        """Whether this smoke executed a multi-key equi-join path."""

        return self.envelope.field_bool("join_multi_key_runtime_execution", False) is True

    @property
    def join_matched_row_count(self) -> int:
        """Return the number of matched join rows before filtering."""

        return self.envelope.field_int("join_matched_row_count", 0) or 0

    @property
    def join_candidate_row_count(self) -> int:
        """Return the number of join candidate pairs evaluated."""

        return self.envelope.field_int("join_candidate_row_count", 0) or 0

    @property
    def join_unmatched_left_row_count(self) -> int:
        """Return the number of unmatched left rows emitted by an outer/anti join."""

        return self.envelope.field_int("join_unmatched_left_row_count", 0) or 0

    @property
    def join_unmatched_right_row_count(self) -> int:
        """Return the number of unmatched right rows emitted by a right/full outer join."""

        return self.envelope.field_int("join_unmatched_right_row_count", 0) or 0

    @property
    def join_rows_output(self) -> int:
        """Return the number of output rows emitted by the join smoke."""

        return self.envelope.field_int("join_rows_output", 0) or 0

    @property
    def join_memory_estimate_bytes(self) -> int:
        """Return the scoped in-memory join estimate emitted by the smoke."""

        return self.envelope.field_int("join_memory_estimate_bytes", 0) or 0

    @property
    def join_computed_projection_runtime_execution(self) -> bool:
        """Whether this smoke executed computed projections over joined rows."""

        return (
            self.envelope.field_bool(
                "join_computed_projection_runtime_execution", False
            )
            is True
        )

    @property
    def join_order_by_top_n_runtime_execution(self) -> bool:
        """Whether this smoke executed top-N ordering over joined rows."""

        return self.envelope.field_bool("join_order_by_top_n_runtime_execution", False) is True

    @property
    def join_projection_operator_family(self) -> str | None:
        """Return the join projection operator-family label when present."""

        return self.envelope.field("join_projection_operator_family")

    @property
    def join_aggregate_runtime_execution(self) -> bool:
        """Whether this smoke executed an admitted join aggregate path."""

        return self.envelope.field_bool("join_aggregate_runtime_execution", False) is True

    @property
    def join_aggregate_operator_family(self) -> str | None:
        """Return the join aggregate operator family label when present."""

        return self.envelope.field("join_aggregate_operator_family")

    @property
    def join_aggregate_group_count(self) -> int:
        """Return the number of grouped join aggregate rows emitted before the limit."""

        return self.envelope.field_int("join_aggregate_group_count", 0) or 0

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

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return the compact evidence summary for this scoped SQL smoke."""

        return self.envelope.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return the compact claim summary for this scoped SQL smoke."""

        return self.envelope.claim_summary


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

        return tuple(
            envelope.field("public_workflow_resolved_internal_command") or envelope.command
            for envelope in self.envelopes
        )

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
            envelope.field("public_workflow_resolved_internal_command") or envelope.command
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
class SessionCacheSmokeReport:
    """Typed view over the scoped GAR-4L/5I CLI session-cache smoke."""

    envelope: OutputEnvelope

    @property
    def session_id(self) -> str:
        """Return the scoped session identifier."""

        return _required_field(self.envelope, "session_id")

    @property
    def session_runtime_status(self) -> str:
        """Return the session-cache runtime status."""

        return _required_field(self.envelope, "session_runtime_status")

    @property
    def cache_artifact_order(self) -> tuple[str, ...]:
        """Return cache artifact families represented by this smoke."""

        return _csv_values(self.envelope.field("cache_artifact_order"))

    @property
    def invalidation_reason_order(self) -> tuple[str, ...]:
        """Return invalidation reasons exercised by this smoke."""

        return _csv_values(self.envelope.field("invalidation_reason_order"))

    @property
    def cache_hit_count(self) -> int:
        """Return the cache-hit count."""

        return self.envelope.field_int("cache_hit_count", 0) or 0

    @property
    def cache_miss_count(self) -> int:
        """Return the cache-miss count."""

        return self.envelope.field_int("cache_miss_count", 0) or 0

    @property
    def invalidation_count(self) -> int:
        """Return the invalidation count."""

        return self.envelope.field_int("invalidation_count", 0) or 0

    @property
    def buffer_reuse_count(self) -> int:
        """Return the scoped scratch-buffer reuse count."""

        return self.envelope.field_int("buffer_reuse_count", 0) or 0

    @property
    def source_state_id(self) -> str:
        """Return the SourceState id represented by the smoke."""

        return _required_field(self.envelope, "source_state_id")

    @property
    def prepared_state_id(self) -> str:
        """Return the prepared-state id represented by the smoke."""

        return _required_field(self.envelope, "prepared_state_id")

    @property
    def output_plan_id(self) -> str:
        """Return the OutputPlan id represented by the smoke."""

        return _required_field(self.envelope, "output_plan_id")

    @property
    def lifecycle_closed_and_cleaned(self) -> bool:
        """Whether explicit close and cleanup completed."""

        return self.envelope.field_bool("lifecycle_closed_and_cleaned", False) is True

    @property
    def no_fallback_no_external_engine(self) -> bool:
        """Whether the smoke preserved no fallback and no external engine execution."""

        return (
            self.envelope.field_bool("fallback_attempted", True) is False
            and self.envelope.field_bool("fallback_execution_allowed", True) is False
            and self.envelope.field_bool("external_engine_invoked", True) is False
            and self.envelope.field_bool("no_fallback_no_external_engine", False) is True
        )

    @property
    def optimizer_trace_id(self) -> str:
        """Return the linked optimizer trace id."""

        return _required_field(self.envelope, "optimizer_trace_id")

    def optimizer_rule_status(self, rule_id: str) -> str:
        """Return one linked optimizer rule status."""

        normalized = rule_id.strip().lower().replace("-", "_")
        return _required_field(self.envelope, f"optimizer_rule_{normalized}_status")


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
class PreparedVortexScanPushdownRow:
    """One prepared/native Vortex Scan pushdown capability row."""

    row_id: str
    scenario: str
    pushdown_status: str
    filter_required: bool
    projection_required: bool
    limit_required: bool
    filter_pushed_down: bool
    projection_pushed_down: bool
    limit_pushed_down: bool
    filter_status: str
    projection_status: str
    limit_status: str
    residual_limit_status: str
    residual_limit_executor: str
    filter_columns_read: tuple[str, ...]
    output_columns_read: tuple[str, ...]
    filter_only_columns_read: tuple[str, ...]
    blocker_id: str
    blocker_reason: str
    benchmark_refs: tuple[str, ...]
    claim_gate_status: str
    claim_boundary: str
    fallback_attempted: bool
    external_engine_invoked: bool


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
class PublicWorkflowRoute:
    """Typed view over the side-effect-free public workflow route envelope."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the public workflow route schema version."""

        return _required_field(self.envelope, "public_workflow_route_schema_version")

    @property
    def route_id(self) -> str:
        """Return the resolved public route identifier."""

        return _required_field(self.envelope, "route_id")

    @property
    def route_status(self) -> str:
        """Return the route admission status."""

        return _required_field(self.envelope, "route_status")

    @property
    def resolved_internal_command(self) -> str:
        """Return the internal ShardLoom command selected for admitted execution."""

        return _required_field(self.envelope, "resolved_internal_command")

    @property
    def surface(self) -> str:
        """Return the public surface that requested the route."""

        return _required_field(self.envelope, "surface")

    @property
    def start_state(self) -> str:
        """Return the declared route start state."""

        return _required_field(self.envelope, "start_state")

    @property
    def vortex_normalization_point(self) -> str:
        """Return the route's Vortex normalization boundary."""

        return _required_field(self.envelope, "vortex_normalization_point")

    @property
    def execution_mode(self) -> str:
        """Return the internal execution mode selected by the route."""

        return _required_field(self.envelope, "execution_mode")

    @property
    def preparation_included(self) -> bool:
        """Whether compatibility preparation is part of the route."""

        return self.envelope.field_bool("preparation_included", False) is True

    @property
    def query_timing_starts_after_preparation(self) -> bool:
        """Whether query timing starts after preparation for this route."""

        return (
            self.envelope.field_bool("query_timing_starts_after_preparation", False)
            is True
        )

    @property
    def fanout_output_count(self) -> int:
        """Return the number of fanout outputs declared on the route."""

        return self.envelope.field_int("fanout_output_count", 0) or 0

    @property
    def fanout_outputs(self) -> tuple[str, ...]:
        """Return declared fanout targets as ``format=path`` entries."""

        value = self.envelope.field("fanout_outputs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_primitive(self) -> str | None:
        """Return the declared native Vortex primitive payload, if present."""

        value = self.envelope.field("vortex_primitive")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_predicate(self) -> str | None:
        """Return the declared native Vortex predicate payload, if present."""

        value = self.envelope.field("vortex_predicate")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_columns(self) -> tuple[str, ...]:
        """Return declared native Vortex projection columns."""

        value = self.envelope.field("vortex_columns")
        return () if value in {None, "", "none"} else _csv_values(value)

    @property
    def fallback_attempted(self) -> bool:
        """Whether the route inspection attempted fallback execution."""

        return self.envelope.field_bool("fallback_attempted", False) is True

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the route inspection invoked an external engine."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def blocker_id(self) -> str | None:
        """Return the deterministic blocker ID when the route is blocked."""

        value = self.envelope.field("blocker_id")
        if value in {None, "", "none"}:
            return None
        return value

    @property
    def side_effect_free(self) -> bool:
        """Whether the route command reports side-effect-free planning."""

        return self.envelope.field_bool("route_side_effect_free", False) is True

    def as_dict(self) -> dict[str, Any]:
        """Return a compact dictionary for notebooks and simple integrations."""

        return {
            "schema_version": self.schema_version,
            "route_id": self.route_id,
            "route_status": self.route_status,
            "resolved_internal_command": self.resolved_internal_command,
            "surface": self.surface,
            "start_state": self.start_state,
            "vortex_normalization_point": self.vortex_normalization_point,
            "execution_mode": self.execution_mode,
            "preparation_included": self.preparation_included,
            "query_timing_starts_after_preparation": self.query_timing_starts_after_preparation,
            "fanout_output_count": self.fanout_output_count,
            "fanout_outputs": self.fanout_outputs,
            "vortex_primitive": self.vortex_primitive,
            "vortex_predicate": self.vortex_predicate,
            "vortex_columns": self.vortex_columns,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "blocker_id": self.blocker_id,
            "side_effect_free": self.side_effect_free,
        }


@dataclass(frozen=True, slots=True)
class PublicWorkflowExecution:
    """Typed view over a public workflow run/prepare envelope with attached route metadata."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the public workflow facade schema version."""

        return _required_field(self.envelope, "public_workflow_facade_schema_version")

    @property
    def facade_command(self) -> str:
        """Return the high-level facade command that emitted the envelope."""

        return _required_field(self.envelope, "public_workflow_facade_command")

    @property
    def route_attached(self) -> bool:
        """Whether a public route envelope was attached to this execution."""

        return self.envelope.field_bool("public_workflow_route_attached", False) is True

    @property
    def route_id(self) -> str:
        """Return the public route identifier attached to execution."""

        return _required_field(self.envelope, "public_workflow_route_id")

    @property
    def route_status(self) -> str:
        """Return the attached route admission status."""

        return _required_field(self.envelope, "public_workflow_route_status")

    @property
    def resolved_internal_command(self) -> str:
        """Return the internal command selected by the route."""

        return _required_field(self.envelope, "public_workflow_resolved_internal_command")

    @property
    def vortex_normalization_point(self) -> str:
        """Return the attached Vortex normalization boundary."""

        return _required_field(self.envelope, "public_workflow_vortex_normalization_point")

    @property
    def execution_mode(self) -> str:
        """Return the attached execution mode."""

        return _required_field(self.envelope, "public_workflow_execution_mode")

    @property
    def runtime_execution(self) -> bool:
        """Whether the emitted envelope reports runtime execution."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def preparation_included(self) -> bool:
        """Whether route preparation is part of this facade request."""

        return (
            self.envelope.field_bool("public_workflow_preparation_included", False)
            is True
        )

    @property
    def fanout_output_count(self) -> int:
        """Return the number of fanout outputs declared on the public workflow."""

        return self.envelope.field_int("public_workflow_fanout_output_count", 0) or 0

    @property
    def fanout_outputs(self) -> tuple[str, ...]:
        """Return declared public workflow fanout targets as ``format=path`` entries."""

        value = self.envelope.field("public_workflow_fanout_outputs")
        return () if value in {None, "", "none"} else tuple(value.split(";"))

    @property
    def vortex_primitive(self) -> str | None:
        """Return the declared public native Vortex primitive payload."""

        value = self.envelope.field("public_workflow_vortex_primitive")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_predicate(self) -> str | None:
        """Return the declared public native Vortex predicate payload."""

        value = self.envelope.field("public_workflow_vortex_predicate")
        return None if value in {None, "", "none"} else value

    @property
    def vortex_columns(self) -> tuple[str, ...]:
        """Return declared public native Vortex projection columns."""

        value = self.envelope.field("public_workflow_vortex_columns")
        return () if value in {None, "", "none"} else _csv_values(value)

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return self.envelope.field_bool("fallback_attempted", False) is True

    @property
    def external_engine_invoked(self) -> bool:
        """Whether an external engine was invoked."""

        return _envelope_external_engine_invoked(self.envelope)

    @property
    def public_workflow_fallback_attempted(self) -> bool:
        """Whether the attached public route reports fallback attempted."""

        return (
            self.envelope.field_bool("public_workflow_fallback_attempted", False)
            is True
        )

    @property
    def public_workflow_external_engine_invoked(self) -> bool:
        """Whether the attached public route reports external engine invocation."""

        return (
            self.envelope.field_bool("public_workflow_external_engine_invoked", False)
            is True
        )

    @property
    def blocker_id(self) -> str | None:
        """Return the attached route blocker ID when blocked."""

        value = self.envelope.field("public_workflow_blocker_id")
        if value in {None, "", "none"}:
            return None
        return value

    def as_dict(self) -> dict[str, Any]:
        """Return a compact dictionary for notebooks and simple integrations."""

        return {
            "schema_version": self.schema_version,
            "facade_command": self.facade_command,
            "route_attached": self.route_attached,
            "route_id": self.route_id,
            "route_status": self.route_status,
            "resolved_internal_command": self.resolved_internal_command,
            "vortex_normalization_point": self.vortex_normalization_point,
            "execution_mode": self.execution_mode,
            "runtime_execution": self.runtime_execution,
            "preparation_included": self.preparation_included,
            "fanout_output_count": self.fanout_output_count,
            "fanout_outputs": self.fanout_outputs,
            "vortex_primitive": self.vortex_primitive,
            "vortex_predicate": self.vortex_predicate,
            "vortex_columns": self.vortex_columns,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "public_workflow_fallback_attempted": self.public_workflow_fallback_attempted,
            "public_workflow_external_engine_invoked": (
                self.public_workflow_external_engine_invoked
            ),
            "blocker_id": self.blocker_id,
        }


@dataclass(frozen=True, slots=True)
class CommandMetadataReport:
    """Typed view over the side-effect-free CLI command registry metadata."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the command registry metadata schema version."""

        return _required_field(self.envelope, "command_registry_schema_version")

    @property
    def registered_command_count(self) -> int:
        """Return the number of commands in the registry."""

        return self.envelope.field_int("registered_command_count", 0) or 0

    @property
    def support_state_vocabulary(self) -> tuple[str, ...]:
        """Return the registry support-state vocabulary."""

        return _csv_values(self.envelope.field("command_registry_support_state_vocabulary"))

    @property
    def user_surface_graduation_posture_vocabulary(self) -> tuple[str, ...]:
        """Return the user-surface graduation posture vocabulary."""

        return _csv_values(
            self.envelope.field(
                "command_registry_user_surface_graduation_posture_vocabulary"
            )
        )

    @property
    def registered_commands(self) -> tuple[str, ...]:
        """Return registered CLI command names in usage order."""

        return _csv_values(self.envelope.field("registered_commands"))

    @property
    def registered_command_families(self) -> Mapping[str, str]:
        """Return command name to command-family mappings."""

        return _csv_key_value_map(self.envelope.field("registered_command_families"))

    @property
    def registered_command_support_states(self) -> Mapping[str, str]:
        """Return command name to support-state mappings."""

        return _csv_key_value_map(self.envelope.field("registered_command_support_states"))

    @property
    def registered_command_user_surface_graduation_postures(self) -> Mapping[str, str]:
        """Return command name to user-surface graduation posture mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_user_surface_graduation_postures")
        )

    @property
    def registered_command_side_effect_levels(self) -> Mapping[str, str]:
        """Return command name to side-effect-level mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_side_effect_levels")
        )

    @property
    def registered_command_feature_gate_statuses(self) -> Mapping[str, str]:
        """Return command name to feature-gate status mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_feature_gate_statuses")
        )

    @property
    def registered_command_input_contracts(self) -> Mapping[str, str]:
        """Return command name to registry input-contract mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_input_contracts")
        )

    @property
    def registered_command_output_contracts(self) -> Mapping[str, str]:
        """Return command name to registry output-contract mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_output_contracts")
        )

    @property
    def registered_command_owning_phase_items(self) -> Mapping[str, str]:
        """Return command name to owning phase or gate mappings."""

        return _csv_key_value_map(
            self.envelope.field("registered_command_owning_phase_items")
        )

    @property
    def selected_command(self) -> str | None:
        """Return the selected command, when the CLI request targeted one command."""

        return self.envelope.field("selected_command")

    @property
    def selected_command_family(self) -> str | None:
        """Return the selected command family, when present."""

        return self.envelope.field("selected_command_family")

    @property
    def selected_command_support_state(self) -> str | None:
        """Return the selected command support state, when present."""

        return self.envelope.field("selected_command_support_state")

    @property
    def selected_command_user_surface_graduation_posture(self) -> str | None:
        """Return the selected command user-surface graduation posture, when present."""

        return self.envelope.field("selected_command_user_surface_graduation_posture")

    @property
    def selected_command_side_effect_level(self) -> str | None:
        """Return the selected command side-effect level, when present."""

        return self.envelope.field("selected_command_side_effect_level")

    @property
    def selected_command_usage_fragment(self) -> str | None:
        """Return the selected command usage fragment, when present."""

        return self.envelope.field("selected_command_usage_fragment")

    @property
    def selected_command_feature_gate_status(self) -> str | None:
        """Return the selected command feature-gate status, when present."""

        return self.envelope.field("selected_command_feature_gate_status")

    @property
    def selected_command_input_contract(self) -> str | None:
        """Return the selected command input contract, when present."""

        return self.envelope.field("selected_command_input_contract")

    @property
    def selected_command_output_contract(self) -> str | None:
        """Return the selected command output contract, when present."""

        return self.envelope.field("selected_command_output_contract")

    @property
    def selected_command_evidence_fields(self) -> tuple[str, ...]:
        """Return the selected command evidence fields, when present."""

        return _csv_values(
            (self.envelope.field("selected_command_evidence_fields") or "").replace(
                "|", ","
            )
        )

    @property
    def selected_command_owning_phase_item(self) -> str | None:
        """Return the selected command owning phase or gate, when present."""

        return self.envelope.field("selected_command_owning_phase_item")

    @property
    def fallback_attempted(self) -> bool:
        """Whether command metadata attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether command metadata invoked an external execution engine."""

        return _envelope_external_engine_invoked(self.envelope)

    def family_for(self, command: str) -> str:
        """Return the registered family for a command."""

        return self.registered_command_families[command]

    def support_state_for(self, command: str) -> str:
        """Return the registered support state for a command."""

        return self.registered_command_support_states[command]

    def user_surface_graduation_posture_for(self, command: str) -> str:
        """Return the registered user-surface graduation posture for a command."""

        return self.registered_command_user_surface_graduation_postures[command]

    def side_effect_level_for(self, command: str) -> str:
        """Return the registered side-effect level for a command."""

        return self.registered_command_side_effect_levels[command]

    def feature_gate_status_for(self, command: str) -> str:
        """Return the registered feature-gate status for a command."""

        return self.registered_command_feature_gate_statuses[command]

    def input_contract_for(self, command: str) -> str:
        """Return the registered input contract for a command."""

        return self.registered_command_input_contracts[command]

    def output_contract_for(self, command: str) -> str:
        """Return the registered output contract for a command."""

        return self.registered_command_output_contracts[command]

    def owning_phase_item_for(self, command: str) -> str:
        """Return the registered owning phase or gate for a command."""

        return self.registered_command_owning_phase_items[command]


@dataclass(frozen=True, slots=True)
class EvidenceSchemaRegistryReport:
    """Typed view over the side-effect-free evidence field schema registry."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the evidence field registry schema version."""

        return _required_field(
            self.envelope, "evidence_schema_registry_schema_version"
        )

    @property
    def surface_count(self) -> int:
        """Return the number of registered evidence surfaces."""

        return (
            self.envelope.field_int("evidence_schema_registry_surface_count", 0)
            or 0
        )

    @property
    def field_count(self) -> int:
        """Return the total registered evidence field count."""

        return (
            self.envelope.field_int("evidence_schema_registry_field_count", 0) or 0
        )

    @property
    def surface_order(self) -> tuple[str, ...]:
        """Return registered evidence surfaces in deterministic order."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_surface_order")
        )

    @property
    def dtype_vocabulary(self) -> tuple[str, ...]:
        """Return the registry dtype vocabulary."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_dtype_vocabulary")
        )

    @property
    def cardinality_vocabulary(self) -> tuple[str, ...]:
        """Return the registry cardinality vocabulary."""

        return _csv_values(
            self.envelope.field("evidence_schema_registry_cardinality_vocabulary")
        )

    @property
    def selected_surface(self) -> str | None:
        """Return the selected evidence surface, when one was requested."""

        return self.envelope.field("selected_surface")

    @property
    def selected_surface_field_order(self) -> tuple[str, ...]:
        """Return the selected surface field order, when present."""

        return _csv_values(self.envelope.field("selected_surface_field_order"))

    @property
    def fallback_attempted(self) -> bool:
        """Whether evidence schema rendering attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool(
                "evidence_schema_registry_fallback_attempted", False
            )
            is True
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether evidence schema rendering invoked an external execution engine."""

        return (
            _envelope_external_engine_invoked(self.envelope)
            or self.envelope.field_bool(
                "evidence_schema_registry_external_engine_invoked", False
            )
            is True
        )

    def field_order_for(self, surface_id: str) -> tuple[str, ...]:
        """Return the declared field order for a surface."""

        return _csv_values(
            self.envelope.field(
                f"evidence_schema_surface_{surface_id}_field_order"
            )
        )

    def python_accessor_mapping_for(self, surface_id: str) -> str:
        """Return the declared Python accessor mapping for a surface."""

        return _required_field(
            self.envelope,
            f"evidence_schema_surface_{surface_id}_python_accessor_mapping",
        )

    def required_no_fallback_fields_for(self, surface_id: str) -> tuple[str, ...]:
        """Return no-fallback fields required by the surface contract."""

        return _csv_values(
            self.envelope.field(
                f"evidence_schema_surface_{surface_id}_required_no_fallback_fields"
            )
        )

    def dtype_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared dtype for one evidence field."""

        return _required_field(
            self.envelope, f"{self._field_prefix(surface_id, field_key)}_dtype"
        )

    def cardinality_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared cardinality for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_cardinality",
        )

    def no_fallback_semantics_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared no-fallback semantics for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_no_fallback_semantics",
        )

    def support_state_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared support state for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_support_state",
        )

    def python_accessor_for(self, surface_id: str, field_key: str) -> str:
        """Return the declared Python accessor mapping for one evidence field."""

        return _required_field(
            self.envelope,
            f"{self._field_prefix(surface_id, field_key)}_python_accessor_mapping",
        )

    @staticmethod
    def _field_prefix(surface_id: str, field_key: str) -> str:
        field_id = field_key.replace("-", "_")
        return f"evidence_schema_field_{surface_id}_{field_id}"


@dataclass(frozen=True, slots=True)
class ProductionUnsupportedDiagnosticRow:
    """One production-family unsupported diagnostic row from `runs-today`."""

    row_id: str
    production_family: str
    user_surface: tuple[str, ...]
    entrypoint_kind: str
    support_status: str
    diagnostic_code: str
    blocker_id: str
    message: str
    next_action: str
    required_evidence: tuple[str, ...]
    claim_gate_status: str
    route_scope: str
    fallback_attempted: bool
    external_engine_invoked: bool
    side_effects_performed: bool


@dataclass(frozen=True, slots=True)
class RunsTodaySupportRow:
    """One current-support row from the `runs-today` matrix."""

    row_id: str
    family: str
    surface: tuple[str, ...]
    support_state: str
    feature_gate: str
    evidence_refs: tuple[str, ...]
    blocker_id: str
    claim_gate_status: str
    claim_boundary: str
    runtime_execution: bool
    data_read: bool
    write_io: bool
    fallback_attempted: bool
    external_engine_invoked: bool


@dataclass(frozen=True, slots=True)
class RunsTodaySupportMatrix:
    """Typed view over the generated current-support matrix."""

    envelope: OutputEnvelope

    @property
    def schema_version(self) -> str:
        """Return the generated matrix schema version."""

        return _required_field(self.envelope, "runs_today_schema_version")

    @property
    def matrix_id(self) -> str:
        """Return the generated matrix identifier."""

        return _required_field(self.envelope, "runs_today_matrix_id")

    @property
    def support_state_vocabulary(self) -> tuple[str, ...]:
        """Return the support-state vocabulary used by this matrix."""

        return _csv_values(self.envelope.field("runs_today_support_state_vocabulary"))

    @property
    def families(self) -> tuple[str, ...]:
        """Return support-row families in declared order."""

        return _csv_values(self.envelope.field("runs_today_family_order"))

    @property
    def row_ids(self) -> tuple[str, ...]:
        """Return support rows in declared order."""

        return _csv_values(self.envelope.field("runs_today_row_order"))

    @property
    def rows(self) -> tuple[RunsTodaySupportRow, ...]:
        """Return typed support rows in declared order."""

        rows: list[RunsTodaySupportRow] = []
        for row_id in self.row_ids:
            prefix = f"runs_today_row_{row_id}_"
            rows.append(
                RunsTodaySupportRow(
                    row_id=row_id,
                    family=_required_field(self.envelope, f"{prefix}family"),
                    surface=_csv_values(self.envelope.field(f"{prefix}surface")),
                    support_state=_required_field(self.envelope, f"{prefix}support_state"),
                    feature_gate=_required_field(self.envelope, f"{prefix}feature_gate"),
                    evidence_refs=_csv_values(
                        self.envelope.field(f"{prefix}evidence_refs")
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    claim_boundary=_required_field(
                        self.envelope,
                        f"{prefix}claim_boundary",
                    ),
                    runtime_execution=self.envelope.field_bool(
                        f"{prefix}runtime_execution",
                        False,
                    )
                    is True,
                    data_read=self.envelope.field_bool(f"{prefix}data_read", False) is True,
                    write_io=self.envelope.field_bool(f"{prefix}write_io", False) is True,
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
    def executable_row_count(self) -> int:
        """Return executable row count."""

        return self.envelope.field_int("runs_today_executable_row_count", 0) or 0

    @property
    def feature_gated_row_count(self) -> int:
        """Return feature-gated row count."""

        return self.envelope.field_int("runs_today_feature_gated_row_count", 0) or 0

    @property
    def diagnostic_only_row_count(self) -> int:
        """Return diagnostic-only row count."""

        return self.envelope.field_int("runs_today_diagnostic_only_row_count", 0) or 0

    @property
    def report_only_row_count(self) -> int:
        """Return report-only row count."""

        return self.envelope.field_int("runs_today_report_only_row_count", 0) or 0

    @property
    def blocked_row_count(self) -> int:
        """Return blocked row count."""

        return self.envelope.field_int("runs_today_blocked_row_count", 0) or 0

    @property
    def future_row_count(self) -> int:
        """Return future row count."""

        return self.envelope.field_int("runs_today_future_row_count", 0) or 0

    @property
    def all_rows_no_fallback_no_external_engine(self) -> bool:
        """Whether all rows report no fallback and no external engine invocation."""

        return (
            self.envelope.field_bool(
                "runs_today_all_rows_no_fallback_no_external_engine",
                False,
            )
            is True
        )

    @property
    def performance_claim_allowed(self) -> bool:
        """Whether this matrix authorizes a performance claim."""

        return self.envelope.field_bool("runs_today_performance_claim_allowed", True) is True

    @property
    def package_publication_allowed(self) -> bool:
        """Whether this matrix authorizes package publication."""

        return self.envelope.field_bool("runs_today_package_publication_allowed", True) is True

    @property
    def production_unsupported_diagnostic_schema_version(self) -> str:
        """Return the production-family unsupported diagnostic catalog schema."""

        return _required_field(
            self.envelope,
            "production_unsupported_diagnostic_schema_version",
        )

    @property
    def production_unsupported_diagnostic_row_ids(self) -> tuple[str, ...]:
        """Return production unsupported diagnostic row identifiers."""

        return _csv_values(
            self.envelope.field("production_unsupported_diagnostic_row_order")
        )

    @property
    def production_unsupported_diagnostic_rows(
        self,
    ) -> tuple[ProductionUnsupportedDiagnosticRow, ...]:
        """Return production unsupported diagnostics in declared order."""

        rows: list[ProductionUnsupportedDiagnosticRow] = []
        for row_id in self.production_unsupported_diagnostic_row_ids:
            prefix = f"production_unsupported_diagnostic_row_{row_id}_"
            rows.append(
                ProductionUnsupportedDiagnosticRow(
                    row_id=row_id,
                    production_family=_required_field(
                        self.envelope,
                        f"{prefix}production_family",
                    ),
                    user_surface=_csv_values(
                        self.envelope.field(f"{prefix}user_surface")
                    ),
                    entrypoint_kind=_required_field(
                        self.envelope,
                        f"{prefix}entrypoint_kind",
                    ),
                    support_status=_required_field(
                        self.envelope,
                        f"{prefix}support_status",
                    ),
                    diagnostic_code=_required_field(
                        self.envelope,
                        f"{prefix}diagnostic_code",
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    message=_required_field(self.envelope, f"{prefix}message"),
                    next_action=_required_field(self.envelope, f"{prefix}next_action"),
                    required_evidence=_csv_values(
                        self.envelope.field(f"{prefix}required_evidence")
                    ),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    route_scope=_required_field(self.envelope, f"{prefix}route_scope"),
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
                    side_effects_performed=self.envelope.field_bool(
                        f"{prefix}side_effects_performed",
                        True,
                    )
                    is True,
                )
            )
        return tuple(rows)

    @property
    def production_unsupported_diagnostic_all_rows_safe(self) -> bool:
        """Whether every production unsupported diagnostic is no-fallback/no-effect."""

        return (
            self.envelope.field_bool(
                "production_unsupported_diagnostic_all_rows_fallback_attempted_false",
                False,
            )
            is True
            and self.envelope.field_bool(
                "production_unsupported_diagnostic_all_rows_external_engine_invoked_false",
                False,
            )
            is True
            and self.envelope.field_bool(
                "production_unsupported_diagnostic_all_rows_side_effects_performed_false",
                False,
            )
            is True
        )

    def production_unsupported_diagnostic_row(
        self,
        row_id: str,
    ) -> ProductionUnsupportedDiagnosticRow:
        """Return a single production unsupported diagnostic row by id."""

        normalized = row_id.strip().replace("-", "_")
        for row in self.production_unsupported_diagnostic_rows:
            if row.row_id == normalized:
                return row
        raise KeyError(row_id)

    def rows_by_family(self, family: str) -> tuple[RunsTodaySupportRow, ...]:
        """Return rows matching one family."""

        return tuple(row for row in self.rows if row.family == family)

    def rows_by_support_state(self, support_state: str) -> tuple[RunsTodaySupportRow, ...]:
        """Return rows matching one support state."""

        return tuple(row for row in self.rows if row.support_state == support_state)

    def row(self, row_id: str) -> RunsTodaySupportRow:
        """Return a single support row by id."""

        for row in self.rows:
            if row.row_id == row_id:
                return row
        raise KeyError(row_id)


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
    def prepared_vortex_scan_pushdown_status(self) -> str:
        """Return the prepared/native Vortex Scan pushdown matrix status."""

        return _required_field(self.envelope, "prepared_vortex_scan_pushdown_status")

    @property
    def prepared_vortex_scan_pushdown_rows(self) -> tuple[PreparedVortexScanPushdownRow, ...]:
        """Return prepared/native Vortex Scan pushdown capability rows."""

        rows: list[PreparedVortexScanPushdownRow] = []
        for row_id in _csv_values(self.envelope.field("prepared_vortex_scan_pushdown_row_order")):
            prefix = f"prepared_vortex_scan_pushdown_row_{row_id}_"
            rows.append(
                PreparedVortexScanPushdownRow(
                    row_id=row_id,
                    scenario=_required_field(self.envelope, f"{prefix}scenario"),
                    pushdown_status=_required_field(
                        self.envelope,
                        f"{prefix}pushdown_status",
                    ),
                    filter_required=self.envelope.field_bool(
                        f"{prefix}filter_required",
                        False,
                    )
                    is True,
                    projection_required=self.envelope.field_bool(
                        f"{prefix}projection_required",
                        False,
                    )
                    is True,
                    limit_required=self.envelope.field_bool(
                        f"{prefix}limit_required",
                        False,
                    )
                    is True,
                    filter_pushed_down=self.envelope.field_bool(
                        f"{prefix}filter_pushed_down",
                        False,
                    )
                    is True,
                    projection_pushed_down=self.envelope.field_bool(
                        f"{prefix}projection_pushed_down",
                        False,
                    )
                    is True,
                    limit_pushed_down=self.envelope.field_bool(
                        f"{prefix}limit_pushed_down",
                        False,
                    )
                    is True,
                    filter_status=_required_field(self.envelope, f"{prefix}filter_status"),
                    projection_status=_required_field(
                        self.envelope,
                        f"{prefix}projection_status",
                    ),
                    limit_status=_required_field(self.envelope, f"{prefix}limit_status"),
                    residual_limit_status=_required_field(
                        self.envelope,
                        f"{prefix}residual_limit_status",
                    ),
                    residual_limit_executor=_required_field(
                        self.envelope,
                        f"{prefix}residual_limit_executor",
                    ),
                    filter_columns_read=_csv_values(
                        self.envelope.field(f"{prefix}filter_columns_read")
                    ),
                    output_columns_read=_csv_values(
                        self.envelope.field(f"{prefix}output_columns_read")
                    ),
                    filter_only_columns_read=_csv_values(
                        self.envelope.field(f"{prefix}filter_only_columns_read")
                    ),
                    blocker_id=_required_field(self.envelope, f"{prefix}blocker_id"),
                    blocker_reason=_required_field(self.envelope, f"{prefix}blocker_reason"),
                    benchmark_refs=_csv_values(self.envelope.field(f"{prefix}benchmark_refs")),
                    claim_gate_status=_required_field(
                        self.envelope,
                        f"{prefix}claim_gate_status",
                    ),
                    claim_boundary=_required_field(self.envelope, f"{prefix}claim_boundary"),
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
    def prepared_vortex_scan_pushdown_all_rows_no_fallback(self) -> bool:
        """Whether every scan pushdown row preserves no-fallback evidence."""

        return (
            self.envelope.field_bool(
                "prepared_vortex_scan_pushdown_all_rows_no_fallback",
                False,
            )
            is True
        )

    @property
    def prepared_vortex_scan_pushdown_all_rows_external_engine_free(self) -> bool:
        """Whether every scan pushdown row avoids external-engine execution."""

        return (
            self.envelope.field_bool(
                "prepared_vortex_scan_pushdown_all_rows_external_engine_invoked_false",
                False,
            )
            is True
        )

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
    def prepared_native_vortex_lifecycle_status(self) -> str | None:
        """Return the prepared/native Vortex artifact lifecycle status."""

        return self._compute_flow_field("prepared_native_vortex_lifecycle_status")

    @property
    def prepared_native_vortex_lifecycle_output_status(self) -> str | None:
        """Return the lifecycle output/replay status."""

        return self._compute_flow_field("prepared_native_vortex_lifecycle_output_status")

    @property
    def prepared_native_vortex_lifecycle_no_standalone_lane(self) -> bool:
        """Whether lifecycle evidence stayed in the prepared/native route."""

        return self._compute_flow_bool("prepared_native_vortex_lifecycle_no_standalone_lane")

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


class _RestApiSurfaceParityMixin:
    """Common REST parity fields shared by contract, lifecycle, and event views."""

    __slots__ = ()

    @property
    def rest_api_surface_parity_schema_version(self) -> str | None:
        """Return the REST surface parity schema version."""

        return self.envelope.field("rest_api_surface_parity_schema_version")

    @property
    def rest_api_surface_parity_surface_id(self) -> str | None:
        """Return the REST surface identifier used by parity checks."""

        return self.envelope.field("rest_api_surface_parity_surface_id")

    @property
    def rest_api_surface_parity_status(self) -> str | None:
        """Return the REST parity status for this surface."""

        return self.envelope.field("rest_api_surface_parity_status")

    @property
    def rest_api_cli_python_field_parity(self) -> bool:
        """Whether the CLI output exposes the common Python-readable REST fields."""

        return self.envelope.field_bool("rest_api_cli_python_field_parity", False) is True

    @property
    def rest_api_runtime_execution(self) -> bool:
        """Whether the REST parity contract reports runtime execution."""

        return self.envelope.field_bool(
            "rest_api_runtime_execution",
            self.envelope.field_bool("runtime_execution", False),
        ) is True

    @property
    def rest_api_runtime_equivalent_api_claim_allowed(self) -> bool:
        """Whether this surface permits a broad runtime-equivalent REST API claim."""

        return (
            self.envelope.field_bool(
                "rest_api_runtime_equivalent_api_claim_allowed",
                False,
            )
            is True
        )

    @property
    def rest_api_policy_fields(self) -> tuple[str, ...]:
        """Return policy fields every REST surface projects for CLI/Python parity."""

        return _csv_values(self.envelope.field("rest_api_policy_fields"))

    @property
    def rest_api_mode_selection_fields(self) -> tuple[str, ...]:
        """Return execution-mode selection fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_mode_selection_fields"))

    @property
    def rest_api_evidence_fields(self) -> tuple[str, ...]:
        """Return evidence fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_evidence_fields"))

    @property
    def rest_api_evidence_refs(self) -> tuple[str, ...]:
        """Return surface-specific evidence references carried by the REST view."""

        return _csv_values(self.envelope.field("rest_api_evidence_refs"))

    @property
    def rest_api_claim_gate_status(self) -> str | None:
        """Return the REST claim-gate status for this surface."""

        return self.envelope.field("rest_api_claim_gate_status")

    @property
    def rest_api_claim_gate_reason(self) -> str | None:
        """Return the reason behind the REST claim-gate status."""

        return self.envelope.field("rest_api_claim_gate_reason")

    @property
    def rest_api_no_fallback_fields(self) -> tuple[str, ...]:
        """Return no-fallback fields every REST surface projects."""

        return _csv_values(self.envelope.field("rest_api_no_fallback_fields"))

    @property
    def rest_api_fallback_attempted(self) -> bool:
        """Whether REST handling attempted fallback execution."""

        return self.envelope.fallback.attempted or (
            self.envelope.field_bool(
                "rest_api_fallback_attempted",
                self.envelope.field_bool("fallback_attempted", False),
            )
            is True
        )

    @property
    def rest_api_external_engine_invoked(self) -> bool:
        """Whether REST handling invoked an external engine."""

        return (
            self.envelope.field_bool("rest_api_external_engine_invoked", False) is True
            or _envelope_external_engine_invoked(self.envelope)
        )

    @property
    def rest_api_execution_delegated(self) -> bool:
        """Whether REST handling delegated execution."""

        return (
            self.envelope.field_bool(
                "rest_api_execution_delegated",
                self.envelope.field_bool("execution_delegated", False),
            )
            is True
        )

    @property
    def rest_api_no_fallback_no_external_engine(self) -> bool:
        """Whether REST parity proves no fallback, no external engine, and no delegation."""

        explicit = self.envelope.field_bool("rest_api_no_fallback_no_external_engine")
        if explicit is not None:
            return explicit is True
        return not (
            self.rest_api_fallback_attempted
            or self.rest_api_external_engine_invoked
            or self.rest_api_execution_delegated
        )


@dataclass(frozen=True, slots=True)
class RestApiContractPlan(_RestApiSurfaceParityMixin):
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
class RestApiPlanPreview(_RestApiSurfaceParityMixin):
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
class RestApiLocalLifecycle(_RestApiSurfaceParityMixin):
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
    def engine_mode(self) -> str | None:
        """Return the engine mode routed through the local lifecycle."""

        return self.envelope.field("engine_mode")

    @property
    def control_plane_invoked(self) -> bool:
        """Whether the local lifecycle control-plane envelope was invoked."""

        return self.envelope.field_bool("control_plane_invoked", False) is True

    @property
    def control_plane_scope(self) -> str | None:
        """Return the local control-plane scope."""

        return self.envelope.field("control_plane_scope")

    @property
    def network_policy(self) -> str | None:
        """Return the lifecycle network policy."""

        return self.envelope.field("network_policy")

    @property
    def checkpoint_state_posture(self) -> str | None:
        """Return the checkpoint/state posture."""

        return self.envelope.field("checkpoint_state_posture")

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
    def live_fixture_invoked(self) -> bool:
        """Whether the in-memory live fixture runtime was invoked."""

        return self.envelope.field_bool("live_fixture_invoked", False) is True

    @property
    def hybrid_fixture_invoked(self) -> bool:
        """Whether the in-memory hybrid fixture runtime was invoked."""

        return self.envelope.field_bool("hybrid_fixture_invoked", False) is True

    @property
    def remote_worker_invoked(self) -> bool:
        """Whether any remote worker was invoked."""

        return self.envelope.field_bool("remote_worker_invoked", False) is True

    @property
    def distributed_runtime_status(self) -> str | None:
        """Return distributed runtime status."""

        return self.envelope.field("distributed_runtime_status")

    @property
    def distributed_worker_blocker_id(self) -> str | None:
        """Return the stable distributed worker blocker id."""

        return self.envelope.field("distributed_worker_blocker_id")

    @property
    def distributed_claim_gate_status(self) -> str | None:
        """Return distributed claim-gate status."""

        return self.envelope.field("distributed_claim_gate_status")

    @property
    def small_result_boundary(self) -> str | None:
        """Return the small-result transfer boundary."""

        return self.envelope.field("small_result_boundary")

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
class RestApiEventStream(_RestApiSurfaceParityMixin):
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
class RestApiSecurityGovernance(_RestApiSurfaceParityMixin):
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
class RestApiDataPlane(_RestApiSurfaceParityMixin):
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
class LiveHybridStateTransitionReport:
    """Typed view over the bounded live/hybrid state-transition fixture."""

    envelope: OutputEnvelope

    @property
    def selected_engine_mode(self) -> str | None:
        """Return the selected fixture engine mode."""

        return self.envelope.field("selected_engine_mode")

    @property
    def transition_kind(self) -> str | None:
        """Return the state-transition fixture kind."""

        return self.envelope.field("transition_kind")

    @property
    def snapshot_epoch(self) -> int:
        """Return the deterministic target snapshot epoch."""

        return self.envelope.field_int("snapshot_epoch", 0) or 0

    @property
    def attempt_count(self) -> int:
        """Return the number of simulated attempts."""

        return self.envelope.field_int("attempt_count", 0) or 0

    @property
    def attempt_outcomes(self) -> tuple[str, ...]:
        """Return attempt outcomes in deterministic order."""

        return _csv_values(self.envelope.field("attempt_outcome_order"))

    @property
    def all_certified(self) -> bool:
        """Whether freshness, state, and transition evidence is certified."""

        return all(
            self.envelope.field(key) == "certified"
            for key in (
                "freshness_certificate_status",
                "state_certificate_status",
                "state_transition_certificate_status",
            )
        )

    @property
    def cleanup_completed(self) -> bool:
        """Whether cooperative cancellation cleanup completed."""

        return self.envelope.field_bool("cancellation_cleanup_completed", False) is True

    @property
    def partial_output_committed(self) -> bool:
        """Whether the cancelled attempt committed partial output."""

        return self.envelope.field_bool("partial_output_committed", False) is True

    @property
    def durable_checkpoint_store_used(self) -> bool:
        """Whether a durable checkpoint store was used."""

        return self.envelope.field_bool("durable_checkpoint_store_used", False) is True

    @property
    def exactly_once_claim_allowed(self) -> bool:
        """Whether the fixture authorizes an exactly-once claim."""

        return self.envelope.field_bool("exactly_once_claim_allowed", False) is True

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

    def doctor(self, *, check: bool = True) -> OutputEnvelope:
        """Return the side-effect-free v1 doctor envelope."""

        return self.run(["doctor"], check=check)

    def support_bundle(
        self,
        *,
        note: str | None = None,
        include_defaults: bool = True,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a redacted support-bundle envelope without writing files."""

        args: list[CommandPart] = ["support-bundle"]
        if note is not None:
            args.extend(["--note", note])
        if include_defaults:
            args.append("--include-defaults")
        return self.run(args, check=check)

    def runs_today(self, *, check: bool = True) -> RunsTodaySupportMatrix:
        """Return the generated current-support matrix."""

        return RunsTodaySupportMatrix(self.run(["runs-today"], check=check))

    def command_metadata(
        self, command: str | None = None, *, check: bool = True
    ) -> CommandMetadataReport:
        """Return side-effect-free CLI command registry metadata."""

        args = ["command-metadata"]
        if command is not None:
            args.append(command)
        return CommandMetadataReport(self.run(args, check=check))

    def evidence_schema(
        self, surface: str | None = None, *, check: bool = True
    ) -> EvidenceSchemaRegistryReport:
        """Return side-effect-free evidence field schema registry metadata."""

        args = ["evidence-schema"]
        if surface is not None:
            args.append(surface)
        return EvidenceSchemaRegistryReport(self.run(args, check=check))

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

    def public_workflow_route(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "auto",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        generated_source_kind: str | None = None,
        generated_schema: str | None = None,
        generated_rows: str | None = None,
        generated_range_start: int | None = None,
        generated_range_end: int | None = None,
        generated_range_step: int | None = None,
        generated_range_column: str | None = None,
        fanout_outputs: FanoutOutputs | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PublicWorkflowRoute:
        """Return the side-effect-free public route envelope for a declared workflow."""

        args: list[CommandPart] = ["route", surface]
        if input_uri is not None:
            args.extend(["--input", str(input_uri)])
        if input_format is not None:
            args.extend(["--input-format", input_format])
        if sql_statement is not None:
            args.extend(["--sql", sql_statement])
        if plan_summary is not None:
            args.extend(["--plan", plan_summary])
        args.extend(["--request", requested_output])
        if output_ref is not None:
            args.extend(["--output", str(output_ref)])
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            args.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        args.extend(["--execution-policy", execution_policy])
        args.extend(["--materialization-policy", materialization_policy])
        args.extend(["--evidence-level", evidence_level])
        if bounded is not None:
            args.extend(["--bounded", "true" if bounded else "false"])
        if generated_source_kind is not None:
            args.extend(["--generated-source-kind", generated_source_kind])
        if generated_schema is not None:
            args.extend(["--generated-schema", generated_schema])
        if generated_rows is not None:
            args.extend(["--generated-rows", generated_rows])
        if generated_range_start is not None:
            args.extend(["--generated-range-start", str(generated_range_start)])
        if generated_range_end is not None:
            args.extend(["--generated-range-end", str(generated_range_end)])
        if generated_range_step is not None:
            args.extend(["--generated-range-step", str(generated_range_step)])
        if generated_range_column is not None:
            args.extend(["--generated-range-column", generated_range_column])
        _append_public_vortex_payload_args(
            args,
            vortex_primitive=vortex_primitive,
            vortex_predicate=vortex_predicate,
            vortex_columns=vortex_columns,
            vortex_source_order_limit=vortex_source_order_limit,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return PublicWorkflowRoute(self.run(args, check=check))

    def public_workflow_run(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "auto",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        allow_overwrite: bool = False,
        generated_source_kind: str | None = None,
        generated_schema: str | None = None,
        generated_rows: str | None = None,
        generated_range_start: int | None = None,
        generated_range_end: int | None = None,
        generated_range_step: int | None = None,
        generated_range_column: str | None = None,
        fanout_outputs: FanoutOutputs | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Run an admitted public workflow through the shared route facade."""

        args = self._public_workflow_facade_args(
            "run",
            surface,
            input_uri=input_uri,
            input_format=input_format,
            sql_statement=sql_statement,
            plan_summary=plan_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy=execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=bounded,
            allow_overwrite=allow_overwrite,
            generated_source_kind=generated_source_kind,
            generated_schema=generated_schema,
            generated_rows=generated_rows,
            generated_range_start=generated_range_start,
            generated_range_end=generated_range_end,
            generated_range_step=generated_range_step,
            generated_range_column=generated_range_column,
            fanout_outputs=fanout_outputs,
            vortex_primitive=vortex_primitive,
            vortex_predicate=vortex_predicate,
            vortex_columns=vortex_columns,
            vortex_source_order_limit=vortex_source_order_limit,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return PublicWorkflowExecution(self.run(args, check=check))

    def public_workflow_prepare(
        self,
        surface: str,
        *,
        input_uri: str | os.PathLike[str],
        output_ref: str | os.PathLike[str],
        input_format: str | None = None,
        plan_summary: str | None = None,
        evidence_level: str = "runtime_smoke",
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Prepare an admitted public workflow input through the shared route facade."""

        args = self._public_workflow_facade_args(
            "prepare",
            surface,
            input_uri=input_uri,
            input_format=input_format,
            plan_summary=plan_summary,
            requested_output="prepare",
            output_ref=output_ref,
            execution_policy="prepare_once",
            materialization_policy="bounded",
            evidence_level=evidence_level,
            bounded=True,
        )
        return PublicWorkflowExecution(self.run(args, check=check))

    def _public_workflow_facade_args(
        self,
        command: str,
        surface: str,
        *,
        input_uri: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        sql_statement: str | None = None,
        plan_summary: str | None = None,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "auto",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        allow_overwrite: bool = False,
        generated_source_kind: str | None = None,
        generated_schema: str | None = None,
        generated_rows: str | None = None,
        generated_range_start: int | None = None,
        generated_range_end: int | None = None,
        generated_range_step: int | None = None,
        generated_range_column: str | None = None,
        fanout_outputs: FanoutOutputs | None = None,
        vortex_primitive: str | None = None,
        vortex_predicate: str | None = None,
        vortex_columns: str | Sequence[str] | None = None,
        vortex_source_order_limit: int | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
    ) -> list[CommandPart]:
        args: list[CommandPart] = [command, surface]
        if input_uri is not None:
            args.extend(["--input", str(input_uri)])
        if input_format is not None:
            args.extend(["--input-format", input_format])
        if sql_statement is not None:
            args.extend(["--sql", sql_statement])
        if plan_summary is not None:
            args.extend(["--plan", plan_summary])
        args.extend(["--request", requested_output])
        if output_ref is not None:
            args.extend(["--output", str(output_ref)])
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            args.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        args.extend(["--execution-policy", execution_policy])
        args.extend(["--materialization-policy", materialization_policy])
        args.extend(["--evidence-level", evidence_level])
        if bounded is not None:
            args.extend(["--bounded", "true" if bounded else "false"])
        if allow_overwrite:
            args.append("--allow-overwrite")
        if generated_source_kind is not None:
            args.extend(["--generated-source-kind", generated_source_kind])
        if generated_schema is not None:
            args.extend(["--generated-schema", generated_schema])
        if generated_rows is not None:
            args.extend(["--generated-rows", generated_rows])
        if generated_range_start is not None:
            args.extend(["--generated-range-start", str(generated_range_start)])
        if generated_range_end is not None:
            args.extend(["--generated-range-end", str(generated_range_end)])
        if generated_range_step is not None:
            args.extend(["--generated-range-step", str(generated_range_step)])
        if generated_range_column is not None:
            args.extend(["--generated-range-column", generated_range_column])
        _append_public_vortex_payload_args(
            args,
            vortex_primitive=vortex_primitive,
            vortex_predicate=vortex_predicate,
            vortex_columns=vortex_columns,
            vortex_source_order_limit=vortex_source_order_limit,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        )
        return args

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

    def live_hybrid_state_transition_smoke(
        self,
        *,
        check: bool = True,
    ) -> LiveHybridStateTransitionReport:
        """Run the bounded CG-22 state-transition retry/cancel/cleanup fixture."""

        return LiveHybridStateTransitionReport(
            self.run(["live-hybrid-state-transition-smoke"], check=check)
        )

    def explain(self, operation: str, *, check: bool = True) -> OutputEnvelope:
        """Return the report-only explain envelope for an operation summary."""

        return self.run(["explain", operation], check=check)

    def optimizer_plan(self, *, check: bool = True) -> EvidenceAwareOptimizerTraceReport:
        """Return the report-only evidence-aware optimizer trace."""

        return EvidenceAwareOptimizerTraceReport(
            self.run(["optimizer-plan"], check=check)
        )

    def session_cache_smoke(self, *, check: bool = True) -> SessionCacheSmokeReport:
        """Run the scoped CLI session-cache lifecycle smoke."""

        return SessionCacheSmokeReport(
            self.run(["session-cache-smoke"], check=check)
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
        fanout_outputs: FanoutOutputs | None = None,
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
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            command.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
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
        fanout_outputs: FanoutOutputs | None = None,
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
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            command.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def generated_source_sequence_smoke(
        self,
        output_path: str | os.PathLike[str],
        start: int,
        end: int,
        *,
        step: int = 1,
        column: str = "value",
        output_format: str = "jsonl",
        fanout_outputs: FanoutOutputs | None = None,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Run the scoped local engine-native sequence generated-output smoke command."""

        command: list[CommandPart] = [
            "generated-source-sequence-smoke",
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
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            command.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def generated_source_sql_smoke(
        self,
        output_path: str | os.PathLike[str],
        statement: str,
        *,
        output_format: str = "jsonl",
        fanout_outputs: FanoutOutputs | None = None,
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
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            command.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return GeneratedSourceWriteReport(self.run(command, check=check))

    def sql_local_source_smoke(
        self,
        statement: str,
        *,
        output_path: str | os.PathLike[str] | None = None,
        output_format: str = "inline-jsonl",
        fanout_outputs: FanoutOutputs | None = None,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport:
        """Run the scoped local-source SQL smoke command."""

        command: list[CommandPart] = [
            "sql-local-source-smoke",
            statement,
            "--output-format",
            output_format,
        ]
        if output_path is not None:
            command.extend(["--output", str(output_path)])
        for fanout_format, fanout_path in _iter_fanout_outputs(fanout_outputs):
            command.extend(["--fanout-output", f"{fanout_format}={fanout_path}"])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return SqlLocalSourceSmokeReport(self.run(command, check=check))

    def vortex_ingest_smoke(
        self,
        source_path: str | os.PathLike[str],
        target_vortex_path: str | os.PathLike[str],
        *,
        input_format: str | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        delta_source_path: str | os.PathLike[str] | None = None,
        delta_target_vortex_path: str | os.PathLike[str] | None = None,
        delta_update_mode: str = "append-only",
        check: bool = True,
    ) -> VortexIngestSmokeReport:
        """Run the scoped local `vortex_ingest` prepare-once smoke command."""

        command: list[CommandPart] = [
            "vortex-ingest-smoke",
            str(source_path),
            str(target_vortex_path),
        ]
        if input_format is not None:
            command.extend(["--input-format", input_format])
        if allow_overwrite:
            command.append("--allow-overwrite")
        if certification_level != "ingest_certified":
            command.extend(["--certification-level", certification_level])
        if delta_source_path is not None:
            command.extend(["--delta-source", str(delta_source_path)])
        if delta_target_vortex_path is not None:
            command.extend(["--delta-target", str(delta_target_vortex_path)])
        if delta_update_mode != "append-only":
            command.extend(["--delta-update-mode", delta_update_mode])
        return VortexIngestSmokeReport(self.run(command, check=check))

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

        if primitive.strip().lower() in {"count", "count_all"}:
            return self.public_workflow_run(
                "cli",
                input_uri=dataset_uri,
                input_format="vortex",
                requested_output="collect",
                execution_policy="native_vortex",
                materialization_policy="zero_decode",
                evidence_level="runtime_smoke",
                bounded=True,
                vortex_primitive=primitive,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ).envelope
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

        if execute_local_primitive:
            if memory_gb is None or max_parallelism is None:
                raise ValueError(
                    "--execute-local-primitive requires both memory_gb and max_parallelism"
                )
            return self.public_workflow_run(
                "cli",
                input_uri=dataset_uri,
                input_format="vortex",
                requested_output="collect",
                execution_policy="native_vortex",
                materialization_policy="zero_decode",
                evidence_level="runtime_smoke",
                bounded=True,
                vortex_primitive="count_where",
                vortex_predicate=predicate,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ).envelope
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
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter` with optional explicit local execution."""

        if execute_local_primitive:
            if memory_gb is None or max_parallelism is None:
                raise ValueError(
                    "--execute-local-primitive requires both memory_gb and max_parallelism"
                )
            return self.public_workflow_run(
                "cli",
                input_uri=dataset_uri,
                input_format="vortex",
                requested_output="collect",
                execution_policy="native_vortex",
                materialization_policy="zero_decode",
                evidence_level="runtime_smoke",
                bounded=True,
                vortex_primitive="filter",
                vortex_predicate=predicate,
                vortex_source_order_limit=source_order_limit,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ).envelope
        args = ["vortex-filter", str(dataset_uri), predicate]
        if source_order_limit is not None:
            args.extend(
                [
                    "--limit",
                    str(_positive_int("source_order_limit", source_order_limit)),
                ]
            )
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
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-project` with optional explicit local execution."""

        if execute_local_primitive:
            if memory_gb is None or max_parallelism is None:
                raise ValueError(
                    "--execute-local-primitive requires both memory_gb and max_parallelism"
                )
            return self.public_workflow_run(
                "cli",
                input_uri=dataset_uri,
                input_format="vortex",
                requested_output="collect",
                execution_policy="native_vortex",
                materialization_policy="zero_decode",
                evidence_level="runtime_smoke",
                bounded=True,
                vortex_primitive="project",
                vortex_columns=columns,
                vortex_source_order_limit=source_order_limit,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ).envelope
        args = ["vortex-project", str(dataset_uri), _columns_arg(columns)]
        if source_order_limit is not None:
            args.extend(
                [
                    "--limit",
                    str(_positive_int("source_order_limit", source_order_limit)),
                ]
            )
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
        source_order_limit: int | None = None,
        execute_local_primitive: bool = False,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run `vortex-filter-project` with optional explicit local execution."""

        if execute_local_primitive:
            if memory_gb is None or max_parallelism is None:
                raise ValueError(
                    "--execute-local-primitive requires both memory_gb and max_parallelism"
                )
            return self.public_workflow_run(
                "cli",
                input_uri=dataset_uri,
                input_format="vortex",
                requested_output="collect",
                execution_policy="native_vortex",
                materialization_policy="zero_decode",
                evidence_level="runtime_smoke",
                bounded=True,
                vortex_primitive="filter_project",
                vortex_predicate=predicate,
                vortex_columns=columns,
                vortex_source_order_limit=source_order_limit,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            ).envelope
        args = [
            "vortex-filter-project",
            str(dataset_uri),
            predicate,
            _columns_arg(columns),
        ]
        if source_order_limit is not None:
            args.extend(
                [
                    "--limit",
                    str(_positive_int("source_order_limit", source_order_limit)),
                ]
            )
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
        cdc_delta_input: str | os.PathLike[str] | None = None,
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
        if cdc_delta_input is not None:
            args.extend(["--cdc-delta", str(cdc_delta_input)])
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
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
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
        if memory_gb is not None:
            args.extend(["--memory-gb", str(memory_gb)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        return self.run(args, check=check)

    def traditional_analytics_vortex_batch_run(
        self,
        scenarios: str | Sequence[str],
        fact_vortex: str | os.PathLike[str],
        dim_vortex: str | os.PathLike[str],
        *,
        cdc_delta_vortex: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        execution_mode: str | None = None,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the scoped prepared/native Vortex batch command."""

        args = [
            "traditional-analytics-vortex-batch-run",
            _scenario_csv_arg(scenarios),
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
        if evidence_level is not None:
            args.extend(["--evidence-level", evidence_level])
        if memory_gb is not None:
            args.extend(["--memory-gb", str(memory_gb)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        return self.run(args, check=check)

    def traditional_analytics_prepare_batch_run(
        self,
        scenarios: str | Sequence[str],
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str],
        input_format: str | None = None,
        cdc_delta_input: str | os.PathLike[str] | None = None,
        result_workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Prepare compatibility inputs once, then run a prepared Vortex batch in one CLI process."""

        args = [
            "traditional-analytics-prepare-batch-run",
            _scenario_csv_arg(scenarios),
            str(fact_input),
            str(dim_input),
            "--workspace",
            str(workspace),
        ]
        if input_format is not None:
            args.extend(["--input-format", input_format])
        if cdc_delta_input is not None:
            args.extend(["--cdc-delta", str(cdc_delta_input)])
        if result_workspace is not None:
            args.extend(["--result-workspace", str(result_workspace)])
        if write_result_vortex:
            args.append("--write-result-vortex")
        if evidence_level is not None:
            args.extend(["--evidence-level", evidence_level])
        if memory_gb is not None:
            args.extend(["--memory-gb", str(memory_gb)])
        if max_parallelism is not None:
            args.extend(["--max-parallelism", str(max_parallelism)])
        return self.run(args, check=check)

    def prepare_traditional_analytics_vortex_artifacts(
        self,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str],
        input_format: str | None = None,
        cdc_delta_input: str | os.PathLike[str] | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PreparedVortexArtifacts:
        """Prepare reusable local Vortex artifacts through the certified ingest/stage path."""

        envelope = self.traditional_analytics_run(
            "small change over large base" if cdc_delta_input is not None else "csv/file ingest",
            fact_input,
            dim_input,
            workspace=workspace,
            input_format=input_format,
            cdc_delta_input=cdc_delta_input,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            execution_mode="compatibility_import_certified",
            check=check,
        )
        return PreparedVortexArtifacts(prepare=envelope)

    def prepare_and_run_traditional_analytics_vortex_batch(
        self,
        scenarios: str | Sequence[str],
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        *,
        workspace: str | os.PathLike[str],
        input_format: str | None = None,
        cdc_delta_input: str | os.PathLike[str] | None = None,
        result_workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> PreparedVortexBatchResult:
        """Prepare local compatibility inputs once, then run a prepared Vortex batch.

        This convenience helper uses the single-process prepare/batch route so callers do not
        pay for a separate prepare command followed by a separate batch command. Use
        :meth:`prepare_traditional_analytics_vortex_artifacts` when the caller needs to manage
        prepared artifact lifecycles explicitly across multiple later commands.
        """

        envelope = self.traditional_analytics_prepare_batch_run(
            scenarios,
            fact_input,
            dim_input,
            workspace=workspace,
            input_format=input_format,
            cdc_delta_input=cdc_delta_input,
            result_workspace=result_workspace,
            write_result_vortex=write_result_vortex,
            evidence_level=evidence_level,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )
        return PreparedVortexBatchResult(prepare=envelope, batch=envelope)

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

    def benchmark_constitution(
        self, scope: str | None = None, *, check: bool = True
    ) -> OutputEnvelope:
        """Return benchmark constitution validation for the optional scope."""

        args = ["benchmark-constitution"]
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

    def plan_import(
        self,
        format_kind: str,
        source_label: str,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return a plan import portability envelope for the requested format."""

        return self.run(["plan-import", format_kind, source_label], check=check)

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

    def local_table_metadata_read_smoke(
        self,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the scoped local-manifest table metadata read smoke."""

        return self.run(["local-table-metadata-read-smoke"], check=check)

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

    def object_store_read_smoke(
        self,
        local_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        byte_range: tuple[int, int] | None = None,
        public_fixture_path: str | os.PathLike[str] | None = None,
        fixture_listing: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run an explicit object-store read smoke for an admitted fixture profile."""

        command = ["object-store-read-smoke", str(local_object_path), "--profile", profile]
        if public_fixture_path is not None:
            command.extend(["--public-fixture-path", str(public_fixture_path)])
        if fixture_listing:
            command.append("--fixture-listing")
        if byte_range is not None:
            offset, length = byte_range
            command.extend(["--range", f"{offset}:{length}"])
        return self.run(command, check=check)

    def object_store_partition_discovery_smoke(
        self,
        local_partition_root: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        partition_columns: Sequence[str] | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run scoped local-emulator key=value partition discovery."""

        command = [
            "object-store-partition-discovery-smoke",
            str(local_partition_root),
            "--profile",
            profile,
        ]
        if partition_columns is not None:
            columns = [
                str(column).strip()
                for column in partition_columns
                if str(column).strip()
            ]
            if columns:
                command.extend(["--partition-columns", ",".join(columns)])
        return self.run(command, check=check)

    def object_store_write_smoke(
        self,
        source_path: str | os.PathLike[str],
        target_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the explicit local-emulator staged object-store write smoke."""

        command = [
            "object-store-write-smoke",
            str(source_path),
            str(target_object_path),
            "--profile",
            profile,
        ]
        if idempotency_key is not None:
            command.extend(["--idempotency-key", idempotency_key])
        if allow_overwrite:
            command.append("--allow-overwrite")
        if rollback_after_commit:
            command.append("--rollback-after-commit")
        return self.run(command, check=check)

    def local_table_append_commit_rehearsal_smoke(
        self,
        target_manifest_path: str | os.PathLike[str],
        *,
        profile: str = "local-manifest",
        idempotency_key: str | None = None,
        allow_overwrite: bool = False,
        rollback_after_commit: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local-manifest table append commit rehearsal smoke."""

        command = [
            "local-table-append-commit-rehearsal-smoke",
            str(target_manifest_path),
            "--profile",
            profile,
        ]
        if idempotency_key is not None:
            command.extend(["--idempotency-key", idempotency_key])
        if allow_overwrite:
            command.append("--allow-overwrite")
        if rollback_after_commit:
            command.append("--rollback-after-commit")
        return self.run(command, check=check)

    def object_store_write_recovery_smoke(
        self,
        target_object_path: str | os.PathLike[str],
        *,
        profile: str = "local-emulator",
        idempotency_key: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run local-emulator object-store write recovery replay."""

        command = [
            "object-store-write-recovery-smoke",
            str(target_object_path),
            "--profile",
            profile,
        ]
        if idempotency_key is not None:
            command.extend(["--idempotency-key", idempotency_key])
        return self.run(command, check=check)

    def local_table_commit_recovery_smoke(
        self,
        target_manifest_path: str | os.PathLike[str],
        *,
        profile: str = "local-manifest",
        idempotency_key: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local-manifest table commit recovery smoke."""

        command = [
            "local-table-commit-recovery-smoke",
            str(target_manifest_path),
            "--profile",
            profile,
        ]
        if idempotency_key is not None:
            command.extend(["--idempotency-key", idempotency_key])
        return self.run(command, check=check)

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

    def extension_registry(self, *, check: bool = True) -> OutputEnvelope:
        """Return the side-effect-free extension registry snapshot."""

        return self.run(["extension-registry"], check=check)

    def extension_inspect(
        self,
        extension_id: str | None = None,
        *,
        manifest_path: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Inspect extension manifest metadata without loading extension code."""

        if manifest_path is not None:
            return self.run(["extension-inspect", "--manifest", manifest_path], check=check)
        if extension_id is None:
            raise ValueError("extension_id or manifest_path is required")
        return self.run(["extension-inspect", extension_id], check=check)

    def udf_runtime_plan(
        self,
        runtime: str = "unknown",
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Return UDF runtime posture, including the admitted built-in fixture."""

        return self.run(["udf-runtime-plan", runtime], check=check)

    def udf_local_scalar_fixture_smoke(
        self,
        values: Sequence[int | None] | str,
        *,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the built-in deterministic nullable-int64 scalar UDF fixture."""

        if isinstance(values, str):
            encoded_values = values
        else:
            encoded_values = ",".join("null" if value is None else str(value) for value in values)
        return self.run(["udf-local-scalar-fixture-smoke", encoded_values], check=check)

    def embedding_vector_local_fixture_smoke(
        self,
        texts: Sequence[str] | str,
        *,
        query: str | None = None,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the built-in deterministic embedding/vector fixture."""

        if isinstance(texts, str):
            encoded_texts = texts
        else:
            encoded_texts = ";".join(str(text) for text in texts)
        command: list[CommandPart] = ["embedding-vector-local-fixture-smoke", encoded_texts]
        if query is not None:
            command.extend(["--query", query])
        return self.run(command, check=check)

    def sqlite_local_import_export_smoke(
        self,
        database_path: str | os.PathLike[str],
        *,
        table: str,
        export_jsonl: str | os.PathLike[str],
        roundtrip_db: str | os.PathLike[str],
        order_by: str | None = None,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> OutputEnvelope:
        """Run the local SQLite file import/export fixture smoke."""

        command: list[CommandPart] = [
            "sqlite-local-import-export-smoke",
            str(database_path),
            "--table",
            table,
            "--export-jsonl",
            str(export_jsonl),
            "--roundtrip-db",
            str(roundtrip_db),
        ]
        if order_by is not None:
            command.extend(["--order-by", order_by])
        if allow_overwrite:
            command.append("--allow-overwrite")
        return self.run(command, check=check)

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
        redacted_command = _redact_command_for_error(command)
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
        envelope = self._parse_stdout(completed.stdout, redacted_command)
        if check and (completed.returncode != 0 or envelope.is_error):
            raise ShardLoomCommandError(
                command=redacted_command,
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


def _redact_command_for_error(command: Sequence[str]) -> tuple[str, ...]:
    return tuple(_redact_command_part_for_error(part) for part in command)


def _redact_command_part_for_error(part: str) -> str:
    return _URI_WITH_AUTHORITY_RE.sub(_redact_uri_match_for_error, part)


def _redact_uri_match_for_error(match: re.Match[str]) -> str:
    scheme = match.group("scheme")
    body = match.group("body")
    body_without_query = re.split(r"[?#]", body, maxsplit=1)[0]
    authority, separator, path = body_without_query.partition("/")
    redacted_authority = _redact_uri_authority_for_error(scheme, authority)
    return f"{scheme}://{redacted_authority}{separator}{path}"


def _redact_uri_authority_for_error(scheme: str, authority: str) -> str:
    normalized_scheme = scheme.lower()
    if normalized_scheme in {"abfs", "abfss"} and not _adls_authority_has_userinfo(
        authority
    ):
        return authority
    if "@" not in authority:
        return authority
    return "<redacted>@" + authority.rsplit("@", 1)[1]


def _adls_authority_has_userinfo(authority: str) -> bool:
    if authority.count("@") > 1:
        return True
    container, separator, _account = authority.partition("@")
    return bool(separator and ":" in container)


def _required_field(envelope: OutputEnvelope, key: str) -> str:
    value = envelope.field(key)
    if value is None or value == "":
        raise ShardLoomProtocolError(
            f"ShardLoom command {envelope.command!r} did not emit required field {key!r}"
        )
    return value


def _required_field_any(envelope: OutputEnvelope, *keys: str) -> str:
    for key in keys:
        value = envelope.field(key)
        if value:
            return value
    joined = ", ".join(repr(key) for key in keys)
    raise ShardLoomProtocolError(
        f"ShardLoom command {envelope.command!r} did not emit any required field from {joined}"
    )


def _iter_fanout_outputs(
    fanout_outputs: FanoutOutputs | None,
) -> tuple[tuple[str, str], ...]:
    if fanout_outputs is None:
        return ()
    if isinstance(fanout_outputs, Mapping):
        items = fanout_outputs.items()
    else:
        items = fanout_outputs
    normalized: list[tuple[str, str]] = []
    for item in items:
        fanout_format, fanout_path = item
        normalized.append((str(fanout_format), str(fanout_path)))
    return tuple(normalized)


def _jsonl_object_rows(value: str, *, field_name: str) -> tuple[Mapping[str, Any], ...]:
    rows: list[Mapping[str, Any]] = []
    for line_number, line in enumerate(value.splitlines(), start=1):
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ShardLoomProtocolError(
                f"ShardLoom field {field_name} contains invalid JSONL at line {line_number}"
            ) from exc
        if not isinstance(payload, Mapping):
            raise ShardLoomProtocolError(
                f"ShardLoom field {field_name} line {line_number} is not a JSON object"
            )
        rows.append(dict(payload))
    return tuple(rows)


def _csv_values(value: str | None) -> tuple[str, ...]:
    if value is None or value == "" or value == "none":
        return ()
    row = next(csv.reader([value]), [])
    return tuple(part.strip() for part in row if part.strip())


ABSENT_REFERENCE_VALUES = {
    "none",
    "null",
    "not_requested",
    "not_applicable",
    "not_available",
    "not_applicable_inline_result",
}


def _is_absent_csv_value(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in ABSENT_REFERENCE_VALUES


def _csv_present_values(value: str | None) -> tuple[str, ...]:
    return tuple(part for part in _csv_values(value) if not _is_absent_csv_value(part))


def _csv_output_plan_required_columns(value: str | None) -> tuple[str, ...]:
    return tuple(
        part
        for part in _csv_values(value)
        if part.strip().lower()
        not in {
            *ABSENT_REFERENCE_VALUES,
            "not_applicable_inline_result",
        }
    )


def _csv_reference_values(value: str | None) -> tuple[str, ...]:
    return _csv_present_values(value)


def _csv_key_value_map(value: str | None) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in _csv_values(value):
        if "=" not in part:
            continue
        key, item_value = part.split("=", 1)
        fields[key] = item_value
    return fields


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


def _scenario_csv_arg(scenarios: str | Sequence[str]) -> str:
    if isinstance(scenarios, str):
        value = scenarios
    else:
        values = [str(scenario).strip() for scenario in scenarios]
        if not values:
            raise ValueError("scenarios must not be empty")
        value = ",".join(values)
    if value.strip() == "":
        raise ValueError("scenarios must not be empty")
    if any(part.strip() == "" for part in value.split(",")):
        raise ValueError("scenarios must not contain empty tokens")
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


def _append_public_vortex_payload_args(
    args: list[CommandPart],
    *,
    vortex_primitive: str | None,
    vortex_predicate: str | None,
    vortex_columns: str | Sequence[str] | None,
    vortex_source_order_limit: int | None,
    memory_gb: int | None,
    max_parallelism: int | None,
) -> None:
    if vortex_primitive is not None:
        args.extend(["--vortex-primitive", vortex_primitive])
    if vortex_predicate is not None:
        args.extend(["--vortex-predicate", vortex_predicate])
    if vortex_columns is not None:
        args.extend(["--vortex-columns", _columns_arg(vortex_columns)])
    if vortex_source_order_limit is not None:
        args.extend(
            [
                "--vortex-source-order-limit",
                str(_positive_int("vortex_source_order_limit", vortex_source_order_limit)),
            ]
        )
    if memory_gb is not None:
        args.extend(["--memory-gb", str(_positive_int("memory_gb", memory_gb))])
    if max_parallelism is not None:
        args.extend(
            ["--max-parallelism", str(_positive_int("max_parallelism", max_parallelism))]
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
