"""Prepared Vortex route helpers for compatibility-file user workflows."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from ._compat import dataclass
from .client import (
    ETL_INPUT_FORMATS,
    PreparedVortexArtifacts,
    PreparedVortexBatchResult,
    ShardLoomClient,
)
from .models import OutputEnvelope


_COMPATIBILITY_INPUT_FORMATS = frozenset(ETL_INPUT_FORMATS - {"vortex"})
_FORMAT_ALIASES = {
    "json": "jsonl",
    "json-lines": "jsonl",
    "json_lines": "jsonl",
    "ndjson": "jsonl",
    "arrow": "arrow-ipc",
    "arrow_ipc": "arrow-ipc",
    "ipc": "arrow-ipc",
    "feather": "arrow-ipc",
}
_REUSE_MANIFEST_SCHEMA_VERSION = "shardloom.python.prepared_vortex_reuse_manifest.v1"
_SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_admission_digest_policy.v1"
)
_SOURCE_ADMISSION_PACKET_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.source_admission_packet.v1"
)
_PREPARED_STATE_INDEX_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.prepared_state_index.v1"
)
_PREPARED_STATE_DEPENDENCY_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.prepared_state_dependency.v1"
)
_PREPARED_STATE_PARTIAL_REPAIR_SCHEMA_VERSION = (
    "shardloom.traditional_analytics.prepared_state_partial_repair.v1"
)
_PREPARED_BATCH_DEPENDENCY_CHECKED_ROLES = "fact_input,dim_input,cdc_delta_input,prepare_policy,source_admission_packet,prepared_artifact_fact,prepared_artifact_dim,prepared_artifact_cdc_delta,no_fallback_policy"
_REUSE_MANIFEST_DIR = ".shardloom"
_REUSE_MANIFEST_FILE = "prepared-vortex-reuse-manifest.json"
_REUSE_INDEX_FILE = "prepared-state-index.json"


def _normalize_input_format(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    normalized = _FORMAT_ALIASES.get(normalized, normalized)
    if normalized not in _COMPATIBILITY_INPUT_FORMATS:
        raise ValueError(
            "prepared Vortex compatibility routes require input_format to be one of "
            f"{sorted(_COMPATIBILITY_INPUT_FORMATS)}; got {value!r}. "
            "Use read_vortex/native_vortex routes for existing .vortex input."
        )
    return normalized


def _format_from_suffix(value: str | os.PathLike[str]) -> str | None:
    suffix = Path(value).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".arrow", ".ipc", ".feather"}:
        return "arrow-ipc"
    if suffix == ".avro":
        return "avro"
    if suffix == ".orc":
        return "orc"
    if suffix == ".vortex":
        raise ValueError(
            "prepared Vortex compatibility routes start from raw compatibility input; "
            "use read_vortex/native_vortex routes for existing .vortex input."
        )
    return None


def _infer_input_format(
    fact_input: str | os.PathLike[str],
    dim_input: str | os.PathLike[str],
) -> str:
    fact_format = _format_from_suffix(fact_input)
    dim_format = _format_from_suffix(dim_input)
    if fact_format is not None and dim_format is not None and fact_format != dim_format:
        raise ValueError(
            "prepared Vortex compatibility routes require fact and dimension inputs "
            "to infer the same input_format; "
            f"got fact={fact_format!r} and dim={dim_format!r}. "
            "Pass input_format explicitly only when the inputs are intentionally handled "
            "as one compatibility format."
        )
    if fact_format is not None and dim_format is not None and fact_format == dim_format:
        return fact_format
    if fact_format is not None and dim_format is None:
        return fact_format
    if dim_format is not None and fact_format is None:
        return dim_format
    return "csv"


def _as_check(default: bool, override: bool | None) -> bool:
    return default if override is None else override


def _stable_json_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _normalized_path(value: str | os.PathLike[str]) -> str:
    return str(Path(value).expanduser().resolve(strict=False))


def _file_content_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _local_path_fingerprint(value: str | os.PathLike[str] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    path = Path(value).expanduser()
    normalized = _normalized_path(path)
    if path.is_file():
        stat = path.stat()
        return {
            "path": normalized,
            "exists": True,
            "kind": "local_file_sha256_size_mtime",
            "size_bytes": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "content_digest": _file_content_digest(path),
            "content_digest_status": "computed_for_local_file_fingerprint",
            "digest_policy": "content_digest_size_mtime_normal_warm_reuse",
        }
    if path.is_dir():
        total_size = 0
        max_mtime = 0
        digest = hashlib.sha256()
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            stat = child.stat()
            relative = child.relative_to(path).as_posix()
            child_digest = _file_content_digest(child)
            total_size += stat.st_size
            max_mtime = max(max_mtime, stat.st_mtime_ns)
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(str(stat.st_size).encode("ascii"))
            digest.update(b"\0")
            digest.update(str(stat.st_mtime_ns).encode("ascii"))
            digest.update(b"\0")
            digest.update(child_digest.encode("ascii"))
            digest.update(b"\0")
        return {
            "path": normalized,
            "exists": True,
            "kind": "local_directory_tree_sha256_size_mtime",
            "size_bytes": total_size,
            "mtime_ns": max_mtime,
            "content_digest": "sha256:" + digest.hexdigest(),
            "content_digest_status": "computed_for_directory_tree_fingerprint",
            "digest_policy": "directory_tree_digest_required_until_metadata_tree_policy_exists",
        }
    return {
        "path": normalized,
        "exists": False,
        "kind": "local_path_missing",
        "size_bytes": None,
        "mtime_ns": None,
        "content_digest": None,
        "content_digest_status": "not_available_path_missing",
        "digest_policy": "metadata_size_mtime_normal_warm_reuse",
    }


def _artifact_fingerprint_from_field(fields: Mapping[str, str], *keys: str) -> dict[str, Any] | None:
    for key in keys:
        value = fields.get(key)
        if value:
            return _local_path_fingerprint(value)
    return None


def _field_any(fields: Mapping[str, str], *keys: str, default: str = "") -> str:
    for key in keys:
        value = fields.get(key)
        if value not in {None, ""}:
            return value
    return default


def _bool_field(fields: Mapping[str, str], key: str) -> bool:
    return str(fields.get(key, "false")).strip().lower() == "true"


def _manifest_path(workspace: str | os.PathLike[str]) -> Path:
    return Path(workspace).expanduser() / _REUSE_MANIFEST_DIR / _REUSE_MANIFEST_FILE


def _index_path(workspace: str | os.PathLike[str]) -> Path:
    return Path(workspace).expanduser() / _REUSE_MANIFEST_DIR / _REUSE_INDEX_FILE


def _prepared_state_index_payload(
    manifest_payload: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    prepare_fields = manifest_payload.get("prepare_fields")
    fields = prepare_fields if isinstance(prepare_fields, Mapping) else {}
    artifacts = manifest_payload.get("prepared_artifacts")
    artifact_map = artifacts if isinstance(artifacts, Mapping) else {}

    def field(key: str) -> str:
        value = fields.get(key)
        return "" if value in {None, ""} else str(value)

    def artifact_path(role: str) -> str:
        artifact = artifact_map.get(role)
        if isinstance(artifact, Mapping):
            return str(artifact.get("path") or "none")
        return "none"

    def artifact_digest(role: str) -> str:
        artifact = artifact_map.get(role)
        if isinstance(artifact, Mapping):
            return str(artifact.get("digest") or "none")
        return "none"

    prepare_policy = manifest_payload.get("prepare_policy")
    prepare_policy_digest = (
        _stable_json_digest(prepare_policy)
        if isinstance(prepare_policy, Mapping)
        else "missing_prepare_policy"
    )
    key = {
        "schema_version": _PREPARED_STATE_INDEX_SCHEMA_VERSION,
        "source_admission_packet_digest": str(
            manifest_payload.get("source_admission_packet_digest")
            or "missing_source_admission_packet_digest"
        ),
        "schema_hash": "traditional_fact_dim_cdc_schema.v1",
        "route_family": "compatibility_prepare_to_prepared_native_vortex",
        "layout_policy": {
            "strategy": field("vortex_array_build_strategy"),
            "input_layout": field("vortex_array_build_input_layout"),
        },
        "native_io_status": field("native_io_certificate_status"),
        "artifact_refs": {
            "fact": artifact_path("fact"),
            "dim": artifact_path("dim"),
            "cdc_delta": artifact_path("cdc_delta"),
        },
        "artifact_digests": {
            "fact": artifact_digest("fact"),
            "dim": artifact_digest("dim"),
            "cdc_delta": artifact_digest("cdc_delta"),
        },
        "prepare_policy_digest": prepare_policy_digest,
    }
    index_digest = _stable_json_digest(key)
    return (
        {
            "schema_version": _PREPARED_STATE_INDEX_SCHEMA_VERSION,
            "index_digest": index_digest,
            "index_key": key,
            "manifest_digest": str(
                manifest_payload.get("manifest_digest") or "missing_manifest_digest"
            ),
            "manifest_path": str(
                manifest_payload.get("manifest_path")
                or "<workspace>/.shardloom/prepared-vortex-reuse-manifest.json"
            ),
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_boundary": (
                "workspace-local prepared-state index only; lookup never bypasses "
                "manifest digest, source fingerprint, artifact fingerprint, replay proof, "
                "Native I/O certificate, or no-fallback checks"
            ),
        },
        index_digest,
    )


def _prepared_state_index_digest_from_manifest(manifest_payload: Mapping[str, Any]) -> str:
    return _prepared_state_index_payload(manifest_payload)[1]


def _write_index_manifest(
    workspace: str | os.PathLike[str],
    manifest_payload: Mapping[str, Any],
) -> str:
    index_payload, index_digest = _prepared_state_index_payload(manifest_payload)
    index_file = _index_path(workspace)
    index_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = index_file.with_suffix(".tmp")
    tmp_path.write_text(
        json.dumps(index_payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(index_file)
    return index_digest


@dataclass(frozen=True, slots=True)
class _PreparedStateReuseDecision:
    hit: bool
    reason: str
    invalidation_reason: str
    manifest_digest: str | None
    manifest: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class PreparedVortexQuery:
    """Deferred single-scenario query over a compatibility prepared-Vortex route."""

    route: "CompatibilityPreparedVortexRoute"
    scenario: str
    result_workspace: str | os.PathLike[str] | None = None
    evidence_level: str | None = None
    memory_gb: int | None = None
    max_parallelism: int | None = None

    @property
    def route_id(self) -> str:
        """Return the route id represented by this single-scenario query."""

        return "local_file_prepare_once_first_query"

    @property
    def execution_mode(self) -> str:
        """Return the selected ShardLoom execution mode for the query."""

        return "prepared_vortex"

    @property
    def preparation_included(self) -> bool:
        """Whether the route starts from raw input and includes preparation."""

        return True

    @property
    def query_timing_starts_after_preparation(self) -> bool:
        """Whether prepared query timing is distinct from preparation timing."""

        return True

    def collect(self, *, check: bool | None = None) -> PreparedVortexBatchResult:
        """Run the single prepared query and return the ShardLoom route evidence."""

        return self.route.run(
            self.scenario,
            result_workspace=self.result_workspace,
            evidence_level=self.evidence_level,
            memory_gb=self.memory_gb,
            max_parallelism=self.max_parallelism,
            check=check,
        )

    def write_vortex(
        self,
        result_workspace: str | os.PathLike[str] | None = None,
        *,
        evidence_level: str | None = None,
        check: bool | None = None,
    ) -> PreparedVortexBatchResult:
        """Run the query and request a Vortex result sink from the route."""

        return self.route.run(
            self.scenario,
            result_workspace=result_workspace or self.result_workspace,
            write_result_vortex=True,
            evidence_level=evidence_level or self.evidence_level,
            memory_gb=self.memory_gb,
            max_parallelism=self.max_parallelism,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class CompatibilityPreparedVortexRoute:
    """Explicit raw compatibility input -> VortexPreparedState -> prepared query route."""

    client: ShardLoomClient
    fact_input: str | os.PathLike[str]
    dim_input: str | os.PathLike[str]
    workspace: str | os.PathLike[str]
    input_format: str
    cdc_delta_input: str | os.PathLike[str] | None = None
    result_workspace: str | os.PathLike[str] | None = None
    evidence_level: str | None = None
    memory_gb: int | None = None
    max_parallelism: int | None = None
    check: bool = True

    @classmethod
    def from_inputs(
        cls,
        *,
        client: ShardLoomClient,
        fact_input: str | os.PathLike[str],
        dim_input: str | os.PathLike[str],
        workspace: str | os.PathLike[str],
        input_format: str | None = None,
        cdc_delta_input: str | os.PathLike[str] | None = None,
        result_workspace: str | os.PathLike[str] | None = None,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool = True,
    ) -> "CompatibilityPreparedVortexRoute":
        """Build a route handle with an inferred or explicit compatibility input format."""

        normalized_format = (
            _normalize_input_format(input_format)
            if input_format is not None
            else _infer_input_format(fact_input, dim_input)
        )
        return cls(
            client=client,
            fact_input=fact_input,
            dim_input=dim_input,
            workspace=workspace,
            input_format=normalized_format,
            cdc_delta_input=cdc_delta_input,
            result_workspace=result_workspace,
            evidence_level=evidence_level,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    @property
    def route_id(self) -> str:
        """Return the first-query route id for this handle."""

        return "local_file_prepare_once_first_query"

    @property
    def batch_route_id(self) -> str:
        """Return the prepare-once batch route id for this handle."""

        return "local_file_prepare_once_batch"

    @property
    def start_state(self) -> str:
        """Return the user-visible route start state."""

        return "raw_compat_source"

    @property
    def source_route(self) -> str:
        """Return the source route used before preparation."""

        return "compatibility_import_certified"

    @property
    def preparation_route(self) -> str:
        """Return the preparation route used by this handle."""

        return "vortex_ingest_prepare_once"

    @property
    def execution_mode(self) -> str:
        """Return the single-query execution mode."""

        return "prepared_vortex"

    @property
    def batch_execution_mode(self) -> str:
        """Return the batch route execution mode."""

        return "shardloom-prepare-batch"

    @property
    def vortex_normalization_point(self) -> str:
        """Return the route's Vortex normalization boundary."""

        return "SourceState -> vortex_ingest -> VortexPreparedState"

    @property
    def route_runtime_status(self) -> str:
        """Return the route runtime support status."""

        return "scoped_runtime_supported"

    @property
    def fallback_attempted(self) -> bool:
        """Whether the route handle itself has attempted fallback execution."""

        return False

    @property
    def external_engine_invoked(self) -> bool:
        """Whether the route handle itself has invoked an external engine."""

        return False

    @property
    def preparation_included_in_route(self) -> bool:
        """Whether route execution starts from raw input and includes preparation."""

        return True

    @property
    def query_timing_starts_after_preparation(self) -> bool:
        """Whether per-query timings start after the prepare-once boundary."""

        return True

    @property
    def timing_contract(self) -> str:
        """Return the compact timing contract for user/agent display."""

        return (
            "The route starts at raw compatibility input and prepares once into "
            "VortexPreparedState. Query timing starts after preparation; route evidence "
            "keeps prepare, batch query, result sink, and no-fallback fields separate."
        )

    def route_fields(self) -> dict[str, Any]:
        """Return a side-effect-free route summary for agents and diagnostics."""

        return {
            "route_id": self.route_id,
            "batch_route_id": self.batch_route_id,
            "start_state": self.start_state,
            "source_route": self.source_route,
            "preparation_route": self.preparation_route,
            "execution_mode": self.execution_mode,
            "batch_execution_mode": self.batch_execution_mode,
            "input_format": self.input_format,
            "vortex_normalization_point": self.vortex_normalization_point,
            "route_runtime_status": self.route_runtime_status,
            "preparation_included_in_route": self.preparation_included_in_route,
            "query_timing_starts_after_preparation": self.query_timing_starts_after_preparation,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "prepared_state_reuse_scope": "workspace_manifest_local_vortex_artifacts",
            "prepared_state_reuse_manifest_path": str(_manifest_path(self.workspace)),
            "prepared_state_reuse_policy": _REUSE_MANIFEST_SCHEMA_VERSION,
            "prepared_state_index_schema_version": _PREPARED_STATE_INDEX_SCHEMA_VERSION,
            "prepared_state_index_path": str(_index_path(self.workspace)),
            "timing_contract": self.timing_contract,
        }

    def prepare(self, *, check: bool | None = None) -> PreparedVortexArtifacts:
        """Run certified compatibility preparation and return reusable Vortex artifacts."""

        decision = self._prepared_state_reuse_decision()
        if decision.hit and decision.manifest is not None:
            return PreparedVortexArtifacts(
                self._prepare_envelope_from_manifest(
                    decision.manifest,
                    decision,
                    command="prepared-vortex-reuse-manifest",
                )
            )

        artifacts = self.client.prepare_traditional_analytics_vortex_artifacts(
            self.fact_input,
            self.dim_input,
            workspace=self.workspace,
            input_format=self.input_format,
            cdc_delta_input=self.cdc_delta_input,
            memory_gb=self.memory_gb,
            max_parallelism=self.max_parallelism,
            check=_as_check(self.check, check),
        )
        artifacts = PreparedVortexArtifacts(
            self._envelope_with_reuse_decision(artifacts.prepare, decision)
        )
        self._write_reuse_manifest(artifacts.prepare)
        return artifacts

    def query(
        self,
        scenario: str,
        *,
        result_workspace: str | os.PathLike[str] | None = None,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
    ) -> PreparedVortexQuery:
        """Build a single-scenario prepared query over this route."""

        return PreparedVortexQuery(
            route=self,
            scenario=scenario,
            result_workspace=result_workspace or self.result_workspace,
            evidence_level=evidence_level or self.evidence_level,
            memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
            max_parallelism=(
                max_parallelism if max_parallelism is not None else self.max_parallelism
            ),
        )

    def run(
        self,
        scenario: str,
        *,
        result_workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool | None = None,
    ) -> PreparedVortexBatchResult:
        """Run one scenario through the prepare-once first-query route."""

        return self.run_batch(
            scenario,
            result_workspace=result_workspace,
            write_result_vortex=write_result_vortex,
            evidence_level=evidence_level,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    def run_batch(
        self,
        scenarios: str | Sequence[str],
        *,
        result_workspace: str | os.PathLike[str] | None = None,
        write_result_vortex: bool = False,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        check: bool | None = None,
    ) -> PreparedVortexBatchResult:
        """Prepare once, then run one or more prepared Vortex scenarios."""

        decision = self._prepared_state_reuse_decision()
        if decision.hit and decision.manifest is not None:
            manifest = decision.manifest
            artifacts = self._manifest_artifacts(manifest)
            batch_envelope = self.client.traditional_analytics_vortex_batch_run(
                scenarios,
                artifacts["fact_vortex_path"],
                artifacts["dim_vortex_path"],
                cdc_delta_vortex=artifacts.get("cdc_delta_vortex_path") or None,
                workspace=result_workspace or self.result_workspace,
                write_result_vortex=write_result_vortex,
                execution_mode="prepared_vortex",
                evidence_level=evidence_level or self.evidence_level,
                memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
                max_parallelism=(
                    max_parallelism if max_parallelism is not None else self.max_parallelism
                ),
                check=_as_check(self.check, check),
            )
            return PreparedVortexBatchResult(
                prepare=self._prepare_envelope_from_manifest(
                    manifest,
                    decision,
                    command="prepared-vortex-reuse-manifest",
                ),
                batch=self._combine_reuse_batch_envelope(batch_envelope, manifest, decision),
            )

        result = self.client.prepare_and_run_traditional_analytics_vortex_batch(
            scenarios,
            self.fact_input,
            self.dim_input,
            workspace=self.workspace,
            input_format=self.input_format,
            cdc_delta_input=self.cdc_delta_input,
            result_workspace=result_workspace or self.result_workspace,
            write_result_vortex=write_result_vortex,
            evidence_level=evidence_level or self.evidence_level,
            memory_gb=memory_gb if memory_gb is not None else self.memory_gb,
            max_parallelism=(
                max_parallelism if max_parallelism is not None else self.max_parallelism
            ),
            check=_as_check(self.check, check),
        )
        result = PreparedVortexBatchResult(
            prepare=self._envelope_with_reuse_decision(result.prepare, decision),
            batch=self._envelope_with_reuse_decision(result.batch, decision),
        )
        self._write_reuse_manifest(result.prepare)
        return result

    def _prepare_policy(self) -> dict[str, Any]:
        """Return the strict prepared-artifact policy used for manifest reuse."""

        artifact_root = _normalized_path(self.workspace)
        return {
            "input_format": self.input_format,
            "artifact_root": artifact_root,
            "artifact_layout": "fact.vortex,dim.vortex,optional_cdc_delta.vortex",
            "artifact_output_policy": "caller_owned_workspace_local_vortex_artifacts",
            "cdc_delta_present": self.cdc_delta_input is not None,
            "memory_gb": self.memory_gb,
            "max_parallelism": self.max_parallelism,
            "vortex_normalization_point": self.vortex_normalization_point,
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }

    def _source_admission_packet(
        self,
        fact_input: Mapping[str, Any] | None,
        dim_input: Mapping[str, Any] | None,
        cdc_delta_input: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Return the metadata-first source-admission packet for manifest reuse."""

        packet: dict[str, Any] = {
            "schema_version": _SOURCE_ADMISSION_PACKET_SCHEMA_VERSION,
            "packet_kind": "local_source_admission_prediction",
            "route_family": "compatibility_prepare_to_prepared_native_vortex",
            "route_id": self.route_id,
            "batch_route_id": self.batch_route_id,
            "input_format": self.input_format,
            "source_path_fingerprint_kind": "local_path_sha256_size_mtime",
            "digest_policy": {
                "schema_version": _SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION,
                "status": "content_digest_fingerprint",
                "normal_warm_reuse_content_digest_requested": True,
                "claim_grade_content_digest_required": True,
                "full_content_digest_status": "computed_for_local_source_fingerprint",
                "claim_boundary": (
                    "local warm reuse compares normalized path, size, mtime, and content "
                    "digest; claim-grade publication rows still require the full benchmark "
                    "publication evidence gate"
                ),
            },
            "fact_input": fact_input,
            "dim_input": dim_input,
            "cdc_delta_input": cdc_delta_input,
            "artifact_root": _normalized_path(self.workspace),
            "fallback_attempted": False,
            "external_engine_invoked": False,
        }
        packet["packet_digest"] = _stable_json_digest(packet)
        return packet

    def _reuse_request_payload(self) -> dict[str, Any]:
        """Return a fingerprint-backed prepared-state reuse request payload."""

        fact_input = _local_path_fingerprint(self.fact_input)
        dim_input = _local_path_fingerprint(self.dim_input)
        cdc_delta_input = _local_path_fingerprint(self.cdc_delta_input)
        source_admission_packet = self._source_admission_packet(
            fact_input,
            dim_input,
            cdc_delta_input,
        )
        payload = {
            "schema_version": _REUSE_MANIFEST_SCHEMA_VERSION,
            "route_id": self.route_id,
            "batch_route_id": self.batch_route_id,
            "fact_input": fact_input,
            "dim_input": dim_input,
            "cdc_delta_input": cdc_delta_input,
            "source_admission_packet": source_admission_packet,
            "source_admission_packet_digest": source_admission_packet["packet_digest"],
            "prepare_policy": self._prepare_policy(),
        }
        payload["route_request_digest"] = _stable_json_digest(payload)
        return payload

    def _prepared_state_reuse_decision(self) -> _PreparedStateReuseDecision:
        """Return the manifest-backed reuse decision for this route."""

        manifest_file = _manifest_path(self.workspace)
        request_payload = self._reuse_request_payload()
        request_digest = str(request_payload["route_request_digest"])
        if not manifest_file.exists():
            return _PreparedStateReuseDecision(
                hit=False,
                reason="no_reuse_manifest",
                invalidation_reason="no_reuse_manifest",
                manifest_digest=None,
            )
        try:
            manifest_payload = json.loads(manifest_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_unreadable",
                invalidation_reason=f"reuse_manifest_unreadable:{exc.__class__.__name__}",
                manifest_digest=None,
            )
        if not isinstance(manifest_payload, Mapping):
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_invalid_shape",
                invalidation_reason="reuse_manifest_invalid_shape",
                manifest_digest=None,
            )
        manifest_digest = str(manifest_payload.get("manifest_digest") or "")
        if manifest_payload.get("schema_version") != _REUSE_MANIFEST_SCHEMA_VERSION:
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_schema_mismatch",
                invalidation_reason="reuse_manifest_schema_mismatch",
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        expected_manifest_digest = _stable_json_digest(
            {
                str(key): value
                for key, value in manifest_payload.items()
                if key != "manifest_digest"
            }
        )
        if manifest_digest != expected_manifest_digest:
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_digest_mismatch",
                invalidation_reason="reuse_manifest_digest_mismatch",
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        if manifest_payload.get("route_request_digest") != request_digest:
            reason = self._request_invalidation_reason(manifest_payload, request_payload)
            return _PreparedStateReuseDecision(
                hit=False,
                reason=reason,
                invalidation_reason=reason,
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        artifact_reason = self._artifact_invalidation_reason(manifest_payload)
        if artifact_reason != "none":
            return _PreparedStateReuseDecision(
                hit=False,
                reason=artifact_reason,
                invalidation_reason=artifact_reason,
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        if manifest_payload.get("fallback_attempted") is True:
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_fallback_attempted",
                invalidation_reason="reuse_manifest_fallback_attempted",
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        if manifest_payload.get("external_engine_invoked") is True:
            return _PreparedStateReuseDecision(
                hit=False,
                reason="reuse_manifest_external_engine_invoked",
                invalidation_reason="reuse_manifest_external_engine_invoked",
                manifest_digest=manifest_digest or None,
                manifest=manifest_payload,
            )
        return _PreparedStateReuseDecision(
            hit=True,
            reason="manifest_fingerprints_match",
            invalidation_reason="none",
            manifest_digest=manifest_digest or None,
            manifest=manifest_payload,
        )

    def _request_invalidation_reason(
        self,
        manifest_payload: Mapping[str, Any],
        request_payload: Mapping[str, Any],
    ) -> str:
        for key in ("fact_input", "dim_input", "cdc_delta_input"):
            if manifest_payload.get(key) != request_payload.get(key):
                return f"{key}_fingerprint_changed"
        if manifest_payload.get("prepare_policy") != request_payload.get("prepare_policy"):
            return "prepare_policy_changed"
        if manifest_payload.get("source_admission_packet_digest") != request_payload.get(
            "source_admission_packet_digest"
        ):
            return "source_admission_packet_changed"
        return "route_request_digest_mismatch"

    def _artifact_invalidation_reason(self, manifest_payload: Mapping[str, Any]) -> str:
        artifacts = manifest_payload.get("prepared_artifacts")
        if not isinstance(artifacts, Mapping):
            return "reuse_manifest_missing_prepared_artifacts"
        prepare_policy = manifest_payload.get("prepare_policy")
        cdc_required = (
            isinstance(prepare_policy, Mapping)
            and prepare_policy.get("cdc_delta_present") is True
        )
        for role in ("fact", "dim", "cdc_delta"):
            stored = artifacts.get(role)
            if stored is None and role == "cdc_delta":
                if cdc_required:
                    return "cdc_delta_prepared_artifact_manifest_missing"
                continue
            if not isinstance(stored, Mapping):
                return f"{role}_prepared_artifact_manifest_missing"
            path = stored.get("path")
            if not path:
                return f"{role}_prepared_artifact_path_missing"
            current = _local_path_fingerprint(str(path))
            if current != stored.get("fingerprint"):
                return f"{role}_prepared_artifact_fingerprint_changed"
        return "none"

    def _manifest_artifacts(self, manifest_payload: Mapping[str, Any]) -> dict[str, str]:
        artifacts = manifest_payload.get("prepared_artifacts")
        if not isinstance(artifacts, Mapping):
            raise ValueError("prepared-state reuse manifest is missing prepared_artifacts")
        fact = artifacts.get("fact")
        dim = artifacts.get("dim")
        cdc = artifacts.get("cdc_delta")
        if not isinstance(fact, Mapping) or not isinstance(dim, Mapping):
            raise ValueError("prepared-state reuse manifest is missing fact/dim artifacts")
        result = {
            "fact_vortex_path": str(fact.get("path") or ""),
            "dim_vortex_path": str(dim.get("path") or ""),
        }
        if isinstance(cdc, Mapping) and cdc.get("path"):
            result["cdc_delta_vortex_path"] = str(cdc["path"])
        return result

    def _write_reuse_manifest(self, envelope: OutputEnvelope) -> None:
        """Write/update the workspace manifest after a successful preparation."""

        fields = envelope.field_map
        fallback_attempted = envelope.fallback.attempted or _bool_field(
            fields,
            "fallback_attempted",
        )
        external_engine_invoked = _bool_field(fields, "external_engine_invoked")
        if envelope.status != "success" or fallback_attempted or external_engine_invoked:
            return
        fact_artifact = _artifact_fingerprint_from_field(
            fields,
            "prepare_batch_fact_vortex_path",
            "prepared_artifact_fact_ref",
            "fact_vortex_path",
        )
        dim_artifact = _artifact_fingerprint_from_field(
            fields,
            "prepare_batch_dim_vortex_path",
            "prepared_artifact_dim_ref",
            "dim_vortex_path",
        )
        if (
            fact_artifact is None
            or dim_artifact is None
            or not fact_artifact.get("exists")
            or not dim_artifact.get("exists")
        ):
            return
        cdc_artifact = _artifact_fingerprint_from_field(
            fields,
            "prepare_batch_cdc_delta_vortex_path",
            "prepared_artifact_cdc_delta_ref",
            "cdc_delta_vortex_path",
        )
        if self.cdc_delta_input is not None and (
            cdc_artifact is None or not cdc_artifact.get("exists")
        ):
            return
        manifest_prepare_fields = {
            key: value
            for key, value in fields.items()
            if key
            not in {
                "prepared_state_reuse_hit",
                "prepared_state_reuse_reason",
                "prepared_state_reuse_manifest_digest",
                "invalidation_reason",
            }
        }
        request_payload = self._reuse_request_payload()
        prepared_artifacts: dict[str, Any] = {
            "fact": {
                "path": fact_artifact["path"],
                "fingerprint": fact_artifact,
                "digest": _field_any(
                    fields,
                    "prepare_batch_fact_vortex_digest",
                    "prepared_artifact_fact_digest",
                    default=str(fact_artifact.get("content_digest") or ""),
                ),
            },
            "dim": {
                "path": dim_artifact["path"],
                "fingerprint": dim_artifact,
                "digest": _field_any(
                    fields,
                    "prepare_batch_dim_vortex_digest",
                    "prepared_artifact_dim_digest",
                    default=str(dim_artifact.get("content_digest") or ""),
                ),
            },
        }
        if cdc_artifact is not None and cdc_artifact.get("exists"):
            prepared_artifacts["cdc_delta"] = {
                "path": cdc_artifact["path"],
                "fingerprint": cdc_artifact,
                "digest": _field_any(
                    fields,
                    "prepare_batch_cdc_delta_vortex_digest",
                    "prepared_artifact_cdc_delta_digest",
                    default=str(cdc_artifact.get("content_digest") or ""),
                ),
            }
        source_admission_packet_artifact_manifest_hash = _stable_json_digest(
            prepared_artifacts
        )
        manifest_payload: dict[str, Any] = {
            **request_payload,
            "created_unix_seconds": int(time.time()),
            "manifest_path": str(_manifest_path(self.workspace)),
            "prepare_command": envelope.command,
            "prepare_fields": manifest_prepare_fields,
            "prepared_artifacts": prepared_artifacts,
            "source_admission_packet_artifact_manifest_hash": (
                source_admission_packet_artifact_manifest_hash
            ),
            "source_admission_digest_policy_schema_version": (
                _SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION
            ),
            "source_admission_digest_policy_status": "content_digest_fingerprint",
            "source_admission_full_content_digest_requested": True,
            "source_admission_full_content_digest_required_for_claim_grade": True,
            "source_admission_digest_policy_claim_boundary": (
                "normal local warm reuse compares normalized path, size, mtime, and content "
                "digest; claim-grade publication evidence must still pass the publication "
                "evidence gate"
            ),
            "prepared_state_dependency_schema_version": _field_any(
                fields,
                "prepare_batch_prepared_state_dependency_schema_version",
                default=_PREPARED_STATE_DEPENDENCY_SCHEMA_VERSION,
            ),
            "prepared_state_dependency_status": _field_any(
                fields,
                "prepare_batch_prepared_state_dependency_status",
                default="manifest_dependencies_registered_after_prepare",
            ),
            "prepared_state_dependency_checked_roles": _field_any(
                fields,
                "prepare_batch_prepared_state_dependency_checked_roles",
                default=_PREPARED_BATCH_DEPENDENCY_CHECKED_ROLES,
            ),
            "prepared_state_dependency_changed_roles": _field_any(
                fields,
                "prepare_batch_prepared_state_dependency_changed_roles",
                default="workspace_manifest",
            ),
            "prepared_state_dependency_recheck_policy": _field_any(
                fields,
                "prepare_batch_prepared_state_dependency_recheck_policy",
                default=(
                    "validate_manifest_digest_route_request_source_fingerprints_"
                    "artifact_fingerprints_no_fallback_before_reuse"
                ),
            ),
            "prepared_state_partial_repair_schema_version": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_schema_version",
                default=_PREPARED_STATE_PARTIAL_REPAIR_SCHEMA_VERSION,
            ),
            "prepared_state_partial_repair_status": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_status",
                default="blocked_missing_base_manifest_full_prepare_required",
            ),
            "prepared_state_partial_repair_changed_roles": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_changed_roles",
                default="workspace_manifest",
            ),
            "prepared_state_partial_repair_reused_roles": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_reused_roles",
                default="none",
            ),
            "prepared_state_partial_repair_repaired_roles": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_repaired_roles",
                default="none",
            ),
            "prepared_state_partial_repair_invalidated_derived_states": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_invalidated_derived_states",
                default="all_prepared_state_derived_indexes",
            ),
            "prepared_state_partial_repair_replay_proof": _field_any(
                fields,
                "prepare_batch_prepared_state_partial_repair_replay_proof",
                default="not_applicable_full_prepare",
            ),
            "prepared_state_partial_repair_regeneration_performed": _bool_field(
                fields,
                "prepare_batch_prepared_state_partial_repair_regeneration_performed",
            ),
            "prepared_state_partial_repair_stale_segment_reuse_allowed": _bool_field(
                fields,
                "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed",
            ),
            "source_state_id": _field_any(
                fields,
                "prepare_batch_source_state_id",
                "source_state_id",
            ),
            "source_state_digest": _field_any(
                fields,
                "prepare_batch_source_state_digest",
                "source_state_digest",
            ),
            "prepared_state_id": _field_any(
                fields,
                "prepare_batch_prepared_state_id",
                "prepared_state_id",
            ),
            "prepared_state_digest": _field_any(
                fields,
                "prepare_batch_prepared_state_digest",
                "prepared_state_digest",
            ),
            "fallback_attempted": fallback_attempted,
            "external_engine_invoked": external_engine_invoked,
        }
        manifest_payload["manifest_digest"] = _stable_json_digest(manifest_payload)
        manifest_file = _manifest_path(self.workspace)
        manifest_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = manifest_file.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(
                manifest_payload,
                sort_keys=True,
                indent=2,
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(manifest_file)
        _write_index_manifest(self.workspace, manifest_payload)

    def _prepare_envelope_from_manifest(
        self,
        manifest_payload: Mapping[str, Any],
        decision: _PreparedStateReuseDecision,
        *,
        command: str,
    ) -> OutputEnvelope:
        prepare_fields = manifest_payload.get("prepare_fields")
        fields = dict(prepare_fields) if isinstance(prepare_fields, Mapping) else {}
        artifacts = self._manifest_artifacts(manifest_payload)
        index_digest = _prepared_state_index_digest_from_manifest(manifest_payload)
        fields.update(
            {
                "fact_vortex_path": artifacts["fact_vortex_path"],
                "dim_vortex_path": artifacts["dim_vortex_path"],
                "cdc_delta_vortex_path": artifacts.get("cdc_delta_vortex_path", ""),
                "prepared_artifact_fact_ref": artifacts["fact_vortex_path"],
                "prepared_artifact_dim_ref": artifacts["dim_vortex_path"],
                "prepared_artifact_cdc_delta_ref": artifacts.get(
                    "cdc_delta_vortex_path",
                    "",
                ),
                "prepare_batch_fact_vortex_path": artifacts["fact_vortex_path"],
                "prepare_batch_dim_vortex_path": artifacts["dim_vortex_path"],
                "prepare_batch_cdc_delta_vortex_path": artifacts.get(
                    "cdc_delta_vortex_path",
                    "",
                ),
                "prepared_state_reused": "true",
                "prepared_state_reuse_hit": str(decision.hit).lower(),
                "prepared_state_reuse_reason": decision.reason,
                "prepared_state_reuse_manifest_digest": decision.manifest_digest or "none",
                "invalidation_reason": decision.invalidation_reason,
                "prepared_state_index_schema_version": _PREPARED_STATE_INDEX_SCHEMA_VERSION,
                "prepared_state_index_digest": index_digest,
                "prepared_state_index_lookup_status": "workspace_index_manifest_hit",
                "prepared_state_dependency_schema_version": str(
                    manifest_payload.get("prepared_state_dependency_schema_version")
                    or _PREPARED_STATE_DEPENDENCY_SCHEMA_VERSION
                ),
                "prepared_state_dependency_status": "manifest_dependencies_matched",
                "prepared_state_dependency_checked_roles": str(
                    manifest_payload.get("prepared_state_dependency_checked_roles")
                    or _PREPARED_BATCH_DEPENDENCY_CHECKED_ROLES
                ),
                "prepared_state_dependency_changed_roles": "none",
                "prepared_state_partial_repair_schema_version": str(
                    manifest_payload.get("prepared_state_partial_repair_schema_version")
                    or _PREPARED_STATE_PARTIAL_REPAIR_SCHEMA_VERSION
                ),
                "prepared_state_partial_repair_status": "not_needed_manifest_hit",
                "prepared_state_partial_repair_changed_roles": "none",
                "prepared_state_partial_repair_reused_roles": "fact_input,dim_input,cdc_delta_input",
                "prepared_state_partial_repair_repaired_roles": "none",
                "prepared_state_partial_repair_invalidated_derived_states": "none",
                "prepared_state_partial_repair_regeneration_performed": "false",
                "prepared_state_partial_repair_stale_segment_reuse_allowed": "false",
                "fallback_attempted": "false",
                "external_engine_invoked": "false",
            }
        )
        return OutputEnvelope.from_field_mapping(
            fields,
            command=command,
            summary="prepared Vortex state reuse manifest",
            human_text="prepared Vortex state reuse manifest",
            fallback_attempted=False,
        )

    def _envelope_with_reuse_decision(
        self,
        envelope: OutputEnvelope,
        decision: _PreparedStateReuseDecision,
    ) -> OutputEnvelope:
        fields = dict(envelope.field_map)
        fields.update(
            {
                "prepared_state_reuse_hit": str(decision.hit).lower(),
                "prepared_state_reuse_reason": decision.reason,
                "prepared_state_reuse_manifest_digest": decision.manifest_digest or "none",
                "invalidation_reason": decision.invalidation_reason,
                "fallback_attempted": str(envelope.fallback.attempted).lower(),
                "external_engine_invoked": _field_any(
                    fields,
                    "external_engine_invoked",
                    default="false",
                ),
            }
        )
        return OutputEnvelope.from_field_mapping(
            fields,
            command=envelope.command,
            status=envelope.status,
            summary=envelope.summary,
            human_text=envelope.human_text,
            fallback_attempted=envelope.fallback.attempted,
        )

    def _combine_reuse_batch_envelope(
        self,
        batch_envelope: OutputEnvelope,
        manifest_payload: Mapping[str, Any],
        decision: _PreparedStateReuseDecision,
    ) -> OutputEnvelope:
        fields = dict(batch_envelope.field_map)
        artifacts = self._manifest_artifacts(manifest_payload)
        index_digest = _prepared_state_index_digest_from_manifest(manifest_payload)
        prepared_artifacts = manifest_payload.get("prepared_artifacts")
        artifact_fields = prepared_artifacts if isinstance(prepared_artifacts, Mapping) else {}
        fact_artifact = artifact_fields.get("fact")
        dim_artifact = artifact_fields.get("dim")
        cdc_artifact = artifact_fields.get("cdc_delta")
        fact_digest = (
            str(fact_artifact.get("digest"))
            if isinstance(fact_artifact, Mapping)
            else ""
        )
        dim_digest = (
            str(dim_artifact.get("digest"))
            if isinstance(dim_artifact, Mapping)
            else ""
        )
        cdc_digest = (
            str(cdc_artifact.get("digest"))
            if isinstance(cdc_artifact, Mapping)
            else ""
        )
        result_sink_requested = _bool_field(fields, "result_sink_requested")
        result_sink_verified = _bool_field(fields, "all_result_sink_replays_verified")
        lifecycle_status = (
            "prepared_vortex_lifecycle_complete_with_output_replay"
            if result_sink_requested and result_sink_verified
            else "prepared_vortex_lifecycle_scan_complete_output_not_requested"
        )
        lifecycle_output_status = (
            "vortex_result_sink_written_and_replay_verified"
            if result_sink_requested and result_sink_verified
            else (
                "vortex_result_sink_requested_replay_incomplete"
                if result_sink_requested
                else "vortex_result_sink_not_requested"
            )
        )
        fields.update(
            {
                "prepare_batch_schema_version": "shardloom.traditional_analytics.prepare_and_batch.v1",
                "prepare_batch_runtime_status": "workspace_prepared_state_reused_then_prepared_batch_supported",
                "prepare_batch_route": "compatibility_import_certified_manifest_reuse_to_prepared_vortex_batch",
                "prepare_batch_preparation_command": "prepared-vortex-reuse-manifest",
                "prepare_batch_batch_command": "traditional-analytics-vortex-batch-run",
                "prepare_batch_preparation_scenario": "prepared-state reuse manifest",
                "prepare_batch_preparation_input_format": self.input_format,
                "prepare_batch_preparation_timing_scope": "workspace_manifest_reuse_skips_compatibility_prepare",
                "prepare_batch_preparation_micros": "0",
                "prepare_batch_source_to_columnar_micros": "0",
                "prepare_batch_vortex_array_build_micros": "0",
                "prepare_batch_vortex_write_micros": "0",
                "prepare_batch_vortex_reopen_verify_micros": "0",
                "prepare_batch_preparation_included_in_batch_timing": "false",
                "prepare_batch_query_timing_starts_after_preparation": "true",
                "prepare_batch_prepared_state_created": "false",
                "prepare_batch_prepared_state_reused": "true",
                "prepare_batch_prepared_state_reuse_hit": str(decision.hit).lower(),
                "prepare_batch_prepared_state_reuse_reason": decision.reason,
                "prepare_batch_prepared_state_reuse_manifest_digest": decision.manifest_digest
                or "none",
                "prepared_state_reuse_hit": str(decision.hit).lower(),
                "prepared_state_reuse_reason": decision.reason,
                "prepared_state_reuse_manifest_digest": decision.manifest_digest or "none",
                "invalidation_reason": decision.invalidation_reason,
                "prepare_batch_prepared_state_id": str(
                    manifest_payload.get("prepared_state_id") or ""
                ),
                "prepare_batch_prepared_state_digest": str(
                    manifest_payload.get("prepared_state_digest") or ""
                ),
                "prepare_batch_source_state_id": str(
                    manifest_payload.get("source_state_id") or ""
                ),
                "prepare_batch_source_state_digest": str(
                    manifest_payload.get("source_state_digest") or ""
                ),
                "prepare_batch_source_admission_packet_schema_version": (
                    _SOURCE_ADMISSION_PACKET_SCHEMA_VERSION
                ),
                "prepare_batch_source_admission_packet_status": "packet_reuse",
                "prepare_batch_source_admission_packet_digest": str(
                    manifest_payload.get("source_admission_packet_digest") or ""
                ),
                "prepare_batch_source_admission_packet_artifact_manifest_hash": str(
                    manifest_payload.get(
                        "source_admission_packet_artifact_manifest_hash"
                    )
                    or ""
                ),
                "prepare_batch_source_admission_digest_policy_schema_version": (
                    _SOURCE_ADMISSION_DIGEST_POLICY_SCHEMA_VERSION
                ),
                "prepare_batch_source_admission_digest_policy_status": (
                    "content_digest_fingerprint_reuse_hit"
                ),
                "prepare_batch_source_admission_full_content_digest_requested": "true",
                "prepare_batch_source_admission_full_content_digest_micros": "0",
                "prepare_batch_prepared_state_lookup_timing_schema_version": (
                    "shardloom.traditional_analytics.prepared_state_lookup_timing.v1"
                ),
                "prepare_batch_prepared_state_lookup_status": "workspace_manifest_hit",
                "prepare_batch_prepared_state_index_schema_version": (
                    _PREPARED_STATE_INDEX_SCHEMA_VERSION
                ),
                "prepare_batch_prepared_state_index_lookup_status": (
                    "workspace_index_manifest_hit"
                ),
                "prepare_batch_prepared_state_index_digest": index_digest,
                "prepare_batch_prepared_state_index_key_components": (
                    "source_admission_packet_digest,schema_hash,route_family,"
                    "layout_policy,native_io_status,artifact_refs,artifact_digests,"
                    "prepare_policy_digest"
                ),
                "prepare_batch_prepared_state_index_source_packet_digest": str(
                    manifest_payload.get("source_admission_packet_digest") or ""
                ),
                "prepare_batch_prepared_state_index_external_engine_invoked": "false",
                "prepare_batch_prepared_state_manifest_lookup_micros": "0",
                "prepare_batch_prepared_state_cache_hit_micros": "0",
                "prepare_batch_prepared_state_cache_miss_create_micros": "0",
                "prepare_batch_prepared_state_artifact_write_micros": "0",
                "prepare_batch_prepared_state_artifact_register_micros": "0",
                "prepare_batch_prepared_state_replay_verification_micros": "0",
                "prepare_batch_prepared_state_dependency_schema_version": str(
                    manifest_payload.get("prepared_state_dependency_schema_version")
                    or _PREPARED_STATE_DEPENDENCY_SCHEMA_VERSION
                ),
                "prepare_batch_prepared_state_dependency_status": (
                    "manifest_dependencies_matched"
                ),
                "prepare_batch_prepared_state_dependency_checked_roles": str(
                    manifest_payload.get("prepared_state_dependency_checked_roles")
                    or _PREPARED_BATCH_DEPENDENCY_CHECKED_ROLES
                ),
                "prepare_batch_prepared_state_dependency_changed_roles": "none",
                "prepare_batch_prepared_state_dependency_manifest_digest": (
                    decision.manifest_digest or "none"
                ),
                "prepare_batch_prepared_state_dependency_source_packet_digest": str(
                    manifest_payload.get("source_admission_packet_digest") or ""
                ),
                "prepare_batch_prepared_state_dependency_artifact_manifest_hash": str(
                    manifest_payload.get(
                        "source_admission_packet_artifact_manifest_hash"
                    )
                    or ""
                ),
                "prepare_batch_prepared_state_dependency_recheck_policy": str(
                    manifest_payload.get("prepared_state_dependency_recheck_policy")
                    or (
                        "validate_manifest_digest_route_request_source_fingerprints_"
                        "artifact_fingerprints_no_fallback_before_reuse"
                    )
                ),
                "prepare_batch_prepared_state_dependency_fallback_attempted": "false",
                "prepare_batch_prepared_state_dependency_external_engine_invoked": "false",
                "prepare_batch_prepared_state_partial_repair_schema_version": str(
                    manifest_payload.get("prepared_state_partial_repair_schema_version")
                    or _PREPARED_STATE_PARTIAL_REPAIR_SCHEMA_VERSION
                ),
                "prepare_batch_prepared_state_partial_repair_status": (
                    "not_needed_manifest_hit"
                ),
                "prepare_batch_prepared_state_partial_repair_blocker_id": (
                    "not_applicable_manifest_dependencies_matched"
                ),
                "prepare_batch_prepared_state_partial_repair_changed_roles": "none",
                "prepare_batch_prepared_state_partial_repair_reused_roles": (
                    "fact_input,dim_input,cdc_delta_input"
                ),
                "prepare_batch_prepared_state_partial_repair_repaired_roles": "none",
                "prepare_batch_prepared_state_partial_repair_invalidated_derived_states": (
                    "none"
                ),
                "prepare_batch_prepared_state_partial_repair_micros": "0",
                "prepare_batch_prepared_state_partial_repair_replay_proof": (
                    "not_needed_manifest_hit"
                ),
                "prepare_batch_prepared_state_partial_repair_repairable_segment_count": "0",
                "prepare_batch_prepared_state_partial_repair_regeneration_performed": "false",
                "prepare_batch_prepared_state_partial_repair_stale_segment_reuse_allowed": "false",
                "prepare_batch_prepared_state_partial_repair_claim_boundary": (
                    "workspace manifest reuse hit; no role-scoped repair was needed and "
                    "no stale changed-role artifact was reused"
                ),
                "prepare_batch_prepared_artifact_reuse_count": "1",
                "prepare_batch_prepared_artifact_cleanup_policy": "caller_owned_workspace_cleanup",
                "prepare_batch_prepared_artifact_reuse_eligible": "true",
                "prepare_batch_fact_vortex_path": artifacts["fact_vortex_path"],
                "prepare_batch_dim_vortex_path": artifacts["dim_vortex_path"],
                "prepare_batch_cdc_delta_vortex_path": artifacts.get("cdc_delta_vortex_path", ""),
                "prepare_batch_fact_vortex_digest": fact_digest,
                "prepare_batch_dim_vortex_digest": dim_digest,
                "prepare_batch_cdc_delta_vortex_digest": cdc_digest,
                "prepare_batch_fallback_attempted": "false",
                "prepare_batch_external_engine_invoked": "false",
                "prepare_batch_claim_gate_status": "not_claim_grade",
                "prepare_batch_claim_boundary": "Scoped workspace-manifest prepared-state reuse plus prepared/native batch evidence only; no hidden global cache, daemon, object-store cache, performance, production, SQL/DataFrame, or Spark-displacement claim",
                "prepare_batch_lifecycle_schema_version": "shardloom.traditional_analytics.prepared_native_vortex_lifecycle.v1",
                "prepare_batch_lifecycle_report_id": "gar-runtime-impl-6e.python_workspace_manifest_reuse",
                "prepare_batch_lifecycle_status": lifecycle_status,
                "prepare_batch_lifecycle_route": "UniversalIngress->SourceState->workspace_manifest_reuse->VortexPreparedState->prepared_vortex_batch->vortex_result_sink_if_requested",
                "prepare_batch_lifecycle_stage_order": "source_state,prepared_state_reuse,scan_pushdown,materialization_decode,result_sink,claim_gate",
                "prepare_batch_lifecycle_source_state_id": str(
                    manifest_payload.get("source_state_id") or ""
                ),
                "prepare_batch_lifecycle_source_state_digest": str(
                    manifest_payload.get("source_state_digest") or ""
                ),
                "prepare_batch_lifecycle_prepared_state_id": str(
                    manifest_payload.get("prepared_state_id") or ""
                ),
                "prepare_batch_lifecycle_prepared_state_digest": str(
                    manifest_payload.get("prepared_state_digest") or ""
                ),
                "prepare_batch_lifecycle_artifact_refs": (
                    f"fact={artifacts['fact_vortex_path']},dim={artifacts['dim_vortex_path']},"
                    f"cdc_delta={artifacts.get('cdc_delta_vortex_path', 'none') or 'none'}"
                ),
                "prepare_batch_lifecycle_artifact_digests": (
                    f"fact={fact_digest},dim={dim_digest},cdc_delta={cdc_digest or 'none'}"
                ),
                "prepare_batch_lifecycle_preparation_status": "prepared_state_reused_from_workspace_manifest",
                "prepare_batch_lifecycle_write_reopen_status": "prepared_artifacts_reused_manifest_fingerprints_verified",
                "prepare_batch_lifecycle_scan_status": "all_requested_scenarios_scanned_from_prepared_vortex",
                "prepare_batch_lifecycle_scan_pushdown_statuses": _field_any(
                    fields,
                    "scan_pushdown_statuses",
                    default="reported_by_prepared_vortex_batch",
                ),
                "prepare_batch_lifecycle_materialization_decode_status": "reported_by_prepared_vortex_batch",
                "prepare_batch_lifecycle_decoded_scenario_count": _field_any(
                    fields,
                    "decoded_scenario_count",
                    default="0",
                ),
                "prepare_batch_lifecycle_materialized_scenario_count": _field_any(
                    fields,
                    "materialized_scenario_count",
                    default="0",
                ),
                "prepare_batch_lifecycle_output_status": lifecycle_output_status,
                "prepare_batch_lifecycle_result_sink_requested": str(
                    result_sink_requested
                ).lower(),
                "prepare_batch_lifecycle_result_sink_replay_verified": str(
                    result_sink_verified
                ).lower(),
                "prepare_batch_lifecycle_native_io_certificate_status": (
                    "certified"
                    if _bool_field(fields, "all_native_io_certificates_certified")
                    else "evidence_incomplete"
                ),
                "prepare_batch_lifecycle_no_standalone_lane": "true",
                "prepare_batch_lifecycle_fallback_attempted": "false",
                "prepare_batch_lifecycle_external_engine_invoked": "false",
                "prepare_batch_lifecycle_claim_gate_status": "not_claim_grade",
                "fallback_attempted": "false",
                "external_engine_invoked": "false",
            }
        )
        return OutputEnvelope.from_field_mapping(
            fields,
            command="traditional-analytics-prepared-state-reuse-batch-run",
            summary="prepared Vortex manifest reuse plus prepared batch",
            human_text="prepared Vortex manifest reuse plus prepared batch",
            fallback_attempted=False,
        )
