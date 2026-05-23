"""Caller-owned in-process session helpers for scoped ShardLoom reuse."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import (
    FanoutOutputs,
    ShardLoomClient,
    SqlLocalSourceSmokeReport,
    VortexIngestSmokeReport,
)
from .query import (
    LazyFrame,
    UnsupportedWorkflowOperationReport,
    _normalize_fanout_outputs,
    _normalize_local_output_format,
    _sql_source_refs,
)


@dataclass(frozen=True, slots=True)
class LocalFileFingerprint:
    """Evidence-safe local file fingerprint used for session reuse decisions."""

    path: str
    exists: bool
    size_bytes: int | None
    mtime_ns: int | None
    content_digest: str | None
    fingerprint_kind: str = "local_file_sha256_size_mtime"

    @property
    def reuse_digest(self) -> str:
        """Return a stable digest over the fingerprint tuple."""

        parts = (
            self.fingerprint_kind,
            self.path,
            "exists" if self.exists else "missing",
            "" if self.size_bytes is None else str(self.size_bytes),
            "" if self.mtime_ns is None else str(self.mtime_ns),
            "" if self.content_digest is None else self.content_digest,
        )
        payload = "\0".join(parts).encode("utf-8")
        return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class SessionPreparedState:
    """Prepared-state handle returned by a `ShardLoomSession` prepare call."""

    session_id: str
    report: VortexIngestSmokeReport
    reuse_hit: bool
    reuse_reason: str
    source_fingerprint: LocalFileFingerprint
    target_fingerprint: LocalFileFingerprint

    @property
    def session_state_scope(self) -> str:
        """Return the scope for this session-owned state."""

        return "in_process_python_local"

    @property
    def prepared_state_id(self) -> str:
        """Return the underlying `VortexPreparedState` identifier."""

        return self.report.prepared_state_id

    @property
    def prepared_state_digest(self) -> str:
        """Return the underlying `VortexPreparedState` digest."""

        return self.report.prepared_state_digest

    @property
    def source_state_id(self) -> str | None:
        """Return the SourceState identifier when the CLI emitted one."""

        return self.report.envelope.field("source_state_id")

    @property
    def source_state_digest(self) -> str | None:
        """Return the SourceState digest when the CLI emitted one."""

        return self.report.envelope.field("source_state_digest")

    @property
    def source_state_reuse_hit(self) -> bool:
        """Whether this session reused the source/prepared-state pair."""

        return self.reuse_hit

    @property
    def prepared_state_reuse_hit(self) -> bool:
        """Whether this session reused the prepared Vortex artifact."""

        return self.reuse_hit

    @property
    def source_state_reuse_reason(self) -> str:
        """Return the source-state reuse or invalidation reason."""

        return self.reuse_reason

    @property
    def prepared_state_reuse_reason(self) -> str:
        """Return the prepared-state reuse or invalidation reason."""

        return self.reuse_reason

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return self.report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether an external engine was invoked."""

        return self.report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str:
        """Return the underlying claim-gate status."""

        return self.report.claim_gate_status

    def evidence(self) -> dict[str, Any]:
        """Return a compact session reuse evidence dictionary."""

        return {
            "session_id": self.session_id,
            "session_state_scope": self.session_state_scope,
            "source_state_id": self.source_state_id,
            "source_state_digest": self.source_state_digest,
            "prepared_state_id": self.prepared_state_id,
            "prepared_state_digest": self.prepared_state_digest,
            "source_state_reuse_hit": self.source_state_reuse_hit,
            "prepared_state_reuse_hit": self.prepared_state_reuse_hit,
            "reuse_reason": self.reuse_reason,
            "source_fingerprint_kind": self.source_fingerprint.fingerprint_kind,
            "source_content_digest": self.source_fingerprint.content_digest,
            "source_size": self.source_fingerprint.size_bytes,
            "source_mtime": self.source_fingerprint.mtime_ns,
            "prepared_artifact_content_digest": self.target_fingerprint.content_digest,
            "prepared_artifact_size": self.target_fingerprint.size_bytes,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "claim_gate_status": self.claim_gate_status,
        }


@dataclass(frozen=True, slots=True)
class SessionSqlResult:
    """SQL/query-builder result handle returned by a `ShardLoomSession`."""

    session_id: str
    report: SqlLocalSourceSmokeReport
    operation: str
    reuse_hit: bool
    reuse_reason: str
    source_fingerprints: tuple[LocalFileFingerprint, ...]
    output_fingerprints: tuple[LocalFileFingerprint, ...] = ()

    @property
    def session_state_scope(self) -> str:
        """Return the scope for this session-owned state."""

        return "in_process_python_local"

    @property
    def source_state_reuse_hit(self) -> bool:
        """Whether this session reused the source state for this result."""

        return self.reuse_hit

    @property
    def source_state_id(self) -> str | None:
        """Return the CLI SourceState id for this result when available."""

        return self.report.source_state_id

    @property
    def source_state_digest(self) -> str | None:
        """Return the CLI SourceState digest for this result when available."""

        return self.report.source_state_digest

    @property
    def source_state_contract_schema_version(self) -> str | None:
        """Return the CLI SourceState contract schema version when available."""

        return self.report.source_state_contract_schema_version

    @property
    def source_state_read_plan(self) -> str | None:
        """Return the local SourceState read-plan status when available."""

        return self.report.source_state_read_plan

    @property
    def source_state_projection_pushdown_status(self) -> str | None:
        """Return the reader projection pushdown status when available."""

        return self.report.source_state_projection_pushdown_status

    @property
    def user_surface_runtime_scope(self) -> str | None:
        """Return whether SQL/Python compute used the common runtime."""

        return self.report.user_surface_runtime_scope

    @property
    def format_specific_boundary_scope(self) -> str | None:
        """Return where format-specific behavior was allowed."""

        return self.report.format_specific_boundary_scope

    @property
    def format_specific_compute_path(self) -> bool:
        """Whether the result used a format-specific compute path."""

        return self.report.format_specific_compute_path

    @property
    def source_state_materialization_layout(self) -> str | None:
        """Return the local SourceState materialization layout when available."""

        return self.report.source_state_materialization_layout

    @property
    def source_state_parse_normalization(self) -> str | None:
        """Return the local SourceState parse/normalization route when available."""

        return self.report.source_state_parse_normalization

    @property
    def source_state_columnar_preserved(self) -> bool:
        """Whether the CLI preserved a columnar SourceState boundary."""

        return self.report.source_state_columnar_preserved

    @property
    def source_state_record_batch_count(self) -> int:
        """Return the local SourceState record-batch count."""

        return self.report.source_state_record_batch_count

    @property
    def source_to_columnar_millis(self) -> int:
        """Return source-to-columnar adapter time in milliseconds."""

        return self.report.source_to_columnar_millis

    @property
    def source_state_runtime_consumption_layout(self) -> str | None:
        """Return the runtime layout that consumed the SourceState."""

        return self.report.source_state_runtime_consumption_layout

    @property
    def source_state_scalar_runtime_materialization_required(self) -> bool:
        """Whether the SQL runtime still materialized scalar rows."""

        return self.report.source_state_scalar_runtime_materialization_required

    @property
    def source_state_materialized_columns(self) -> tuple[str, ...]:
        """Return local SourceState materialized columns."""

        return self.report.source_state_materialized_columns

    @property
    def source_state_reader_projection_columns(self) -> tuple[str, ...]:
        """Return local SourceState reader projection columns."""

        return self.report.source_state_reader_projection_columns

    @property
    def output_plan_reuse_hit(self) -> bool:
        """Whether this session reused output/result evidence for this result."""

        return self.reuse_hit and self.operation in {"write", "fanout"}

    @property
    def result_replay_reuse_hit(self) -> bool:
        """Whether this session reused a previously replay-verified result."""

        return self.output_plan_reuse_hit

    @property
    def output_plan_digest(self) -> str | None:
        """Return the output-plan digest when the CLI emitted one."""

        return self.report.envelope.field("output_plan_digest")

    @property
    def fallback_attempted(self) -> bool:
        """Whether fallback execution was attempted."""

        return self.report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether an external engine was invoked."""

        return self.report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str | None:
        """Return the underlying claim-gate status."""

        return self.report.envelope.field("claim_gate_status")

    def evidence(self) -> dict[str, Any]:
        """Return a compact session result reuse evidence dictionary."""

        return {
            "session_id": self.session_id,
            "session_state_scope": self.session_state_scope,
            "operation": self.operation,
            "source_state_reuse_hit": self.source_state_reuse_hit,
            "source_state_id": self.source_state_id,
            "source_state_digest": self.source_state_digest,
            "source_state_contract_schema_version": self.source_state_contract_schema_version,
            "source_state_read_plan": self.source_state_read_plan,
            "source_state_projection_pushdown_status": self.source_state_projection_pushdown_status,
            "user_surface_runtime_scope": self.user_surface_runtime_scope,
            "format_specific_boundary_scope": self.format_specific_boundary_scope,
            "format_specific_compute_path": self.format_specific_compute_path,
            "source_state_materialization_layout": self.source_state_materialization_layout,
            "source_state_parse_normalization": self.source_state_parse_normalization,
            "source_state_columnar_preserved": self.source_state_columnar_preserved,
            "source_state_record_batch_count": self.source_state_record_batch_count,
            "source_to_columnar_millis": self.source_to_columnar_millis,
            "source_state_runtime_consumption_layout": self.source_state_runtime_consumption_layout,
            "source_state_scalar_runtime_materialization_required": (
                self.source_state_scalar_runtime_materialization_required
            ),
            "source_state_materialized_columns": self.source_state_materialized_columns,
            "source_state_reader_projection_columns": self.source_state_reader_projection_columns,
            "output_plan_reuse_hit": self.output_plan_reuse_hit,
            "result_replay_reuse_hit": self.result_replay_reuse_hit,
            "reuse_reason": self.reuse_reason,
            "source_fingerprint_digests": tuple(
                fingerprint.reuse_digest for fingerprint in self.source_fingerprints
            ),
            "output_fingerprint_digests": tuple(
                fingerprint.reuse_digest for fingerprint in self.output_fingerprints
            ),
            "output_plan_digest": self.output_plan_digest,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "claim_gate_status": self.claim_gate_status,
        }


@dataclass(frozen=True, slots=True)
class _PreparedCacheEntry:
    report: VortexIngestSmokeReport
    source_fingerprint: LocalFileFingerprint
    target_fingerprint: LocalFileFingerprint


@dataclass(frozen=True, slots=True)
class _SqlCacheEntry:
    report: SqlLocalSourceSmokeReport
    operation: str
    source_fingerprints: tuple[LocalFileFingerprint, ...]
    output_fingerprints: tuple[LocalFileFingerprint, ...]


class ShardLoomSession:
    """Explicit local session for scoped SourceState/VortexPreparedState reuse.

    The session is caller-owned and in-process only. It does not start a daemon,
    persist a distributed cache, invoke external engines, or change execution
    providers. Reuse is admitted only when local fingerprints still match.
    """

    def __init__(
        self,
        client: ShardLoomClient,
        *,
        engine: str = "auto",
        session_id: str | None = None,
    ) -> None:
        self.client = client
        self.engine = engine
        self.session_id = (
            _require_non_empty("session_id", session_id)
            if session_id is not None
            else f"shardloom-session-{uuid.uuid4().hex}"
        )
        self.session_state_scope = "in_process_python_local"
        self._prepared_cache: dict[
            tuple[str, str, str],
            _PreparedCacheEntry,
        ] = {}
        self._sql_cache: dict[tuple[object, ...], _SqlCacheEntry] = {}
        self._closed = False
        self._cache_hits = 0
        self._cache_misses = 0
        self._source_state_reuse_count = 0
        self._prepared_artifact_reuse_count = 0
        self._output_plan_reuse_count = 0
        self._result_replay_reuse_count = 0

    @property
    def closed(self) -> bool:
        """Whether this session has been explicitly closed."""

        return self._closed

    @property
    def cache_hit_count(self) -> int:
        """Return the session cache-hit count."""

        return self._cache_hits

    @property
    def cache_miss_count(self) -> int:
        """Return the session cache-miss count."""

        return self._cache_misses

    @property
    def source_state_reuse_count(self) -> int:
        """Return the SourceState reuse count."""

        return self._source_state_reuse_count

    @property
    def prepared_artifact_reuse_count(self) -> int:
        """Return the prepared-artifact reuse count."""

        return self._prepared_artifact_reuse_count

    @property
    def output_plan_reuse_count(self) -> int:
        """Return the OutputPlan reuse count."""

        return self._output_plan_reuse_count

    @property
    def result_replay_reuse_count(self) -> int:
        """Return the result replay reuse count."""

        return self._result_replay_reuse_count

    def prepare_vortex(
        self,
        source_path: str | os.PathLike[str],
        target_vortex_path: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        reuse: bool = True,
        check: bool = True,
    ) -> SessionPreparedState:
        """Prepare or reuse a local `VortexPreparedState` within this session."""

        self._ensure_open()
        normalized_source = _normalized_path(source_path)
        normalized_target = _normalized_path(target_vortex_path)
        normalized_certification = _require_non_empty(
            "certification_level",
            certification_level,
        )
        key = (normalized_source, normalized_target, normalized_certification)
        source_fingerprint = _fingerprint_file(source_path)
        target_fingerprint = _fingerprint_file(target_vortex_path)
        entry = self._prepared_cache.get(key)

        if reuse and entry is not None:
            reuse_reason = _reuse_reason(
                entry,
                source_fingerprint=source_fingerprint,
                target_fingerprint=target_fingerprint,
            )
            if reuse_reason == "source_and_prepared_artifact_fingerprints_match":
                self._cache_hits += 1
                self._source_state_reuse_count += 1
                self._prepared_artifact_reuse_count += 1
                return SessionPreparedState(
                    session_id=self.session_id,
                    report=entry.report,
                    reuse_hit=True,
                    reuse_reason=reuse_reason,
                    source_fingerprint=source_fingerprint,
                    target_fingerprint=target_fingerprint,
                )
        else:
            reuse_reason = "reuse_disabled" if not reuse else "no_cached_prepared_state"

        self._cache_misses += 1
        report = self.client.vortex_ingest_smoke(
            source_path,
            target_vortex_path,
            allow_overwrite=allow_overwrite,
            certification_level=normalized_certification,
            check=check,
        )
        source_fingerprint = _fingerprint_file(source_path)
        target_fingerprint = _fingerprint_file(target_vortex_path)
        if source_fingerprint.exists and target_fingerprint.exists:
            self._prepared_cache[key] = _PreparedCacheEntry(
                report=report,
                source_fingerprint=source_fingerprint,
                target_fingerprint=target_fingerprint,
            )
        return SessionPreparedState(
            session_id=self.session_id,
            report=report,
            reuse_hit=False,
            reuse_reason=reuse_reason,
            source_fingerprint=source_fingerprint,
            target_fingerprint=target_fingerprint,
        )

    def collect(
        self,
        frame: LazyFrame,
        *,
        reuse: bool = True,
        check: bool = False,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        """Collect rows for an admitted local query-builder workflow with session reuse."""

        self._ensure_open()
        statement = frame._sql_local_source_statement()
        if statement is None:
            return frame.collect(check=check)
        return self._sql_result(
            operation="collect",
            statement=statement,
            execute=lambda: frame.collect(check=check),
            output_paths=(),
            reuse=reuse,
        )

    def write(
        self,
        frame: LazyFrame,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        reuse: bool = True,
        check: bool = True,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        """Write an admitted local query-builder result with session output reuse."""

        self._ensure_open()
        statement = frame._sql_local_source_statement()
        if statement is None:
            return frame.write(
                target_uri,
                output_format=output_format,
                allow_overwrite=allow_overwrite,
                check=check,
            )
        normalized_output_format = _normalize_local_output_format(output_format)
        return self._sql_result(
            operation="write",
            statement=statement,
            execute=lambda: frame.write(
                target_uri,
                output_format=normalized_output_format,
                allow_overwrite=allow_overwrite,
                check=check,
            ),
            output_paths=(target_uri,),
            reuse=reuse,
            output_key=(normalized_output_format, _normalized_path(target_uri)),
        )

    def fanout(
        self,
        frame: LazyFrame,
        outputs: FanoutOutputs,
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        check: bool = True,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        """Write an admitted local query-builder result to fanout sinks with session reuse."""

        self._ensure_open()
        statement = frame._sql_local_source_statement()
        normalized_outputs = _normalize_fanout_outputs(outputs)
        if statement is None:
            return frame.fanout(
                normalized_outputs,
                allow_overwrite=allow_overwrite,
                check=check,
            )
        output_paths = tuple(path for _, path in normalized_outputs)
        output_key = tuple(
            (fmt, _normalized_path(path))
            for fmt, path in normalized_outputs
        )
        return self._sql_result(
            operation="fanout",
            statement=statement,
            execute=lambda: frame.fanout(
                normalized_outputs,
                allow_overwrite=allow_overwrite,
                check=check,
            ),
            output_paths=output_paths,
            reuse=reuse,
            output_key=output_key,
        )

    def close(self) -> dict[str, Any]:
        """Close the session and clear in-process reuse state."""

        if not self._closed:
            self._prepared_cache.clear()
            self._sql_cache.clear()
            self._closed = True
        return self.evidence()

    def evidence(self) -> dict[str, Any]:
        """Return session lifecycle and reuse evidence."""

        return {
            "session_id": self.session_id,
            "session_state_scope": self.session_state_scope,
            "engine_mode": self.engine,
            "cache_hit": self._cache_hits > 0,
            "cache_miss": self._cache_misses > 0,
            "cache_hit_count": self._cache_hits,
            "cache_miss_count": self._cache_misses,
            "source_state_reuse_count": self._source_state_reuse_count,
            "prepared_artifact_reuse_count": self._prepared_artifact_reuse_count,
            "output_plan_reuse_count": self._output_plan_reuse_count,
            "result_replay_reuse_count": self._result_replay_reuse_count,
            "session_closed": self._closed,
            "fallback_attempted": False,
            "external_engine_invoked": False,
            "claim_gate_status": "not_claim_grade",
        }

    def __enter__(self) -> "ShardLoomSession":
        """Enter a context-managed session."""

        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close a context-managed session."""

        self.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("ShardLoomSession is closed")

    def _sql_result(
        self,
        *,
        operation: str,
        statement: str,
        execute: Any,
        output_paths: tuple[str | os.PathLike[str], ...],
        reuse: bool,
        output_key: object = (),
    ) -> SessionSqlResult:
        source_fingerprints = _source_fingerprints(statement)
        output_fingerprints = tuple(_fingerprint_file(path) for path in output_paths)
        key = (
            operation,
            statement,
            output_key,
        )
        entry = self._sql_cache.get(key)
        if reuse and entry is not None:
            reuse_reason = _sql_reuse_reason(
                entry,
                source_fingerprints=source_fingerprints,
                output_fingerprints=output_fingerprints,
            )
            if reuse_reason == "source_and_output_fingerprints_match":
                self._cache_hits += 1
                self._source_state_reuse_count += 1
                if operation in {"write", "fanout"}:
                    self._output_plan_reuse_count += 1
                    self._result_replay_reuse_count += 1
                return SessionSqlResult(
                    session_id=self.session_id,
                    report=entry.report,
                    operation=operation,
                    reuse_hit=True,
                    reuse_reason=reuse_reason,
                    source_fingerprints=source_fingerprints,
                    output_fingerprints=output_fingerprints,
                )
        else:
            reuse_reason = "reuse_disabled" if not reuse else "no_cached_result"

        self._cache_misses += 1
        report = execute()
        source_fingerprints = _source_fingerprints(statement)
        output_fingerprints = tuple(_fingerprint_file(path) for path in output_paths)
        if report.envelope.status == "success" and _cacheable_sql_state(
            source_fingerprints,
            output_fingerprints,
        ):
            self._sql_cache[key] = _SqlCacheEntry(
                report=report,
                operation=operation,
                source_fingerprints=source_fingerprints,
                output_fingerprints=output_fingerprints,
            )
        return SessionSqlResult(
            session_id=self.session_id,
            report=report,
            operation=operation,
            reuse_hit=False,
            reuse_reason=reuse_reason,
            source_fingerprints=source_fingerprints,
            output_fingerprints=output_fingerprints,
        )


def _fingerprint_file(path: str | os.PathLike[str]) -> LocalFileFingerprint:
    normalized = _normalized_path(path)
    local_path = Path(path).expanduser()
    try:
        stat = local_path.stat()
    except FileNotFoundError:
        return LocalFileFingerprint(
            path=normalized,
            exists=False,
            size_bytes=None,
            mtime_ns=None,
            content_digest=None,
        )
    digest = hashlib.sha256()
    with local_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return LocalFileFingerprint(
        path=normalized,
        exists=True,
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        content_digest="sha256:" + digest.hexdigest(),
    )


def _normalized_path(path: str | os.PathLike[str]) -> str:
    return str(Path(path).expanduser().resolve(strict=False))


def _reuse_reason(
    entry: _PreparedCacheEntry,
    *,
    source_fingerprint: LocalFileFingerprint,
    target_fingerprint: LocalFileFingerprint,
) -> str:
    if not source_fingerprint.exists:
        return "source_fingerprint_missing"
    if not target_fingerprint.exists:
        return "prepared_artifact_missing"
    if entry.source_fingerprint != source_fingerprint:
        return "source_fingerprint_changed"
    if entry.target_fingerprint != target_fingerprint:
        return "prepared_artifact_fingerprint_changed"
    return "source_and_prepared_artifact_fingerprints_match"


def _source_fingerprints(statement: str) -> tuple[LocalFileFingerprint, ...]:
    refs = _sql_source_refs(statement)
    return tuple(_fingerprint_file(ref) for ref in refs)


def _cacheable_sql_state(
    source_fingerprints: tuple[LocalFileFingerprint, ...],
    output_fingerprints: tuple[LocalFileFingerprint, ...],
) -> bool:
    if not source_fingerprints:
        return False
    if any(not fingerprint.exists for fingerprint in source_fingerprints):
        return False
    if any(not fingerprint.exists for fingerprint in output_fingerprints):
        return False
    return True


def _sql_reuse_reason(
    entry: _SqlCacheEntry,
    *,
    source_fingerprints: tuple[LocalFileFingerprint, ...],
    output_fingerprints: tuple[LocalFileFingerprint, ...],
) -> str:
    if len(entry.source_fingerprints) != len(source_fingerprints):
        return "source_fingerprint_count_changed"
    if len(entry.output_fingerprints) != len(output_fingerprints):
        return "output_fingerprint_count_changed"
    if any(not fingerprint.exists for fingerprint in source_fingerprints):
        return "source_fingerprint_missing"
    if any(not fingerprint.exists for fingerprint in output_fingerprints):
        return "output_artifact_missing"
    if entry.source_fingerprints != source_fingerprints:
        return "source_fingerprint_changed"
    if entry.output_fingerprints != output_fingerprints:
        return "output_artifact_fingerprint_changed"
    return "source_and_output_fingerprints_match"


def _require_non_empty(label: str, value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label} must not be empty")
    return text
