"""Caller-owned in-process session helpers for scoped ShardLoom reuse."""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence, overload

from ._compat import dataclass
from .client import (
    FanoutOutputs,
    ShardLoomClient,
    VortexIngestSmokeReport,
)
from .models import RuntimeEnvelopeValidationReport
from .execution_resources import (
    ExecutionResourceLimits, ExecutionResources, optional_resources, resolve_resources,
    merge_resource_limits,
)
from .query import (
    LazyFrame,
    UnsupportedWorkflowOperationReport,
    Result,
    _normalize_fanout_outputs,
    _normalize_local_output_format,
    read as read_source,
    read_arrow_ipc,
    read_avro,
    read_csv,
    read_json,
    read_orc,
    read_parquet,
    read_vortex,
    sql as sql_workflow,
)


@dataclass(frozen=True, slots=True)
class LocalFileFingerprint:
    """Evidence-safe local path fingerprint used for session reuse decisions."""

    path: str
    exists: bool
    size_bytes: int | None
    mtime_ns: int | None
    content_digest: str | None
    fingerprint_kind: str = "local_file_size_mtime"
    identity_source: str = "local_file_metadata"
    tree_walk_performed: bool = False
    files_walked: int = 0
    stats_performed: int = 0

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
            self.identity_source,
            str(self.tree_walk_performed).lower(),
            str(self.files_walked),
            str(self.stats_performed),
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

    @property
    def runtime_validation(self) -> RuntimeEnvelopeValidationReport:
        """Validate the prepared-state envelope before using it as runtime evidence."""

        return self.report.envelope.runtime_execution_validation(
            surface_id="python_session_prepared_state",
            execution_mode=self.report.envelope.field("execution_mode") or "vortex_ingest",
        )

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
            "source_fingerprint_identity_source": self.source_fingerprint.identity_source,
            "source_fingerprint_tree_walk_performed": (
                self.source_fingerprint.tree_walk_performed
            ),
            "source_fingerprint_files_walked": self.source_fingerprint.files_walked,
            "source_fingerprint_stats_performed": self.source_fingerprint.stats_performed,
            "source_content_digest": self.source_fingerprint.content_digest,
            "source_size": self.source_fingerprint.size_bytes,
            "source_mtime": self.source_fingerprint.mtime_ns,
            "prepared_artifact_content_digest": self.target_fingerprint.content_digest,
            "prepared_artifact_size": self.target_fingerprint.size_bytes,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "claim_gate_status": self.claim_gate_status,
        }


@dataclass(frozen=True, slots=True, init=False)
class SessionSqlResult(Result):
    """Compatibility Result with session reuse details; it owns the same data.

    New callers can consume schema, values and details directly. ``report`` and
    the original constructor remain available for existing session callers.
    """

    session_id: str
    report: Result
    operation: str
    reuse_hit: bool
    reuse_reason: str

    def __init__(self, session_id: str, report: Result, operation: str,
                 reuse_hit: bool, reuse_reason: str) -> None:
        Result.__init__(self, report.workflow, operation, report.envelope)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "report", report)
        object.__setattr__(self, "reuse_hit", reuse_hit)
        object.__setattr__(self, "reuse_reason", reuse_reason)

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
    def output_plan_reuse_hit(self) -> bool:
        """Whether the native writer reused its prepared query declaration."""

        return self.reuse_hit and self.operation in {"write", "fanout"}

    @property
    def result_replay_reuse_hit(self) -> bool:
        """Return explicit native result-replay evidence, when supplied."""

        return self.report.envelope.field_bool("result_replay_reuse_hit", False) is True

    @property
    def output_plan_digest(self) -> str | None:
        """Return the output-plan digest when the CLI emitted one."""

        return self.report.envelope.field("output_plan_digest")

    @property
    def plan_digest(self) -> str | None:
        """Return the SQL/runtime plan digest when the CLI emitted one."""

        return self.report.envelope.field("plan_digest")

    @property
    def source_schema_digest(self) -> str | None:
        """Return the source-schema digest when the CLI emitted one."""

        return self.report.envelope.field("source_schema_digest")

    @property
    def execution_certificate_ref(self) -> str | None:
        """Return the execution certificate reference when present."""

        return self.report.envelope.field("execution_certificate_ref")

    @property
    def runtime_validation(self) -> RuntimeEnvelopeValidationReport:
        """Validate the SQL/result envelope before using it as runtime evidence."""

        return self.report.envelope.runtime_execution_validation(
            surface_id=f"python_session_{self.operation}",
        )

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
            "output_plan_reuse_hit": self.output_plan_reuse_hit,
            "result_replay_reuse_hit": self.result_replay_reuse_hit,
            "reuse_reason": self.reuse_reason,
            "plan_digest": self.plan_digest,
            "source_schema_digest": self.source_schema_digest,
            "query_answer_cached": False,
            "native_completed_executions": self.report.envelope.field_int(
                "resident_completed_executions"
            ),
            "output_plan_digest": self.output_plan_digest,
            "execution_certificate_ref": self.execution_certificate_ref,
            "runtime_envelope_validation_status": self.runtime_validation.status,
            "runtime_envelope_validation_schema_version": (
                self.runtime_validation.schema_version
            ),
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "claim_gate_status": self.claim_gate_status,
        }


@dataclass(frozen=True, slots=True)
class _PreparedCacheEntry:
    report: VortexIngestSmokeReport
    source_fingerprint: LocalFileFingerprint
    target_fingerprint: LocalFileFingerprint










def _session_frame(session, frame):
    from dataclasses import replace
    if frame.client is not session.client:
        raise ValueError("a session cannot execute a frame from another context")
    if frame.source._session is session:
        return frame
    source = replace(frame.source, _session=session)
    # Changing the Python action owner must retain the immutable native source
    # declaration, including single-use producer identity.
    object.__setattr__(source, "_native_plan", frame.source._native_plan)
    result = replace(frame, source=source)
    object.__setattr__(result, "_native_plan", frame._native_plan)
    return result


class ShardLoomSession:
    """Explicit local session for scoped SourceState/VortexPreparedState reuse.

    The client owns the native worker, which admits prepared query state and
    executes each request. Python retains no query answers. Explicit artifact
    preparation may reuse a caller-owned file while its fingerprints match.
    """

    def __init__(
        self,
        client: ShardLoomClient,
        *,
        engine: str = "auto",
        session_id: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        resource_limits: ExecutionResourceLimits | None = None,
    ) -> None:
        limits = merge_resource_limits(resource_limits, getattr(client, "resource_limits", None))
        self._resources = optional_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources, inherited=getattr(client, "resources", None),
            limits=limits, origin="session",
        )
        self._resource_limits = merge_resource_limits(
            limits, self._resources.limits if self._resources is not None else None,
        )
        self.client = client
        self.engine = engine
        self.session_id = (
            _require_non_empty("session_id", session_id)
            if session_id is not None
            else f"shardloom-session-{uuid.uuid4().hex}"
        )
        self.session_state_scope = "in_process_python_local"
        self._prepared_cache: dict[
            tuple[str, str, str, str | None, ExecutionResources],
            _PreparedCacheEntry,
        ] = {}
        self._closed = False
        self._cache_hits = 0
        self._cache_misses = 0
        self._source_state_reuse_count = 0
        self._prepared_artifact_reuse_count = 0
        self._output_plan_reuse_count = 0
        self._result_replay_reuse_count = 0
        self._last_reuse_reason: str | None = None
        self._last_invalidation_reason: str | None = None
        from ._connected import Catalog
        self.catalog = Catalog(self.client)

    @property
    def resources(self) -> ExecutionResources | None:
        """Return the allocation inherited by this session's complete operations."""

        return self._resources

    @property
    def resource_limits(self) -> ExecutionResourceLimits | None:
        """Return explicit session ceilings independently of the current grant."""

        return self._resource_limits

    @property
    def closed(self) -> bool:
        """Whether this session has been explicitly closed."""

        return self._closed or self.client.closed

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

    def read(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy source by inferring the local adapter."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_source(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_csv(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy CSV source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_csv(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_json(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy flat JSON, JSONL, or NDJSON source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_json(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_parquet(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy Parquet source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_parquet(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_arrow_ipc(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy Arrow IPC source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_arrow_ipc(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_avro(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy Avro source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_avro(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_orc(
        self,
        uri: str | os.PathLike[str],
        *,
        schema: Mapping[str, object] | None = None,
    ) -> LazyFrame:
        """Declare a session-bound lazy ORC source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_orc(
                uri,
                schema=schema,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def read_vortex(self, uri: str | os.PathLike[str]) -> LazyFrame:
        """Declare a session-bound lazy native Vortex source."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=read_vortex(
                uri,
                client=self.client,
                engine_mode=self.engine,
                resources=self.resources, resource_limits=self.resource_limits,
            ),
        )

    def sql(self, statement: object, *, parameters=None, relations=None) -> LazyFrame:
        """Bind SQL into the shared lazy DataFrame with this session's catalog."""

        self._ensure_open()
        return _session_frame(
            session=self,
            frame=sql_workflow(statement, client=self.client, resources=self.resources,
                resource_limits=self.resource_limits, parameters=parameters,
                relations=self.catalog.bindings(relations)),
        )

    def register(self, name, frame, *, replace=False):
        self._ensure_open()
        return self.catalog.register(name, frame, replace=replace)

    def table(self, name):
        self._ensure_open()
        return self.catalog.table(name)

    def list_tables(self):
        self._ensure_open()
        return self.catalog.list()

    def drop_view(self, name, *, if_exists=False):
        self._ensure_open()
        return self.catalog.drop(name, if_exists=if_exists)

    def prepare_vortex(
        self,
        source_path: str | os.PathLike[str],
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        input_format: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        reuse: bool = True,
        check: bool = True,
    ) -> SessionPreparedState:
        """Prepare or reuse one local Vortex artifact within this session."""

        self._ensure_open()
        resources = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources, inherited=self.resources, limits=self.resource_limits,
        )
        if target_vortex_path is None:
            raise ValueError("prepare_vortex requires target_vortex_path")
        normalized_source = _normalized_path(source_path)
        normalized_target = _normalized_path(target_vortex_path)
        normalized_certification = _require_non_empty(
            "certification_level",
            certification_level,
        )
        key = (
            normalized_source,
            normalized_target,
            normalized_certification,
            input_format,
            resources,
        )
        source_fingerprint = _fingerprint_file_metadata(source_path)
        target_fingerprint = _fingerprint_file_metadata(target_vortex_path)
        entry = self._prepared_cache.get(key)

        if reuse and entry is not None:
            reuse_reason = _reuse_reason_from_metadata(
                entry,
                source_fingerprint=source_fingerprint,
                target_fingerprint=target_fingerprint,
            )
            if reuse_reason == "source_and_prepared_artifact_fingerprints_match":
                self._cache_hits += 1
                self._source_state_reuse_count += 1
                self._prepared_artifact_reuse_count += 1
                self._last_reuse_reason = reuse_reason
                self._last_invalidation_reason = None
                return SessionPreparedState(
                    session_id=self.session_id,
                    report=entry.report,
                    reuse_hit=True,
                    reuse_reason=reuse_reason,
                    source_fingerprint=entry.source_fingerprint,
                    target_fingerprint=entry.target_fingerprint,
                )
        else:
            reuse_reason = "reuse_disabled" if not reuse else "no_cached_prepared_state"

        self._last_reuse_reason = reuse_reason
        self._last_invalidation_reason = (
            None
            if reuse_reason in {"no_cached_prepared_state", "reuse_disabled"}
            else reuse_reason
        )
        self._cache_misses += 1
        report = self.client.vortex_prepare(
            source_path,
            target_vortex_path,
            input_format=input_format,
            allow_overwrite=allow_overwrite,
            certification_level=normalized_certification,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
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
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
    ) -> SessionSqlResult:
        """Collect rows for an admitted local query-builder workflow with session reuse."""

        self._ensure_open()
        resources = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources, inherited=self.resources or frame.resources,
            limits=merge_resource_limits(self.resource_limits, frame.resource_limits),
        )
        return _session_frame(self, frame).collect(
            check=check, reuse=reuse,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
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
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        """Write an admitted local query-builder result with session output reuse."""

        self._ensure_open()
        resources = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources, inherited=self.resources or frame.resources,
            limits=merge_resource_limits(self.resource_limits, frame.resource_limits),
        )
        normalized_output_format = _normalize_local_output_format(output_format)
        return _session_frame(self, frame).write(
            target_uri, output_format=normalized_output_format,
            allow_overwrite=allow_overwrite, check=check, reuse=reuse,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
        )

    def fanout(
        self,
        frame: LazyFrame,
        outputs: FanoutOutputs,
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        """Write an admitted local query-builder result to fanout sinks with session reuse."""

        self._ensure_open()
        resources = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism,
            resources=resources, inherited=self.resources or frame.resources,
            limits=merge_resource_limits(self.resource_limits, frame.resource_limits),
        )
        normalized_outputs = _normalize_fanout_outputs(outputs)
        return _session_frame(self, frame).fanout(
            normalized_outputs, allow_overwrite=allow_overwrite,
            check=check, reuse=reuse,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
            max_parallelism=max_parallelism,
        )

    def close(self) -> dict[str, Any]:
        """Close the session and clear in-process reuse state."""

        if not self._closed:
            self._prepared_cache.clear()
            self.catalog.clear()
            self.client.close()
            self._closed = True
        return self.evidence()

    def cancel(self) -> None:
        """Cancel current work through this session's shared context owner."""
        self._ensure_open()
        self.client.cancel()

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
            "last_reuse_reason": self._last_reuse_reason,
            "last_invalidation_reason": self._last_invalidation_reason,
            "session_closed": self.closed,
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
        from .errors import ShardLoomClosedError
        if self.closed:
            raise ShardLoomClosedError()

    @overload
    def _sql_result(self, *, operation: Literal["collect"], execute: Any, reuse: bool) -> SessionSqlResult: ...

    @overload
    def _sql_result(self, *, operation: str, execute: Any, reuse: bool) -> SessionSqlResult | UnsupportedWorkflowOperationReport: ...

    def _sql_result(
        self,
        *,
        operation: str,
        execute: Any,
        reuse: bool,
    ) -> SessionSqlResult | UnsupportedWorkflowOperationReport:
        # The client owns the native worker and its prepared operations. Every
        # terminal call executes there; Python never caches a completed answer.
        if not reuse:
            self.client.release_prepared()
        report = execute()
        if isinstance(report, SessionSqlResult) and report.session_id == self.session_id:
            return report
        if isinstance(report, UnsupportedWorkflowOperationReport):
            if operation != "collect":
                return report
            report = Result(report.workflow, operation, report.envelope)
        reused = report.envelope.status == "success" and any(
            report.envelope.field_bool(field, False) is True
            for field in (
                "resident_relational_declaration_reused",
                "resident_unary_lowering_reused",
                "resident_aggregate_lowering_reused",
            )
        )
        reuse_reason = "native_preparation_reused" if reused else "native_preparation_not_reused"
        if reused:
            self._cache_hits += 1
            self._source_state_reuse_count += 1
            if operation in {"write", "fanout"}:
                self._output_plan_reuse_count += 1
        else:
            self._cache_misses += 1
        self._last_reuse_reason = reuse_reason
        self._last_invalidation_reason = None
        return SessionSqlResult(
            session_id=self.session_id,
            report=report,
            operation=operation,
            reuse_hit=reused,
            reuse_reason=reuse_reason,
        )


def _fingerprint_file(
    path: str | os.PathLike[str],
    *,
    content_digest: bool = False,
) -> LocalFileFingerprint:
    metadata = _fingerprint_file_metadata(path)
    if not metadata.exists or not content_digest:
        return metadata
    local_path = Path(path).expanduser()
    if metadata.fingerprint_kind.startswith("local_directory_"):
        content_digest_value, size_bytes, mtime_ns, files_walked, stats_performed = (
            _directory_tree_content_digest(local_path)
        )
        return LocalFileFingerprint(
            path=metadata.path,
            exists=True,
            size_bytes=size_bytes,
            mtime_ns=mtime_ns,
            content_digest=content_digest_value,
            fingerprint_kind="local_directory_tree_sha256_size_mtime",
            identity_source="recursive_tree_explicit_proof",
            tree_walk_performed=True,
            files_walked=files_walked,
            stats_performed=stats_performed,
        )
    return LocalFileFingerprint(
        path=metadata.path,
        exists=True,
        size_bytes=metadata.size_bytes,
        mtime_ns=metadata.mtime_ns,
        content_digest=_file_content_digest(local_path),
        fingerprint_kind="local_file_sha256_size_mtime",
        identity_source="local_file_explicit_proof_digest",
    )


def _fingerprint_file_metadata(path: str | os.PathLike[str]) -> LocalFileFingerprint:
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
            fingerprint_kind="local_path_missing",
            identity_source="missing_path",
        )
    if local_path.is_dir():
        return LocalFileFingerprint(
            path=normalized,
            exists=True,
            size_bytes=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            content_digest=None,
            fingerprint_kind="local_directory_root_size_mtime_source_state_candidate",
            identity_source="root_metadata_source_state_candidate",
            tree_walk_performed=False,
            files_walked=0,
            stats_performed=1,
        )
    return LocalFileFingerprint(
        path=normalized,
        exists=True,
        size_bytes=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        content_digest=None,
        fingerprint_kind="local_file_size_mtime",
        identity_source="local_file_metadata",
        stats_performed=1,
    )


def _file_content_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _directory_tree_content_digest(path: Path) -> tuple[str, int, int, int, int]:
    root_stat = path.stat()
    total_size = root_stat.st_size
    max_mtime = root_stat.st_mtime_ns
    files_walked = 0
    stats_performed = 1
    digest = hashlib.sha256()
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        stat = child.stat()
        stats_performed += 1
        files_walked += 1
        total_size += stat.st_size
        max_mtime = max(max_mtime, stat.st_mtime_ns)
        relative = child.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        digest.update(b"\0")
        digest.update(_file_content_digest(child).encode("ascii"))
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest(), total_size, max_mtime, files_walked, stats_performed


def _normalized_path(path: str | os.PathLike[str]) -> str:
    return str(Path(path).expanduser().resolve(strict=False))


def _metadata_matches(
    stored: LocalFileFingerprint,
    current: LocalFileFingerprint,
) -> bool:
    return (
        stored.path == current.path
        and stored.exists == current.exists
        and stored.size_bytes == current.size_bytes
        and stored.mtime_ns == current.mtime_ns
    )


def _reuse_reason_from_metadata(
    entry: _PreparedCacheEntry,
    *,
    source_fingerprint: LocalFileFingerprint,
    target_fingerprint: LocalFileFingerprint,
) -> str:
    if not source_fingerprint.exists:
        return "source_fingerprint_missing"
    if not target_fingerprint.exists:
        return "prepared_artifact_missing"
    if not _metadata_matches(entry.source_fingerprint, source_fingerprint):
        return "source_fingerprint_changed"
    if not _metadata_matches(entry.target_fingerprint, target_fingerprint):
        return "prepared_artifact_fingerprint_changed"
    return "source_and_prepared_artifact_fingerprints_match"


def _require_non_empty(label: str, value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label} must not be empty")
    return text
