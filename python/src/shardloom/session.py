"""Caller-owned in-process session helpers for scoped ShardLoom reuse."""

from __future__ import annotations

import hashlib
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import ShardLoomClient, VortexIngestSmokeReport


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
class _PreparedCacheEntry:
    report: VortexIngestSmokeReport
    source_fingerprint: LocalFileFingerprint
    target_fingerprint: LocalFileFingerprint


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
        self._closed = False
        self._cache_hits = 0
        self._cache_misses = 0
        self._source_state_reuse_count = 0
        self._prepared_artifact_reuse_count = 0
        self._output_plan_reuse_count = 0

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

    def close(self) -> dict[str, Any]:
        """Close the session and clear in-process reuse state."""

        if not self._closed:
            self._prepared_cache.clear()
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


def _require_non_empty(label: str, value: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{label} must not be empty")
    return text
