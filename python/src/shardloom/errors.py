"""Errors raised by the ShardLoom Python CLI client."""

from __future__ import annotations

from typing import Any, Sequence

from ._compat import dataclass
from .models import (
    OUTPUT_ENVELOPE_SCHEMA_VERSION,
    Diagnostic,
    FallbackStatus,
    OutputEnvelope,
)

NO_FALLBACK_REASON = (
    "ShardLoom prohibits Spark, DataFusion, DuckDB, Polars, Velox, and other "
    "fallback execution engines."
)


class ShardLoomProtocolError(RuntimeError):
    """Raised when CLI output does not match the expected JSON protocol."""


class ShardLoomBinaryNotFoundError(RuntimeError):
    """Raised when an explicit ShardLoom CLI command cannot resolve a binary."""

    def __init__(
        self,
        message: str,
        *,
        reason: str | None = None,
        suggested_next_step: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.fallback = FallbackStatus(
            attempted=False,
            allowed=False,
            engine=None,
            reason=NO_FALLBACK_REASON,
        )
        self.diagnostics = (
            Diagnostic(
                code="SL_BINARY_NOT_FOUND",
                severity="error",
                category="configuration",
                message="ShardLoom CLI binary could not be resolved.",
                feature="python_cli_binary_resolution",
                reason=reason or message,
                suggested_next_step=suggested_next_step
                or "Install the ShardLoom CLI package, put `shardloom` on PATH, "
                "or set SHARDLOOM_BIN to a valid binary.",
                fallback=self.fallback,
            ),
        )

    def __str__(self) -> str:
        return self.message

    def to_error_payload(self, command: str = "shardloom") -> dict[str, Any]:
        """Return a `shardloom.output.v2`-shaped missing-binary error payload."""

        fields = [
            {"key": "fallback_attempted", "value": "false"},
            {"key": "fallback_execution_allowed", "value": "false"},
            {"key": "binary_resolved", "value": "false"},
        ]
        return {
            "schema_version": OUTPUT_ENVELOPE_SCHEMA_VERSION,
            "command": command,
            "status": "error",
            "summary": self.message,
            "human_text": self.message,
            "fallback": _fallback_payload(self.fallback),
            "diagnostics": [
                _diagnostic_payload(diagnostic) for diagnostic in self.diagnostics
            ],
            "result": {"fields": []},
            "result_refs": [],
            "artifacts": [],
            "artifact_refs": [],
            "certificates": [],
            "policy": {"fields": fields[:2]},
            "lifecycle": {
                "fields": [
                    {"key": "command_family", "value": "python_binary_resolution"},
                    {"key": "binary_resolved", "value": "false"},
                ]
            },
            "capability_snapshot": {"fields": []},
            "fields": fields,
        }


@dataclass(slots=True)
class ShardLoomCommandError(RuntimeError):
    """Raised when an explicit ShardLoom CLI command returns an error envelope."""

    command: Sequence[str]
    returncode: int
    envelope: OutputEnvelope
    stderr: str

    def __str__(self) -> str:
        fallback = self.envelope.fallback
        return (
            f"ShardLoom command failed with status {self.envelope.status!r} "
            f"and return code {self.returncode}: {self.envelope.summary}. "
            f"fallback_attempted={fallback.attempted}"
        )


def _fallback_payload(fallback: FallbackStatus) -> dict[str, Any]:
    return {
        "attempted": fallback.attempted,
        "allowed": fallback.allowed,
        "engine": fallback.engine,
        "reason": fallback.reason,
    }


def _diagnostic_payload(diagnostic: Diagnostic) -> dict[str, Any]:
    return {
        "code": diagnostic.code,
        "severity": diagnostic.severity,
        "category": diagnostic.category,
        "message": diagnostic.message,
        "feature": diagnostic.feature,
        "reason": diagnostic.reason,
        "suggested_next_step": diagnostic.suggested_next_step,
        "fallback": _fallback_payload(diagnostic.fallback),
    }
