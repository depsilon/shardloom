"""Errors raised by the ShardLoom Python CLI client."""

from __future__ import annotations

import subprocess
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


class ShardLoomError(Exception):
    """Common base for ShardLoom configuration, lifecycle and execution errors."""


class ShardLoomProtocolError(ShardLoomError, RuntimeError):
    """Raised when CLI output does not match the expected JSON protocol."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.diagnostics = (_configuration_diagnostic("SL_PROTOCOL_ERROR", message,
            "Check that the Python package and CLI versions match. Inspect any output before retrying."),)


class ShardLoomInstallationError(ShardLoomError, ImportError):
    """The selected installation/backend cannot provide native execution."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.diagnostics = (_configuration_diagnostic("SL_NATIVE_UNAVAILABLE", message,
            "Install a native ShardLoom build for this platform, or explicitly select transport='subprocess' with a supported CLI installation."),)


class ShardLoomClosedError(ShardLoomError, RuntimeError):
    """New work was requested after its caller-owned context was closed."""

    def __init__(self) -> None:
        message = "ShardLoom context/client is closed; retained completed results remain valid"
        super().__init__(message)
        self.diagnostics = (_configuration_diagnostic("SL_CONTEXT_CLOSED", message,
            "Create a new context with explicit execution resources."),)


class ShardLoomTimeoutError(ShardLoomError, subprocess.TimeoutExpired, TimeoutError):
    """An execution deadline expired, preserving both historical timeout catches."""

    def __init__(self, command, timeout, output=None, stderr=None, *, message=None):
        subprocess.TimeoutExpired.__init__(self, command, timeout, output, stderr)
        self.message = message
        self.diagnostics = (_configuration_diagnostic(
            "SL_EXECUTION_TIMEOUT", str(self),
            "Inspect output destinations before explicitly retrying with a suitable deadline.",
            category="execution",
        ),)

    def __str__(self) -> str:
        return self.message or subprocess.TimeoutExpired.__str__(self)


def _configuration_diagnostic(
    code: str, message: str, next_step: str, *, category: str = "configuration",
) -> Diagnostic:
    return Diagnostic(
        code=code, severity="error", category=category, message=message,
        feature="python_context", reason=message, suggested_next_step=next_step,
        fallback=FallbackStatus(attempted=False, allowed=False, engine=None, reason=NO_FALLBACK_REASON),
    )


class ShardLoomBinaryNotFoundError(ShardLoomError, RuntimeError):
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
class ShardLoomCommandError(ShardLoomError, RuntimeError):
    """Raised when an explicit ShardLoom CLI command returns an error envelope."""

    command: Sequence[str]
    returncode: int
    envelope: OutputEnvelope
    stderr: str

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        return self.envelope.diagnostics

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


def _cancelled_error(command: Sequence[str], *, forced: bool) -> ShardLoomCommandError:
    message = "ShardLoom execution was cancelled; no request was replayed."
    if forced:
        message += " Process termination means output publication and staged cleanup cannot be confirmed."
    envelope = OutputEnvelope.from_field_mapping(
        {"cancellation_requested": "true", "process_termination_forced": str(forced).lower()},
        command=command[0] if command else "run", status="error", summary=message, human_text=message,
    )
    raw = dict(envelope.raw)
    diagnostic = _configuration_diagnostic("SL_EXECUTION_CANCELLED", message,
        "Inspect any output destinations before explicitly retrying the operation.", category="execution")
    raw["diagnostics"] = [_diagnostic_payload(diagnostic)]
    return ShardLoomCommandError(command, 2, OutputEnvelope.from_json(raw), "")


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
