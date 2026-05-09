"""Typed models for ShardLoom CLI JSON envelopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

OUTPUT_ENVELOPE_SCHEMA_VERSION = "shardloom.output.v1"
REQUIRED_OUTPUT_ENVELOPE_FIELDS = frozenset(
    {
        "schema_version",
        "command",
        "status",
        "summary",
        "human_text",
        "fallback",
        "diagnostics",
        "fields",
    }
)


@dataclass(frozen=True, slots=True)
class FallbackStatus:
    """Fallback status copied from a ShardLoom output envelope."""

    attempted: bool
    allowed: bool
    engine: str | None
    reason: str

    @classmethod
    def from_json(cls, payload: Mapping[str, Any] | None) -> "FallbackStatus":
        data = payload or {}
        return cls(
            attempted=bool(data.get("attempted", False)),
            allowed=bool(data.get("allowed", False)),
            engine=_optional_str(data.get("engine")),
            reason=str(data.get("reason", "")),
        )


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """Diagnostic entry copied from a ShardLoom output envelope."""

    code: str
    severity: str
    category: str
    message: str
    feature: str | None
    reason: str | None
    suggested_next_step: str | None
    fallback: FallbackStatus

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "Diagnostic":
        return cls(
            code=str(payload.get("code", "")),
            severity=str(payload.get("severity", "")),
            category=str(payload.get("category", "")),
            message=str(payload.get("message", "")),
            feature=_optional_str(payload.get("feature")),
            reason=_optional_str(payload.get("reason")),
            suggested_next_step=_optional_str(payload.get("suggested_next_step")),
            fallback=FallbackStatus.from_json(_mapping_or_none(payload.get("fallback"))),
        )


@dataclass(frozen=True, slots=True)
class FieldEntry:
    """Key/value field entry copied from a ShardLoom output envelope."""

    key: str
    value: str

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "FieldEntry":
        return cls(key=str(payload.get("key", "")), value=str(payload.get("value", "")))


@dataclass(frozen=True, slots=True)
class OutputEnvelope:
    """Parsed ShardLoom `OutputEnvelope` JSON payload."""

    schema_version: str
    command: str
    status: str
    summary: str
    human_text: str
    fallback: FallbackStatus
    diagnostics: tuple[Diagnostic, ...]
    fields: tuple[FieldEntry, ...]
    raw: Mapping[str, Any]

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "OutputEnvelope":
        missing = sorted(REQUIRED_OUTPUT_ENVELOPE_FIELDS.difference(payload.keys()))
        if missing:
            raise ValueError(f"ShardLoom output envelope missing required fields: {missing}")
        schema_version = str(payload.get("schema_version", ""))
        if schema_version != OUTPUT_ENVELOPE_SCHEMA_VERSION:
            raise ValueError(
                "unsupported ShardLoom output envelope schema_version: "
                f"{schema_version}"
            )
        diagnostics = tuple(
            Diagnostic.from_json(item)
            for item in _sequence(payload.get("diagnostics"))
            if isinstance(item, Mapping)
        )
        fields = tuple(
            FieldEntry.from_json(item)
            for item in _sequence(payload.get("fields"))
            if isinstance(item, Mapping)
        )
        return cls(
            schema_version=schema_version,
            command=str(payload.get("command", "")),
            status=str(payload.get("status", "")),
            summary=str(payload.get("summary", "")),
            human_text=str(payload.get("human_text", "")),
            fallback=FallbackStatus.from_json(_mapping_or_none(payload.get("fallback"))),
            diagnostics=diagnostics,
            fields=fields,
            raw=dict(payload),
        )

    @property
    def is_error(self) -> bool:
        """Whether the command status represents an error or unsupported result."""

        return self.status in {"error", "unsupported"} or any(
            diagnostic.severity in {"error", "fatal"} for diagnostic in self.diagnostics
        )

    @property
    def field_map(self) -> dict[str, str]:
        """Return envelope fields as a convenience mapping."""

        return {entry.key: entry.value for entry in self.fields}


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, list):
        return tuple(value)
    return ()
