"""Typed models for ShardLoom CLI JSON envelopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

OUTPUT_ENVELOPE_SCHEMA_VERSION = "shardloom.output.v2"
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
TYPED_OUTPUT_PAYLOAD_FIELDS = frozenset(
    {
        "result",
        "result_refs",
        "artifacts",
        "artifact_refs",
        "certificates",
        "policy",
        "lifecycle",
        "capability_snapshot",
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
class EvidenceSummary:
    """Compact user-facing summary of what a ShardLoom command proved."""

    command: str
    status: str
    execution_mode: str | None
    engine_mode: str | None
    source_format: str | None
    output_path: str | None
    output_row_count: int | None
    source_io_performed: bool | None
    output_io_performed: bool | None
    generated_source_kind: str | None
    generated_source_row_count: int | None
    output_native_io_certificate_status: str | None
    materialization_boundary: str | None
    fallback_attempted: bool
    external_engine_invoked: bool
    claim_gate_status: str | None

    def as_dict(self) -> dict[str, Any]:
        """Return this summary as a plain mapping for simple printing/JSON encoding."""

        return {
            "command": self.command,
            "status": self.status,
            "execution_mode": self.execution_mode,
            "engine_mode": self.engine_mode,
            "source_format": self.source_format,
            "output_path": self.output_path,
            "output_row_count": self.output_row_count,
            "source_io_performed": self.source_io_performed,
            "output_io_performed": self.output_io_performed,
            "generated_source_kind": self.generated_source_kind,
            "generated_source_row_count": self.generated_source_row_count,
            "output_native_io_certificate_status": self.output_native_io_certificate_status,
            "materialization_boundary": self.materialization_boundary,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "claim_gate_status": self.claim_gate_status,
        }


@dataclass(frozen=True, slots=True)
class ClaimSummary:
    """Compact user-facing summary of what may and may not be claimed."""

    status: str
    claim_gate_status: str | None
    support_status: str | None
    blocker_id: str | None
    fallback_attempted: bool
    external_engine_invoked: bool
    public_performance_claim_allowed: bool

    def as_dict(self) -> dict[str, Any]:
        """Return this summary as a plain mapping for simple printing/JSON encoding."""

        return {
            "status": self.status,
            "claim_gate_status": self.claim_gate_status,
            "support_status": self.support_status,
            "blocker_id": self.blocker_id,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "public_performance_claim_allowed": self.public_performance_claim_allowed,
        }


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
    result: Mapping[str, Any]
    result_refs: tuple[Mapping[str, Any], ...]
    artifacts: tuple[Mapping[str, Any], ...]
    artifact_refs: tuple[Mapping[str, Any], ...]
    certificates: tuple[Mapping[str, Any], ...]
    policy: Mapping[str, Any]
    lifecycle: Mapping[str, Any]
    capability_snapshot: Mapping[str, Any]
    fields: tuple[FieldEntry, ...]
    raw: Mapping[str, Any]

    @classmethod
    def from_json(cls, payload: Mapping[str, Any]) -> "OutputEnvelope":
        missing = sorted(
            (REQUIRED_OUTPUT_ENVELOPE_FIELDS | TYPED_OUTPUT_PAYLOAD_FIELDS).difference(
                payload.keys()
            )
        )
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
            result=dict(_mapping_or_none(payload.get("result")) or {}),
            result_refs=_mapping_sequence(payload.get("result_refs")),
            artifacts=_mapping_sequence(payload.get("artifacts")),
            artifact_refs=_mapping_sequence(payload.get("artifact_refs")),
            certificates=_mapping_sequence(payload.get("certificates")),
            policy=dict(_mapping_or_none(payload.get("policy")) or {}),
            lifecycle=dict(_mapping_or_none(payload.get("lifecycle")) or {}),
            capability_snapshot=dict(
                _mapping_or_none(payload.get("capability_snapshot")) or {}
            ),
            fields=fields,
            raw=dict(payload),
        )

    @property
    def is_error(self) -> bool:
        """Whether the command status represents an error or unsupported result."""

        return self.status in {"error", "unsupported"}

    @property
    def has_error_diagnostics(self) -> bool:
        """Whether the report includes error/fatal diagnostics for inspection."""

        return any(
            diagnostic.severity in {"error", "fatal"} for diagnostic in self.diagnostics
        )

    @property
    def field_map(self) -> dict[str, str]:
        """Return typed payload fields plus the temporary legacy mirror.

        The typed `result`, `policy`, `lifecycle`, and `capability_snapshot`
        payloads are the primary machine-readable surface. The flat `fields`
        mirror remains a compatibility fallback while CLI command families
        migrate.
        """

        merged = {entry.key: entry.value for entry in self.fields}
        for payload in (
            self.result,
            self.policy,
            self.lifecycle,
            self.capability_snapshot,
        ):
            merged.update(_typed_payload_field_map(payload))
        return merged

    @property
    def legacy_field_map(self) -> dict[str, str]:
        """Return only the temporary flat `fields` mirror."""

        return {entry.key: entry.value for entry in self.fields}

    def field(self, key: str, default: str | None = None) -> str | None:
        """Return a field value by key, preserving the CLI string value."""

        return self.field_map.get(key, default)

    def field_bool(self, key: str, default: bool | None = None) -> bool | None:
        """Return a boolean field parsed from ShardLoom's stable string values."""

        value = self.field(key)
        if value is None:
            return default
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
        raise ValueError(f"field {key!r} is not a boolean value: {value!r}")

    def field_int(self, key: str, default: int | None = None) -> int | None:
        """Return an integer field parsed from ShardLoom's stable string values."""

        value = self.field(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"field {key!r} is not an integer value: {value!r}") from exc

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return a compact summary of runtime, I/O, certificate, and claim fields."""

        return EvidenceSummary(
            command=self.command,
            status=self.status,
            execution_mode=self.field("execution_mode"),
            engine_mode=self.field("engine_mode"),
            source_format=self.field("source_format"),
            output_path=self.field("output_path"),
            output_row_count=self.field_int("output_row_count"),
            source_io_performed=_field_bool_or_none(self, "source_io_performed"),
            output_io_performed=_field_bool_or_none(self, "output_io_performed"),
            generated_source_kind=self.field("generated_source_kind"),
            generated_source_row_count=self.field_int("generated_source_row_count"),
            output_native_io_certificate_status=self.field(
                "output_native_io_certificate_status"
            ),
            materialization_boundary=self.field("materialization_boundary"),
            fallback_attempted=self.fallback.attempted
            or self.field_bool("fallback_attempted", False) is True,
            external_engine_invoked=self.field_bool("external_engine_invoked", False)
            is True,
            claim_gate_status=self.field("claim_gate_status"),
        )

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return a compact claim-boundary summary."""

        claim_gate_status = self.field("claim_gate_status")
        return ClaimSummary(
            status=self.status,
            claim_gate_status=claim_gate_status,
            support_status=self.field("support_status"),
            blocker_id=self.field("blocker_id"),
            fallback_attempted=self.fallback.attempted
            or self.field_bool("fallback_attempted", False) is True,
            external_engine_invoked=self.field_bool("external_engine_invoked", False)
            is True,
            public_performance_claim_allowed=claim_gate_status == "claim_grade",
        )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _field_bool_or_none(envelope: OutputEnvelope, key: str) -> bool | None:
    value = envelope.field(key)
    if value is None:
        return None
    return envelope.field_bool(key)


def _mapping_or_none(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    return None


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, list):
        return tuple(value)
    return ()


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in _sequence(value) if isinstance(item, Mapping))


def _typed_payload_field_map(payload: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(item.get("key", "")): str(item.get("value", ""))
        for item in _sequence(payload.get("fields"))
        if isinstance(item, Mapping)
    }
