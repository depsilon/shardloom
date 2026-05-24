"""Typed models for ShardLoom CLI JSON envelopes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

OUTPUT_ENVELOPE_SCHEMA_VERSION = "shardloom.output.v2"
RUNTIME_EXECUTION_ENVELOPE_VALIDATION_SCHEMA_VERSION = (
    "shardloom.runtime_execution_envelope_validation.v1"
)
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
class RuntimeEnvelopeValidationIssue:
    """One runtime-envelope validation blocker."""

    code: str
    field: str
    message: str

    def as_dict(self) -> dict[str, str]:
        """Return this issue as a plain JSON-serializable mapping."""

        return {
            "code": self.code,
            "field": self.field,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class RuntimeEnvelopeValidationReport:
    """Versioned validation result for a runtime-claim envelope."""

    schema_version: str
    surface_id: str
    command: str
    status: str
    runtime_expected: bool
    execution_mode: str | None
    claim_gate_status: str | None
    fallback_attempted: bool
    external_engine_invoked: bool
    issues: tuple[RuntimeEnvelopeValidationIssue, ...]

    @property
    def passed(self) -> bool:
        """Whether the runtime envelope satisfied the required evidence contract."""

        return self.status == "passed"

    @property
    def blockers(self) -> tuple[str, ...]:
        """Return human-readable blockers for release gates and tests."""

        return tuple(issue.message for issue in self.issues)

    @property
    def missing_fields(self) -> tuple[str, ...]:
        """Return required fields or field groups that were missing."""

        return tuple(
            issue.field for issue in self.issues if issue.code == "missing_required_field"
        )

    @property
    def invalid_fields(self) -> tuple[str, ...]:
        """Return fields present with invalid or unsafe values."""

        return tuple(
            issue.field for issue in self.issues if issue.code != "missing_required_field"
        )

    @property
    def runtime_claim_allowed(self) -> bool:
        """Whether this envelope can support a production runtime claim."""

        return self.passed and self.claim_gate_status == "claim_grade"

    def as_dict(self) -> dict[str, Any]:
        """Return this validation report as a plain mapping."""

        return {
            "schema_version": self.schema_version,
            "surface_id": self.surface_id,
            "command": self.command,
            "status": self.status,
            "runtime_expected": self.runtime_expected,
            "execution_mode": self.execution_mode,
            "claim_gate_status": self.claim_gate_status,
            "fallback_attempted": self.fallback_attempted,
            "external_engine_invoked": self.external_engine_invoked,
            "runtime_claim_allowed": self.runtime_claim_allowed,
            "missing_fields": self.missing_fields,
            "invalid_fields": self.invalid_fields,
            "blockers": self.blockers,
            "issues": [issue.as_dict() for issue in self.issues],
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

    def runtime_execution_validation(
        self,
        *,
        surface_id: str = "runtime",
        runtime_expected: bool = True,
        execution_mode: str | None = None,
    ) -> RuntimeEnvelopeValidationReport:
        """Validate this envelope before treating it as runtime evidence."""

        return validate_runtime_execution_envelope(
            self,
            surface_id=surface_id,
            runtime_expected=runtime_expected,
            execution_mode=execution_mode,
        )


def validate_runtime_execution_envelope(
    envelope: OutputEnvelope,
    *,
    surface_id: str = "runtime",
    runtime_expected: bool = True,
    execution_mode: str | None = None,
) -> RuntimeEnvelopeValidationReport:
    """Return a versioned no-fallback runtime evidence validation report."""

    issues: list[RuntimeEnvelopeValidationIssue] = []
    selected_execution_mode = (
        execution_mode
        or envelope.field("selected_execution_mode")
        or envelope.field("execution_mode")
    )

    for field in ("fallback_attempted", "external_engine_invoked", "claim_gate_status"):
        if not _field_present(envelope, field):
            issues.append(
                RuntimeEnvelopeValidationIssue(
                    code="missing_required_field",
                    field=field,
                    message=f"runtime envelope is missing required field {field}",
                )
            )
    for field in ("fallback_attempted", "external_engine_invoked"):
        if _field_present(envelope, field) and _safe_field_bool(envelope, field) is None:
            issues.append(
                RuntimeEnvelopeValidationIssue(
                    code="invalid_runtime_field",
                    field=field,
                    message=f"runtime envelope field {field} must be true or false",
                )
            )

    fallback_attempted = envelope.fallback.attempted or (
        _safe_field_bool(envelope, "fallback_attempted") is True
    )
    external_engine_invoked = (
        _safe_field_bool(envelope, "external_engine_invoked") is True
    )
    if fallback_attempted:
        issues.append(
            RuntimeEnvelopeValidationIssue(
                code="unsafe_runtime_field",
                field="fallback_attempted",
                message="runtime envelope attempted fallback execution",
            )
        )
    if external_engine_invoked:
        issues.append(
            RuntimeEnvelopeValidationIssue(
                code="unsafe_runtime_field",
                field="external_engine_invoked",
                message="runtime envelope invoked an external execution engine",
            )
        )

    if runtime_expected and envelope.status == "success":
        _require_any_field(
            envelope,
            issues,
            field_group="route_state_ref",
            fields=(
                "source_state_id",
                "source_state_digest",
                "prepared_state_id",
                "prepared_state_digest",
                "output_plan_digest",
                "generated_source_plan_digest",
                "vortex_artifact_digest",
                "plan_id",
            ),
        )
        _require_any_field(
            envelope,
            issues,
            field_group="materialization_or_decode_evidence",
            fields=(
                "materialization_boundary",
                "source_state_materialization_layout",
                "source_state_runtime_consumption_layout",
                "source_state_scalar_runtime_materialization_required",
                "operator_temporary_materialization_used",
                "representation_transitions",
                "data_decoded",
                "data_materialized",
            ),
        )
        if not _execution_certificate_present(envelope):
            issues.append(
                RuntimeEnvelopeValidationIssue(
                    code="missing_required_field",
                    field="execution_certificate",
                    message=(
                        "runtime envelope is missing execution_certificate_ref, "
                        "execution_certificate_refs, or a typed execution_certificate"
                    ),
                )
            )

    if selected_execution_mode == "prepared_vortex":
        for field in ("prepared_state_id", "prepared_state_digest"):
            if not _field_present(envelope, field):
                issues.append(
                    RuntimeEnvelopeValidationIssue(
                        code="missing_required_field",
                        field=field,
                        message=f"prepared_vortex envelope is missing {field}",
                    )
                )

    if selected_execution_mode == "compatibility_import_certified":
        if envelope.field("timing_scope") != "cold_certified_end_to_end":
            issues.append(
                RuntimeEnvelopeValidationIssue(
                    code="invalid_runtime_field",
                    field="timing_scope",
                    message=(
                        "compatibility_import_certified envelope must disclose "
                        "timing_scope=cold_certified_end_to_end"
                    ),
                )
            )
        if _safe_field_bool(envelope, "preparation_included") is not True:
            issues.append(
                RuntimeEnvelopeValidationIssue(
                    code="invalid_runtime_field",
                    field="preparation_included",
                    message=(
                        "compatibility_import_certified envelope must disclose "
                        "preparation_included=true"
                    ),
                )
            )

    status = "passed" if not issues else "blocked"
    return RuntimeEnvelopeValidationReport(
        schema_version=RUNTIME_EXECUTION_ENVELOPE_VALIDATION_SCHEMA_VERSION,
        surface_id=surface_id,
        command=envelope.command,
        status=status,
        runtime_expected=runtime_expected,
        execution_mode=selected_execution_mode,
        claim_gate_status=envelope.field("claim_gate_status"),
        fallback_attempted=fallback_attempted,
        external_engine_invoked=external_engine_invoked,
        issues=tuple(issues),
    )


def _field_present(envelope: OutputEnvelope, key: str) -> bool:
    value = envelope.field(key)
    if value is None:
        return False
    return value.strip().lower() not in {"", "none", "not_applicable", "missing"}


def _safe_field_bool(envelope: OutputEnvelope, key: str) -> bool | None:
    try:
        return envelope.field_bool(key)
    except ValueError:
        return None


def _require_any_field(
    envelope: OutputEnvelope,
    issues: list[RuntimeEnvelopeValidationIssue],
    *,
    field_group: str,
    fields: tuple[str, ...],
) -> None:
    if any(_field_present(envelope, field) for field in fields):
        return
    issues.append(
        RuntimeEnvelopeValidationIssue(
            code="missing_required_field",
            field=field_group,
            message=(
                "runtime envelope is missing required evidence group "
                f"{field_group}: one of {','.join(fields)}"
            ),
        )
    )


def _execution_certificate_present(envelope: OutputEnvelope) -> bool:
    if _field_present(envelope, "execution_certificate_ref"):
        return True
    if _field_present(envelope, "execution_certificate_refs"):
        return True
    return any(
        str(certificate.get("kind", "")) == "execution_certificate"
        or str(certificate.get("id", "")).startswith("execution.")
        or ".execution." in str(certificate.get("id", ""))
        for certificate in envelope.certificates
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
