"""Pure schema, quality, profile, quarantine, and preview report models for workflows."""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any, Mapping

from ._compat import dataclass
from .models import ClaimSummary, EvidenceSummary

if TYPE_CHECKING:
    from .query import LazyFrame, Result


@dataclass(frozen=True, slots=True)
class WorkflowSchemaField:
    """Observed schema field for a bounded ShardLoom local-source workflow."""

    name: str
    dtype: str
    nullable: bool
    declared_dtype: str | None
    observed_non_null_count: int
    null_count: int

    @property
    def observed_row_count(self) -> int:
        """Return rows observed while inferring this field."""

        return self.observed_non_null_count + self.null_count


@dataclass(frozen=True, slots=True)
class WorkflowSchemaReport:
    """Schema report backed by an admitted local-source runtime smoke."""

    workflow: "LazyFrame"
    smoke_report: Result
    fields: tuple[WorkflowSchemaField, ...]

    @property
    def field_names(self) -> tuple[str, ...]:
        """Return schema field names in stable observed order."""

        return tuple(field.name for field in self.fields)

    @property
    def schema_map(self) -> dict[str, str]:
        """Return a field-to-dtype mapping."""

        return {field.name: field.dtype for field in self.fields}

    @property
    def nullable_fields(self) -> tuple[str, ...]:
        """Return fields observed with null or missing values."""

        return tuple(field.name for field in self.fields if field.nullable)

    @property
    def observed_row_count(self) -> int:
        """Return the bounded row count used for schema discovery."""

        return len(self.smoke_report.result_rows)

    @property
    def fallback_attempted(self) -> bool:
        """Whether schema discovery attempted fallback execution."""

        return self.smoke_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether schema discovery invoked an external execution engine."""

        return self.smoke_report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str | None:
        """Return the claim-gate status of the backing runtime smoke."""

        return self.smoke_report.claim_gate_status

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return compact evidence from the backing runtime smoke."""

        return self.smoke_report.evidence_summary

    def field(self, name: str) -> WorkflowSchemaField:
        """Return one schema field by name."""

        for field in self.fields:
            if field.name == name:
                return field
        raise KeyError(f"schema field {name!r} was not observed")


@dataclass(frozen=True, slots=True)
class WorkflowSchemaMismatch:
    """One schema validation mismatch."""

    field: str
    expected_dtype: str
    observed_dtype: str | None


@dataclass(frozen=True, slots=True)
class WorkflowSchemaValidationReport:
    """Validation report for an expected schema against observed ShardLoom rows."""

    schema_report: WorkflowSchemaReport
    expected_schema: tuple[tuple[str, str], ...]
    missing_fields: tuple[str, ...]
    unexpected_fields: tuple[str, ...]
    dtype_mismatches: tuple[WorkflowSchemaMismatch, ...]

    @property
    def valid(self) -> bool:
        """Whether the observed schema satisfies the expected schema exactly."""

        return (
            not self.missing_fields
            and not self.unexpected_fields
            and not self.dtype_mismatches
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether validation attempted fallback execution."""

        return self.schema_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether validation invoked an external execution engine."""

        return self.schema_report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str | None:
        """Return the claim-gate status of the backing runtime smoke."""

        return self.schema_report.claim_gate_status


@dataclass(frozen=True, slots=True)
class _WorkflowDataQualityCheckSpec:
    """Parsed bounded data-quality check syntax."""

    kind: str
    column: str
    raw: str
    pattern: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowDataQualityCheckResult:
    """Result for one bounded data-quality check."""

    check: str
    column: str
    passed: bool
    failing_row_count: int
    message: str


@dataclass(frozen=True, slots=True)
class WorkflowDataQualityReport:
    """Bounded data-quality summary over an admitted local-source workflow."""

    schema_report: WorkflowSchemaReport
    checks: tuple[WorkflowDataQualityCheckResult, ...] = ()

    @property
    def row_count(self) -> int:
        """Return the bounded row count inspected by the report."""

        return self.schema_report.observed_row_count

    @property
    def field_count(self) -> int:
        """Return the number of observed fields."""

        return len(self.schema_report.fields)

    @property
    def null_counts(self) -> dict[str, int]:
        """Return observed null-or-missing counts by field."""

        return {field.name: field.null_count for field in self.schema_report.fields}

    @property
    def passed(self) -> bool:
        """Whether every requested data-quality check passed."""

        return all(check.passed for check in self.checks)

    @property
    def fallback_attempted(self) -> bool:
        """Whether data-quality reporting attempted fallback execution."""

        return self.schema_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether data-quality reporting invoked an external execution engine."""

        return self.schema_report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str | None:
        """Return the claim-gate status of the backing runtime smoke."""

        return self.schema_report.claim_gate_status


@dataclass(frozen=True, slots=True)
class WorkflowProfileReport:
    """Bounded runtime profile over an admitted local-source workflow."""

    workflow: "LazyFrame"
    smoke_report: Result
    schema_report: WorkflowSchemaReport
    limit: int

    @property
    def profile_kind(self) -> str:
        """Return the profile contract label."""

        return "bounded_local_source_runtime_profile"

    @property
    def materialization_boundary(self) -> str:
        """Return the explicit decoded materialization boundary."""

        return "bounded_inline_jsonl_profile"

    @property
    def row_count(self) -> int:
        """Return the bounded row count inspected by the profile."""

        return self.schema_report.observed_row_count

    @property
    def field_count(self) -> int:
        """Return the number of observed fields."""

        return len(self.schema_report.fields)

    @property
    def null_counts(self) -> dict[str, int]:
        """Return observed null-or-missing counts by field."""

        return {field.name: field.null_count for field in self.schema_report.fields}

    @property
    def rows(self) -> tuple[Mapping[str, Any], ...]:
        """Return bounded rows used to build the profile."""

        return self.smoke_report.result_rows

    @property
    def runtime_execution(self) -> bool:
        """Whether the backing runtime smoke executed."""

        return self.smoke_report.runtime_execution

    @property
    def data_read(self) -> bool:
        """Whether the backing runtime smoke read source data."""

        return self.smoke_report.data_read

    @property
    def write_io(self) -> bool:
        """Whether profile collection wrote output."""

        return False

    @property
    def fallback_attempted(self) -> bool:
        """Whether profile collection attempted fallback execution."""

        return self.smoke_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether profile collection invoked an external execution engine."""

        return self.smoke_report.external_engine_invoked

    @property
    def claim_gate_status(self) -> str | None:
        """Return the backing runtime smoke claim-gate status."""

        return self.smoke_report.claim_gate_status

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return compact evidence from the backing runtime smoke."""

        return self.smoke_report.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return compact claim posture from the backing runtime smoke."""

        return self.smoke_report.claim_summary


@dataclass(frozen=True, slots=True)
class WorkflowQuarantineReport:
    """Bounded quarantine report over an admitted local-source workflow."""

    workflow: "LazyFrame"
    quality_report: WorkflowDataQualityReport
    checks: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]
    limit: int
    target_uri: str | None
    output_format: str | None
    sink_report: Result | None = None

    @property
    def quarantine_policy(self) -> str:
        """Return the scoped quarantine policy label."""

        return "bounded_local_source_quarantine.v1"

    @property
    def materialization_boundary(self) -> str:
        """Return the explicit decoded classification boundary."""

        return "bounded_inline_jsonl_quarantine_classification"

    @property
    def quarantine_status(self) -> str:
        """Return the bounded quarantine outcome status."""

        if self.sink_report is not None:
            return "written"
        if not self.rows:
            return "not_emitted_no_quarantine_rows"
        if self.target_uri is not None:
            return "not_emitted_non_pushdown_check"
        return "report_only"

    @property
    def row_count(self) -> int:
        """Return the bounded row count inspected by the report."""

        return self.quality_report.row_count

    @property
    def quarantined_row_count(self) -> int:
        """Return the number of bounded rows selected for quarantine."""

        return len(self.rows)

    @property
    def output_path(self) -> str | None:
        """Return the quarantine sink path when a local sink was written."""

        return None if self.sink_report is None else self.sink_report.output_path

    @property
    def output_commit_status(self) -> str | None:
        """Return the local sink commit status when emitted."""

        return None if self.sink_report is None else self.sink_report.output_commit_status

    @property
    def output_native_io_certificate_status(self) -> str | None:
        """Return the local sink Native I/O certificate status when emitted."""

        if self.sink_report is None:
            return None
        return self.sink_report.native_io_certificate_status

    @property
    def result_replay_verified(self) -> bool:
        """Whether a written quarantine sink was replay verified."""

        return self.sink_report is not None and self.sink_report.output_replay_verified

    @property
    def runtime_execution(self) -> bool:
        """Whether the backing runtime smoke executed."""

        return self.quality_report.schema_report.smoke_report.runtime_execution

    @property
    def data_read(self) -> bool:
        """Whether the backing runtime smoke read source data."""

        return self.quality_report.schema_report.smoke_report.data_read

    @property
    def write_io(self) -> bool:
        """Whether quarantine emitted a local sink through ShardLoom."""

        return self.sink_report is not None and self.sink_report.output_path is not None

    @property
    def fallback_attempted(self) -> bool:
        """Whether quarantine attempted fallback execution."""

        return self.quality_report.fallback_attempted or (
            self.sink_report.fallback_attempted if self.sink_report is not None else False
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether quarantine invoked an external execution engine."""

        return self.quality_report.external_engine_invoked or (
            self.sink_report.external_engine_invoked if self.sink_report is not None else False
        )

    @property
    def claim_gate_status(self) -> str | None:
        """Return the most specific backing claim-gate status."""

        if self.sink_report is not None:
            return self.sink_report.claim_gate_status
        return self.quality_report.claim_gate_status

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return compact evidence from the sink or classification runtime."""

        if self.sink_report is not None:
            return self.sink_report.evidence_summary
        return self.quality_report.schema_report.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return compact claim posture from the backing runtime."""

        if self.sink_report is not None:
            return self.sink_report.claim_summary
        return self.quality_report.schema_report.smoke_report.claim_summary


@dataclass(frozen=True, slots=True)
class WorkflowNotebookPreview:
    """Bounded notebook/display preview with explicit materialization evidence."""

    workflow: "LazyFrame"
    smoke_report: Result
    limit: int

    @property
    def rows(self) -> tuple[Mapping[str, Any], ...]:
        """Return decoded preview rows from ShardLoom's bounded inline result."""

        return self.smoke_report.result_rows

    @property
    def row_count(self) -> int:
        """Return the number of decoded preview rows."""

        return len(self.rows)

    @property
    def schema_report(self) -> WorkflowSchemaReport:
        """Return schema evidence inferred from the same bounded rows."""

        from .query import _workflow_schema_report

        return _workflow_schema_report(self.workflow, self.smoke_report)

    @property
    def fallback_attempted(self) -> bool:
        """Whether preview materialization attempted fallback execution."""

        return self.smoke_report.fallback_attempted

    @property
    def external_engine_invoked(self) -> bool:
        """Whether preview materialization invoked an external execution engine."""

        return self.smoke_report.external_engine_invoked

    @property
    def materialization_boundary(self) -> str:
        """Return the explicit decoded display boundary label."""

        return "bounded_inline_jsonl_to_notebook_display"

    def to_python_objects(self) -> tuple[Mapping[str, Any], ...]:
        """Return decoded rows for callers that want the display payload."""

        return self.rows

    def to_html(self) -> str:
        """Render a small HTML table for notebook frontends."""

        from .query import _display_cell, _row_field_order

        columns = _row_field_order(self.rows)
        if not columns:
            return "<table><tbody></tbody></table>"
        header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
        body_rows = []
        for row in self.rows:
            cells = "".join(
                f"<td>{html.escape(_display_cell(row.get(column)))}</td>"
                for column in columns
            )
            body_rows.append(f"<tr>{cells}</tr>")
        body = "".join(body_rows)
        return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"

    def _repr_html_(self) -> str:
        """Notebook HTML representation."""

        return self.to_html()
