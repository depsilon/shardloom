"""Lazy workflow planning helpers for the ShardLoom Python surface."""

from __future__ import annotations

import ast
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Mapping, Sequence, cast
from urllib.parse import quote

from .client import (
    Binary,
    DEFAULT_PROFILE_ORDER,
    EngineSelectionPlan,
    GeneratedSourceWriteReport,
    ShardLoomClient,
    SqlLocalSourceSmokeReport,
)
from .models import ClaimSummary, Diagnostic, EvidenceSummary, OutputEnvelope

SUPPORTED_SOURCE_FORMATS = ("vortex", "csv", "json", "parquet")


@dataclass(frozen=True, slots=True)
class WorkflowSource:
    """A declared workflow source that is not read during construction."""

    source_format: str
    uri: str
    schema: tuple[tuple[str, str], ...] = ()

    @property
    def schema_map(self) -> dict[str, str]:
        """Return the optional declared schema as a dict."""

        return dict(self.schema)

    def to_summary(self) -> str:
        """Return a deterministic source summary for CLI explain/estimate calls."""

        return f"read_{self.source_format}({self.uri})"


@dataclass(frozen=True, slots=True)
class WorkflowOperation:
    """A lazy query-builder operation."""

    kind: str
    values: tuple[str, ...]

    def to_summary(self) -> str:
        """Return a deterministic operation summary."""

        if self.kind == "filter":
            return f"filter({self.values[0]})"
        if self.kind == "select":
            return f"select({','.join(self.values)})"
        if self.kind == "limit":
            return f"limit({self.values[0]})"
        return f"{self.kind}({','.join(self.values)})"


@dataclass(frozen=True, slots=True)
class PredicateExpression:
    """A scoped SQL predicate expression for ShardLoom local-source smokes."""

    sql: str

    def __str__(self) -> str:
        return self.sql

    def __and__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical AND predicate."""

        return PredicateExpression(f"({self.sql} AND {_predicate_sql(other)})")

    def __or__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical OR predicate."""

        return PredicateExpression(f"({self.sql} OR {_predicate_sql(other)})")

    def __invert__(self) -> "PredicateExpression":
        """Return a scoped logical NOT predicate."""

        return PredicateExpression(f"NOT {self.sql}")


@dataclass(frozen=True, slots=True)
class ColumnExpression:
    """A scoped column expression for Python query-builder predicates."""

    sql: str

    def __str__(self) -> str:
        return self.sql

    def __eq__(self, value: object) -> PredicateExpression:  # type: ignore[override]
        if value is None:
            return self.is_null()
        return self._compare("=", value)

    def __ne__(self, value: object) -> PredicateExpression:  # type: ignore[override]
        if value is None:
            return self.is_not_null()
        return self._compare("!=", value)

    def __lt__(self, value: object) -> PredicateExpression:
        return self._compare("<", value)

    def __le__(self, value: object) -> PredicateExpression:
        return self._compare("<=", value)

    def __gt__(self, value: object) -> PredicateExpression:
        return self._compare(">", value)

    def __ge__(self, value: object) -> PredicateExpression:
        return self._compare(">=", value)

    def _compare(self, operator: str, value: object) -> PredicateExpression:
        return PredicateExpression(f"{self.sql} {operator} {_sql_literal(value)}")

    def is_null(self) -> PredicateExpression:
        """Return a scoped `IS NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NULL")

    def is_not_null(self) -> PredicateExpression:
        """Return a scoped `IS NOT NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT NULL")

    def like(self, pattern: object) -> PredicateExpression:
        """Return a scoped SQL LIKE predicate.

        The runtime admits only prefix, suffix, and contains forms. Unsupported
        LIKE patterns still block in the ShardLoom CLI before fallback.
        """

        return PredicateExpression(f"{self.sql} LIKE {_sql_string_literal(pattern)}")

    def contains(self, needle: object) -> PredicateExpression:
        """Return a scoped substring predicate lowered to `LIKE '%needle%'`."""

        value = _like_needle("contains needle", needle)
        return self.like(f"%{value}%")

    def startswith(self, prefix: object) -> PredicateExpression:
        """Return a scoped prefix predicate lowered to `LIKE 'prefix%'`."""

        value = _like_needle("startswith prefix", prefix)
        return self.like(f"{value}%")

    def endswith(self, suffix: object) -> PredicateExpression:
        """Return a scoped suffix predicate lowered to `LIKE '%suffix'`."""

        value = _like_needle("endswith suffix", suffix)
        return self.like(f"%{value}")

    def isin(self, *values: object) -> PredicateExpression:
        """Return a scoped bounded `IN (...)` predicate."""

        normalized = _normalize_in_values(values)
        joined = ",".join(_sql_literal(value) for value in normalized)
        return PredicateExpression(f"{self.sql} IN ({joined})")

    def between(self, lower: object, upper: object) -> PredicateExpression:
        """Return a scoped inclusive range predicate.

        The expression lowers to an admitted `>=` / `<=` predicate pair so the
        CLI remains responsible for runtime admission, evidence, and blockers.
        """

        return PredicateExpression(
            f"({self.sql} >= {_sql_literal(lower)} AND {self.sql} <= {_sql_literal(upper)})"
        )

    def cast(self, dtype: object) -> "ColumnExpression":
        """Return a scoped `CAST(column AS dtype)` expression for comparisons."""

        normalized_dtype = _normalize_cast_dtype(dtype)
        return ColumnExpression(f"CAST({self.sql} AS {normalized_dtype})")


@dataclass(frozen=True, slots=True)
class WorkflowCertificationReport:
    """Report-only certificate surfaces for a lazy workflow."""

    workflow: "LazyFrame"
    execution_certificate_plan: OutputEnvelope
    native_io_envelope_plan: OutputEnvelope
    certification_capabilities: OutputEnvelope

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all certificate-related envelopes."""

        return (
            self.execution_certificate_plan,
            self.native_io_envelope_plan,
            self.certification_capabilities,
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether any certificate surface attempted fallback execution."""

        return any(envelope.fallback.attempted for envelope in self.envelopes)

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return certificate and capability diagnostics."""

        return tuple(
            diagnostic
            for envelope in self.envelopes
            for diagnostic in envelope.diagnostics
        )


@dataclass(frozen=True, slots=True)
class UnsupportedWorkflowReport:
    """Aggregated diagnostics for report-only lazy workflow inspection."""

    workflow: "LazyFrame"
    input_plan: OutputEnvelope
    explain: OutputEnvelope
    estimate: OutputEnvelope
    certification: WorkflowCertificationReport

    @property
    def envelopes(self) -> tuple[OutputEnvelope, ...]:
        """Return all envelopes collected for the report."""

        return (
            self.input_plan,
            self.explain,
            self.estimate,
            *self.certification.envelopes,
        )

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Return all diagnostics across the collected envelopes."""

        return tuple(
            diagnostic
            for envelope in self.envelopes
            for diagnostic in envelope.diagnostics
        )

    @property
    def fallback_attempted(self) -> bool:
        """Whether any inspected surface attempted fallback execution."""

        return any(envelope.fallback.attempted for envelope in self.envelopes)

    @property
    def unsupported_reasons(self) -> tuple[str, ...]:
        """Return stable unsupported diagnostic reasons/messages."""

        reasons: list[str] = []
        for diagnostic in self.diagnostics:
            if diagnostic.reason:
                reasons.append(diagnostic.reason)
            elif diagnostic.message:
                reasons.append(diagnostic.message)
        return tuple(dict.fromkeys(reasons))

    @property
    def materialization_boundaries(self) -> tuple[str, ...]:
        """Return materialization-related fields from collected envelopes."""

        boundaries: list[str] = []
        for envelope in self.envelopes:
            for key, value in envelope.field_map.items():
                if "materialization" in key and value not in {"", "false", "none"}:
                    boundaries.append(f"{envelope.command}:{key}={value}")
        return tuple(dict.fromkeys(boundaries))


@dataclass(frozen=True, slots=True)
class GeneratedRowsSource:
    """Scoped source-free user rows that can write a local smoke output."""

    schema_arg: str
    rows_arg: str
    client: ShardLoomClient
    source_kind: str = "user_rows"
    rows: tuple[tuple[tuple[str, object], ...], ...] = ()

    def select(self, *columns: object) -> "GeneratedRowsSource":
        """Project a scoped source-free row set before writing it locally.

        This is a generated-row convenience path, not broad DataFrame runtime.
        The transformed rows still write through ShardLoom's generated-source
        local-output command and preserve the no-source/no-fallback evidence
        emitted by that command.
        """

        selected = _normalize_generated_select_columns(columns)
        available = self._column_names()
        missing = tuple(column for column in selected if column not in available)
        if missing:
            raise ValueError(
                "generated row projection referenced unknown column(s): "
                + ", ".join(missing)
            )
        projected_rows = [
            {column: dict(row)[column] for column in selected} for row in self.rows
        ]
        return _generated_rows_source(
            projected_rows,
            client=self.client,
            source_kind=self.source_kind,
        )

    def with_column(self, name: object, expression: object) -> "GeneratedRowsSource":
        """Add or replace one deterministic literal column before local output.

        The first admitted slice intentionally supports only `lit(...)`
        expressions or direct Python bool/int/float literals. Broader
        expression-backed generated DataFrame runtime remains blocked until
        the expression engine and evidence model are promoted.
        """

        column = _require_non_empty("generated column name", name)
        literal = _generated_literal_expression(expression)
        transformed_rows = []
        for row in self.rows:
            updated = dict(row)
            updated[column] = literal
            transformed_rows.append(updated)
        return _generated_rows_source(
            transformed_rows,
            client=self.client,
            source_kind=self.source_kind,
        )

    def _column_names(self) -> tuple[str, ...]:
        if not self.rows:
            raise ValueError("generated row transforms require retained row values")
        return tuple(column for column, _value in self.rows[0])

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write generated user rows to a scoped local output sink with evidence."""

        return self.client.generated_source_user_rows_smoke(
            target_uri,
            self.schema_arg,
            self.rows_arg,
            source_kind=self.source_kind,
            output_format=output_format,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class GeneratedRangeSource:
    """Scoped ShardLoom-native integer generator that can write a local smoke output."""

    start: int
    end: int
    step: int
    column: str
    client: ShardLoomClient
    source_kind: str = "range"

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write the generated integer source to a scoped local output sink with evidence."""

        if self.source_kind == "sequence":
            return self.client.generated_source_sequence_smoke(
                target_uri,
                self.start,
                self.end,
                step=self.step,
                column=self.column,
                output_format=output_format,
                allow_overwrite=allow_overwrite,
                check=check,
            )
        return self.client.generated_source_range_smoke(
            target_uri,
            self.start,
            self.end,
            step=self.step,
            column=self.column,
            output_format=output_format,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class GeneratedSqlSource:
    """Scoped source-free SQL literal/VALUES query that can write local smoke output."""

    statement: str
    client: ShardLoomClient

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write admitted source-free SQL generated rows to a scoped local output sink."""

        return self.client.generated_source_sql_smoke(
            target_uri,
            self.statement,
            output_format=output_format,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class SqlWorkflow:
    """A scoped SQL workflow entry point over currently admitted ShardLoom SQL paths."""

    statement: str
    client: ShardLoomClient

    @property
    def operation_summary(self) -> str:
        """Return a deterministic SQL workflow summary."""

        return "sql(statement)"

    def collect(
        self,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Collect rows when the statement is admitted by the local-source SQL smoke."""

        if _is_source_free_sql_statement(self.statement):
            return self._unsupported_operation(
                "sql-source-free-projection",
                "source_free_sql_collect_requires_write_output",
                check=check,
            )
        if _is_local_source_sql_statement(self.statement):
            return self.client.sql_local_source_smoke(self.statement, check=check)
        return self._unsupported_operation("sql", self.statement, check=check)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Write an admitted SQL result to a scoped local output."""

        normalized_output_format = _normalize_local_output_format(output_format)
        if _is_source_free_sql_statement(self.statement):
            return self.client.generated_source_sql_smoke(
                target_uri,
                self.statement,
                output_format=normalized_output_format,
                allow_overwrite=allow_overwrite,
                check=check,
            )
        if _is_local_source_sql_statement(self.statement):
            return self.client.sql_local_source_smoke(
                self.statement,
                output_path=target_uri,
                output_format=normalized_output_format,
                allow_overwrite=allow_overwrite,
                check=check,
            )
        return self._unsupported_operation("sql", self.statement, check=check)

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def _unsupported_operation(
        self,
        operation: str,
        target_ref: str | None = None,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        workflow = LazyFrame(
            source=WorkflowSource("sql", "statement"),
            client=self.client,
            operations=(WorkflowOperation("sql", (self.statement,)),),
        )
        envelope = self.client.workflow_unsupported_plan(
            operation,
            self.operation_summary,
            target_ref,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=workflow,
            operation=operation,
            envelope=envelope,
        )


@dataclass(frozen=True, slots=True)
class UnsupportedWorkflowOperationReport:
    """Report-only unsupported diagnostic for a single workflow affordance."""

    workflow: "LazyFrame"
    operation: str
    envelope: OutputEnvelope

    @property
    def blocker_id(self) -> str | None:
        """Return the stable blocker ID for this unsupported workflow method."""

        return self.envelope.field("blocker_id")

    @property
    def required_evidence(self) -> tuple[str, ...]:
        """Return evidence required before the operation can be certified."""

        value = self.envelope.field("required_evidence", "") or ""
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def suggested_next_action(self) -> str | None:
        """Return the deterministic next action surfaced by the CLI."""

        return self.envelope.field("suggested_next_action")

    @property
    def fallback_attempted(self) -> bool:
        """Whether the unsupported-report path attempted fallback execution."""

        return (
            self.envelope.fallback.attempted
            or self.envelope.field_bool("fallback_attempted", False) is True
        )

    @property
    def runtime_execution(self) -> bool:
        """Whether runtime execution occurred while building this report."""

        return self.envelope.field_bool("runtime_execution", False) is True

    @property
    def data_read(self) -> bool:
        """Whether data was read while building this report."""

        return self.envelope.field_bool("data_read", False) is True

    @property
    def write_io(self) -> bool:
        """Whether write I/O occurred while building this report."""

        return self.envelope.field_bool("write_io", False) is True

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return the compact evidence summary for this unsupported diagnostic."""

        return self.envelope.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return the compact claim summary for this unsupported diagnostic."""

        return self.envelope.claim_summary


@dataclass(frozen=True, slots=True)
class LazyFrame:
    """A lazy ShardLoom workflow plan.

    The object records the requested source and transformations only. It does
    not read data, infer schema, probe object stores, materialize output, or
    invoke external engines. Explicit inspection methods lower the declaration
    to existing ShardLoom CLI JSON report surfaces.
    """

    source: WorkflowSource
    client: ShardLoomClient
    operations: tuple[WorkflowOperation, ...] = ()
    engine_mode: str = "auto"

    @property
    def source_format(self) -> str:
        """Return the declared input source format."""

        return self.source.source_format

    @property
    def uri(self) -> str:
        """Return the declared input URI/path."""

        return self.source.uri

    @property
    def operation_summary(self) -> str:
        """Return a deterministic logical-plan summary for report surfaces."""

        parts = [self.source.to_summary()]
        parts.extend(operation.to_summary() for operation in self.operations)
        return " -> ".join(parts)

    def with_engine(self, engine_mode: str) -> "LazyFrame":
        """Return this lazy workflow with a different requested engine mode."""

        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=self.operations,
            engine_mode=_normalize_engine_mode(engine_mode),
        )

    def filter(self, predicate: object) -> "LazyFrame":
        """Return a lazy plan with an added filter predicate."""

        value = str(predicate).strip()
        if not value:
            raise ValueError("filter predicate must not be empty")
        return self._append(WorkflowOperation("filter", (value,)))

    def where(self, predicate: object) -> "LazyFrame":
        """Alias for `filter(...)` using familiar SQL/DataFrame naming."""

        return self.filter(predicate)

    def select(self, *columns: object) -> "LazyFrame":
        """Return a lazy plan with an added projection."""

        return self._append(WorkflowOperation("select", _normalize_columns(columns)))

    def with_column(
        self,
        name: str,
        expression: object,
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped literal-column workflow when admitted."""

        column_name = _normalize_output_column_name(name)
        try:
            literal = _generated_literal_expression(expression)
        except (TypeError, ValueError):
            expression_text = _require_non_empty("column expression", expression)
            return self._unsupported_operation(
                "with-column",
                f"{column_name}={expression_text}",
                check=check,
            )
        if self._can_append_literal_column(column_name):
            return self._append(
                WorkflowOperation("with_column", (column_name, _sql_literal(literal)))
            )
        return self._unsupported_operation(
            "with-column",
            f"{column_name}={_sql_literal(literal)}",
            check=check,
        )

    def limit(self, count: int) -> "LazyFrame":
        """Return a lazy plan with an added limit."""

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("limit count must be an integer")
        if count < 0:
            raise ValueError("limit count must be non-negative")
        return self._append(WorkflowOperation("limit", (str(count),)))

    def plan(self, *, check: bool = False) -> OutputEnvelope:
        """Return a side-effect-free input/read planning envelope."""

        if self.source.source_format == "vortex":
            return self.client.vortex_read_plan(self.source.uri, check=check)
        return self.client.input_plan(
            self.source.uri,
            source_format=self.source.source_format,
            check=check,
        )

    def explain(self, *, check: bool = False) -> OutputEnvelope:
        """Return the CLI explain envelope for this logical workflow summary."""

        return self.client.explain(self.operation_summary, check=check)

    def estimate(self, *, check: bool = False) -> OutputEnvelope:
        """Return the CLI estimate envelope for this logical workflow summary."""

        return self.client.estimate(self.operation_summary, check=check)

    def profile(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for runtime profile collection."""

        return self._unsupported_operation("profile", check=check)

    def collect(
        self,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Collect rows for admitted local CSV/flat JSONL SQL smoke shapes."""

        if statement := self._sql_local_source_statement():
            return self.client.sql_local_source_smoke(statement, check=check)
        return self._unsupported_operation("collect", check=check)

    def count(
        self,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a scoped row-count report for admitted local workflows."""

        if self._can_append_scalar_aggregate():
            return (
                self._append(WorkflowOperation("aggregate", ("count(*)",)))
                .limit(1)
                .collect(check=check)
            )
        return self._unsupported_operation("count", check=check)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport:
        """Write an admitted local CSV/flat JSONL SQL smoke result to a local sink."""

        normalized_output_format = _normalize_local_output_format(output_format)
        statement = self._sql_local_source_statement()
        if statement is None:
            raise ValueError(
                "LazyFrame.write currently requires a local CSV or flat JSONL/NDJSON source with "
                "select(...), optional filter(...), and limit(...) operations, "
                "aggregate(...), optional filter(...), and limit(...) operations, or "
                "optional filter(...), group_by(...).agg(...), and limit(...) operations, or "
                "select(...), optional filter(...), sort(...), and limit(...) operations"
            )
        return self.client.sql_local_source_smoke(
            statement,
            output_path=target_uri,
            output_format=normalized_output_format,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def to_pandas(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for pandas materialization."""

        return self._unsupported_operation("to-pandas", check=check)

    def to_arrow(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow materialization."""

        return self._unsupported_operation("to-arrow", check=check)

    def to_arrow_table(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow table materialization."""

        return self._unsupported_operation("to-arrow-table", check=check)

    def to_arrow_ipc(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Arrow IPC materialization."""

        return self._unsupported_operation("to-arrow-ipc", check=check)

    def to_numpy(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for NumPy materialization."""

        return self._unsupported_operation("to-numpy", check=check)

    def to_python_objects(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Python-object materialization."""

        return self._unsupported_operation("to-python-objects", check=check)

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for native Vortex workflow writes."""

        return self._unsupported_operation("write-vortex", str(target_uri), check=check)

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for Parquet compatibility exports."""

        return self._unsupported_operation("write-parquet", str(target_uri), check=check)

    def sql(
        self,
        statement: str,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for SQL workflow execution."""

        target = _require_non_empty("sql statement", statement)
        return self._unsupported_operation("sql", target, check=check)

    def join(
        self,
        other: "LazyFrame | str",
        *,
        on: str | Sequence[str],
        how: str = "inner",
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped local CSV inner equi-join workflow when admitted."""

        columns = ",".join(_normalize_columns((on,)))
        normalized_columns = tuple(column for column in columns.split(",") if column)
        normalized_how = how.strip().lower()
        right_uri: str
        right_summary: str
        right_operations: tuple[WorkflowOperation, ...] = ()
        right_format = "csv"
        if isinstance(other, LazyFrame):
            right_format = other.source.source_format
            right_uri = other.source.uri
            right_summary = other.operation_summary
            right_operations = other.operations
        else:
            right_uri = _require_non_empty("join right source", other)
            right_summary = right_uri
        target = f"{normalized_how}:{columns}:{right_summary}"
        if (
            self.source.source_format == "csv"
            and right_format == "csv"
            and not right_operations
            and normalized_how in {"inner", "inner_equi", "inner-equi"}
            and len(normalized_columns) == 1
            and _is_local_csv_source_ref(right_uri)
        ):
            key = normalized_columns[0]
            return self._append(
                WorkflowOperation(
                    "join",
                    (right_uri, key, key, "inner", "f", "d"),
                )
            )
        return self._unsupported_operation("join", target, check=check)

    def group_by(self, *columns: object) -> "GroupedLazyFrame":
        """Return a grouped lazy workflow handle for scoped aggregation."""

        return GroupedLazyFrame(
            workflow=self,
            columns=_normalize_columns(columns),
        )

    def aggregate(
        self,
        *expressions: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scalar aggregate workflow when admitted, otherwise report unsupported."""

        values = _normalize_columns(expressions)
        target = ",".join(values)
        if self._can_append_scalar_aggregate():
            return self._append(WorkflowOperation("aggregate", values))
        return self._unsupported_operation("aggregate", target, check=check)

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scalar aggregate workflow for positional expressions when admitted."""

        values = list(_normalize_columns(expressions)) if expressions else []
        values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        if not named_expressions and self._can_append_scalar_aggregate():
            return self._append(WorkflowOperation("aggregate", tuple(values)))
        return self._unsupported_operation("agg", ",".join(values), check=check)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped sort workflow when admitted, otherwise report unsupported."""

        normalized_columns = _normalize_columns(columns)
        direction = "desc" if descending else "asc"
        target = f"{direction}:{','.join(normalized_columns)}"
        if self._can_append_sort(normalized_columns):
            return self._append(WorkflowOperation("sort", (direction, normalized_columns[0])))
        return self._unsupported_operation("sort", target, check=check)

    def window(
        self,
        *expressions: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for DataFrame window functions."""

        return self._unsupported_operation(
            "window",
            ",".join(_normalize_columns(expressions)),
            check=check,
        )

    def schema_contract(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for schema contract enforcement."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema contract must not be empty")
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("schema-contract", target, check=check)

    def schema(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for workflow schema discovery."""

        return self._unsupported_operation("schema", check=check)

    def describe_schema(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for rich schema description."""

        return self._unsupported_operation("describe-schema", check=check)

    def validate_schema(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for workflow schema validation."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema validation contract must not be empty")
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("validate-schema", target, check=check)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality checks."""

        return self._unsupported_operation(
            "data-quality",
            ",".join(_normalize_columns(checks)),
            check=check,
        )

    def data_quality(
        self,
        *checks: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Alias for data-quality check unsupported reporting."""

        return self.data_quality_check(*checks, check=check)

    def data_quality_summary(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality summary output."""

        return self._unsupported_operation("data-quality-summary", check=check)

    def quarantine(
        self,
        target_uri: str | os.PathLike[str] | None = None,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for data-quality quarantine output."""

        target = "none" if target_uri is None else str(target_uri)
        return self._unsupported_operation("quarantine", target, check=check)

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded local preview when admitted, otherwise report unsupported."""

        _validate_positive_row_count("preview limit", limit)
        if _is_query_builder_local_source(self.source):
            return self.limit(limit).collect(check=check)
        return self._unsupported_operation("preview", str(limit), check=check)

    def head(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report using familiar DataFrame naming."""

        _validate_positive_row_count("head limit", limit)
        if _is_query_builder_local_source(self.source):
            return self.limit(limit).collect(check=check)
        return self._unsupported_operation("head", str(limit), check=check)

    def take(
        self,
        count: int,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report for the requested row count."""

        _validate_positive_row_count("take count", count)
        if _is_query_builder_local_source(self.source):
            return self.limit(count).collect(check=check)
        return self._unsupported_operation("take", str(count), check=check)

    def display(self, *, check: bool = False) -> UnsupportedWorkflowOperationReport:
        """Return the unsupported report for rich notebook display."""

        return self._unsupported_operation("display", check=check)

    def certify(self, *, check: bool = False) -> WorkflowCertificationReport:
        """Return report-only certificate surfaces for this workflow."""

        return WorkflowCertificationReport(
            workflow=self,
            execution_certificate_plan=self.client.execution_certificate_plan(check=check),
            native_io_envelope_plan=self.client.native_io_envelope_plan(check=check),
            certification_capabilities=self.client.capabilities("certification", check=check),
        )

    def engine_selection(
        self,
        *,
        boundedness: str = "snapshot",
        update_mode: str = "snapshot",
        output_mode: str = "snapshot",
        check: bool = False,
    ) -> EngineSelectionPlan:
        """Return engine selection/rejection for this lazy workflow."""

        return self.client.engine_selection_plan(
            self.engine_mode,
            boundedness=boundedness,
            update_mode=update_mode,
            output_mode=output_mode,
            check=check,
        )

    def unsupported_report(self, *, check: bool = False) -> UnsupportedWorkflowReport:
        """Collect unsupported diagnostics and no-fallback evidence for the workflow."""

        return UnsupportedWorkflowReport(
            workflow=self,
            input_plan=self.plan(check=check),
            explain=self.explain(check=check),
            estimate=self.estimate(check=check),
            certification=self.certify(check=check),
        )

    def _append(self, operation: WorkflowOperation) -> "LazyFrame":
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=(*self.operations, operation),
            engine_mode=self.engine_mode,
        )

    def _unsupported_operation(
        self,
        operation: str,
        target_ref: str | None = None,
        *,
        check: bool,
    ) -> UnsupportedWorkflowOperationReport:
        envelope = self.client.workflow_unsupported_plan(
            operation,
            self.operation_summary,
            target_ref,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=self,
            operation=operation,
            envelope=envelope,
        )

    def _can_append_scalar_aggregate(self) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by"}
            for operation in self.operations
        )

    def _can_append_group_by_aggregate(self, columns: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source) or len(columns) != 1:
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by"}
            for operation in self.operations
        )

    def _can_append_sort(self, columns: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source) or len(columns) != 1:
            return False
        return all(
            operation.kind not in {"aggregate", "group_by", "sort"}
            for operation in self.operations
        )

    def _can_append_literal_column(self, column_name: str) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        saw_projection = False
        for operation in self.operations:
            if operation.kind == "select":
                saw_projection = True
                if column_name in operation.values:
                    return False
            elif operation.kind == "filter":
                continue
            elif operation.kind == "with_column":
                if column_name == operation.values[0]:
                    return False
                continue
            else:
                return False
        return saw_projection

    def _append_group_by_aggregate(
        self,
        columns: tuple[str, ...],
        expressions: tuple[str, ...],
    ) -> "LazyFrame":
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=(
                *self.operations,
                WorkflowOperation("group_by", columns),
                WorkflowOperation("aggregate", expressions),
            ),
            engine_mode=self.engine_mode,
        )

    def _sql_local_source_statement(self) -> str | None:
        if not _is_query_builder_local_source(self.source):
            return None
        projection_list: tuple[str, ...] | None = None
        aggregate_list: tuple[str, ...] | None = None
        group_by_list: tuple[str, ...] | None = None
        literal_columns: list[tuple[str, str]] = []
        join_info: tuple[str, str, str, str, str, str] | None = None
        sort_key: tuple[str, str] | None = None
        predicate: str | None = None
        limit: str | None = None
        for operation in self.operations:
            if operation.kind == "select" and projection_list is None:
                projection_list = operation.values
            elif operation.kind == "aggregate" and aggregate_list is None:
                aggregate_list = operation.values
            elif operation.kind == "group_by" and group_by_list is None:
                group_by_list = operation.values
            elif operation.kind == "with_column":
                literal_columns.append((operation.values[0], operation.values[1]))
            elif operation.kind == "sort" and sort_key is None:
                sort_key = (operation.values[0], operation.values[1])
            elif operation.kind == "join" and join_info is None:
                join_info = operation.values  # type: ignore[assignment]
            elif operation.kind == "filter" and predicate is None:
                predicate = operation.values[0]
            elif operation.kind == "limit" and limit is None:
                limit = operation.values[0]
            else:
                return None
        if limit is None:
            return None
        if join_info is not None:
            if (
                projection_list is None
                or aggregate_list is not None
                or group_by_list is not None
                or literal_columns
                or sort_key is not None
            ):
                return None
            right_uri, left_key, right_key, _how, left_alias, right_alias = join_info
            select_clause = ",".join(projection_list)
            source_uri = _quote_sql_local_source_path(self.source.uri)
            right_source_uri = _quote_sql_local_source_path(right_uri)
            return (
                f"SELECT {select_clause} FROM {source_uri} AS {left_alias} "
                f"INNER JOIN {right_source_uri} AS {right_alias} "
                f"ON {left_alias}.{left_key} = {right_alias}.{right_key}"
                f"{_optional_sql_where_clause(predicate)} LIMIT {limit}"
            )
        if projection_list is not None:
            if aggregate_list is not None or group_by_list is not None:
                return None
            select_values = list(projection_list)
            select_values.extend(
                f"{literal} AS {column}" for column, literal in literal_columns
            )
            select_clause = ",".join(select_values)
            group_by_clause = ""
        elif aggregate_list is not None:
            if literal_columns:
                return None
            if group_by_list is not None:
                select_clause = ",".join((*group_by_list, *aggregate_list))
                group_by_clause = f" GROUP BY {','.join(group_by_list)}"
            else:
                select_clause = ",".join(aggregate_list)
                group_by_clause = ""
        else:
            if literal_columns:
                return None
            select_clause = "*"
            group_by_clause = ""
        if sort_key is not None and (aggregate_list is not None or group_by_list is not None):
            return None
        order_by_clause = ""
        if sort_key is not None:
            direction, column = sort_key
            order_by_clause = f" ORDER BY {column} {direction.upper()}"
        source_uri = _quote_sql_local_source_path(self.source.uri)
        return (
            f"SELECT {select_clause} FROM {source_uri}"
            f"{_optional_sql_where_clause(predicate)}{group_by_clause}{order_by_clause} LIMIT {limit}"
        )


@dataclass(frozen=True, slots=True)
class GroupedLazyFrame:
    """Grouped lazy workflow handle for scoped aggregation and blockers."""

    workflow: LazyFrame
    columns: tuple[str, ...]

    @property
    def operation_summary(self) -> str:
        """Return the grouped workflow summary."""

        return f"{self.workflow.operation_summary} -> group_by({','.join(self.columns)})"

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped grouped aggregate workflow when admitted."""

        values = list(_normalize_columns(expressions)) if expressions else []
        values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        target = f"group_by:{','.join(self.columns)};agg:{','.join(values)}"
        if not named_expressions and self.workflow._can_append_group_by_aggregate(
            self.columns
        ):
            return self.workflow._append_group_by_aggregate(self.columns, tuple(values))
        envelope = self.workflow.client.workflow_unsupported_plan(
            "agg",
            self.operation_summary,
            target,
            check=check,
        )
        return UnsupportedWorkflowOperationReport(
            workflow=self.workflow,
            operation="agg",
            envelope=envelope,
        )

    def aggregate(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for grouped `agg`."""

        return self.agg(*expressions, check=check, **named_expressions)


def read_vortex(
    uri: str | os.PathLike[str],
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy native Vortex source."""

    return _read_source(
        "vortex",
        uri,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def col(name: object) -> ColumnExpression:
    """Return a scoped column expression for local ShardLoom predicates."""

    return ColumnExpression(_normalize_expression_column(name))


def column(name: object) -> ColumnExpression:
    """Alias for `col(...)`."""

    return col(name)


def read_csv(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy CSV compatibility source."""

    return _read_source(
        "csv",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_json(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy JSON/NDJSON compatibility source."""

    return _read_source(
        "json",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_parquet(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy Parquet compatibility source."""

    return _read_source(
        "parquet",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient | None = None,
    source_kind: str = "user_rows",
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped source-free generated row set for local output smoke writes."""

    return _generated_rows_source(
        rows,
        client=_client_from_config(client, client_config),
        source_kind=source_kind,
    )


def literal_table(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped source-free literal table for local output smoke writes."""

    return from_rows(
        rows,
        client=client,
        source_kind="literal_table",
        **client_config,
    )


def range(
    start: int,
    end: int,
    *,
    step: int = 1,
    column: str = "value",
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRangeSource:
    """Create a scoped source-free ShardLoom-native range for local output smoke writes."""

    normalized_start = _require_range_int("start", start)
    normalized_end = _require_range_int("end", end)
    normalized_step = _require_range_int("step", step)
    if normalized_step == 0:
        raise ValueError("range step must not be zero")
    normalized_column = _require_non_empty("range column", column)
    return GeneratedRangeSource(
        start=normalized_start,
        end=normalized_end,
        step=normalized_step,
        column=normalized_column,
        client=_client_from_config(client, client_config),
    )


def sequence(
    start: int,
    end: int,
    *,
    step: int = 1,
    column: str = "value",
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRangeSource:
    """Create a scoped source-free ShardLoom-native sequence for local output smoke writes."""

    normalized_start = _require_range_int("start", start)
    normalized_end = _require_range_int("end", end)
    normalized_step = _require_range_int("step", step)
    if normalized_step == 0:
        raise ValueError("sequence step must not be zero")
    normalized_column = _require_non_empty("sequence column", column)
    return GeneratedRangeSource(
        start=normalized_start,
        end=normalized_end,
        step=normalized_step,
        column=normalized_column,
        client=_client_from_config(client, client_config),
        source_kind="sequence",
    )


def sql_values(
    values_clause: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedSqlSource:
    """Create a scoped source-free SQL VALUES generated source for local output smokes."""

    statement = _require_non_empty("SQL VALUES clause", values_clause)
    return GeneratedSqlSource(
        statement=statement,
        client=_client_from_config(client, client_config),
    )


def sql_literal_select(
    expression: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedSqlSource:
    """Create a scoped source-free SQL literal SELECT generated source for local output smokes."""

    statement = _require_non_empty("SQL literal SELECT expression", expression)
    return GeneratedSqlSource(
        statement=statement,
        client=_client_from_config(client, client_config),
    )


def sql(
    statement: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> SqlWorkflow:
    """Create a scoped SQL workflow over currently admitted ShardLoom SQL paths."""

    return SqlWorkflow(
        statement=_require_non_empty("SQL statement", statement),
        client=_client_from_config(client, client_config),
    )


def calendar(
    start: str | date,
    end: str | date,
    *,
    column: str = "date",
    include_parts: bool = True,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped source-free calendar/date dimension for local output.

    Dates are generated in Python with an inclusive `start` and exclusive `end`,
    mirroring `range(start, end)`. The write path still goes through ShardLoom's
    generated-source local-output command and emits no source Native I/O
    certificate because no input dataset is read.
    """

    start_date = _normalize_date("calendar start", start)
    end_date = _normalize_date("calendar end", end)
    if start_date >= end_date:
        raise ValueError("calendar start must be before end")
    column_name = _require_non_empty("calendar column", column)
    rows = []
    current = start_date
    while current < end_date:
        row: dict[str, object] = {column_name: current.isoformat()}
        if include_parts:
            row.update(
                {
                    "year": current.year,
                    "month": current.month,
                    "day": current.day,
                    "day_of_week": current.isoweekday(),
                }
            )
        rows.append(row)
        current += timedelta(days=1)
    return from_rows(
        rows,
        client=client,
        source_kind="calendar",
        **client_config,
    )


def from_pandas(
    dataframe: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for a pandas in-memory input boundary."""

    workflow = _materialized_boundary_workflow(
        "pandas",
        _python_object_boundary_ref("pandas", dataframe),
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)


def from_arrow_table(
    table: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for an Arrow table input boundary."""

    workflow = _materialized_boundary_workflow(
        "arrow_table",
        _python_object_boundary_ref("arrow_table", table),
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)


def from_arrow_ipc(
    source: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> UnsupportedWorkflowOperationReport:
    """Return the unsupported report for an Arrow IPC input boundary."""

    target = (
        str(source)
        if isinstance(source, (str, os.PathLike))
        else _python_object_boundary_ref("arrow_ipc", source)
    )
    workflow = _materialized_boundary_workflow(
        "arrow_ipc",
        target,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )
    return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)


def _generated_rows_args(
    rows: Sequence[Mapping[str, object]],
) -> tuple[str, str, tuple[tuple[tuple[str, object], ...], ...]]:
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise TypeError("rows must be a non-empty sequence of mappings")
    if not rows:
        raise ValueError("rows must not be empty")
    first = rows[0]
    if not isinstance(first, Mapping):
        raise TypeError("rows must contain mappings")
    if any(not isinstance(key, str) for key in first.keys()):
        raise TypeError("generated row column names must be strings")
    columns = tuple(first.keys())
    if not columns or any(column.strip() == "" for column in columns):
        raise ValueError("row column names must not be empty")
    if len(set(columns)) != len(columns):
        raise ValueError("row column names must be unique")
    value_types = tuple(_generated_value_type(first[column]) for column in columns)
    row_tokens: list[str] = []
    normalized_rows: list[tuple[tuple[str, object], ...]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise TypeError(f"row {index} is not a mapping")
        if any(not isinstance(key, str) for key in row.keys()):
            raise TypeError("generated row column names must be strings")
        row_keys = tuple(row.keys())
        if row_keys != columns:
            raise ValueError(
                "all generated rows must have the same columns in the same order"
            )
        parts = []
        normalized_row = []
        for column, value_type in zip(columns, value_types):
            value = row[column]
            parts.append(
                f"{_generated_token(column)}={_generated_token(_generated_value(value_type, value))}"
            )
            normalized_row.append((column, value))
        row_tokens.append(",".join(parts))
        normalized_rows.append(tuple(normalized_row))
    schema_arg = ",".join(
        f"{_generated_token(column)}:{value_type}"
        for column, value_type in zip(columns, value_types)
    )
    return schema_arg, ";".join(row_tokens), tuple(normalized_rows)


def _generated_rows_source(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient,
    source_kind: str,
) -> GeneratedRowsSource:
    schema_arg, rows_arg, normalized_rows = _generated_rows_args(rows)
    return GeneratedRowsSource(
        schema_arg=schema_arg,
        rows_arg=rows_arg,
        client=client,
        source_kind=_normalize_generated_source_kind(source_kind),
        rows=normalized_rows,
    )


def _generated_value_type(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("float generated row values must be finite")
        return "float64"
    if isinstance(value, str):
        return "utf8"
    raise TypeError(
        "generated row values must be bool, int, float, or str for the scoped local smoke"
    )


def _generated_value(value_type: str, value: object) -> str:
    if value_type == "bool":
        if not isinstance(value, bool):
            raise TypeError("generated bool columns must contain only bool values")
        return "true" if value else "false"
    if value_type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("generated int64 columns must contain only int values")
        return str(value)
    if value_type == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("generated float64 columns must contain only numeric values")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("float generated row values must be finite")
        return str(numeric)
    if value_type == "utf8":
        if not isinstance(value, str):
            raise TypeError("generated utf8 columns must contain only str values")
        return value
    raise ValueError(f"unsupported generated value type {value_type!r}")


def _normalize_generated_source_kind(value: str) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in {"user_rows", "literal_table", "calendar"}:
        raise ValueError(
            "generated source kind must be one of ('user_rows', 'literal_table', 'calendar')"
        )
    return normalized


def _normalize_generated_select_columns(columns: tuple[object, ...]) -> tuple[str, ...]:
    if len(columns) == 1 and isinstance(columns[0], Sequence) and not isinstance(
        columns[0],
        (str, bytes, bytearray),
    ):
        values = tuple(columns[0])
    else:
        values = columns
    if not values:
        raise ValueError("generated row projection must include at least one column")
    normalized = tuple(
        _require_non_empty("generated projection column", value) for value in values
    )
    if len(set(normalized)) != len(normalized):
        raise ValueError("generated row projection columns must be unique")
    return normalized


def _generated_literal_expression(expression: object) -> object:
    if isinstance(expression, str):
        text = expression.strip()
        if not text:
            raise ValueError("literal with_column expression must not be empty")
        if not (text.startswith("lit(") and text.endswith(")")):
            raise ValueError(
                "literal with_column currently supports only lit(...) expressions "
                "or direct Python bool/int/float literals"
            )
        inner = text[4:-1].strip()
        if not inner:
            raise ValueError("lit(...) expression must include a value")
        lowered = inner.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        if lowered in {"null", "none"}:
            raise ValueError("literal with_column does not support null literals yet")
        try:
            parsed = ast.literal_eval(inner)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(
                "lit(...) expression must contain a bool, int, float, or quoted string"
            ) from exc
        _generated_value_type(parsed)
        return parsed
    _generated_value_type(expression)
    return expression


def _normalize_date(name: str, value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a date or ISO date string")
    text = value.strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO date string like YYYY-MM-DD") from exc


def _require_range_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"range {name} must be an integer")
    return value


def _validate_positive_row_count(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _generated_token(value: str) -> str:
    return quote(value, safe="")


def _read_source(
    source_format: str,
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    normalized = source_format.strip().lower().replace("_", "-")
    if normalized not in SUPPORTED_SOURCE_FORMATS:
        raise ValueError(
            f"source_format must be one of {SUPPORTED_SOURCE_FORMATS}; got {source_format!r}"
        )
    return LazyFrame(
        source=WorkflowSource(
            source_format=normalized,
            uri=str(uri),
            schema=_normalize_schema(schema),
        ),
        client=_client_from_config(client, client_config),
        engine_mode=_normalize_engine_mode(engine_mode),
    )


def _materialized_boundary_workflow(
    source_format: str,
    uri: str,
    *,
    client: ShardLoomClient | None,
    engine_mode: str,
    **client_config: object,
) -> LazyFrame:
    return LazyFrame(
        source=WorkflowSource(
            source_format=source_format,
            uri=uri,
        ),
        client=_client_from_config(client, client_config),
        engine_mode=_normalize_engine_mode(engine_mode),
    )


def _python_object_boundary_ref(kind: str, value: object) -> str:
    value_type = type(value)
    return f"{kind}:{value_type.__module__}.{value_type.__qualname__}"


def _client_from_config(
    client: ShardLoomClient | None,
    client_config: Mapping[str, object],
) -> ShardLoomClient:
    if client is not None:
        if client_config:
            raise ValueError("client cannot be combined with client configuration arguments")
        return client
    config = dict(client_config)
    binary = config.pop("binary", None)
    env = config.pop("env", None)
    cwd = config.pop("cwd", None)
    repo_root = config.pop("repo_root", None)
    profile_order = config.pop("profile_order", None)
    timeout = config.pop("timeout", None)
    if config:
        unknown = ", ".join(sorted(str(key) for key in config))
        raise TypeError(f"unknown client configuration argument(s): {unknown}")
    if repo_root is not None:
        return ShardLoomClient.from_repo(
            repo_root,
            binary=_optional_binary(binary),
            env=_optional_env(env),
            cwd=_optional_path(cwd),
            profile_order=_optional_profile_order(profile_order) or DEFAULT_PROFILE_ORDER,
            timeout=_optional_timeout(timeout),
        )
    return ShardLoomClient.from_env(
        env=_optional_env(env),
        binary=_optional_binary(binary),
        cwd=_optional_path(cwd),
        profile_order=_optional_profile_order(profile_order),
        timeout=_optional_timeout(timeout),
    )


def _normalize_schema(schema: Mapping[str, object] | None) -> tuple[tuple[str, str], ...]:
    if schema is None:
        return ()
    return tuple((str(key), str(value)) for key, value in schema.items())


def _normalize_engine_mode(engine_mode: str) -> str:
    normalized = engine_mode.strip().lower().replace("_", "-")
    if normalized not in {"auto", "batch", "live", "hybrid"}:
        raise ValueError("engine_mode must be one of ('auto', 'batch', 'live', 'hybrid')")
    return normalized


def _normalize_columns(columns: Sequence[object]) -> tuple[str, ...]:
    if len(columns) == 1 and _is_non_string_sequence(columns[0]):
        values = [str(column).strip() for column in columns[0]]
    else:
        values = [str(column).strip() for column in columns]
    values = [value for value in values if value]
    if not values:
        raise ValueError("select columns must not be empty")
    return tuple(values)


def _require_non_empty(name: str, value: object) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    return text


def _normalize_expression_column(value: object) -> str:
    column = _require_non_empty("column expression", value)
    parts = column.split(".")
    if len(parts) > 2 or not all(_is_sql_identifier(part) for part in parts):
        raise ValueError(
            "column expressions admit only bare column names or alias.column references"
        )
    return column


def _normalize_output_column_name(value: object) -> str:
    column = _require_non_empty("output column name", value)
    if not _is_sql_identifier(column):
        raise ValueError("output column names admit only bare SQL identifiers")
    return column


def _normalize_cast_dtype(value: object) -> str:
    dtype = _require_non_empty("cast dtype", value).lower()
    if dtype not in {"int64", "float64", "utf8", "boolean", "date32"}:
        raise ValueError(
            "cast dtype must be one of ('int64', 'float64', 'utf8', 'boolean', 'date32')"
        )
    return dtype


def _sql_string_literal(value: object) -> str:
    text = _require_non_empty("string literal", value)
    return "'" + text.replace("'", "''") + "'"


def _sql_literal(value: object) -> str:
    if value is None:
        raise ValueError("SQL NULL comparisons must use is_null() or is_not_null()")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("SQL float literals must be finite")
        return str(value)
    if isinstance(value, datetime):
        return f"DATE '{value.date().isoformat()}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return _sql_string_literal(value)
    raise TypeError(
        "SQL predicate literals must be bool, int, float, str, date, datetime, or None"
    )


def _predicate_sql(value: object) -> str:
    if isinstance(value, PredicateExpression):
        return value.sql
    text = str(value).strip()
    if not text:
        raise ValueError("predicate expression must not be empty")
    return text


def _like_needle(name: str, value: object) -> str:
    text = _require_non_empty(name, value)
    if "%" in text or "_" in text:
        raise ValueError(f"{name} must not contain SQL LIKE wildcard characters")
    return text


def _normalize_in_values(values: tuple[object, ...]) -> tuple[object, ...]:
    if len(values) == 1 and _is_non_string_sequence(values[0]):
        normalized = tuple(values[0])
    else:
        normalized = values
    if not normalized:
        raise ValueError("IN predicates require at least one value")
    if any(value is None for value in normalized):
        raise ValueError("IN predicates do not admit NULL values; use is_null()")
    if len(normalized) > 32:
        raise ValueError("IN predicates admit at most 32 values")
    return normalized


def _is_source_free_sql_statement(statement: str) -> bool:
    normalized = statement.strip().rstrip(";").strip()
    if _starts_with_sql_keyword(normalized, "values"):
        return True
    if _is_source_free_sql_generator_statement(normalized):
        return True
    return _starts_with_sql_keyword(normalized, "select") and not _contains_sql_keyword_outside_quotes(
        normalized,
        "from",
    )


def _is_source_free_sql_generator_statement(statement: str) -> bool:
    if not _starts_with_sql_keyword(statement, "select"):
        return False
    select_body = statement[len("select") :].strip()
    if not select_body.startswith("*"):
        return False
    after_star = select_body[1:].strip()
    if not _starts_with_sql_keyword(after_star, "from"):
        return False
    source_ref = after_star[len("from") :].strip().lower()
    return (
        source_ref.startswith("generate_series(")
        or source_ref.startswith("generate_series (")
        or source_ref.startswith("range(")
        or source_ref.startswith("range (")
    ) and source_ref.endswith(")")


def _is_local_source_sql_statement(statement: str) -> bool:
    normalized = statement.strip()
    return (
        _starts_with_sql_keyword(normalized, "select")
        and _contains_sql_keyword_outside_quotes(normalized, "from")
        and any(_is_local_source_sql_ref(value) for value in _single_quoted_sql_strings(normalized))
    )


def _is_local_source_sql_ref(value: str) -> bool:
    lower = value.strip().lower()
    if "://" in lower or lower.startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return False
    return lower.endswith((".csv", ".jsonl", ".ndjson"))


def _is_local_csv_source_ref(value: str) -> bool:
    lower = value.strip().lower()
    return _is_local_source_sql_ref(value) and lower.endswith(".csv")


def _is_local_jsonl_source_ref(value: str) -> bool:
    lower = value.strip().lower()
    return _is_local_source_sql_ref(value) and lower.endswith((".jsonl", ".ndjson"))


def _is_query_builder_local_source(source: WorkflowSource) -> bool:
    if source.source_format == "csv":
        return _is_local_csv_source_ref(source.uri)
    if source.source_format == "json":
        return _is_local_jsonl_source_ref(source.uri)
    return False


def _single_quoted_sql_strings(statement: str) -> tuple[str, ...]:
    values: list[str] = []
    in_quote = False
    current: list[str] = []
    index = 0
    while index < len(statement):
        char = statement[index]
        if char != "'":
            if in_quote:
                current.append(char)
            index += 1
            continue
        if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
            current.append("'")
            index += 2
            continue
        if in_quote:
            values.append("".join(current))
            current = []
            in_quote = False
        else:
            current = []
            in_quote = True
        index += 1
    return tuple(values)


def _starts_with_sql_keyword(statement: str, keyword: str) -> bool:
    lower = statement.lower()
    needle = keyword.lower()
    if not lower.startswith(needle):
        return False
    if len(statement) == len(needle):
        return True
    return not _is_identifier_char(statement[len(needle)])


def _contains_sql_keyword_outside_quotes(statement: str, keyword: str) -> bool:
    lower = statement.lower()
    needle = keyword.lower()
    in_quote = False
    index = 0
    while index <= len(statement) - len(needle):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and lower.startswith(needle, index):
            before = statement[index - 1] if index > 0 else ""
            after_index = index + len(needle)
            after = statement[after_index] if after_index < len(statement) else ""
            if not _is_identifier_char(before) and not _is_identifier_char(after):
                return True
        index += 1
    return False


def _is_identifier_char(char: str) -> bool:
    return char.isalnum() or char == "_"


def _is_sql_identifier(value: str) -> bool:
    if not value:
        return False
    first = value[0]
    if not (first == "_" or (first.isascii() and first.isalpha())):
        return False
    return all(ch == "_" or (ch.isascii() and ch.isalnum()) for ch in value[1:])


def _quote_sql_local_source_path(value: str) -> str:
    path = _require_non_empty("SQL local-source path", value)
    if "'" in path:
        raise ValueError(
            "SQL local-source paths containing single quotes are not supported "
            "by the scoped Python query-builder smoke"
        )
    return f"'{path}'"


def _optional_sql_where_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" WHERE {predicate}"


def _normalize_local_output_format(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"jsonl", "json-lines", "ndjson", "inline-jsonl"}:
        return "jsonl"
    if normalized == "csv":
        return "csv"
    raise ValueError("scoped local writes currently support local JSONL or CSV only")


def _is_non_string_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _optional_binary(value: object) -> Binary | None:
    return cast(Binary | None, value)


def _optional_env(value: object) -> Mapping[str, str] | None:
    return cast(Mapping[str, str] | None, value)


def _optional_path(value: object) -> str | os.PathLike[str] | None:
    return cast(str | os.PathLike[str] | None, value)


def _optional_profile_order(value: object) -> Sequence[str] | None:
    return cast(Sequence[str] | None, value)


def _optional_timeout(value: object) -> float | None:
    return cast(float | None, value)
