"""Lazy workflow planning helpers for the ShardLoom Python surface."""

from __future__ import annotations

import ast
import builtins
import hashlib
import html
import importlib
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence, Union, cast
from urllib.parse import quote

from ._compat import dataclass
from .client import (
    Binary,
    CommandPart,
    DEFAULT_PROFILE_ORDER,
    EngineSelectionPlan,
    GeneratedSourceWriteReport,
    PublicWorkflowExecution,
    PublicWorkflowRoute,
    ShardLoomClient,
    SqlLocalSourceSmokeReport,
    VortexIngestSmokeReport,
)
from .models import ClaimSummary, Diagnostic, EvidenceSummary, OutputEnvelope
from .prepared_route import CompatibilityPreparedVortexRoute

SUPPORTED_SOURCE_FORMATS = ("vortex", "csv", "json", "parquet", "arrow-ipc", "avro", "orc")
MAX_DATE_ARITHMETIC_DAYS = 366_000
MAX_TIMESTAMP_ARITHMETIC_SECONDS = MAX_DATE_ARITHMETIC_DAYS * 86_400
_INTERVAL_SECOND_MULTIPLIERS = {
    "DAY": 86_400,
    "HOUR": 3_600,
    "MINUTE": 60,
    "SECOND": 1,
}
_SORT_NULLS_TOKEN_PREFIX = "__sort_nulls__:"


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

        source_method = self.source_format.replace("-", "_")
        return f"read_{source_method}({self.uri})"


@dataclass(frozen=True, slots=True)
class WorkflowOperation:
    """A lazy query-builder operation."""

    kind: str
    values: tuple[str, ...]

    def to_summary(self) -> str:
        """Return a deterministic operation summary."""

        if self.kind == "filter":
            return f"filter({self.values[0]})"
        if self.kind == "having":
            return f"having({self.values[0]})"
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
class WindowExpression:
    """A scoped SQL window expression for ShardLoom local-source smokes."""

    sql: str

    def __str__(self) -> str:
        return self.sql


@dataclass(frozen=True, slots=True)
class IntervalLiteral:
    """A scoped ANSI interval literal for admitted temporal helper functions."""

    value: int
    unit: str

    def __post_init__(self) -> None:
        interval_value = _normalize_interval_integer(self.value)
        unit = _normalize_interval_unit(self.unit)
        multiplier = _INTERVAL_SECOND_MULTIPLIERS[unit]
        if builtins.abs(interval_value * multiplier) > MAX_TIMESTAMP_ARITHMETIC_SECONDS:
            raise ValueError(
                "interval literal admits absolute values within the scoped temporal arithmetic bound"
            )
        object.__setattr__(self, "value", interval_value)
        object.__setattr__(self, "unit", unit)

    @property
    def sql(self) -> str:
        """Return the SQL rendering accepted by ShardLoom temporal helpers."""

        return f"INTERVAL '{self.value}' {self.unit}"

    def __str__(self) -> str:
        return self.sql


@dataclass(frozen=True, slots=True)
class ComplexProjectionExpression:
    """A scoped ARRAY or STRUCT projection expression for local-source rows."""

    sql: str

    def __str__(self) -> str:
        return self.sql


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
        rhs = (
            _parenthesize_numeric_operand(value.sql)
            if isinstance(value, ColumnExpression)
            else _sql_literal(value)
        )
        return PredicateExpression(f"{self.sql} {operator} {rhs}")

    def _numeric_binary(self, operator: str, value: object) -> "ColumnExpression":
        rhs = (
            _parenthesize_numeric_operand(value.sql)
            if isinstance(value, ColumnExpression)
            else _sql_numeric_literal(value)
        )
        return ColumnExpression(
            f"{_parenthesize_numeric_operand(self.sql)} {operator} {rhs}"
        )

    def __add__(self, value: object) -> "ColumnExpression":
        """Return a scoped numeric addition expression for predicates."""

        return self._numeric_binary("+", value)

    def __sub__(self, value: object) -> "ColumnExpression":
        """Return a scoped numeric subtraction expression for predicates."""

        return self._numeric_binary("-", value)

    def __mul__(self, value: object) -> "ColumnExpression":
        """Return a scoped numeric multiplication expression for predicates."""

        return self._numeric_binary("*", value)

    def __truediv__(self, value: object) -> "ColumnExpression":
        """Return a scoped numeric division expression for predicates."""

        return self._numeric_binary("/", value)

    def __abs__(self) -> "ColumnExpression":
        """Return a scoped `ABS(column)` numeric absolute-value expression."""

        return self.abs()

    def abs(self) -> "ColumnExpression":
        """Return a scoped `ABS(column)` numeric absolute-value expression."""

        return ColumnExpression(f"ABS({self.sql})")

    def floor(self) -> "ColumnExpression":
        """Return a scoped `FLOOR(column)` numeric rounding expression."""

        return ColumnExpression(f"FLOOR({self.sql})")

    def ceil(self) -> "ColumnExpression":
        """Return a scoped `CEIL(column)` numeric rounding expression."""

        return ColumnExpression(f"CEIL({self.sql})")

    def round(self) -> "ColumnExpression":
        """Return a scoped `ROUND(column)` numeric rounding expression."""

        return ColumnExpression(f"ROUND({self.sql})")

    def is_null(self) -> PredicateExpression:
        """Return a scoped `IS NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NULL")

    def is_not_null(self) -> PredicateExpression:
        """Return a scoped `IS NOT NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT NULL")

    def is_distinct_from(self, value: object) -> PredicateExpression:
        """Return a scoped SQL `IS DISTINCT FROM` null-safe comparison."""

        return PredicateExpression(
            f"{self.sql} IS DISTINCT FROM {self._null_safe_comparison_rhs(value)}"
        )

    def is_not_distinct_from(self, value: object) -> PredicateExpression:
        """Return a scoped SQL `IS NOT DISTINCT FROM` null-safe comparison."""

        return PredicateExpression(
            f"{self.sql} IS NOT DISTINCT FROM {self._null_safe_comparison_rhs(value)}"
        )

    def _null_safe_comparison_rhs(self, value: object) -> str:
        if value is None:
            return "NULL"
        return (
            _parenthesize_numeric_operand(value.sql)
            if isinstance(value, ColumnExpression)
            else _sql_literal(value)
        )

    def is_true(self) -> PredicateExpression:
        """Return a scoped SQL boolean truth predicate."""

        return PredicateExpression(f"{self.sql} IS TRUE")

    def is_false(self) -> PredicateExpression:
        """Return a scoped SQL boolean false predicate."""

        return PredicateExpression(f"{self.sql} IS FALSE")

    def is_not_true(self) -> PredicateExpression:
        """Return a scoped SQL `IS NOT TRUE` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT TRUE")

    def is_not_false(self) -> PredicateExpression:
        """Return a scoped SQL `IS NOT FALSE` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT FALSE")

    def like(self, pattern: object, *, escape: object | None = None) -> PredicateExpression:
        """Return a scoped SQL LIKE predicate.

        The runtime admits scoped UTF-8 SQL LIKE patterns with `%` and `_`
        wildcards and optional single-character ESCAPE clauses. Locale-aware
        collation and case-folding semantics remain outside this helper's claim
        boundary.
        """

        return PredicateExpression(
            f"{self.sql} LIKE {_sql_string_literal(pattern)}{_like_escape_clause(escape)}"
        )

    def not_like(self, pattern: object, *, escape: object | None = None) -> PredicateExpression:
        """Return a scoped SQL NOT LIKE predicate."""

        return PredicateExpression(
            f"{self.sql} NOT LIKE {_sql_string_literal(pattern)}{_like_escape_clause(escape)}"
        )

    def rlike(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex predicate lowered to SQL `RLIKE`."""

        return PredicateExpression(f"{self.sql} RLIKE {_sql_string_literal(pattern)}")

    def not_rlike(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex negation lowered to SQL `NOT RLIKE`."""

        return PredicateExpression(f"{self.sql} NOT RLIKE {_sql_string_literal(pattern)}")

    def regex(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex predicate."""

        return self.rlike(pattern)

    def not_regex(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex negation."""

        return self.not_rlike(pattern)

    def matches(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex predicate."""

        return self.rlike(pattern)

    def not_matches(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex negation."""

        return self.not_rlike(pattern)

    def contains(self, needle: object) -> PredicateExpression:
        """Return a scoped substring predicate lowered to `LIKE '%needle%'`."""

        value = _like_needle("contains needle", needle)
        return self.like(f"%{value}%")

    def not_contains(self, needle: object) -> PredicateExpression:
        """Return a scoped substring negation lowered to `NOT LIKE '%needle%'`."""

        value = _like_needle("not_contains needle", needle)
        return self.not_like(f"%{value}%")

    def startswith(self, prefix: object) -> PredicateExpression:
        """Return a scoped prefix predicate lowered to `LIKE 'prefix%'`."""

        value = _like_needle("startswith prefix", prefix)
        return self.like(f"{value}%")

    def not_startswith(self, prefix: object) -> PredicateExpression:
        """Return a scoped prefix negation lowered to `NOT LIKE 'prefix%'`."""

        value = _like_needle("not_startswith prefix", prefix)
        return self.not_like(f"{value}%")

    def endswith(self, suffix: object) -> PredicateExpression:
        """Return a scoped suffix predicate lowered to `LIKE '%suffix'`."""

        value = _like_needle("endswith suffix", suffix)
        return self.like(f"%{value}")

    def not_endswith(self, suffix: object) -> PredicateExpression:
        """Return a scoped suffix negation lowered to `NOT LIKE '%suffix'`."""

        value = _like_needle("not_endswith suffix", suffix)
        return self.not_like(f"%{value}")

    def lower(self) -> "ColumnExpression":
        """Return a scoped `LOWER(column)` UTF-8 transform expression."""

        return ColumnExpression(f"LOWER({self.sql})")

    def upper(self) -> "ColumnExpression":
        """Return a scoped `UPPER(column)` UTF-8 transform expression."""

        return ColumnExpression(f"UPPER({self.sql})")

    def trim(self) -> "ColumnExpression":
        """Return a scoped `TRIM(column)` UTF-8 transform expression."""

        return ColumnExpression(f"TRIM({self.sql})")

    def length(self) -> "ColumnExpression":
        """Return a scoped `LENGTH(column)` UTF-8 length expression."""

        return ColumnExpression(f"LENGTH({self.sql})")

    def concat(self, *parts: object) -> "ColumnExpression":
        """Return a scoped `CONCAT(column-or-string-literal, ...)` expression."""

        return concat(self, *parts)

    def substr(self, start: object, length: object) -> "ColumnExpression":
        """Return a scoped 1-based `SUBSTR(column, start, length)` expression."""

        column, _ = _normalize_string_scalar_expression_sql(self.sql)
        normalized_start = _normalize_substring_bound("substring start", start, minimum=1)
        normalized_length = _normalize_substring_bound(
            "substring length", length, minimum=0
        )
        return ColumnExpression(f"SUBSTR({column}, {normalized_start}, {normalized_length})")

    def substring(self, start: object, length: object) -> "ColumnExpression":
        """Alias for `substr(...)`."""

        return self.substr(start, length)

    def left(self, count: object) -> "ColumnExpression":
        """Return a scoped `LEFT(column, count)` UTF-8 expression."""

        column, _ = _normalize_string_scalar_expression_sql(self.sql)
        normalized_count = _normalize_substring_bound("left count", count, minimum=0)
        return ColumnExpression(f"LEFT({column}, {normalized_count})")

    def right(self, count: object) -> "ColumnExpression":
        """Return a scoped `RIGHT(column, count)` UTF-8 expression."""

        column, _ = _normalize_string_scalar_expression_sql(self.sql)
        normalized_count = _normalize_substring_bound("right count", count, minimum=0)
        return ColumnExpression(f"RIGHT({column}, {normalized_count})")

    def replace(self, needle: object, replacement: object) -> "ColumnExpression":
        """Return a scoped `REPLACE(column, needle, replacement)` expression."""

        column, _ = _normalize_string_scalar_expression_sql(self.sql)
        needle_literal = _sql_string_function_literal(
            "replace search literal", needle, allow_empty=False
        )
        replacement_literal = _sql_string_function_literal(
            "replace replacement literal", replacement, allow_empty=True
        )
        return ColumnExpression(
            f"REPLACE({column}, {needle_literal}, {replacement_literal})"
        )

    def unhex(self) -> "ColumnExpression":
        """Return a scoped `UNHEX(<utf8-expression>)` binary helper expression."""

        expression, has_source_column = _normalize_string_scalar_expression_sql(self.sql)
        if not has_source_column:
            raise ValueError("UNHEX expressions require at least one source column")
        return ColumnExpression(f"UNHEX({expression})")

    def from_base64(self) -> "ColumnExpression":
        """Return a scoped `FROM_BASE64(<utf8-expression>)` binary helper expression."""

        expression, has_source_column = _normalize_string_scalar_expression_sql(self.sql)
        if not has_source_column:
            raise ValueError("FROM_BASE64 expressions require at least one source column")
        return ColumnExpression(f"FROM_BASE64({expression})")

    def byte_length(self) -> "ColumnExpression":
        """Return a scoped `BYTE_LENGTH(<binary-expression>)` byte-count expression."""

        expression, has_source_column = _normalize_binary_scalar_expression_sql(self.sql)
        if not has_source_column:
            raise ValueError("BYTE_LENGTH expressions require a source-backed binary expression")
        return ColumnExpression(f"BYTE_LENGTH({expression})")

    def fill_null(self, value: object) -> "ColumnExpression":
        """Return a scoped `COALESCE(column, literal)` null-cleanup expression."""

        return ColumnExpression(f"COALESCE({self.sql}, {_sql_literal(value)})")

    def null_if(self, value: object) -> "ColumnExpression":
        """Return a scoped `NULLIF(column, literal)` null-cleanup expression."""

        return ColumnExpression(f"NULLIF({self.sql}, {_sql_literal(value)})")

    def isin(self, *values: object) -> PredicateExpression:
        """Return a scoped bounded `IN (...)` predicate."""

        normalized = _normalize_in_values(values)
        joined = ",".join(_sql_in_literal(value) for value in normalized)
        return PredicateExpression(f"{self.sql} IN ({joined})")

    def isin_source(
        self,
        source: object,
        column: object,
        *,
        source_alias: object | None = None,
        where: object | None = None,
        group_by: object | None = None,
        having: object | None = None,
        order_by: object | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> PredicateExpression:
        """Return a scoped bounded local-source IN-subquery predicate."""

        source_column = _normalize_expression_column(column)
        source_ref = _sql_in_subquery_source(source, source_alias=source_alias)
        tail = _sql_in_subquery_tail(
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            descending=descending,
            limit=limit,
        )
        return PredicateExpression(
            f"{self.sql} IN (SELECT {source_column} FROM {source_ref}{tail})"
        )

    def any_source(
        self,
        comparison: object,
        source: object,
        column: object,
        *,
        source_alias: object | None = None,
        where: object | None = None,
        group_by: object | None = None,
        having: object | None = None,
        order_by: object | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> PredicateExpression:
        """Return a scoped bounded local-source `ANY (SELECT ...)` predicate."""

        return _quantified_source_predicate(
            self.sql,
            comparison,
            "ANY",
            source,
            column,
            source_alias=source_alias,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            descending=descending,
            limit=limit,
        )

    def all_source(
        self,
        comparison: object,
        source: object,
        column: object,
        *,
        source_alias: object | None = None,
        where: object | None = None,
        group_by: object | None = None,
        having: object | None = None,
        order_by: object | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> PredicateExpression:
        """Return a scoped bounded local-source `ALL (SELECT ...)` predicate."""

        return _quantified_source_predicate(
            self.sql,
            comparison,
            "ALL",
            source,
            column,
            source_alias=source_alias,
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            descending=descending,
            limit=limit,
        )

    def not_in(self, *values: object) -> PredicateExpression:
        """Return a scoped bounded `NOT IN (...)` predicate."""

        normalized = _normalize_in_values(values)
        joined = ",".join(_sql_in_literal(value) for value in normalized)
        return PredicateExpression(f"{self.sql} NOT IN ({joined})")

    def not_in_source(
        self,
        source: object,
        column: object,
        *,
        source_alias: object | None = None,
        where: object | None = None,
        group_by: object | None = None,
        having: object | None = None,
        order_by: object | None = None,
        descending: bool = False,
        limit: int | None = None,
    ) -> PredicateExpression:
        """Return a scoped bounded local-source NOT IN-subquery predicate."""

        source_column = _normalize_expression_column(column)
        source_ref = _sql_in_subquery_source(source, source_alias=source_alias)
        tail = _sql_in_subquery_tail(
            where=where,
            group_by=group_by,
            having=having,
            order_by=order_by,
            descending=descending,
            limit=limit,
        )
        return PredicateExpression(
            f"{self.sql} NOT IN (SELECT {source_column} FROM {source_ref}{tail})"
        )

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

    def try_cast(self, dtype: object) -> "ColumnExpression":
        """Return a scoped `TRY_CAST(column AS dtype)` expression for dirty values."""

        normalized_dtype = _normalize_cast_dtype(dtype)
        return ColumnExpression(f"TRY_CAST({self.sql} AS {normalized_dtype})")

    def date_add_days(self, days: object) -> "ColumnExpression":
        """Return a scoped Date32 day-add expression for date predicates."""

        normalized_days = _normalize_date_arithmetic_days(days)
        return ColumnExpression(f"DATE_ADD_DAYS({self.sql}, {normalized_days})")

    def date_sub_days(self, days: object) -> "ColumnExpression":
        """Return a scoped Date32 day-subtract expression for date predicates."""

        normalized_days = _normalize_date_arithmetic_days(days)
        return ColumnExpression(f"DATE_SUB_DAYS({self.sql}, {normalized_days})")

    def timestamp_add_seconds(self, seconds: object) -> "ColumnExpression":
        """Return a scoped UTC timestamp second-add expression for predicates."""

        normalized_seconds = _normalize_timestamp_arithmetic_seconds(seconds)
        return ColumnExpression(
            f"TIMESTAMP_ADD_SECONDS({self.sql}, {normalized_seconds})"
        )

    def timestamp_sub_seconds(self, seconds: object) -> "ColumnExpression":
        """Return a scoped UTC timestamp second-subtract expression for predicates."""

        normalized_seconds = _normalize_timestamp_arithmetic_seconds(seconds)
        return ColumnExpression(
            f"TIMESTAMP_SUB_SECONDS({self.sql}, {normalized_seconds})"
        )

    def date_diff_days(self, other: object) -> "ColumnExpression":
        """Return a scoped Date32 day-difference expression."""

        return ColumnExpression(
            f"DATE_DIFF_DAYS({self.sql}, {_sql_temporal_difference_arg(other, 'date32')})"
        )

    def timestamp_diff_seconds(self, other: object) -> "ColumnExpression":
        """Return a scoped UTC timestamp second-difference expression."""

        return ColumnExpression(
            f"TIMESTAMP_DIFF_SECONDS({self.sql}, {_sql_temporal_difference_arg(other, 'timestamp_micros')})"
        )

    def date_year(self) -> "ColumnExpression":
        """Return a scoped Date32 year-extract expression for date predicates."""

        return ColumnExpression(f"DATE_YEAR({self.sql})")

    def date_month(self) -> "ColumnExpression":
        """Return a scoped Date32 month-extract expression for date predicates."""

        return ColumnExpression(f"DATE_MONTH({self.sql})")

    def date_day(self) -> "ColumnExpression":
        """Return a scoped Date32 day-of-month extract expression for date predicates."""

        return ColumnExpression(f"DATE_DAY({self.sql})")

    def timestamp_year(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp year-extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_YEAR({self.sql})")

    def timestamp_month(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp month-extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_MONTH({self.sql})")

    def timestamp_day(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp day-of-month extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_DAY({self.sql})")

    def timestamp_hour(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp hour extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_HOUR({self.sql})")

    def timestamp_minute(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp minute extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_MINUTE({self.sql})")

    def timestamp_second(self) -> "ColumnExpression":
        """Return a scoped UTC timestamp second extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_SECOND({self.sql})")


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


class _GeneratedStructuredOutputMixin:
    __slots__ = ()

    def prepare_vortex(
        self,
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        workspace: str | os.PathLike[str] | None = None,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Prepare this generated source into a caller-owned local Vortex artifact.

        Generated rows already originate inside ShardLoom, so this routes through
        the real generated-source Vortex writer instead of a compatibility-file
        ingest. The returned report exposes prepared-state fields; repeated
        compatible calls can reuse the artifact-adjacent manifest for the
        caller-owned local Vortex artifact when source, plan, policy, and
        artifact fingerprints still match.
        """

        stem_method = getattr(self, "_generated_vortex_stem")
        target = _generated_prepared_vortex_target_path(
            stem_method(),
            target_vortex_path=target_vortex_path,
            workspace=workspace,
        )
        return self.write_vortex(  # type: ignore[attr-defined]
            target,
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="parquet")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Parquet sink blocker.
        """

        return self.write(  # type: ignore[attr-defined]
            target_uri,
            output_format="parquet",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Arrow IPC sink blocker.
        """

        return self.write(  # type: ignore[attr-defined]
            target_uri,
            output_format="arrow-ipc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="avro")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Avro sink blocker.
        """

        return self.write(  # type: ignore[attr-defined]
            target_uri,
            output_format="avro",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="orc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic ORC sink blocker.
        """

        return self.write(  # type: ignore[attr-defined]
            target_uri,
            output_format="orc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Alias for `write(..., output_format="vortex")`.

        The CLI must be built with `--features vortex-write`; default binaries
        return ShardLoom's deterministic Vortex sink blocker.
        """

        return self.write(  # type: ignore[attr-defined]
            target_uri,
            output_format="vortex",
            allow_overwrite=allow_overwrite,
            check=check,
        )


@dataclass(frozen=True, slots=True)
class GeneratedRowsSource(_GeneratedStructuredOutputMixin):
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

    def project(self, *columns: object) -> "GeneratedRowsSource":
        """Alias for `select(...)` using familiar DataFrame/project naming."""

        return self.select(*columns)

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

    def with_columns(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        **named_expressions: object,
    ) -> "GeneratedRowsSource":
        """Alias over repeated generated-row `with_column(...)` calls."""

        source = self
        for name, expression in _normalize_named_projection_items(
            "generated rows with_columns",
            columns,
            named_expressions,
        ):
            source = source.with_column(name, expression)
        return source

    def assign(self, **named_expressions: object) -> "GeneratedRowsSource":
        """Alias for `with_columns(...)` using pandas-style naming."""

        return self.with_columns(**named_expressions)

    def _column_names(self) -> tuple[str, ...]:
        if not self.rows:
            raise ValueError("generated row transforms require retained row values")
        return tuple(column for column, _value in self.rows[0])

    def _generated_vortex_stem(self) -> str:
        payload = f"{self.source_kind}\0{self.schema_arg}\0{self.rows_arg}".encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()[:16]
        return f"generated-{self.source_kind}-{digest}"

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write generated user rows to a scoped local output sink with evidence."""

        surface = (
            "dataframe"
            if self.source_kind.startswith("dataframe_")
            else "python"
        )
        execution = self.client.public_workflow_run(
            surface,
            plan_summary=f"generated_source({self.source_kind}) -> write({target_uri})",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=target_uri,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            generated_source_kind=self.source_kind,
            generated_schema=self.schema_arg,
            generated_rows=self.rows_arg,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

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

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write generated user rows to a primary output plus fanout sinks."""

        output_path, output_format, fanout_outputs = _generated_primary_and_fanout_outputs(
            outputs
        )
        execution = self.client.public_workflow_run(
            "dataframe" if self.source_kind.startswith("dataframe_") else "python",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=output_path,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            generated_source_kind=self.source_kind,
            generated_schema=self.schema_arg,
            generated_rows=self.rows_arg,
            fanout_outputs=fanout_outputs,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)


@dataclass(frozen=True, slots=True)
class GeneratedRangeSource(_GeneratedStructuredOutputMixin):
    """Scoped ShardLoom-native integer generator that can write a local smoke output."""

    start: int
    end: int
    step: int
    column: str
    client: ShardLoomClient
    source_kind: str = "range"

    def filter(self, predicate: object) -> "GeneratedRangeQuerySource":
        """Return a scoped generated-range SQL query with one filter predicate."""

        return self._query().filter(predicate)

    def where(self, predicate: object) -> "GeneratedRangeQuerySource":
        """Alias for `filter(...)` using familiar SQL/DataFrame naming."""

        return self.filter(predicate)

    def select(self, *columns: object) -> "GeneratedRangeQuerySource":
        """Return a scoped generated-range SQL query with a source-column projection."""

        return self._query().select(*columns)

    def project(self, *columns: object) -> "GeneratedRangeQuerySource":
        """Alias for `select(...)` using familiar DataFrame/project naming."""

        return self.select(*columns)

    def with_column(
        self,
        name: object,
        expression: object,
    ) -> "GeneratedRangeQuerySource":
        """Return a scoped generated-range SQL query with one computed int64 column."""

        return self._query().with_column(name, expression)

    def with_columns(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        **named_expressions: object,
    ) -> "GeneratedRangeQuerySource":
        """Alias over repeated generated-range `with_column(...)` calls."""

        query = self._query()
        for name, expression in _normalize_named_projection_items(
            "generated range with_columns",
            columns,
            named_expressions,
        ):
            query = query.with_column(name, expression)
        return query

    def assign(self, **named_expressions: object) -> "GeneratedRangeQuerySource":
        """Alias for `with_columns(...)` using pandas-style naming."""

        return self.with_columns(**named_expressions)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Return a scoped generated-range SQL query with one ORDER BY clause."""

        return self._query().sort(*columns, descending=descending)

    def order_by(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using SQL-style naming."""

        return self.sort(*columns, descending=descending)

    def sort_by(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using familiar DataFrame naming."""

        return self.sort(*columns, descending=descending)

    def sort_values(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using pandas-style naming."""

        return self.sort(*columns, descending=descending)

    def limit(self, count: int) -> "GeneratedRangeSource":
        """Limit an engine-native range/sequence before writing local output."""

        normalized_count = _normalize_non_negative_int("generated range limit", count)
        limited_end = _limited_range_end(
            self.start,
            self.end,
            self.step,
            normalized_count,
        )
        return GeneratedRangeSource(
            start=self.start,
            end=limited_end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
        )

    def head(self, limit: int = 5) -> "GeneratedRangeSource":
        """Alias for `limit(...)` using familiar DataFrame preview naming."""

        return self.limit(limit)

    def take(self, count: int) -> "GeneratedRangeSource":
        """Alias for `limit(...)` using familiar DataFrame preview naming."""

        return self.limit(count)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write the generated integer source to a scoped local output sink with evidence."""

        execution = self.client.public_workflow_run(
            "python",
            plan_summary=(
                f"generated_{self.source_kind}({self.start},{self.end},{self.step}) "
                f"-> write({target_uri})"
            ),
            requested_output=_public_write_request_for_format(output_format),
            output_ref=target_uri,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            generated_source_kind=self.source_kind,
            generated_range_start=self.start,
            generated_range_end=self.end,
            generated_range_step=self.step,
            generated_range_column=self.column,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

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

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write an engine-native range/sequence to primary and fanout sinks."""

        output_path, output_format, fanout_outputs = _generated_primary_and_fanout_outputs(
            outputs
        )
        execution = self.client.public_workflow_run(
            "python",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=output_path,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            generated_source_kind=self.source_kind,
            generated_range_start=self.start,
            generated_range_end=self.end,
            generated_range_step=self.step,
            generated_range_column=self.column,
            fanout_outputs=fanout_outputs,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

    def _generated_vortex_stem(self) -> str:
        return f"generated-{self.source_kind}-{self.start}-{self.end}-{self.step}-{self.column}"

    def _query(self) -> "GeneratedRangeQuerySource":
        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
        )


@dataclass(frozen=True, slots=True)
class GeneratedRangeQuerySource(_GeneratedStructuredOutputMixin):
    """Scoped SQL query over a source-free range generator."""

    start: int
    end: int
    step: int
    column: str
    client: ShardLoomClient
    source_kind: str = "range"
    predicate: str | None = None
    select_items: tuple[str, ...] = ()
    sort_key: tuple[str, tuple[str, ...]] | None = None
    limit_count: int | None = None

    def filter(self, predicate: object) -> "GeneratedRangeQuerySource":
        """Return this generated-range query with a scoped filter predicate."""

        if self.predicate is not None:
            raise ValueError("generated range queries admit one filter predicate")
        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
            predicate=_sql_generated_range_expression_sql(predicate, self.column),
            select_items=self.select_items,
            sort_key=self.sort_key,
            limit_count=self.limit_count,
        )

    def where(self, predicate: object) -> "GeneratedRangeQuerySource":
        """Alias for `filter(...)` using familiar SQL/DataFrame naming."""

        return self.filter(predicate)

    def select(self, *columns: object) -> "GeneratedRangeQuerySource":
        """Return this generated-range query with a source-column projection."""

        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
            predicate=self.predicate,
            select_items=_normalize_generated_range_select_items(columns, self.column),
            sort_key=self.sort_key,
            limit_count=self.limit_count,
        )

    def project(self, *columns: object) -> "GeneratedRangeQuerySource":
        """Alias for `select(...)` using familiar DataFrame/project naming."""

        return self.select(*columns)

    def with_column(
        self,
        name: object,
        expression: object,
    ) -> "GeneratedRangeQuerySource":
        """Append one scoped generated-range computed int64 projection."""

        column_name = _normalize_output_column_name(name)
        select_items = self.select_items or _default_generated_range_select_items(
            self.column
        )
        if column_name in _generated_range_select_aliases(select_items):
            raise ValueError("generated range projection aliases must be unique")
        expression_sql = _sql_generated_range_projection_expression(
            expression,
            self.column,
        )
        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
            predicate=self.predicate,
            select_items=select_items + (f"{expression_sql} AS {column_name}",),
            sort_key=self.sort_key,
            limit_count=self.limit_count,
        )

    def with_columns(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        **named_expressions: object,
    ) -> "GeneratedRangeQuerySource":
        """Alias over repeated generated-range query `with_column(...)` calls."""

        query = self
        for name, expression in _normalize_named_projection_items(
            "generated range query with_columns",
            columns,
            named_expressions,
        ):
            query = query.with_column(name, expression)
        return query

    def assign(self, **named_expressions: object) -> "GeneratedRangeQuerySource":
        """Alias for `with_columns(...)` using pandas-style naming."""

        return self.with_columns(**named_expressions)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Return this generated-range query with one source-free ORDER BY clause."""

        if self.sort_key is not None:
            raise ValueError("generated range queries admit one ORDER BY clause")
        sort_columns = _normalize_generated_range_sort_columns(columns)
        direction = "desc" if descending else "asc"
        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
            predicate=self.predicate,
            select_items=self.select_items,
            sort_key=(direction, sort_columns),
            limit_count=self.limit_count,
        )

    def order_by(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using SQL-style naming."""

        return self.sort(*columns, descending=descending)

    def sort_by(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using familiar DataFrame naming."""

        return self.sort(*columns, descending=descending)

    def sort_values(
        self,
        *columns: object,
        descending: bool = False,
    ) -> "GeneratedRangeQuerySource":
        """Alias for `sort(...)` using pandas-style naming."""

        return self.sort(*columns, descending=descending)

    def limit(self, count: int) -> "GeneratedRangeQuerySource":
        """Return this generated-range query with a SQL LIMIT clause."""

        return GeneratedRangeQuerySource(
            start=self.start,
            end=self.end,
            step=self.step,
            column=self.column,
            client=self.client,
            source_kind=self.source_kind,
            predicate=self.predicate,
            select_items=self.select_items,
            sort_key=self.sort_key,
            limit_count=_normalize_non_negative_int("generated range SQL limit", count),
        )

    def head(self, limit: int = 5) -> "GeneratedRangeQuerySource":
        """Alias for `limit(...)` using familiar DataFrame preview naming."""

        return self.limit(limit)

    def take(self, count: int) -> "GeneratedRangeQuerySource":
        """Alias for `limit(...)` using familiar DataFrame preview naming."""

        return self.limit(count)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write the admitted generated-range SQL query to a local output sink."""

        statement = self._statement()
        execution = self.client.public_workflow_run(
            "sql",
            sql_statement=statement,
            plan_summary=f"generated_range_query -> write({target_uri})",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=target_uri,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

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

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write the admitted generated-range SQL query to multiple local sinks."""

        output_path, output_format, fanout_outputs = _generated_primary_and_fanout_outputs(
            outputs
        )
        execution = self.client.public_workflow_run(
            "sql",
            sql_statement=self._statement(),
            plan_summary=f"generated_range_query -> fanout({output_path})",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=output_path,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            fanout_outputs=fanout_outputs,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

    def _statement(self) -> str:
        select_items = self.select_items or _default_generated_range_select_items(
            self.column
        )
        generator = "generate_series" if self.source_kind == "sequence" else "range"
        statement = (
            f"SELECT {', '.join(select_items)} "
            f"FROM {generator}({self.start}, {self.end}, {self.step})"
        )
        if self.predicate is not None:
            statement = f"{statement} WHERE {self.predicate}"
        if self.sort_key is not None:
            direction, columns = self.sort_key
            statement = f"{statement}{_format_order_by_clause(columns, direction)}"
        if self.limit_count is not None:
            statement = f"{statement} LIMIT {self.limit_count}"
        return statement

    def _generated_vortex_stem(self) -> str:
        return f"generated-{self.source_kind}-query"


@dataclass(frozen=True, slots=True)
class GeneratedSqlSource(_GeneratedStructuredOutputMixin):
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

        execution = self.client.public_workflow_run(
            "sql",
            sql_statement=self.statement,
            plan_summary=f"source_free_sql -> write({target_uri})",
            requested_output=_public_write_request_for_format(output_format),
            output_ref=target_uri,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

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

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport:
        """Write source-free SQL generated rows to primary and fanout sinks."""

        output_path, output_format, fanout_outputs = _generated_primary_and_fanout_outputs(
            outputs
        )
        execution = self.client.public_workflow_run(
            "sql",
            sql_statement=self.statement,
            plan_summary=self.operation_summary,
            requested_output=_public_write_request_for_format(output_format),
            output_ref=output_path,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            fanout_outputs=fanout_outputs,
            check=check,
        )
        return GeneratedSourceWriteReport(execution.envelope)

    def _generated_vortex_stem(self) -> str:
        return "generated-sql"


@dataclass(frozen=True, slots=True)
class SqlWorkflow:
    """A scoped SQL workflow entry point over currently admitted ShardLoom SQL paths."""

    statement: str
    client: ShardLoomClient

    @property
    def operation_summary(self) -> str:
        """Return a deterministic SQL workflow summary."""

        return "sql(statement)"

    def route(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "auto",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        check: bool = False,
    ) -> PublicWorkflowRoute:
        """Return the shared public route envelope for this SQL workflow."""

        normalized_bounded = (
            _find_top_level_sql_keyword_outside_quotes(self.statement.strip(), "limit")
            is not None
            if bounded is None and requested_output == "collect"
            else bounded
        )
        return self.client.public_workflow_route(
            "sql",
            sql_statement=self.statement,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy=execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=normalized_bounded,
            check=check,
        )

    def run(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "auto",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Run this SQL workflow through the shared public route facade."""

        normalized_bounded = (
            _find_top_level_sql_keyword_outside_quotes(self.statement.strip(), "limit")
            is not None
            if bounded is None and requested_output == "collect"
            else bounded
        )
        return self.client.public_workflow_run(
            "sql",
            sql_statement=self.statement,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy=execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=normalized_bounded,
            check=check,
        )

    def collect(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
        memory_gb: int = 4,
        max_parallelism: int = 1,
    ) -> (
        SqlLocalSourceSmokeReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Collect rows or run admitted local Vortex SQL primitives."""

        if limit is not None:
            return self.limit(limit).collect(
                check=check,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
            )
        if _is_source_free_sql_statement(self.statement):
            return self._unsupported_operation(
                "sql-source-free-projection",
                "source_free_sql_collect_requires_write_output",
                check=check,
            )
        if report := self._vortex_sql_primitive_collect_report(
            check=check,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        ):
            return report
        if statement := self._bounded_local_source_statement(default_limit=None):
            execution = self.client.public_workflow_run(
                "sql",
                sql_statement=statement,
                plan_summary=self.operation_summary,
                requested_output="collect",
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                check=check,
            )
            return SqlLocalSourceSmokeReport(execution.envelope)
        if _is_local_source_sql_statement(self.statement):
            return self._unsupported_operation(
                "sql-local-source-collect",
                "local_source_sql_collect_requires_explicit_limit",
                check=check,
            )
        return self._unsupported_operation("sql", self.statement, check=check)

    def limit(self, count: int) -> "SqlWorkflow":
        """Return this SQL workflow with an explicit LIMIT when one is absent."""

        statement = _sql_statement_with_limit(self.statement, count)
        return SqlWorkflow(statement=statement, client=self.client)

    def schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return a bounded schema report for admitted local-source SQL."""

        if report := self._bounded_schema_report(check=check):
            return report
        return self._unsupported_operation("schema", self.statement, check=check)

    def describe_schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return detailed bounded schema evidence for admitted local-source SQL."""

        if report := self._bounded_schema_report(check=check):
            return report
        return self._unsupported_operation("describe-schema", self.statement, check=check)

    def validate_schema(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Validate an expected schema against admitted local-source SQL rows."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema validation contract must not be empty")
        if report := self._bounded_schema_report(check=check):
            return _validate_workflow_schema(report, normalized)
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("validate-schema", target, check=check)

    def schema_contract(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Alias for exact bounded schema validation over admitted local-source SQL."""

        return self.validate_schema(schema, check=check)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Run bounded data-quality checks for admitted local-source SQL."""

        normalized_checks = _normalize_columns(checks)
        parsed_checks = _parse_data_quality_checks(normalized_checks)
        if parsed_checks is not None:
            if report := self._bounded_schema_report(check=check):
                return _workflow_data_quality_report(report, parsed_checks)
        return self._unsupported_operation(
            "data-quality",
            ",".join(normalized_checks),
            check=check,
        )

    def data_quality(
        self,
        *checks: object,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Alias for bounded SQL data-quality checks."""

        return self.data_quality_check(*checks, check=check)

    def data_quality_summary(
        self,
        *,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Return bounded null-count and schema summary for admitted SQL."""

        if report := self._bounded_schema_report(check=check):
            return WorkflowDataQualityReport(schema_report=report)
        return self._unsupported_operation(
            "data-quality-summary",
            self.statement,
            check=check,
        )

    def profile(
        self,
        limit: int = 100,
        *,
        check: bool = False,
    ) -> WorkflowProfileReport | UnsupportedWorkflowOperationReport:
        """Return a bounded runtime profile for admitted local-source SQL."""

        _validate_positive_row_count("profile limit", limit)
        if report := self._bounded_materialization_report(limit=limit, check=check):
            workflow = self._report_workflow()
            return WorkflowProfileReport(
                workflow=workflow,
                smoke_report=report,
                schema_report=_workflow_schema_report(workflow, report),
                limit=limit,
            )
        return self._unsupported_operation("profile", self.statement, check=check)

    def quarantine(
        self,
        target_uri: str | os.PathLike[str] | None = None,
        *checks: object,
        output_format: str | None = None,
        limit: int = 100,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> WorkflowQuarantineReport | UnsupportedWorkflowOperationReport:
        """Return bounded quarantine evidence for admitted local-source SQL."""

        _validate_positive_row_count("quarantine limit", limit)
        parsed_checks: tuple[_WorkflowDataQualityCheckSpec, ...] | None = None
        if checks:
            normalized_checks = _normalize_columns(checks)
            parsed_checks = _parse_data_quality_checks(normalized_checks)
            if parsed_checks is None:
                return self._unsupported_operation(
                    "quarantine",
                    ",".join(normalized_checks),
                    check=check,
                )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            workflow = self._report_workflow()
            schema_report = _workflow_schema_report(workflow, report)
            parsed_checks = parsed_checks or _workflow_quarantine_checks(schema_report, ())
            quality_report = _workflow_data_quality_report(schema_report, parsed_checks)
            return WorkflowQuarantineReport(
                workflow=workflow,
                quality_report=quality_report,
                checks=tuple(spec.raw for spec in parsed_checks),
                rows=_workflow_quarantine_rows(schema_report, parsed_checks),
                limit=limit,
                target_uri=None if target_uri is None else str(target_uri),
                output_format=_normalize_optional_quarantine_output_format(
                    target_uri,
                    output_format,
                ),
                sink_report=None,
            )
        target = "none" if target_uri is None else str(target_uri)
        return self._unsupported_operation("quarantine", target, check=check)

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded local preview for admitted local-source SQL."""

        _validate_positive_row_count("preview limit", limit)
        if statement := self._bounded_local_source_statement(default_limit=limit):
            return self.client.sql_local_source_smoke(statement, check=check)
        return self._unsupported_operation("preview", str(limit), check=check)

    def head(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded SQL preview using familiar DataFrame naming."""

        _validate_positive_row_count("head limit", limit)
        if statement := self._bounded_local_source_statement(default_limit=limit):
            return self.client.sql_local_source_smoke(statement, check=check)
        return self._unsupported_operation("head", str(limit), check=check)

    def take(
        self,
        count: int,
        *,
        check: bool = False,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Return a bounded SQL preview for the requested row count."""

        _validate_positive_row_count("take count", count)
        if statement := self._bounded_local_source_statement(default_limit=count):
            return self.client.sql_local_source_smoke(statement, check=check)
        return self._unsupported_operation("take", str(count), check=check)

    def to_python_objects(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> tuple[Mapping[str, Any], ...] | UnsupportedWorkflowOperationReport:
        """Return bounded Python row objects for admitted local-source SQL."""

        if report := self._bounded_materialization_report(limit=limit, check=check):
            return report.result_rows
        return self._unsupported_operation("to-python-objects", self.statement, check=check)

    def to_pandas(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a pandas DataFrame at an explicit bounded materialization boundary."""

        if self._bounded_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-pandas", self.statement, check=check)
        pandas = _optional_module("pandas")
        if pandas is None:
            return self._unsupported_operation(
                "to-pandas",
                "missing optional dependency: pandas",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_pandas(report.result_rows, pandas)
        return self._unsupported_operation("to-pandas", self.statement, check=check)

    def to_arrow(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table at an explicit bounded materialization boundary."""

        if self._bounded_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow", self.statement, check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_table(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow", self.statement, check=check)

    def to_arrow_table(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table for admitted bounded local-source SQL."""

        if self._bounded_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow-table", self.statement, check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-table",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_table(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow-table", self.statement, check=check)

    def to_arrow_ipc(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> bytes | UnsupportedWorkflowOperationReport:
        """Return Arrow IPC stream bytes for admitted bounded local-source SQL."""

        if self._bounded_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow-ipc", self.statement, check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-ipc",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_ipc(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow-ipc", self.statement, check=check)

    def to_numpy(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a NumPy array for admitted bounded local-source SQL rows."""

        if self._bounded_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-numpy", self.statement, check=check)
        numpy = _optional_module("numpy")
        if numpy is None:
            return self._unsupported_operation(
                "to-numpy",
                "missing optional dependency: numpy",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_numpy(report.result_rows, numpy)
        return self._unsupported_operation("to-numpy", self.statement, check=check)

    def display(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> WorkflowNotebookPreview | UnsupportedWorkflowOperationReport:
        """Return a bounded notebook/display preview for admitted local-source SQL."""

        _validate_positive_row_count("display limit", limit)
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return WorkflowNotebookPreview(
                workflow=self._report_workflow(),
                smoke_report=report,
                limit=limit,
            )
        return self._unsupported_operation("display", str(limit), check=check)

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
        return self._public_workflow_write_report(
            target_uri,
            requested_output=_public_write_request_for_format(normalized_output_format),
            allow_overwrite=allow_overwrite,
            check=check,
        )

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

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="parquet")`.

        Local SQL-source Parquet output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Parquet sink blocker.
        """

        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_parquet",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        Local SQL-source Arrow IPC output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Arrow IPC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="arrow-ipc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="avro")`.

        Local SQL-source Avro output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Avro sink blocker.
        """

        return self.write(
            target_uri,
            output_format="avro",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="orc")`.

        Local SQL-source ORC output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic ORC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="orc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="vortex")`.

        Source-free SQL can route through the generated-source Vortex sink, and
        local-source SQL can route through the scoped local-source Vortex sink,
        when the CLI is built with `--features vortex-write`. Default binaries
        return deterministic Vortex sink blockers.
        """

        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_vortex",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> (
        GeneratedSourceWriteReport
        | SqlLocalSourceSmokeReport
        | UnsupportedWorkflowOperationReport
    ):
        """Write an admitted SQL result to primary and fanout local sinks."""

        normalized_outputs = _normalize_fanout_outputs(outputs)
        output_format, output_path = normalized_outputs[0]
        fanout_outputs = normalized_outputs[1:]
        requested_output = _public_write_request_for_format(output_format)
        if _is_source_free_sql_statement(self.statement):
            execution = self.client.public_workflow_run(
                "sql",
                sql_statement=self.statement,
                plan_summary=self.operation_summary,
                requested_output=requested_output,
                output_ref=output_path,
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                allow_overwrite=allow_overwrite,
                fanout_outputs=fanout_outputs,
                check=check,
            )
            return GeneratedSourceWriteReport(execution.envelope)
        if _is_local_source_sql_statement(self.statement):
            execution = self.client.public_workflow_run(
                "sql",
                sql_statement=self.statement,
                plan_summary=self.operation_summary,
                requested_output=requested_output,
                output_ref=output_path,
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                allow_overwrite=allow_overwrite,
                fanout_outputs=fanout_outputs,
                check=check,
            )
            return SqlLocalSourceSmokeReport(execution.envelope)
        return self._unsupported_operation("fanout", self.statement, check=check)

    def _public_workflow_write_report(
        self,
        target_uri: str | os.PathLike[str],
        *,
        requested_output: str,
        allow_overwrite: bool,
        check: bool,
    ) -> GeneratedSourceWriteReport | SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        if _is_source_free_sql_statement(self.statement):
            execution = self.client.public_workflow_run(
                "sql",
                sql_statement=self.statement,
                plan_summary=self.operation_summary,
                requested_output=requested_output,
                output_ref=target_uri,
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                allow_overwrite=allow_overwrite,
                check=check,
            )
            return GeneratedSourceWriteReport(execution.envelope)
        if _is_local_source_sql_statement(self.statement):
            execution = self.client.public_workflow_run(
                "sql",
                sql_statement=self.statement,
                plan_summary=self.operation_summary,
                requested_output=requested_output,
                output_ref=target_uri,
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                allow_overwrite=allow_overwrite,
                check=check,
            )
            return SqlLocalSourceSmokeReport(execution.envelope)
        return self._unsupported_operation("sql", self.statement, check=check)

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

    def _bounded_schema_report(self, *, check: bool) -> WorkflowSchemaReport | None:
        statement = self._bounded_local_source_statement(default_limit=100)
        if statement is None:
            return None
        smoke_report = self.client.sql_local_source_smoke(statement, check=check)
        if smoke_report.envelope.status != "success":
            return None
        return _workflow_schema_report(self._report_workflow(), smoke_report)

    def _bounded_materialization_report(
        self,
        *,
        limit: int | None,
        check: bool,
    ) -> SqlLocalSourceSmokeReport | None:
        statement = self._bounded_local_source_statement(default_limit=limit)
        if statement is None:
            return None
        smoke_report = self.client.sql_local_source_smoke(statement, check=check)
        if smoke_report.envelope.status != "success":
            return None
        return smoke_report

    def _bounded_local_source_statement(self, *, default_limit: int | None) -> str | None:
        if default_limit is not None:
            _validate_positive_row_count("materialization limit", default_limit)
        normalized = self.statement.strip().rstrip(";").strip()
        if not _is_local_source_sql_statement(normalized):
            return None
        limit_index = _find_top_level_sql_keyword_outside_quotes(normalized, "limit")
        if limit_index is not None:
            if default_limit is None:
                return normalized
            return _cap_top_level_sql_limit(normalized, limit_index, default_limit)
        if default_limit is None:
            return None
        return f"{normalized} LIMIT {default_limit}"

    def _vortex_sql_primitive_collect_report(
        self,
        *,
        check: bool,
        memory_gb: int,
        max_parallelism: int,
    ) -> VortexWorkflowExecutionReport | None:
        shape = _vortex_sql_primitive_shape(self.statement)
        if shape is None:
            return None
        memory_gb = _normalize_positive_int("memory_gb", memory_gb)
        max_parallelism = _normalize_positive_int("max_parallelism", max_parallelism)
        if shape.count:
            if shape.predicate:
                envelope = self.client.vortex_count_where(
                    shape.uri,
                    shape.predicate,
                    execute_local_primitive=True,
                    memory_gb=memory_gb,
                    max_parallelism=max_parallelism,
                    check=check,
                )
            else:
                envelope = self.client.vortex_run(
                    shape.uri,
                    "count",
                    memory_gb=memory_gb,
                    max_parallelism=max_parallelism,
                    check=check,
                )
        elif shape.predicate and shape.columns:
            envelope = self.client.vortex_filter_project(
                shape.uri,
                shape.predicate,
                shape.columns,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        elif shape.predicate and shape.columns is None:
            envelope = self.client.vortex_filter(
                shape.uri,
                shape.predicate,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        elif shape.columns and shape.predicate is None:
            envelope = self.client.vortex_project(
                shape.uri,
                shape.columns,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        else:
            return None
        return VortexWorkflowExecutionReport(
            workflow=self._report_workflow(),
            operation="collect",
            envelope=envelope,
        )

    def _report_workflow(self) -> "LazyFrame":
        return LazyFrame(
            source=WorkflowSource("sql", self.statement),
            client=self.client,
            operations=(WorkflowOperation("sql", (self.statement,)),),
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
    def external_engine_invoked(self) -> bool:
        """Whether the unsupported-report path invoked an external engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True

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
class VortexWorkflowExecutionReport:
    """Report for an admitted local Vortex primitive query-builder execution."""

    workflow: "LazyFrame"
    operation: str
    envelope: OutputEnvelope

    @property
    def command(self) -> str:
        """Return the CLI command used for the admitted Vortex primitive."""

        return self.envelope.field("public_workflow_resolved_internal_command") or self.envelope.command

    @property
    def status(self) -> str:
        """Return the CLI command status."""

        return self.envelope.status

    @property
    def mode(self) -> str | None:
        """Return the reported Vortex primitive mode."""

        return self.envelope.field("mode")

    @property
    def primitive(self) -> str | None:
        """Return the reported Vortex primitive name."""

        return self.envelope.field("primitive")

    @property
    def execution(self) -> str | None:
        """Return the reported execution path label."""

        return self.envelope.field("execution")

    @property
    def result_known(self) -> bool:
        """Whether the primitive emitted a known result cardinality."""

        return self.envelope.field_bool("result_known", False) is True or _any_true_field(
            self.envelope,
            (
                "filtered_count_local_execution_result_known",
                "project_local_execution_result_known",
                "filter_project_local_execution_result_known",
                "filter_local_execution_result_known",
            ),
        )

    @property
    def rows_scanned(self) -> int | None:
        """Return the reported local Vortex rows scanned, when present."""

        return _first_int_field(
            self.envelope,
            (
                "local_primitive_rows_scanned",
                "filtered_count_local_execution_rows_scanned",
                "filter_local_execution_rows_scanned",
                "project_local_execution_rows_scanned",
                "filter_project_local_execution_rows_scanned",
            ),
        )

    @property
    def rows_selected(self) -> int | None:
        """Return the reported selected row count, when present."""

        return _first_int_field(
            self.envelope,
            (
                "rows_selected",
                "local_primitive_rows_selected",
                "filtered_count_local_execution_rows_selected",
                "filter_local_execution_rows_selected",
                "filter_project_local_execution_rows_selected",
            ),
        )

    @property
    def rows_projected(self) -> int | None:
        """Return the reported projected row count, when present."""

        return _first_int_field(
            self.envelope,
            (
                "rows_projected",
                "project_local_execution_rows_projected",
                "filter_project_local_execution_rows_projected",
            ),
        )

    @property
    def projected_columns(self) -> tuple[str, ...]:
        """Return projected columns reported by the primitive."""

        value = _first_string_field(
            self.envelope,
            (
                "local_primitive_projected_columns",
                "project_local_execution_projected_columns",
                "filter_project_local_execution_projected_columns",
                "columns",
            ),
        )
        if not value:
            return ()
        return tuple(part.strip() for part in value.split(",") if part.strip())

    @property
    def fallback_attempted(self) -> bool:
        """Whether this primitive path attempted fallback execution."""

        if self.envelope.fallback.attempted:
            return True
        return _any_true_field(
            self.envelope,
            (
                "fallback_attempted",
                "local_primitive_native_io_fallback_attempted",
                "local_primitive_execution_certificate_fallback_attempted",
                "filtered_count_local_execution_fallback_attempted",
                "filter_local_execution_fallback_attempted",
                "project_local_execution_fallback_attempted",
                "filter_project_local_execution_fallback_attempted",
            ),
        )

    @property
    def external_engine_invoked(self) -> bool:
        """Whether this primitive path invoked an external execution engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether the report represents actual local Vortex runtime execution."""

        return self.data_read or _any_true_field(
            self.envelope,
            (
                "local_primitive_report_present",
                "filtered_count_local_execution_result_known",
                "project_local_execution_result_known",
                "filter_project_local_execution_result_known",
                "filter_local_execution_result_known",
            ),
        )

    @property
    def data_read(self) -> bool:
        """Whether the primitive read Vortex data."""

        return _any_true_field(
            self.envelope,
            (
                "data_read",
                "data_io_performed",
                "filtered_count_local_execution_data_read",
                "filter_local_execution_data_read",
                "project_local_execution_data_read",
                "filter_project_local_execution_data_read",
            ),
        )

    @property
    def data_decoded(self) -> bool:
        """Whether the primitive reported decoded-data work."""

        return self.envelope.field_bool("data_decoded", False) is True

    @property
    def data_materialized(self) -> bool:
        """Whether the primitive reported materialized-data work."""

        return self.envelope.field_bool("data_materialized", False) is True

    @property
    def write_io(self) -> bool:
        """Whether the primitive wrote data."""

        return self.envelope.field_bool("write_io", False) is True

    @property
    def claim_gate_status(self) -> str | None:
        """Return the most specific claim gate status reported by the primitive."""

        return _first_string_field(
            self.envelope,
            (
                "filter_project_local_execution_claim_gate_status",
                "filter_local_execution_claim_gate_status",
                "project_local_execution_claim_gate_status",
                "why_claim_gate_status",
                "claim_gate_status",
            ),
        )

    @property
    def evidence_summary(self) -> EvidenceSummary:
        """Return compact evidence from the backing Vortex primitive."""

        return self.envelope.evidence_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return compact claim posture from the backing Vortex primitive."""

        return self.envelope.claim_summary


@dataclass(frozen=True, slots=True)
class _VortexPrimitiveWorkflowShape:
    """Parsed subset of lazy operations admitted by local Vortex primitives."""

    predicate: str | None = None
    columns: tuple[str, ...] | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class _VortexSqlPrimitiveWorkflowShape:
    """Parsed SQL subset admitted by local Vortex primitive commands."""

    uri: str
    predicate: str | None = None
    columns: tuple[str, ...] | None = None
    limit: int | None = None
    count: bool = False


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
    smoke_report: SqlLocalSourceSmokeReport
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

        return not self.missing_fields and not self.unexpected_fields and not self.dtype_mismatches

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
    smoke_report: SqlLocalSourceSmokeReport
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

        return True

    @property
    def data_read(self) -> bool:
        """Whether the backing runtime smoke read source data."""

        return True

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
    sink_report: SqlLocalSourceSmokeReport | None = None

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
        return self.sink_report.output_native_io_certificate_status

    @property
    def result_replay_verified(self) -> bool:
        """Whether a written quarantine sink was replay verified."""

        return self.sink_report is not None and self.sink_report.result_replay_verified

    @property
    def runtime_execution(self) -> bool:
        """Whether the backing runtime smoke executed."""

        return True

    @property
    def data_read(self) -> bool:
        """Whether the backing runtime smoke read source data."""

        return True

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
    smoke_report: SqlLocalSourceSmokeReport
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
        if self._can_append_having():
            return self._append(WorkflowOperation("having", (value,)))
        return self._append(WorkflowOperation("filter", (value,)))

    def where(self, predicate: object) -> "LazyFrame":
        """Alias for `filter(...)` using familiar SQL/DataFrame naming."""

        return self.filter(predicate)

    def query(
        self,
        expr: object,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped pandas-style query predicate when admitted."""

        target_ref = _normalize_query_target(expr, kwargs)
        if not kwargs:
            return self.filter(expr)
        return self._unsupported_operation("query", target_ref, check=check)

    def having(
        self,
        predicate: object,
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a lazy plan with an admitted post-aggregate HAVING predicate."""

        value = str(predicate).strip()
        if not value:
            raise ValueError("HAVING predicate must not be empty")
        if self._can_append_having():
            return self._append(WorkflowOperation("having", (value,)))
        return self._unsupported_operation("having", value, check=check)

    def select(self, *columns: object) -> "LazyFrame":
        """Return a lazy plan with an added projection."""

        return self._append(WorkflowOperation("select", _normalize_columns(columns)))

    def project(self, *columns: object) -> "LazyFrame":
        """Alias for `select(...)` using familiar DataFrame/project naming."""

        return self.select(*columns)

    def rename(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        *,
        check: bool = False,
        **named_columns: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared projection alias rewrite when admitted."""

        items = _normalize_rename_items("rename", columns, named_columns)
        if projection := self._schema_declared_rename_projection(items):
            return self._with_rewritten_projection(projection)
        target_ref = ",".join(f"{source}={target}" for source, target in items)
        return self._unsupported_operation("rename", target_ref, check=check)

    def rename_columns(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        *,
        check: bool = False,
        **named_columns: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `rename(...)` using explicit column-transform naming."""

        return self.rename(columns, check=check, **named_columns)

    def drop(
        self,
        *labels: object,
        columns: object | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared projection drop rewrite when admitted."""

        target_columns = _normalize_drop_columns(labels, columns)
        if projection := self._schema_declared_drop_projection(target_columns):
            return self._with_rewritten_projection(projection)
        target_ref = ",".join(target_columns)
        return self._unsupported_operation("drop", target_ref, check=check)

    def drop_columns(
        self,
        *columns: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `drop(...)` using explicit column-transform naming."""

        return self.drop(*columns, check=check)

    def dropna(
        self,
        *,
        subset: object | None = None,
        how: str = "any",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped schema-declared non-null filter when admitted."""

        target_ref = _normalize_dropna_target(subset=subset, how=how, kwargs=kwargs)
        predicate = self._schema_declared_dropna_predicate(subset, how=how, kwargs=kwargs)
        if predicate is not None:
            return self._with_combined_filter_condition(predicate)
        return self._unsupported_operation("dropna", target_ref, check=check)

    def astype(
        self,
        dtype: object,
        *,
        errors: str = "raise",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped schema-declared cast projection when admitted."""

        target_ref = _normalize_astype_target(dtype=dtype, errors=errors, kwargs=kwargs)
        projection = self._schema_declared_astype_projection(
            dtype,
            errors=errors,
            kwargs=kwargs,
        )
        if projection is not None:
            return self._with_rewritten_projection(projection)
        return self._unsupported_operation("astype", target_ref, check=check)

    def sample(
        self,
        n: int | None = None,
        fraction: float | None = None,
        seed: int | None = None,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for sampling semantics."""

        target_ref = _normalize_sample_target(n=n, fraction=fraction, seed=seed)
        return self._unsupported_operation("sample", target_ref, check=check)

    def explode(
        self,
        *columns: object,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for nested/list column expansion."""

        target_ref = ",".join(_normalize_columns(columns))
        return self._unsupported_operation("explode", target_ref, check=check)

    def merge(
        self,
        other: "LazyFrame | str",
        *,
        on: object | None = None,
        left_on: object | None = None,
        right_on: object | None = None,
        how: str = "inner",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped explicit-key merge alias when admitted."""

        target_ref = _normalize_merge_target(
            other,
            on=on,
            left_on=left_on,
            right_on=right_on,
            how=how,
            kwargs=kwargs,
        )
        if (
            not kwargs
            and on is not None
            and left_on is None
            and right_on is None
            and self._can_lower_merge_to_join(other, on=on, how=how)
        ):
            joined = self.join(other, on=on, how=how, check=check)
            if isinstance(joined, LazyFrame):
                return joined
        return self._unsupported_operation("merge", target_ref, check=check)

    def concat(
        self,
        others: "LazyFrame | str | Sequence[LazyFrame | str]",
        *,
        axis: int = 0,
        join: str = "outer",
        check: bool = False,
        **kwargs: object,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped row-wise concat over matching projected local-source branches."""

        target_ref = _normalize_concat_target(others, axis=axis, join=join, kwargs=kwargs)
        other = _single_lazyframe_target(others)
        projection = self._explicit_set_operation_projection_columns()
        other_projection = (
            other._explicit_set_operation_projection_columns() if other is not None else None
        )
        if (
            axis == 0
            and not kwargs
            and other is not None
            and projection == other_projection
            and projection is not None
        ):
            left = self._sql_local_source_union_branch_statement()
            right = other._sql_local_source_union_branch_statement()
            if left is not None and right is not None:
                return SqlWorkflow(
                    statement=f"{left} UNION ALL {right}",
                    client=self.client,
                )
        return self._unsupported_operation("concat", target_ref, check=check)

    def pivot(
        self,
        *,
        index: object | None = None,
        columns: object | None = None,
        values: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for DataFrame reshape/pivot semantics."""

        target_ref = _normalize_pivot_target(
            index=index,
            columns=columns,
            values=values,
            kwargs=kwargs,
        )
        return self._unsupported_operation("pivot", target_ref, check=check)

    def pivot_table(
        self,
        *,
        values: object | None = None,
        index: object | None = None,
        columns: object | None = None,
        aggfunc: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for aggregate reshape semantics."""

        target_ref = _normalize_pivot_table_target(
            values=values,
            index=index,
            columns=columns,
            aggfunc=aggfunc,
            kwargs=kwargs,
        )
        return self._unsupported_operation("pivot-table", target_ref, check=check)

    def melt(
        self,
        *,
        id_vars: object | None = None,
        value_vars: object | None = None,
        var_name: object | None = None,
        value_name: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for DataFrame unpivot/melt semantics."""

        target_ref = _normalize_melt_target(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            kwargs=kwargs,
        )
        return self._unsupported_operation("melt", target_ref, check=check)

    def rolling(
        self,
        window: object,
        *,
        min_periods: int | None = None,
        center: bool = False,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for rolling-window DataFrame semantics."""

        target_ref = _normalize_rolling_target(
            window,
            min_periods=min_periods,
            center=center,
            kwargs=kwargs,
        )
        return self._unsupported_operation("rolling", target_ref, check=check)

    def duplicated(
        self,
        subset: object | None = None,
        *,
        keep: str | bool = "first",
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for row-duplicate mask semantics."""

        target_ref = _normalize_duplicated_target(subset=subset, keep=keep, kwargs=kwargs)
        return self._unsupported_operation("duplicated", target_ref, check=check)

    def tail(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for source-order tail materialization."""

        _validate_positive_row_count("tail limit", limit)
        return self._unsupported_operation("tail", str(limit), check=check)

    def describe(
        self,
        *columns: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for pandas-style summary statistics."""

        target_ref = _normalize_describe_target(columns, kwargs)
        return self._unsupported_operation("describe", target_ref, check=check)

    def nunique(
        self,
        *columns: object,
        dropna: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped one-column count-distinct aggregate when admitted."""

        target_ref = _normalize_distinct_count_target(
            columns,
            dropna=dropna,
            kwargs=kwargs,
        )
        normalized_columns = _normalize_columns(columns)
        if (
            not kwargs
            and dropna is True
            and len(normalized_columns) == 1
            and self._can_append_nunique(normalized_columns[0])
        ):
            return self.agg(unique_count=count_distinct(normalized_columns[0]))
        return self._unsupported_operation("nunique", target_ref, check=check)

    def value_counts(
        self,
        *columns: object,
        sort: bool = True,
        dropna: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped grouped `count(*)` workflow when admitted."""

        target_ref = _normalize_value_counts_target(
            columns,
            sort=sort,
            dropna=dropna,
            kwargs=kwargs,
        )
        normalized_columns = _normalize_columns(columns)
        if not kwargs and self._can_append_value_counts(normalized_columns):
            frame = self
            if dropna:
                frame = self._with_combined_filter_condition(
                    " AND ".join(
                        f"{column} IS NOT NULL" for column in normalized_columns
                    )
                )
            grouped = frame.group_by(*normalized_columns).count(alias="rows")
            if isinstance(grouped, LazyFrame):
                return grouped.sort("rows", descending=True) if sort else grouped
        return self._unsupported_operation("value-counts", target_ref, check=check)

    def nlargest(
        self,
        n: int,
        columns: object,
        *,
        keep: str = "first",
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped descending top-N workflow when admitted."""

        return self._top_n_by_columns(
            "nlargest",
            n,
            columns,
            descending=True,
            keep=keep,
            check=check,
        )

    def nsmallest(
        self,
        n: int,
        columns: object,
        *,
        keep: str = "first",
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped ascending top-N workflow when admitted."""

        return self._top_n_by_columns(
            "nsmallest",
            n,
            columns,
            descending=False,
            keep=keep,
            check=check,
        )

    def _top_n_by_columns(
        self,
        operation: str,
        n: int,
        columns: object,
        *,
        descending: bool,
        keep: str,
        check: bool,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        normalized_n = _normalize_top_n_count(operation, n)
        normalized_columns = _normalize_columns((columns,))
        normalized_keep = _normalize_top_n_keep(operation, keep)
        target_ref = _normalize_top_n_target(
            n=normalized_n,
            columns=normalized_columns,
            keep=normalized_keep,
        )
        if normalized_keep == "first" and self._can_append_sort(normalized_columns):
            sorted_frame = self._append(
                WorkflowOperation(
                    "sort",
                    ("desc" if descending else "asc", *normalized_columns),
                )
            )
            return sorted_frame.limit(normalized_n)
        return self._unsupported_operation(operation, target_ref, check=check)

    def fillna(
        self,
        value: object | None = None,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared null-fill projection when admitted."""

        target_ref = _normalize_fillna_target(value, kwargs)
        if not kwargs and (projection := self._schema_declared_fillna_projection(value)):
            return self._with_rewritten_projection(projection)
        return self._unsupported_operation("fillna", target_ref, check=check)

    def fill_null(
        self,
        value: object | None = None,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `fillna(...)` using expression-engine null terminology."""

        return self.fillna(value, check=check, **kwargs)

    def isna(
        self,
        *columns: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared `IS NULL` mask projection when admitted."""

        target_ref = _normalize_null_mask_target(columns)
        if projection := self._schema_declared_null_mask_projection(columns, is_not=False):
            return self._with_rewritten_projection(projection)
        return self._unsupported_operation("isna", target_ref, check=check)

    def isnull(
        self,
        *columns: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `isna(...)` using pandas-style naming."""

        return self.isna(*columns, check=check)

    def notna(
        self,
        *columns: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared `IS NOT NULL` mask projection when admitted."""

        target_ref = _normalize_null_mask_target(columns)
        if projection := self._schema_declared_null_mask_projection(columns, is_not=True):
            return self._with_rewritten_projection(projection)
        return self._unsupported_operation("notna", target_ref, check=check)

    def notnull(
        self,
        *columns: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `notna(...)` using pandas-style naming."""

        return self.notna(*columns, check=check)

    def mask(
        self,
        cond: object,
        other: object | None = None,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for conditional replacement semantics."""

        target_ref = _normalize_mask_target(cond=cond, other=other, kwargs=kwargs)
        return self._unsupported_operation("mask", target_ref, check=check)

    def replace(
        self,
        to_replace: object | None = None,
        value: object | None = None,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for broad value replacement semantics."""

        target_ref = _normalize_replace_target(
            to_replace=to_replace,
            value=value,
            kwargs=kwargs,
        )
        return self._unsupported_operation("replace", target_ref, check=check)

    def apply(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for Python callable DataFrame transforms."""

        target_ref = _normalize_callable_transform_target(
            "apply",
            function,
            args,
            kwargs,
        )
        return self._unsupported_operation("apply", target_ref, check=check)

    def pipe(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for Python callable workflow piping."""

        target_ref = _normalize_callable_transform_target(
            "pipe",
            function,
            args,
            kwargs,
        )
        return self._unsupported_operation("pipe", target_ref, check=check)

    def transform(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for pandas-style transform callables."""

        target_ref = _normalize_callable_transform_target(
            "transform",
            function,
            args,
            kwargs,
        )
        return self._unsupported_operation("transform", target_ref, check=check)

    def applymap(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for element-wise DataFrame callables."""

        target_ref = _normalize_callable_transform_target(
            "applymap",
            function,
            args,
            kwargs,
        )
        return self._unsupported_operation("applymap", target_ref, check=check)

    def map(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for element-wise Python callable transforms."""

        target_ref = _normalize_callable_transform_target("map", function, args, kwargs)
        return self._unsupported_operation("map", target_ref, check=check)

    def map_rows(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for row-wise Python callable transforms."""

        target_ref = _normalize_callable_transform_target(
            "map_rows",
            function,
            args,
            kwargs,
        )
        return self._unsupported_operation("map-rows", target_ref, check=check)

    def eval(
        self,
        expr: object,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for pandas expression-engine evaluation."""

        target_ref = _normalize_eval_target(expr, kwargs)
        return self._unsupported_operation("eval", target_ref, check=check)

    def distinct(self) -> "LazyFrame":
        """Return a lazy plan with row-level duplicate removal."""

        return self._append(WorkflowOperation("distinct", ()))

    def union(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION` workflow over two local-source plans."""

        return self._union(other, union_all=False, check=check)

    def union_all(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION ALL` workflow over two local-source plans."""

        return self._union(other, union_all=True, check=check)

    def intersect(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `INTERSECT` workflow over two local-source plans."""

        return self._set_operation(
            other,
            operation="intersect",
            keyword="INTERSECT",
            check=check,
        )

    def except_(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `EXCEPT` workflow over two local-source plans."""

        return self.except_rows(other, check=check)

    def except_rows(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `EXCEPT` workflow over two local-source plans."""

        return self._set_operation(
            other,
            operation="except",
            keyword="EXCEPT",
            check=check,
        )

    def subtract(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Alias for `except_rows(...)` using familiar DataFrame naming."""

        return self.except_rows(other, check=check)

    def drop_duplicates(
        self,
        subset: object | None = None,
        *,
        keep: str | bool = "first",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for row-level `distinct()` when pandas subset/keep semantics are absent."""

        if subset is None and keep == "first" and not kwargs:
            return self.distinct()
        target_ref = _normalize_duplicated_target(
            subset=subset,
            keep=keep,
            kwargs=kwargs,
        )
        return self._unsupported_operation("drop-duplicates", target_ref, check=check)

    def unique(self) -> "LazyFrame":
        """Alias for `distinct()` using familiar DataFrame naming."""

        return self.distinct()

    def set_index(
        self,
        keys: object,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for DataFrame index-state semantics."""

        target_ref = _normalize_index_target("set_index", keys=keys, kwargs=kwargs)
        return self._unsupported_operation("set-index", target_ref, check=check)

    def reset_index(
        self,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for DataFrame index reset semantics."""

        target_ref = _normalize_index_target("reset_index", keys=None, kwargs=kwargs)
        return self._unsupported_operation("reset-index", target_ref, check=check)

    def sort_index(
        self,
        *,
        ascending: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> UnsupportedWorkflowOperationReport:
        """Return a deterministic blocker for DataFrame index ordering semantics."""

        target_ref = _normalize_sort_index_target(ascending=ascending, kwargs=kwargs)
        return self._unsupported_operation("sort-index", target_ref, check=check)

    def with_column(
        self,
        name: str,
        expression: object,
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped computed-column workflow when admitted."""

        column_name = _normalize_output_column_name(name)
        try:
            literal = _generated_literal_expression(expression)
            expression_sql = _sql_literal(literal)
        except (TypeError, ValueError):
            try:
                expression_sql = _sql_computed_projection_expression(expression)
            except (TypeError, ValueError):
                expression_text = _require_non_empty("column expression", expression)
                return self._unsupported_operation(
                    "with-column",
                    f"{column_name}={expression_text}",
                    check=check,
                )
        if self._can_append_projection_column(column_name):
            return self._append(WorkflowOperation("with_column", (column_name, expression_sql)))
        return self._unsupported_operation(
            "with-column",
            f"{column_name}={expression_sql}",
            check=check,
        )

    def with_columns(
        self,
        columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        *,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a workflow with multiple scoped computed columns.

        This is a convenience alias over repeated `with_column(...)` calls. It
        does not widen expression semantics or introduce another execution path.
        """

        items = _normalize_named_projection_items(
            "with_columns",
            columns,
            named_expressions,
        )
        workflow: LazyFrame | UnsupportedWorkflowOperationReport = self
        for name, expression in items:
            if isinstance(workflow, UnsupportedWorkflowOperationReport):
                return workflow
            workflow = workflow.with_column(name, expression, check=check)
        return workflow

    def assign(
        self,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `with_columns(...)` using pandas-style naming."""

        return self.with_columns(**named_expressions)

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

    def route(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str | None = None,
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        check: bool = False,
    ) -> PublicWorkflowRoute:
        """Return the shared public route envelope for this lazy workflow."""

        normalized_bounded = (
            _workflow_has_limit(self.operations)
            if bounded is None and requested_output == "collect"
            else bounded
        )
        return self.client.public_workflow_route(
            "dataframe",
            input_uri=self.source.uri,
            input_format=self.source.source_format,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy="auto" if execution_policy is None else execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=normalized_bounded,
            check=check,
        )

    def run(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str | None = None,
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Run this lazy workflow through the shared public route facade."""

        normalized_bounded = (
            _workflow_has_limit(self.operations)
            if bounded is None and requested_output == "collect"
            else bounded
        )
        return self.client.public_workflow_run(
            "dataframe",
            input_uri=self.source.uri,
            input_format=self.source.source_format,
            sql_statement=self._sql_local_source_statement(),
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy="auto" if execution_policy is None else execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=normalized_bounded,
            check=check,
        )

    def prepare(
        self,
        target_vortex_path: str | os.PathLike[str],
        *,
        evidence_level: str = "runtime_smoke",
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Prepare this source through the shared public route facade."""

        return self.client.public_workflow_prepare(
            "dataframe",
            input_uri=self.source.uri,
            input_format=self.source.source_format,
            output_ref=target_vortex_path,
            plan_summary=self.operation_summary,
            evidence_level=evidence_level,
            check=check,
        )

    def profile(
        self,
        limit: int = 100,
        *,
        check: bool = False,
    ) -> WorkflowProfileReport | UnsupportedWorkflowOperationReport:
        """Return a bounded runtime profile for admitted local-source workflows."""

        _validate_positive_row_count("profile limit", limit)
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return WorkflowProfileReport(
                workflow=self,
                smoke_report=report,
                schema_report=_workflow_schema_report(self, report),
                limit=limit,
            )
        return self._unsupported_operation("profile", str(limit), check=check)

    def collect(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
        memory_gb: int = 4,
        max_parallelism: int = 1,
    ) -> (
        SqlLocalSourceSmokeReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Collect admitted local file rows or run admitted local Vortex primitives."""

        if limit is not None:
            frame = self if _workflow_has_limit(self.operations) else self.limit(limit)
            return frame.collect(
                check=check,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
            )
        if report := self._vortex_local_primitive_collect_report(
            check=check,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        ):
            return report
        if statement := self._sql_local_source_statement():
            execution = self.client.public_workflow_run(
                "dataframe",
                input_uri=self.source.uri,
                input_format=self.source.source_format,
                sql_statement=statement,
                plan_summary=self.operation_summary,
                requested_output="collect",
                materialization_policy="bounded",
                evidence_level="runtime_smoke",
                bounded=True,
                check=check,
            )
            return SqlLocalSourceSmokeReport(execution.envelope)
        return self._unsupported_operation("collect", check=check)

    def count(
        self,
        *,
        check: bool = False,
        memory_gb: int = 4,
        max_parallelism: int = 1,
    ) -> (
        SqlLocalSourceSmokeReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Return a scoped row-count report for admitted local workflows."""

        if report := self._vortex_local_primitive_count_report(
            check=check,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
        ):
            return report
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
        """Write an admitted local source SQL smoke result to a local sink."""

        normalized_output_format = _normalize_local_output_format(output_format)
        statement = self._sql_local_source_statement()
        if statement is None:
            raise ValueError(
                "LazyFrame.write currently requires a local CSV, flat JSONL/NDJSON, flat JSON, feature-gated flat Parquet, feature-gated flat Arrow IPC, feature-gated flat Avro, or feature-gated flat ORC source with "
                "select(...), optional filter(...), and limit(...) operations, "
                "aggregate(...), optional filter(...), and limit(...) operations, or "
                "optional filter(...), group_by(...).agg(...), and limit(...) operations, "
                "select(...), optional filter(...), sort(...), and limit(...) operations, "
                "select(...), optional filter(...), distinct(), optional sort(...), and limit(...) operations, "
                "with_column(...), optional filter(...), and limit(...) operations, or "
                "select(...), optional filter(...), window(...), and limit(...) operations, or "
                "a scoped local-source join with select(...), optional filter(...), and limit(...)"
            )
        return self._public_workflow_write_report(
            target_uri,
            requested_output=_public_write_request_for_format(normalized_output_format),
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

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="parquet")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Parquet sink blocker.
        """

        if self._sql_local_source_statement() is None:
            return self._unsupported_operation("write-parquet", str(target_uri), check=check)
        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_parquet",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Arrow IPC sink blocker.
        """

        if self._sql_local_source_statement() is None:
            return self._unsupported_operation("write-arrow-ipc", str(target_uri), check=check)
        return self.write(
            target_uri,
            output_format="arrow-ipc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="avro")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Avro sink blocker.
        """

        if self._sql_local_source_statement() is None:
            return self._unsupported_operation("write-avro", str(target_uri), check=check)
        return self.write(
            target_uri,
            output_format="avro",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="orc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic ORC sink blocker.
        """

        if self._sql_local_source_statement() is None:
            return self._unsupported_operation("write-orc", str(target_uri), check=check)
        return self.write(
            target_uri,
            output_format="orc",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Write an admitted local source result to multiple local sinks."""

        statement = self._sql_local_source_statement()
        if statement is None:
            return self._unsupported_operation("fanout", check=check)
        normalized_outputs = _normalize_fanout_outputs(outputs)
        output_format, output_path = normalized_outputs[0]
        execution = self.client.public_workflow_run(
            "dataframe",
            input_uri=self.source.uri,
            input_format=self.source.source_format,
            sql_statement=statement,
            plan_summary=self.operation_summary,
            requested_output=_public_write_request_for_format(output_format),
            output_ref=output_path,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            fanout_outputs=normalized_outputs[1:],
            check=check,
        )
        return SqlLocalSourceSmokeReport(execution.envelope)

    def to_pandas(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a pandas DataFrame at an explicit bounded materialization boundary."""

        if self._sql_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-pandas", check=check)
        pandas = _optional_module("pandas")
        if pandas is None:
            return self._unsupported_operation(
                "to-pandas",
                "missing optional dependency: pandas",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_pandas(report.result_rows, pandas)
        return self._unsupported_operation("to-pandas", check=check)

    def to_arrow(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table at an explicit bounded materialization boundary."""

        if self._sql_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow", check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_table(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow", check=check)

    def to_arrow_table(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table for admitted bounded local-source workflows."""

        if self._sql_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow-table", check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-table",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_table(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow-table", check=check)

    def to_arrow_ipc(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> bytes | UnsupportedWorkflowOperationReport:
        """Return Arrow IPC stream bytes for admitted bounded local-source workflows."""

        if self._sql_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-arrow-ipc", check=check)
        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-ipc",
                "missing optional dependency: pyarrow",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_arrow_ipc(report.result_rows, pyarrow)
        return self._unsupported_operation("to-arrow-ipc", check=check)

    def to_numpy(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a NumPy array for admitted bounded local-source workflow rows."""

        if self._sql_local_source_statement(default_limit=limit) is None:
            return self._unsupported_operation("to-numpy", check=check)
        numpy = _optional_module("numpy")
        if numpy is None:
            return self._unsupported_operation(
                "to-numpy",
                "missing optional dependency: numpy",
                check=check,
            )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return _rows_to_numpy(report.result_rows, numpy)
        return self._unsupported_operation("to-numpy", check=check)

    def to_python_objects(
        self,
        *,
        check: bool = False,
    ) -> tuple[Mapping[str, Any], ...] | UnsupportedWorkflowOperationReport:
        """Return bounded Python row objects for admitted local-source workflows."""

        if report := self._bounded_materialization_report(limit=None, check=check):
            return report.result_rows
        return self._unsupported_operation("to-python-objects", check=check)

    def prepare_vortex(
        self,
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        dim: str | os.PathLike[str] | None = None,
        workspace: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        cdc_delta: str | os.PathLike[str] | None = None,
        result_workspace: str | os.PathLike[str] | None = None,
        evidence_level: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        check: bool = True,
    ) -> VortexIngestSmokeReport | CompatibilityPreparedVortexRoute:
        """Prepare this raw local source into a caller-owned `VortexPreparedState`.

        When `workspace` is supplied without `target_vortex_path`, the target is derived as
        `<workspace>/<source-stem>.vortex`. The real CLI `vortex-ingest-smoke` route owns
        fingerprint-backed reuse and fail-closed invalidation through its artifact-adjacent
        manifest. Supplying ``dim=...`` returns the queryable compatibility prepared route used by
        ``ctx.prepare_vortex(..., dim=..., workspace=...).query(...).collect()``.
        """

        if self.engine_mode not in {"auto", "batch"}:
            raise ValueError(
                "LazyFrame.prepare_vortex currently supports engine_mode='auto' or 'batch' "
                "for scoped local batch preparation; live/hybrid preparation remains gated"
            )
        if self.source.source_format == "vortex":
            raise ValueError(
                "LazyFrame.prepare_vortex starts from raw compatibility input; "
                "read_vortex(...) sources are already Vortex-native"
            )
        if not _is_query_builder_local_source(self.source):
            raise ValueError(
                "LazyFrame.prepare_vortex requires a local CSV, JSON/JSONL/NDJSON, Parquet, "
                "Arrow IPC, Avro, or ORC source"
            )
        if self.operations:
            raise ValueError(
                "LazyFrame.prepare_vortex prepares the raw local source before query operators; "
                "call it directly on read_*(...) or use write_vortex(...) for a query-result sink"
            )
        route_requested = any(
            value is not None
            for value in (
                dim,
                input_format,
                cdc_delta,
                result_workspace,
                evidence_level,
                memory_gb,
                max_parallelism,
            )
        )
        if route_requested:
            if dim is None:
                raise ValueError(
                    "LazyFrame.prepare_vortex query routes require dim=... so the traditional "
                    "analytics prepared route has an explicit dimension input"
                )
            if workspace is None:
                raise ValueError(
                    "LazyFrame.prepare_vortex query routes require workspace=... so "
                    "VortexPreparedState artifacts have an explicit caller-owned location"
                )
            if target_vortex_path is not None:
                raise ValueError(
                    "target_vortex_path applies only to the single-source vortex-ingest-smoke "
                    "helper; prepared query routes use workspace=... plus dim=..."
                )
            if allow_overwrite:
                raise ValueError(
                    "allow_overwrite applies only to the single-source vortex-ingest-smoke helper; "
                    "prepared query routes use manifest-based reuse policy"
                )
            if certification_level != "ingest_certified":
                raise ValueError(
                    "certification_level applies only to the single-source vortex-ingest-smoke "
                    "helper; prepared query routes use traditional-analytics route evidence"
                )
            return CompatibilityPreparedVortexRoute.from_inputs(
                client=self.client,
                fact_input=self.source.uri,
                dim_input=dim,
                workspace=workspace,
                input_format=input_format,
                cdc_delta_input=cdc_delta,
                result_workspace=result_workspace,
                evidence_level=evidence_level,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        target = _prepared_vortex_target_path(
            self.source.uri,
            target_vortex_path=target_vortex_path,
            workspace=workspace,
        )
        return self.client.vortex_ingest_smoke(
            self.source.uri,
            target,
            allow_overwrite=allow_overwrite,
            certification_level=certification_level,
            check=check,
        )

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> SqlLocalSourceSmokeReport | UnsupportedWorkflowOperationReport:
        """Write an admitted local source result to a scoped local Vortex sink.

        The CLI must be built with `--features vortex-write`; default binaries
        return ShardLoom's deterministic Vortex sink blocker.
        """

        if self._sql_local_source_statement() is None:
            return self._unsupported_operation("write-vortex", str(target_uri), check=check)
        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_vortex",
            allow_overwrite=allow_overwrite,
            check=check,
        )

    def _public_workflow_write_report(
        self,
        target_uri: str | os.PathLike[str],
        *,
        requested_output: str,
        allow_overwrite: bool,
        check: bool,
    ) -> SqlLocalSourceSmokeReport:
        statement = self._sql_local_source_statement()
        if statement is None:
            raise ValueError(
                "public workflow write facade requires an admitted local-source statement"
            )
        execution = self.client.public_workflow_run(
            "dataframe",
            input_uri=self.source.uri,
            input_format=self.source.source_format,
            sql_statement=statement,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=target_uri,
            materialization_policy="bounded",
            evidence_level="runtime_smoke",
            bounded=True,
            allow_overwrite=allow_overwrite,
            check=check,
        )
        return SqlLocalSourceSmokeReport(execution.envelope)

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
        on: str | Sequence[str] | None = None,
        condition: object | None = None,
        how: str = "inner",
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped local-source join workflow when admitted."""

        normalized_how = _normalize_join_how(how)
        if on is not None and condition is not None:
            raise ValueError("join() accepts either on= equi keys or condition=, not both")
        normalized_condition = (
            None if condition is None else _normalize_join_condition(condition)
        )
        if normalized_how == "cross" and normalized_condition is not None:
            raise ValueError("cross joins do not accept condition=; use filter() after join()")
        normalized_columns = (
            ()
            if on is None
            else tuple(
                _normalize_output_column_name(column)
                for column in _normalize_columns((on,))
            )
        )
        columns = ",".join(normalized_columns)
        right_uri: str
        right_summary: str
        right_operations: tuple[WorkflowOperation, ...] = ()
        right_source_local = False
        if isinstance(other, LazyFrame):
            right_uri = other.source.uri
            right_summary = other.operation_summary
            right_operations = other.operations
            right_source_local = _is_query_builder_local_source(other.source)
        else:
            right_uri = _require_non_empty("join right source", other)
            right_summary = right_uri
            right_source_local = _source_format_for_local_source_ref(right_uri) is not None
        target = f"{normalized_how}:{columns}:{normalized_condition or ''}:{right_summary}"
        if (
            _is_query_builder_local_source(self.source)
            and right_source_local
            and not right_operations
            and (normalized_columns or normalized_condition is not None or normalized_how == "cross")
        ):
            return self._append(
                WorkflowOperation(
                    "join",
                    (
                        right_uri,
                        columns,
                        columns,
                        normalized_how,
                        "f",
                        "d",
                        normalized_condition or "",
                    ),
                )
            )
        return self._unsupported_operation("join", target, check=check)

    def group_by(self, *columns: object) -> "GroupedLazyFrame":
        """Return a grouped lazy workflow handle for scoped aggregation."""

        return GroupedLazyFrame(
            workflow=self,
            columns=_normalize_columns(columns),
        )

    def groupby(self, *columns: object) -> "GroupedLazyFrame":
        """Alias for `group_by(...)` using pandas-style naming."""

        return self.group_by(*columns)

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
        target_values = list(values)
        target_values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        values.extend(
            _format_named_aggregate(name, expression)
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        if self._can_append_scalar_aggregate():
            return self._append(WorkflowOperation("aggregate", tuple(values)))
        return self._unsupported_operation("agg", ",".join(target_values), check=check)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
        nulls: str | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped sort workflow when admitted, otherwise report unsupported."""

        normalized_columns = _normalize_columns(columns)
        direction = "desc" if descending else "asc"
        null_ordering = _normalize_sort_nulls(nulls)
        target = f"{direction}:{','.join(normalized_columns)}"
        if null_ordering is not None:
            target = f"{target}:nulls_{null_ordering}"
        if self._can_append_sort(normalized_columns):
            return self._append(
                WorkflowOperation(
                    "sort",
                    _format_sort_operation_values(
                        direction,
                        normalized_columns,
                        null_ordering,
                    ),
                )
            )
        return self._unsupported_operation("sort", target, check=check)

    def order_by(
        self,
        *columns: object,
        descending: bool = False,
        nulls: str | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `sort(...)` using SQL-style naming."""

        return self.sort(*columns, descending=descending, nulls=nulls, check=check)

    def sort_by(
        self,
        *columns: object,
        descending: bool = False,
        nulls: str | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `sort(...)` using familiar DataFrame naming."""

        return self.sort(*columns, descending=descending, nulls=nulls, check=check)

    def sort_values(
        self,
        *columns: object,
        descending: bool = False,
        nulls: str | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `sort(...)` using pandas-style naming."""

        return self.sort(*columns, descending=descending, nulls=nulls, check=check)

    def window(
        self,
        *expressions: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped window projection workflow when admitted."""

        values = _normalize_window_expressions(expressions)
        target = ",".join(values)
        if self._can_append_window(values):
            return self._append(WorkflowOperation("window", values))
        return self._unsupported_operation("window", target, check=check)

    def schema_contract(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Alias for exact bounded schema validation over admitted local-source workflows."""

        return self.validate_schema(schema, check=check)

    def schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return a bounded schema report for admitted local-source workflows."""

        if report := self._bounded_schema_report(check=check):
            return report
        return self._unsupported_operation("schema", check=check)

    def describe_schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return detailed bounded schema evidence for admitted local-source workflows."""

        if report := self._bounded_schema_report(check=check):
            return report
        return self._unsupported_operation("describe-schema", check=check)

    def validate_schema(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Validate an expected schema against admitted local-source rows."""

        normalized = _normalize_schema(schema)
        if not normalized:
            raise ValueError("schema validation contract must not be empty")
        if report := self._bounded_schema_report(check=check):
            return _validate_workflow_schema(report, normalized)
        target = ",".join(f"{name}:{dtype}" for name, dtype in normalized)
        return self._unsupported_operation("validate-schema", target, check=check)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Run bounded data-quality checks for admitted local-source workflows."""

        normalized_checks = _normalize_columns(checks)
        parsed_checks = _parse_data_quality_checks(normalized_checks)
        if parsed_checks is not None:
            if report := self._bounded_schema_report(check=check):
                return _workflow_data_quality_report(report, parsed_checks)
        return self._unsupported_operation(
            "data-quality",
            ",".join(normalized_checks),
            check=check,
        )

    def data_quality(
        self,
        *checks: object,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Alias for bounded data-quality checks."""

        return self.data_quality_check(*checks, check=check)

    def data_quality_summary(
        self,
        *,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Return bounded null-count and schema summary for admitted workflows."""

        if report := self._bounded_schema_report(check=check):
            return WorkflowDataQualityReport(schema_report=report)
        return self._unsupported_operation("data-quality-summary", check=check)

    def quarantine(
        self,
        target_uri: str | os.PathLike[str] | None = None,
        *checks: object,
        output_format: str | None = None,
        limit: int = 100,
        allow_overwrite: bool = False,
        check: bool = True,
    ) -> WorkflowQuarantineReport | UnsupportedWorkflowOperationReport:
        """Return bounded quarantine evidence for admitted local-source workflows."""

        _validate_positive_row_count("quarantine limit", limit)
        parsed_checks: tuple[_WorkflowDataQualityCheckSpec, ...] | None = None
        if checks:
            normalized_checks = _normalize_columns(checks)
            parsed_checks = _parse_data_quality_checks(normalized_checks)
            if parsed_checks is None:
                return self._unsupported_operation(
                    "quarantine",
                    ",".join(normalized_checks),
                    check=check,
                )
        if report := self._bounded_materialization_report(limit=limit, check=check):
            schema_report = _workflow_schema_report(self, report)
            parsed_checks = parsed_checks or _workflow_quarantine_checks(schema_report, ())
            quality_report = _workflow_data_quality_report(schema_report, parsed_checks)
            rows = _workflow_quarantine_rows(schema_report, parsed_checks)
            normalized_output_format = _normalize_optional_quarantine_output_format(
                target_uri,
                output_format,
            )
            sink_report: SqlLocalSourceSmokeReport | None = None
            if target_uri is not None and rows:
                statement = self._quarantine_pushdown_statement(parsed_checks, limit=limit)
                if statement is not None and normalized_output_format is not None:
                    sink_report = self.client.sql_local_source_smoke(
                        statement,
                        output_path=target_uri,
                        output_format=normalized_output_format,
                        allow_overwrite=allow_overwrite,
                        check=check,
                    )
            return WorkflowQuarantineReport(
                workflow=self,
                quality_report=quality_report,
                checks=tuple(spec.raw for spec in parsed_checks),
                rows=rows,
                limit=limit,
                target_uri=None if target_uri is None else str(target_uri),
                output_format=normalized_output_format,
                sink_report=sink_report,
            )
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

    def display(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> WorkflowNotebookPreview | UnsupportedWorkflowOperationReport:
        """Return a bounded notebook/display preview for admitted workflows."""

        _validate_positive_row_count("display limit", limit)
        if report := self._bounded_materialization_report(limit=limit, check=check):
            return WorkflowNotebookPreview(
                workflow=self,
                smoke_report=report,
                limit=limit,
            )
        return self._unsupported_operation("display", str(limit), check=check)

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

    def _with_rewritten_projection(self, projection: tuple[str, ...]) -> "LazyFrame":
        operations = tuple(
            operation for operation in self.operations if operation.kind != "select"
        )
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=(*operations, WorkflowOperation("select", projection)),
            engine_mode=self.engine_mode,
        )

    def _with_combined_filter_condition(self, predicate: str) -> "LazyFrame":
        operations: list[WorkflowOperation] = []
        filter_seen = False
        for operation in self.operations:
            if operation.kind != "filter":
                operations.append(operation)
                continue
            filter_seen = True
            operations.append(
                WorkflowOperation(
                    "filter",
                    (f"({operation.values[0]}) AND ({predicate})",),
                )
            )
        if not filter_seen:
            operations.append(WorkflowOperation("filter", (predicate,)))
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=tuple(operations),
            engine_mode=self.engine_mode,
        )

    def _union(
        self,
        other: "LazyFrame",
        *,
        union_all: bool,
        check: bool,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        operation = "union-all" if union_all else "union"
        keyword = "UNION ALL" if union_all else "UNION"
        return self._set_operation(other, operation=operation, keyword=keyword, check=check)

    def _set_operation(
        self,
        other: "LazyFrame",
        *,
        operation: str,
        keyword: str,
        check: bool,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        if isinstance(other, LazyFrame):
            left = self._sql_local_source_union_branch_statement()
            right = other._sql_local_source_union_branch_statement()
            if left is not None and right is not None:
                return SqlWorkflow(
                    statement=f"{left} {keyword} {right}",
                    client=self.client,
                )
            target = f"{self.operation_summary};{other.operation_summary}"
        else:
            target = str(other)
        return self._unsupported_operation(operation, target, check=check)

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

    def _vortex_local_primitive_collect_report(
        self,
        *,
        check: bool,
        memory_gb: int,
        max_parallelism: int,
    ) -> VortexWorkflowExecutionReport | None:
        shape = self._vortex_primitive_shape()
        if shape is None:
            return None
        memory_gb = _normalize_positive_int("memory_gb", memory_gb)
        max_parallelism = _normalize_positive_int("max_parallelism", max_parallelism)
        envelope: OutputEnvelope | None = None
        if shape.predicate and shape.columns:
            envelope = self.client.vortex_filter_project(
                self.source.uri,
                shape.predicate,
                shape.columns,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        elif shape.predicate:
            envelope = self.client.vortex_filter(
                self.source.uri,
                shape.predicate,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        elif shape.columns:
            envelope = self.client.vortex_project(
                self.source.uri,
                shape.columns,
                source_order_limit=shape.limit,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        if envelope is None:
            return None
        return VortexWorkflowExecutionReport(
            workflow=self,
            operation="collect",
            envelope=envelope,
        )

    def _vortex_local_primitive_count_report(
        self,
        *,
        check: bool,
        memory_gb: int,
        max_parallelism: int,
    ) -> VortexWorkflowExecutionReport | None:
        shape = self._vortex_primitive_shape()
        if shape is None or shape.columns is not None or shape.limit is not None:
            return None
        memory_gb = _normalize_positive_int("memory_gb", memory_gb)
        max_parallelism = _normalize_positive_int("max_parallelism", max_parallelism)
        if shape.predicate:
            envelope = self.client.vortex_count_where(
                self.source.uri,
                shape.predicate,
                execute_local_primitive=True,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        else:
            envelope = self.client.vortex_run(
                self.source.uri,
                "count",
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                check=check,
            )
        return VortexWorkflowExecutionReport(
            workflow=self,
            operation="count",
            envelope=envelope,
        )

    def _vortex_primitive_shape(self) -> _VortexPrimitiveWorkflowShape | None:
        if self.source.source_format != "vortex":
            return None
        predicate: str | None = None
        columns: tuple[str, ...] | None = None
        limit: int | None = None
        for operation in self.operations:
            if operation.kind == "filter":
                if predicate is not None or limit is not None:
                    return None
                predicate = operation.values[0]
            elif operation.kind == "select":
                if columns is not None or limit is not None:
                    return None
                columns = operation.values
            elif operation.kind == "limit":
                if limit is not None:
                    return None
                parsed_limit = int(operation.values[0])
                if parsed_limit <= 0:
                    return None
                limit = parsed_limit
            else:
                return None
        return _VortexPrimitiveWorkflowShape(
            predicate=predicate,
            columns=columns,
            limit=limit,
        )

    def _can_append_scalar_aggregate(self) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by", "sort"}
            for operation in self.operations
        )

    def _can_append_group_by_aggregate(self, columns: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by", "sort"}
            for operation in self.operations
        )

    def _can_append_value_counts(self, columns: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        if not columns or any(not _is_sql_identifier(column) for column in columns):
            return False
        filter_count = sum(1 for operation in self.operations if operation.kind == "filter")
        return filter_count <= 1 and all(
            operation.kind == "filter" for operation in self.operations
        )

    def _can_append_nunique(self, column: str) -> bool:
        if not _is_query_builder_local_source(self.source) or not _is_sql_identifier(column):
            return False
        filter_count = sum(1 for operation in self.operations if operation.kind == "filter")
        return filter_count <= 1 and all(
            operation.kind == "filter" for operation in self.operations
        )

    def _explicit_set_operation_projection_columns(self) -> tuple[str, ...] | None:
        projection: tuple[str, ...] | None = None
        for operation in self.operations:
            if operation.kind == "filter":
                continue
            if operation.kind == "select" and projection is None:
                if any(not _is_sql_identifier(column) for column in operation.values):
                    return None
                projection = operation.values
                continue
            return None
        return projection

    def _can_append_sort(self, columns: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source) or not columns:
            return False
        if len(set(columns)) != len(columns):
            return False
        if any(operation.kind == "limit" for operation in self.operations):
            return False
        return all(operation.kind != "sort" for operation in self.operations)

    def _can_append_window(self, expressions: tuple[str, ...]) -> bool:
        if not _is_query_builder_local_source(self.source) or not expressions:
            return False
        for operation in self.operations:
            if operation.kind in {"select", "filter", "window"}:
                continue
            return False
        return True

    def _can_append_having(self) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        saw_aggregate = False
        for operation in self.operations:
            if operation.kind == "aggregate":
                saw_aggregate = True
                continue
            if saw_aggregate and operation.kind in {"filter", "having", "sort", "limit"}:
                return False
        return saw_aggregate

    def _can_append_projection_column(self, column_name: str) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        saw_join = False
        saw_projection = False
        for operation in self.operations:
            if operation.kind == "join":
                saw_join = True
                continue
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
            elif operation.kind == "window":
                return False
            elif operation.kind == "having":
                return False
            else:
                return False
        if saw_join and not saw_projection:
            return False
        return True

    def _can_lower_merge_to_join(
        self,
        other: "LazyFrame | str",
        *,
        on: object,
        how: str,
    ) -> bool:
        if not _is_query_builder_local_source(self.source):
            return False
        try:
            normalized_how = _normalize_join_how(how)
            normalized_columns = tuple(
                _normalize_output_column_name(column)
                for column in _normalize_columns((on,))
            )
        except (TypeError, ValueError):
            return False
        if not normalized_columns:
            return False
        if isinstance(other, LazyFrame):
            return (
                _is_query_builder_local_source(other.source)
                and not other.operations
            )
        return _source_format_for_local_source_ref(str(other)) is not None

    def _schema_declared_projection_columns(self) -> tuple[str, ...] | None:
        if not _is_query_builder_local_source(self.source) or not self.source.schema:
            return None
        declared_columns = tuple(name for name, _dtype in self.source.schema)
        if not declared_columns or any(not _is_sql_identifier(name) for name in declared_columns):
            return None
        projection_columns = declared_columns
        for operation in self.operations:
            if operation.kind == "select":
                if any(not _is_sql_identifier(value) for value in operation.values):
                    return None
                projection_columns = operation.values
            elif operation.kind in {"filter", "limit"}:
                continue
            else:
                return None
        return projection_columns

    def _schema_declared_rename_projection(
        self,
        items: tuple[tuple[str, str], ...],
    ) -> tuple[str, ...] | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None:
            return None
        rename_map = dict(items)
        if any(not _is_sql_identifier(source) for source in rename_map):
            raise ValueError("rename source column names admit only bare SQL identifiers")
        missing = tuple(source for source in rename_map if source not in projection_columns)
        if missing:
            raise ValueError(
                "rename referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        output_names = tuple(rename_map.get(column, column) for column in projection_columns)
        if len(set(output_names)) != len(output_names):
            raise ValueError("rename output column names must be unique")
        return tuple(
            column if column == output_name else f"{column} AS {output_name}"
            for column, output_name in zip(projection_columns, output_names)
        )

    def _schema_declared_drop_projection(
        self,
        columns: tuple[str, ...],
    ) -> tuple[str, ...] | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None:
            return None
        if any(not _is_sql_identifier(column) for column in columns):
            return None
        missing = tuple(column for column in columns if column not in projection_columns)
        if missing:
            raise ValueError(
                "drop referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        remaining = tuple(column for column in projection_columns if column not in set(columns))
        if not remaining:
            raise ValueError("drop must leave at least one projected column")
        return remaining

    def _schema_declared_fillna_projection(
        self,
        value: object | None,
    ) -> tuple[str, ...] | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None or value is None:
            return None
        if isinstance(value, Mapping):
            if not value:
                return None
            fill_values: dict[str, str] = {}
            for raw_column, raw_value in value.items():
                column = _require_non_empty("fillna column", raw_column)
                if not _is_sql_identifier(column):
                    raise ValueError("fillna column names admit only bare SQL identifiers")
                fill_literal = _sql_fillna_literal(raw_value)
                if fill_literal is None:
                    return None
                fill_values[column] = fill_literal
            missing = tuple(column for column in fill_values if column not in projection_columns)
            if missing:
                raise ValueError(
                    "fillna referenced unknown declared/projection column(s): "
                    + ", ".join(missing)
                )
        else:
            fill_literal = _sql_fillna_literal(value)
            if fill_literal is None:
                return None
            fill_values = {column: fill_literal for column in projection_columns}
        return tuple(
            f"COALESCE({column}, {fill_values[column]}) AS {column}"
            if column in fill_values
            else column
            for column in projection_columns
        )

    def _schema_declared_null_mask_projection(
        self,
        columns: tuple[object, ...],
        *,
        is_not: bool,
    ) -> tuple[str, ...] | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None:
            return None
        target_columns = _normalize_columns(columns) if columns else projection_columns
        if any(not _is_sql_identifier(column) for column in target_columns):
            return None
        missing = tuple(column for column in target_columns if column not in projection_columns)
        if missing:
            raise ValueError(
                "null-mask referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        null_operator = "IS NOT NULL" if is_not else "IS NULL"
        return tuple(f"{column} {null_operator} AS {column}" for column in target_columns)

    def _schema_declared_dropna_predicate(
        self,
        subset: object | None,
        *,
        how: str,
        kwargs: Mapping[str, object],
    ) -> str | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None or kwargs:
            return None
        if any(operation.kind == "limit" for operation in self.operations):
            return None
        normalized_how = _normalize_dropna_how(how)
        if normalized_how != "any":
            return None
        target_columns = (
            _normalize_columns((subset,)) if subset is not None else projection_columns
        )
        if any(not _is_sql_identifier(column) for column in target_columns):
            return None
        missing = tuple(column for column in target_columns if column not in projection_columns)
        if missing:
            raise ValueError(
                "dropna referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        return " AND ".join(f"{column} IS NOT NULL" for column in target_columns)

    def _schema_declared_astype_projection(
        self,
        dtype: object,
        *,
        errors: str,
        kwargs: Mapping[str, object],
    ) -> tuple[str, ...] | None:
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None or kwargs:
            return None
        normalized_errors = _normalize_astype_errors(errors)
        if normalized_errors != "raise":
            return None
        dtype_map = _normalize_astype_dtype_map(dtype, projection_columns)
        if dtype_map is None:
            return None
        missing = tuple(column for column in dtype_map if column not in projection_columns)
        if missing:
            raise ValueError(
                "astype referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        return tuple(
            f"CAST({column} AS {dtype_map[column]}) AS {column}"
            if column in dtype_map
            else column
            for column in projection_columns
        )

    def _bounded_schema_report(self, *, check: bool) -> WorkflowSchemaReport | None:
        statement = self._sql_local_source_statement(default_limit=100)
        if statement is None:
            return None
        smoke_report = self.client.sql_local_source_smoke(statement, check=check)
        if smoke_report.envelope.status != "success":
            return None
        return _workflow_schema_report(self, smoke_report)

    def _bounded_materialization_report(
        self,
        *,
        limit: int | None,
        check: bool,
    ) -> SqlLocalSourceSmokeReport | None:
        if limit is not None:
            _validate_positive_row_count("materialization limit", limit)
        statement = self._sql_local_source_statement(default_limit=limit)
        if statement is None:
            return None
        smoke_report = self.client.sql_local_source_smoke(statement, check=check)
        if smoke_report.envelope.status != "success":
            return None
        return smoke_report

    def _quarantine_pushdown_statement(
        self,
        checks: tuple[_WorkflowDataQualityCheckSpec, ...],
        *,
        limit: int,
    ) -> str | None:
        predicate = _quarantine_pushdown_predicate(checks)
        if predicate is None:
            return None
        operations: list[WorkflowOperation] = []
        filters: list[str] = []
        saw_select = False
        for operation in self.operations:
            if operation.kind == "select" and not saw_select:
                operations.append(operation)
                saw_select = True
            elif operation.kind == "filter":
                filters.append(operation.values[0])
            elif operation.kind == "limit":
                continue
            else:
                return None
        filters.append(predicate)
        operations.append(
            WorkflowOperation(
                "filter",
                (" AND ".join(f"({value})" for value in filters),),
            )
        )
        operations.append(WorkflowOperation("limit", (str(limit),)))
        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=tuple(operations),
            engine_mode=self.engine_mode,
        )._sql_local_source_statement(default_limit=None)

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

    def _sql_local_source_statement(self, *, default_limit: int | None = None) -> str | None:
        if not _is_query_builder_local_source(self.source):
            return None
        projection_list: tuple[str, ...] | None = None
        aggregate_list: tuple[str, ...] | None = None
        group_by_list: tuple[str, ...] | None = None
        literal_columns: list[tuple[str, str]] = []
        window_expressions: list[str] = []
        join_info: tuple[str, ...] | None = None
        sort_key: tuple[str, tuple[str, ...], str | None] | None = None
        distinct_requested = False
        predicate: str | None = None
        having: str | None = None
        limit: str | None = None
        for operation in self.operations:
            if operation.kind == "select" and projection_list is None:
                if window_expressions or distinct_requested:
                    return None
                projection_list = operation.values
            elif operation.kind == "aggregate" and aggregate_list is None:
                if distinct_requested:
                    return None
                aggregate_list = operation.values
            elif operation.kind == "group_by" and group_by_list is None:
                if distinct_requested:
                    return None
                group_by_list = operation.values
            elif operation.kind == "with_column":
                if window_expressions or distinct_requested:
                    return None
                literal_columns.append((operation.values[0], operation.values[1]))
            elif operation.kind == "window":
                if (
                    aggregate_list is not None
                    or group_by_list is not None
                    or literal_columns
                    or join_info is not None
                    or sort_key is not None
                    or distinct_requested
                    or having is not None
                    or limit is not None
                ):
                    return None
                window_expressions.extend(operation.values)
            elif operation.kind == "sort" and sort_key is None:
                if window_expressions:
                    return None
                sort_key = _parse_sort_operation_values(operation.values)
            elif operation.kind == "join" and join_info is None:
                if aggregate_list is not None or group_by_list is not None or distinct_requested:
                    return None
                join_info = operation.values  # type: ignore[assignment]
            elif operation.kind == "distinct" and not distinct_requested:
                if limit is not None:
                    return None
                distinct_requested = True
            elif operation.kind == "filter" and predicate is None:
                if (
                    aggregate_list is not None
                    or group_by_list is not None
                    or distinct_requested
                    or having is not None
                    or sort_key is not None
                    or window_expressions
                    or limit is not None
                ):
                    return None
                predicate = operation.values[0]
            elif operation.kind == "having" and having is None:
                if aggregate_list is None or sort_key is not None or limit is not None:
                    return None
                having = operation.values[0]
            elif operation.kind == "limit" and limit is None:
                limit = operation.values[0]
            else:
                return None
        if limit is None:
            if default_limit is None:
                return None
            limit = str(default_limit)
        if group_by_list is not None and aggregate_list is None:
            return None
        if join_info is not None:
            if len(join_info) == 6:
                right_uri, left_key, right_key, how, left_alias, right_alias = join_info
                join_condition = ""
            elif len(join_info) == 7:
                (
                    right_uri,
                    left_key,
                    right_key,
                    how,
                    left_alias,
                    right_alias,
                    join_condition,
                ) = join_info
            else:
                return None
            left_keys = tuple(column for column in left_key.split(",") if column)
            right_keys = tuple(column for column in right_key.split(",") if column)
            if how == "cross":
                if left_keys or right_keys or join_condition:
                    return None
                on_clause = ""
            elif join_condition:
                if left_keys or right_keys:
                    return None
                on_clause = f" ON {join_condition}"
            elif len(left_keys) != len(right_keys) or not left_keys:
                return None
            else:
                on_clause = " ON " + " AND ".join(
                    f"{left_alias}.{left_column} = {right_alias}.{right_column}"
                    for left_column, right_column in zip(left_keys, right_keys)
                )
            if aggregate_list is not None:
                if projection_list is not None or literal_columns or window_expressions:
                    return None
                if group_by_list is not None:
                    select_clause = ",".join((*group_by_list, *aggregate_list))
                    group_by_clause = f" GROUP BY {','.join(group_by_list)}"
                else:
                    select_clause = ",".join(aggregate_list)
                    group_by_clause = ""
            else:
                if (
                    projection_list is None
                    or group_by_list is not None
                    or having is not None
                    or window_expressions
                ):
                    return None
                select_values = list(projection_list)
                select_values.extend(
                    f"{literal} AS {column}" for column, literal in literal_columns
                )
                select_clause = ",".join(select_values)
                group_by_clause = ""
            order_by_clause = ""
            if sort_key is not None:
                direction, columns, null_ordering = sort_key
                order_by_clause = _format_order_by_clause(
                    columns,
                    direction,
                    null_ordering,
                )
            source_uri = _quote_sql_local_source_path(self.source.uri)
            right_source_uri = _quote_sql_local_source_path(right_uri)
            join_keyword = _sql_join_keyword(how)
            select_keyword = "SELECT DISTINCT" if distinct_requested else "SELECT"
            return (
                f"{select_keyword} {select_clause} FROM {source_uri} AS {left_alias} "
                f"{join_keyword} {right_source_uri} AS {right_alias}"
                f"{on_clause}"
                f"{_optional_sql_where_clause(predicate)}{group_by_clause}"
                f"{_optional_sql_having_clause(having)}{order_by_clause} LIMIT {limit}"
            )
        if projection_list is not None:
            if aggregate_list is not None or group_by_list is not None:
                return None
            select_values = list(projection_list)
            select_values.extend(
                f"{literal} AS {column}" for column, literal in literal_columns
            )
            select_values.extend(window_expressions)
            select_clause = ",".join(select_values)
            group_by_clause = ""
        elif aggregate_list is not None:
            if literal_columns or window_expressions:
                return None
            if group_by_list is not None:
                select_clause = ",".join((*group_by_list, *aggregate_list))
                group_by_clause = f" GROUP BY {','.join(group_by_list)}"
            else:
                select_clause = ",".join(aggregate_list)
                group_by_clause = ""
        else:
            if having is not None:
                return None
            if literal_columns or window_expressions:
                select_values = ["*"]
                select_values.extend(
                    f"{literal} AS {column}" for column, literal in literal_columns
                )
                select_values.extend(window_expressions)
                select_clause = ",".join(select_values)
            else:
                select_clause = "*"
            group_by_clause = ""
        order_by_clause = ""
        if sort_key is not None:
            direction, columns, null_ordering = sort_key
            order_by_clause = _format_order_by_clause(columns, direction, null_ordering)
        source_uri = _quote_sql_local_source_path(self.source.uri)
        select_keyword = "SELECT DISTINCT" if distinct_requested else "SELECT"
        return (
            f"{select_keyword} {select_clause} FROM {source_uri}"
            f"{_optional_sql_where_clause(predicate)}{group_by_clause}"
            f"{_optional_sql_having_clause(having)}{order_by_clause} LIMIT {limit}"
        )

    def _sql_local_source_union_branch_statement(self) -> str | None:
        if any(operation.kind in {"limit", "sort"} for operation in self.operations):
            return None
        statement = self._sql_local_source_statement(default_limit=1)
        suffix = " LIMIT 1"
        if statement is None or not statement.endswith(suffix):
            return None
        return statement[: -len(suffix)]


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
        target_values = list(values)
        target_values.extend(
            f"{_require_non_empty('aggregate name', name)}={_require_non_empty('aggregate expression', expression)}"
            for name, expression in named_expressions.items()
        )
        values.extend(
            _format_named_aggregate(name, expression)
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        target = f"group_by:{','.join(self.columns)};agg:{','.join(target_values)}"
        if self.workflow._can_append_group_by_aggregate(self.columns):
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

    def count(
        self,
        *,
        alias: object = "rows",
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a grouped `count(*)` workflow using a familiar aggregation shortcut."""

        return self.agg(**{_normalize_output_column_name(alias): "count(*)"}, check=check)


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


def outer(column: object) -> ColumnExpression:
    """Return the reserved outer-row column expression for correlated source predicates."""

    return ColumnExpression(f"outer.{_normalize_output_column_name(column)}")


def interval_days(value: object) -> IntervalLiteral:
    """Return a scoped `INTERVAL '<n>' DAY` literal."""

    return _interval_literal(value, "DAY")


def interval_hours(value: object) -> IntervalLiteral:
    """Return a scoped `INTERVAL '<n>' HOUR` literal."""

    return _interval_literal(value, "HOUR")


def interval_minutes(value: object) -> IntervalLiteral:
    """Return a scoped `INTERVAL '<n>' MINUTE` literal."""

    return _interval_literal(value, "MINUTE")


def interval_seconds(value: object) -> IntervalLiteral:
    """Return a scoped `INTERVAL '<n>' SECOND` literal."""

    return _interval_literal(value, "SECOND")


def row_in(columns: object, rows: object) -> PredicateExpression:
    """Return a scoped bounded row-value `IN ((...),...)` predicate."""

    return _row_value_in_predicate(columns, rows, negated=False)


def row_not_in(columns: object, rows: object) -> PredicateExpression:
    """Return a scoped bounded row-value `NOT IN ((...),...)` predicate."""

    return _row_value_in_predicate(columns, rows, negated=True)


def row_in_source(
    columns: object,
    source: object,
    source_columns: object,
    *,
    source_alias: object | None = None,
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped bounded row-value local-source `IN (SELECT ...)` predicate."""

    return _row_value_in_source_predicate(
        columns,
        source,
        source_columns,
        source_alias=source_alias,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        negated=False,
    )


def row_not_in_source(
    columns: object,
    source: object,
    source_columns: object,
    *,
    source_alias: object | None = None,
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped bounded row-value local-source `NOT IN (SELECT ...)` predicate."""

    return _row_value_in_source_predicate(
        columns,
        source,
        source_columns,
        source_alias=source_alias,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        negated=True,
    )


def any_source(
    column: object,
    comparison: object,
    source: object,
    source_column: object,
    *,
    source_alias: object | None = None,
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped bounded local-source `ANY (SELECT ...)` predicate."""

    return _quantified_source_predicate(
        _normalize_expression_column(column),
        comparison,
        "ANY",
        source,
        source_column,
        source_alias=source_alias,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )


def all_source(
    column: object,
    comparison: object,
    source: object,
    source_column: object,
    *,
    source_alias: object | None = None,
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped bounded local-source `ALL (SELECT ...)` predicate."""

    return _quantified_source_predicate(
        _normalize_expression_column(column),
        comparison,
        "ALL",
        source,
        source_column,
        source_alias=source_alias,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )


def exists_source(
    source: object,
    *,
    source_alias: object | None = None,
    select: object = "*",
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped local-source `EXISTS (SELECT ... FROM ...)` predicate."""

    return _exists_source_predicate(
        source,
        source_alias=source_alias,
        select=select,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        negated=False,
    )


def not_exists_source(
    source: object,
    *,
    source_alias: object | None = None,
    select: object = "*",
    where: object | None = None,
    group_by: object | None = None,
    having: object | None = None,
    order_by: object | None = None,
    descending: bool = False,
    limit: int | None = None,
) -> PredicateExpression:
    """Return a scoped local-source `NOT EXISTS (SELECT ... FROM ...)` predicate."""

    return _exists_source_predicate(
        source,
        source_alias=source_alias,
        select=select,
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        negated=True,
    )


def row_number(
    *,
    order_by: object,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "row_number",
) -> WindowExpression:
    """Return a scoped `ROW_NUMBER() OVER (...) AS alias` expression."""

    return _ranking_window_expression(
        "ROW_NUMBER",
        order_by=order_by,
        partition_by=partition_by,
        descending=descending,
        alias=alias,
    )


def rank(
    *,
    order_by: object,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "rank",
) -> WindowExpression:
    """Return a scoped `RANK() OVER (...) AS alias` expression."""

    return _ranking_window_expression(
        "RANK",
        order_by=order_by,
        partition_by=partition_by,
        descending=descending,
        alias=alias,
    )


def dense_rank(
    *,
    order_by: object,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "dense_rank",
) -> WindowExpression:
    """Return a scoped `DENSE_RANK() OVER (...) AS alias` expression."""

    return _ranking_window_expression(
        "DENSE_RANK",
        order_by=order_by,
        partition_by=partition_by,
        descending=descending,
        alias=alias,
    )


def _ranking_window_expression(
    function_name: str,
    *,
    order_by: object,
    partition_by: object | None,
    descending: bool,
    alias: object,
) -> WindowExpression:
    """Return a scoped ranking window expression."""

    if order_by is None:
        raise ValueError(f"{function_name.lower()} order_by must not be empty")
    order_columns = _normalize_columns((order_by,))
    partition_columns = _normalize_optional_columns(partition_by)
    direction = "DESC" if descending else "ASC"
    order_clause = ",".join(f"{column} {direction}" for column in order_columns)
    partition_clause = (
        "" if not partition_columns else f"PARTITION BY {','.join(partition_columns)} "
    )
    output_alias = _normalize_output_column_name(alias)
    return WindowExpression(
        f"{function_name}() OVER ({partition_clause}ORDER BY {order_clause}) AS {output_alias}"
    )


def case_when(predicate: object, then_value: object, else_value: object) -> ColumnExpression:
    """Return a scoped single-branch `CASE WHEN` computed-column expression."""

    then_branch = _sql_case_branch(then_value)
    else_branch = _sql_case_branch(else_value)
    return ColumnExpression(
        f"CASE WHEN {_predicate_sql(predicate)} THEN {then_branch} ELSE {else_branch} END"
    )


def count_distinct(column_expression: object) -> str:
    """Return a scoped `count(DISTINCT column)` aggregate expression."""

    if isinstance(column_expression, ColumnExpression):
        column_sql = column_expression.sql
    else:
        column_sql = _normalize_expression_column(column_expression)
    return f"count(DISTINCT {column_sql})"


def null_if(column_expression: object, value: object) -> ColumnExpression:
    """Return a scoped `NULLIF(column, literal)` computed-column expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("null_if requires a shardloom column expression")
    return column_expression.null_if(value)


def try_cast(column_expression: object, dtype: object) -> ColumnExpression:
    """Return a scoped `TRY_CAST(column AS dtype)` dirty-value expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("try_cast requires a shardloom column expression")
    return column_expression.try_cast(dtype)


def length(column_expression: object) -> ColumnExpression:
    """Return a scoped `LENGTH(column)` UTF-8 length expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("length requires a shardloom column expression")
    return column_expression.length()


def concat(*parts: object) -> ColumnExpression:
    """Return a scoped `CONCAT(column-or-string-literal, ...)` expression."""

    if len(parts) < 2:
        raise ValueError("concat requires at least two arguments")
    sql_parts: list[str] = []
    has_source_column = False
    for index, part in enumerate(parts):
        sql, is_source_column = _sql_string_function_text_arg(
            part, f"concat argument {index + 1}"
        )
        sql_parts.append(sql)
        has_source_column = has_source_column or is_source_column
    if not has_source_column:
        raise ValueError("concat requires at least one shardloom column expression")
    return ColumnExpression(f"CONCAT({', '.join(sql_parts)})")


def substr(column_expression: object, start: object, length: object) -> ColumnExpression:
    """Return a scoped 1-based `SUBSTR(column, start, length)` expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("substr requires a shardloom column expression")
    return column_expression.substr(start, length)


def substring(column_expression: object, start: object, length: object) -> ColumnExpression:
    """Alias for `substr(...)`."""

    return substr(column_expression, start, length)


def left(column_expression: object, count: object) -> ColumnExpression:
    """Return a scoped `LEFT(column, count)` UTF-8 expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("left requires a shardloom column expression")
    return column_expression.left(count)


def right(column_expression: object, count: object) -> ColumnExpression:
    """Return a scoped `RIGHT(column, count)` UTF-8 expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("right requires a shardloom column expression")
    return column_expression.right(count)


def replace(column_expression: object, needle: object, replacement: object) -> ColumnExpression:
    """Return a scoped `REPLACE(column, needle, replacement)` expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("replace requires a shardloom column expression")
    return column_expression.replace(needle, replacement)


def unhex(column_expression: object) -> ColumnExpression:
    """Return a scoped `UNHEX(column)` binary helper projection expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("unhex requires a shardloom column expression")
    return column_expression.unhex()


def from_base64(column_expression: object) -> ColumnExpression:
    """Return a scoped `FROM_BASE64(column)` binary helper projection expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("from_base64 requires a shardloom column expression")
    return column_expression.from_base64()


def byte_length(column_expression: object) -> ColumnExpression:
    """Return a scoped `BYTE_LENGTH(<binary-expression>)` byte-count expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("byte_length requires a shardloom column expression")
    return column_expression.byte_length()


def array(*values: object) -> ComplexProjectionExpression:
    """Return a scoped `ARRAY[...]` projection over scalar SQL literals."""

    if len(values) == 1 and _is_non_string_sequence(values[0]):
        raw_values = tuple(values[0])
    else:
        raw_values = values
    elements = ",".join(_sql_complex_projection_literal(value) for value in raw_values)
    return ComplexProjectionExpression(f"ARRAY[{elements}]")


def struct(*columns: object) -> ComplexProjectionExpression:
    """Return a scoped `STRUCT(...)` projection over source columns."""

    if len(columns) == 1 and _is_non_string_sequence(columns[0]):
        raw_columns = tuple(columns[0])
    else:
        raw_columns = columns
    if not raw_columns:
        raise ValueError("struct projection requires at least one source column")
    normalized = tuple(_normalize_expression_column(column) for column in raw_columns)
    if len(set(normalized)) != len(normalized):
        raise ValueError("struct projection source columns must be unique")
    return ComplexProjectionExpression(f"STRUCT({', '.join(normalized)})")


def abs(column_expression: object) -> ColumnExpression:
    """Return a scoped `ABS(column)` numeric absolute-value expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("abs requires a shardloom column expression")
    return column_expression.abs()


def floor(column_expression: object) -> ColumnExpression:
    """Return a scoped `FLOOR(column)` numeric rounding expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("floor requires a shardloom column expression")
    return column_expression.floor()


def ceil(column_expression: object) -> ColumnExpression:
    """Return a scoped `CEIL(column)` numeric rounding expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("ceil requires a shardloom column expression")
    return column_expression.ceil()


def round(column_expression: object) -> ColumnExpression:  # type: ignore[override]
    """Return a scoped `ROUND(column)` numeric rounding expression."""

    if not isinstance(column_expression, ColumnExpression):
        raise TypeError("round requires a shardloom column expression")
    return column_expression.round()


def column(name: object) -> ColumnExpression:
    """Alias for `col(...)`."""

    return col(name)


def _source_kind_from_path(uri: str | os.PathLike[str]) -> str:
    suffix = Path(uri).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix in {".json", ".jsonl", ".ndjson"}:
        return "json"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".arrow", ".ipc", ".feather"}:
        return "arrow-ipc"
    if suffix == ".avro":
        return "avro"
    if suffix == ".orc":
        return "orc"
    if suffix == ".vortex":
        return "vortex"
    admitted = ".csv, .json, .jsonl, .ndjson, .parquet, .arrow, .ipc, .feather, .avro, .orc, .vortex"
    raise ValueError(
        f"ShardLoom cannot infer a local source adapter for {uri!s}; "
        f"admitted local source extensions are {admitted}"
    )


def read(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy local source by inferring the adapter from the path extension."""

    source_kind = _source_kind_from_path(uri)
    if source_kind == "vortex":
        if schema is not None:
            raise ValueError("read(..., schema=...) is not supported for Vortex sources")
        return read_vortex(uri, client=client, engine_mode=engine_mode, **client_config)
    return _read_source(
        source_kind,
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


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
    """Declare a lazy flat JSON, JSONL, or NDJSON compatibility source."""

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
    """Declare a lazy Parquet compatibility source.

    Scoped local Parquet projection/filter/limit workflows lower to
    `sql-local-source-smoke`; binaries built without `universal-format-io`
    return ShardLoom's deterministic Parquet adapter blocker.
    """

    return _read_source(
        "parquet",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_arrow_ipc(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy Arrow IPC compatibility source.

    Scoped local Arrow IPC projection/filter/limit workflows lower to
    `sql-local-source-smoke`; binaries built without `universal-format-io`
    return ShardLoom's deterministic Arrow IPC adapter blocker. This is a
    local file adapter, not an in-memory Arrow table fallback.
    """

    return _read_source(
        "arrow-ipc",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_avro(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy Avro compatibility source.

    Scoped local Avro projection/filter/limit workflows lower to
    `sql-local-source-smoke`; binaries built without `universal-format-io`
    return ShardLoom's deterministic Avro adapter blocker. This is a local
    flat scalar file smoke, not broad Avro schema-evolution support.
    """

    return _read_source(
        "avro",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_orc(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy ORC compatibility source.

    Scoped local ORC projection/filter/limit workflows lower to
    `sql-local-source-smoke`; binaries built without `universal-format-io`
    return ShardLoom's deterministic ORC adapter blocker. This is a local flat
    scalar file smoke, not broad ORC stripe/statistics runtime support.
    """

    return _read_source(
        "orc",
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


def dataframe_source_free_projection(
    *expressions: object,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped one-row DataFrame-style literal projection.

    This is source-free generated output, not broad DataFrame execution. The
    admitted expression surface is deliberately literal-only and lowers to the
    generated-source local-output command so the CLI emits generated-source,
    output-sink, and no-fallback evidence.
    """

    return _generated_rows_source(
        [_dataframe_source_free_projection_row(expressions)],
        client=_client_from_config(client, client_config),
        source_kind="dataframe_source_free_projection",
    )


def dataframe_generated_with_column(
    name: object,
    expression: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> GeneratedRowsSource:
    """Create a scoped one-row generated DataFrame with one literal column.

    This admits the narrow source-free `with_column` helper advertised by the
    generated-output capability matrix. It is not broad DataFrame expression
    execution; source-backed generated rows and range expressions still use
    `from_rows(...).with_column(...)` and `range(...).with_column(...)`.
    """

    column = _require_non_empty("generated DataFrame column name", name)
    literal = _generated_literal_expression(expression)
    return _generated_rows_source(
        [{column: literal}],
        client=_client_from_config(client, client_config),
        source_kind="dataframe_generated_with_column",
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
) -> GeneratedRowsSource | UnsupportedWorkflowOperationReport:
    """Create a scoped generated-row source from a pandas DataFrame-like object."""

    resolved_client = _client_from_config(client, client_config)
    workflow = _materialized_boundary_workflow(
        "pandas",
        _python_object_boundary_ref("pandas", dataframe),
        client=resolved_client,
        engine_mode=engine_mode,
    )
    rows = _pandas_like_records(dataframe)
    if rows is None:
        return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)
    try:
        return _generated_rows_source(
            rows,
            client=resolved_client,
            source_kind="user_rows",
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)


def from_arrow_table(
    table: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> GeneratedRowsSource | UnsupportedWorkflowOperationReport:
    """Create a scoped generated-row source from an Arrow table-like object."""

    resolved_client = _client_from_config(client, client_config)
    workflow = _materialized_boundary_workflow(
        "arrow_table",
        _python_object_boundary_ref("arrow_table", table),
        client=resolved_client,
        engine_mode=engine_mode,
    )
    rows = _arrow_table_like_records(table)
    if rows is None:
        return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)
    try:
        return _generated_rows_source(
            rows,
            client=resolved_client,
            source_kind="user_rows",
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)


def from_arrow_ipc(
    source: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> GeneratedRowsSource | UnsupportedWorkflowOperationReport:
    """Create a scoped generated-row source from an Arrow IPC stream/file."""

    resolved_client = _client_from_config(client, client_config)
    target = (
        str(source)
        if isinstance(source, (str, os.PathLike))
        else _python_object_boundary_ref("arrow_ipc", source)
    )
    workflow = _materialized_boundary_workflow(
        "arrow_ipc",
        target,
        client=resolved_client,
        engine_mode=engine_mode,
    )
    pyarrow = _optional_module("pyarrow")
    if pyarrow is None:
        return workflow._unsupported_operation(
            "from-arrow-ipc",
            "missing optional dependency: pyarrow",
            check=check,
        )
    try:
        rows = _arrow_table_like_records(_read_arrow_ipc_table(source, pyarrow))
    except Exception:
        rows = None
    if rows is None:
        return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)
    try:
        return _generated_rows_source(
            rows,
            client=resolved_client,
            source_kind="user_rows",
        )
    except (TypeError, ValueError):
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
    if normalized not in {
        "user_rows",
        "literal_table",
        "calendar",
        "dataframe_source_free_projection",
        "dataframe_generated_with_column",
    }:
        raise ValueError(
            "generated source kind must be one of ('user_rows', 'literal_table', 'calendar', 'dataframe_source_free_projection', 'dataframe_generated_with_column')"
        )
    return normalized


def _dataframe_source_free_projection_row(
    expressions: tuple[object, ...],
) -> dict[str, object]:
    if not expressions:
        raise ValueError("DataFrame source-free projection must include at least one expression")
    if len(expressions) == 1 and isinstance(expressions[0], Mapping):
        row: dict[str, object] = {}
        for raw_name, raw_value in expressions[0].items():
            name = _normalize_output_column_name(raw_name)
            if name in row:
                raise ValueError("DataFrame source-free projection aliases must be unique")
            _generated_value_type(raw_value)
            row[name] = raw_value
        if not row:
            raise ValueError("DataFrame source-free projection mapping must not be empty")
        return row

    row = {}
    for expression in expressions:
        name, value = _dataframe_source_free_projection_item(expression)
        if name in row:
            raise ValueError("DataFrame source-free projection aliases must be unique")
        row[name] = value
    return row


def _dataframe_source_free_projection_item(expression: object) -> tuple[str, object]:
    if (
        isinstance(expression, Sequence)
        and not isinstance(expression, (str, bytes, bytearray))
        and len(expression) == 2
    ):
        name = _normalize_output_column_name(expression[0])
        value = expression[1]
        if isinstance(value, str) and value.strip().startswith("lit("):
            value = _generated_literal_expression(value)
        else:
            _generated_value_type(value)
        return name, value
    if isinstance(expression, str):
        return _parse_dataframe_literal_alias_expression(expression)
    raise TypeError(
        "DataFrame source-free projection expressions must be mappings, "
        "(alias, literal) pairs, or lit(...).alias(...) strings"
    )


def _parse_dataframe_literal_alias_expression(expression: str) -> tuple[str, object]:
    text = expression.strip()
    if not text:
        raise ValueError("DataFrame source-free projection expression must not be empty")
    try:
        parsed = ast.parse(text, mode="eval").body
    except SyntaxError as exc:
        raise ValueError(
            "DataFrame source-free projection strings must use lit(...).alias('name')"
        ) from exc
    if not (
        isinstance(parsed, ast.Call)
        and isinstance(parsed.func, ast.Attribute)
        and parsed.func.attr == "alias"
        and isinstance(parsed.func.value, ast.Call)
        and isinstance(parsed.func.value.func, ast.Name)
        and parsed.func.value.func.id == "lit"
        and len(parsed.func.value.args) == 1
        and not parsed.func.value.keywords
        and len(parsed.args) == 1
        and not parsed.keywords
    ):
        raise ValueError(
            "DataFrame source-free projection strings must use lit(...).alias('name')"
        )
    alias_node = parsed.args[0]
    if not isinstance(alias_node, ast.Constant) or not isinstance(alias_node.value, str):
        raise ValueError("DataFrame source-free projection alias must be a string literal")
    try:
        value = ast.literal_eval(parsed.func.value.args[0])
    except (SyntaxError, ValueError) as exc:
        raise ValueError(
            "DataFrame source-free projection lit(...) must contain a bool, int, float, or string literal"
        ) from exc
    _generated_value_type(value)
    return _normalize_output_column_name(alias_node.value), value


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


def _default_generated_range_select_items(public_column: str) -> tuple[str, ...]:
    alias = _normalize_output_column_name(public_column)
    return (f"value AS {alias}",)


def _normalize_generated_range_select_items(
    columns: tuple[object, ...],
    public_column: str,
) -> tuple[str, ...]:
    if len(columns) == 1 and _is_non_string_sequence(columns[0]):
        values = tuple(columns[0])
    else:
        values = columns
    if not values:
        raise ValueError("generated range projection must include the range column")
    if len(values) != 1:
        raise ValueError("generated range select currently admits only the range column once")
    raw = values[0].sql if isinstance(values[0], ColumnExpression) else str(values[0])
    column = _rewrite_generated_range_column_sql(raw, public_column)
    if _normalize_expression_column(column) != "value":
        raise ValueError("generated range select currently admits only the range column")
    return _default_generated_range_select_items(public_column)


def _generated_range_select_aliases(select_items: tuple[str, ...]) -> tuple[str, ...]:
    aliases: list[str] = []
    for item in select_items:
        upper = item.upper()
        marker = " AS "
        marker_index = upper.rfind(marker)
        if marker_index >= 0:
            aliases.append(item[marker_index + len(marker) :].strip())
        else:
            aliases.append(item.strip())
    return tuple(aliases)


def _normalize_generated_range_sort_columns(columns: tuple[object, ...]) -> tuple[str, ...]:
    normalized = tuple(
        _normalize_output_column_name(column) for column in _normalize_columns(columns)
    )
    if len(set(normalized)) != len(normalized):
        raise ValueError("generated range ORDER BY keys must be unique")
    return normalized


def _sql_generated_range_expression_sql(expression: object, public_column: str) -> str:
    return _rewrite_generated_range_column_sql(_predicate_sql(expression), public_column)


def _sql_generated_range_projection_expression(
    expression: object,
    public_column: str,
) -> str:
    if isinstance(expression, ColumnExpression):
        return _rewrite_generated_range_column_sql(expression.sql, public_column)
    try:
        literal = _generated_literal_expression(expression)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "generated range computed columns admit shardloom column expressions "
            "or int64 literal expressions only"
        ) from exc
    if isinstance(literal, bool) or not isinstance(literal, int):
        raise ValueError("generated range computed-column literals must be int64 values")
    return str(literal)


def _rewrite_generated_range_column_sql(raw: str, public_column: str) -> str:
    text = _require_non_empty("generated range SQL expression", raw)
    public = _normalize_output_column_name(public_column)
    if public == "value":
        return text
    rewritten: list[str] = []
    in_quote = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == "'":
            rewritten.append(char)
            if in_quote and index + 1 < len(text) and text[index + 1] == "'":
                rewritten.append(text[index + 1])
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and (char == "_" or char.isalpha()):
            end = index + 1
            while end < len(text) and _is_identifier_char(text[end]):
                end += 1
            token = text[index:end]
            rewritten.append("value" if token == public else token)
            index = end
            continue
        rewritten.append(char)
        index += 1
    if in_quote:
        raise ValueError("generated range SQL expression has an unclosed string literal")
    return "".join(rewritten)


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


def _normalize_non_negative_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _normalize_positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _first_string_field(envelope: OutputEnvelope, keys: Sequence[str]) -> str | None:
    for key in keys:
        value = envelope.field(key)
        if value is None:
            continue
        normalized = value.strip()
        if not normalized or normalized.lower() in {"none", "unknown"}:
            continue
        return normalized
    return None


def _first_int_field(envelope: OutputEnvelope, keys: Sequence[str]) -> int | None:
    for key in keys:
        value = envelope.field(key)
        if value is None:
            continue
        normalized = value.strip().lower()
        if not normalized or normalized in {"none", "unknown"}:
            continue
        try:
            return int(normalized)
        except ValueError:
            continue
    return None


def _any_true_field(envelope: OutputEnvelope, keys: Sequence[str]) -> bool:
    for key in keys:
        value = envelope.field(key)
        if value is not None and value.strip().lower() == "true":
            return True
    return False


def _range_row_count(start: int, end: int, step: int) -> int:
    if step == 0:
        raise ValueError("range step must not be zero")
    if (step > 0 and start >= end) or (step < 0 and start <= end):
        return 0
    distance = end - start if step > 0 else start - end
    stride = step if step > 0 else -step
    return (distance + stride - 1) // stride


def _limited_range_end(start: int, end: int, step: int, count: int) -> int:
    if count == 0:
        return start
    if _range_row_count(start, end, step) <= count:
        return end
    return start + (step * count)


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


def _prepared_vortex_target_path(
    source_uri: str,
    *,
    target_vortex_path: str | os.PathLike[str] | None,
    workspace: str | os.PathLike[str] | None,
) -> str | os.PathLike[str]:
    if target_vortex_path is not None and workspace is not None:
        raise ValueError(
            "prepare_vortex accepts either target_vortex_path or workspace, not both"
        )
    if target_vortex_path is not None:
        return target_vortex_path
    if workspace is None:
        raise ValueError(
            "prepare_vortex requires target_vortex_path or workspace=... so the "
            "caller-owned VortexPreparedState artifact location is explicit"
        )
    source_name = Path(source_uri).name or "source"
    stem = Path(source_name).stem or "source"
    return Path(workspace).expanduser() / f"{stem}.vortex"


def _generated_prepared_vortex_target_path(
    stem: str,
    *,
    target_vortex_path: str | os.PathLike[str] | None,
    workspace: str | os.PathLike[str] | None,
) -> str | os.PathLike[str]:
    if target_vortex_path is not None and workspace is not None:
        raise ValueError(
            "generated prepare_vortex accepts either target_vortex_path or workspace, not both"
        )
    if target_vortex_path is not None:
        return target_vortex_path
    if workspace is None:
        raise ValueError(
            "generated prepare_vortex requires target_vortex_path or workspace=... so the "
            "caller-owned VortexPreparedState artifact location is explicit"
        )
    return Path(workspace).expanduser() / f"{_safe_generated_vortex_stem(stem)}.vortex"


def _safe_generated_vortex_stem(stem: str) -> str:
    normalized = "".join(
        char.lower() if char.isalnum() else "-" for char in str(stem).strip()
    ).strip("-")
    while "--" in normalized:
        normalized = normalized.replace("--", "-")
    return normalized or "generated-source"


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


def _normalize_named_projection_items(
    context: str,
    columns: Mapping[str, object] | Sequence[tuple[object, object]] | None,
    named_expressions: Mapping[str, object],
) -> tuple[tuple[str, object], ...]:
    """Normalize ordered alias/expression pairs for multi-column projection helpers."""

    raw_items: list[tuple[object, object]] = []
    if columns is not None:
        if isinstance(columns, Mapping):
            raw_items.extend(columns.items())
        elif _is_non_string_sequence(columns):
            for item in columns:
                if not _is_non_string_sequence(item) or len(item) != 2:
                    raise ValueError(
                        f"{context} sequence entries must be (name, expression) pairs"
                    )
                name, expression = item
                raw_items.append((name, expression))
        else:
            raise TypeError(
                f"{context} columns must be a mapping or sequence of (name, expression) pairs"
            )
    raw_items.extend(named_expressions.items())
    if not raw_items:
        raise ValueError(f"{context} expressions must not be empty")

    normalized: list[tuple[str, object]] = []
    seen: set[str] = set()
    for name, expression in raw_items:
        column_name = _normalize_output_column_name(name)
        if column_name in seen:
            raise ValueError(f"{context} output column names must be unique")
        seen.add(column_name)
        normalized.append((column_name, expression))
    return tuple(normalized)


def _normalize_rename_items(
    context: str,
    columns: Mapping[str, object] | Sequence[tuple[object, object]] | None,
    named_columns: Mapping[str, object],
) -> tuple[tuple[str, str], ...]:
    """Normalize ordered source/target column pairs for rename diagnostics."""

    raw_items: list[tuple[object, object]] = []
    if columns is not None:
        if isinstance(columns, Mapping):
            raw_items.extend(columns.items())
        elif _is_non_string_sequence(columns):
            for item in columns:
                if not _is_non_string_sequence(item) or len(item) != 2:
                    raise ValueError(f"{context} sequence entries must be (source, target) pairs")
                source, target = item
                raw_items.append((source, target))
        else:
            raise TypeError(
                f"{context} columns must be a mapping or sequence of (source, target) pairs"
            )
    raw_items.extend(named_columns.items())
    if not raw_items:
        raise ValueError(f"{context} columns must not be empty")

    normalized: list[tuple[str, str]] = []
    seen_sources: set[str] = set()
    seen_targets: set[str] = set()
    for source, target in raw_items:
        source_name = _require_non_empty("source column name", source)
        target_name = _normalize_output_column_name(target)
        if source_name in seen_sources:
            raise ValueError(f"{context} source column names must be unique")
        if target_name in seen_targets:
            raise ValueError(f"{context} target column names must be unique")
        seen_sources.add(source_name)
        seen_targets.add(target_name)
        normalized.append((source_name, target_name))
    return tuple(normalized)


def _normalize_drop_columns(
    labels: Sequence[object],
    columns: object | None,
) -> tuple[str, ...]:
    raw_columns: list[object] = []
    if len(labels) == 1 and _is_non_string_sequence(labels[0]):
        raw_columns.extend(labels[0])
    else:
        raw_columns.extend(labels)
    if columns is not None:
        if _is_non_string_sequence(columns):
            raw_columns.extend(columns)
        else:
            raw_columns.append(columns)
    values = [_require_non_empty("drop column", column) for column in raw_columns]
    if not values:
        raise ValueError("drop columns must not be empty")
    duplicates = [column for column in dict.fromkeys(values) if values.count(column) > 1]
    if duplicates:
        raise ValueError("drop columns must be unique")
    return tuple(values)


def _normalize_sample_target(
    *,
    n: int | None,
    fraction: float | None,
    seed: int | None,
) -> str:
    if n is not None and fraction is not None:
        raise ValueError("sample accepts either n or fraction, not both")
    parts: list[str] = []
    if fraction is None:
        sample_n = 1 if n is None else _normalize_non_negative_int("sample n", n)
        parts.append(f"n={sample_n}")
    else:
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
            raise TypeError("sample fraction must be numeric")
        fraction_value = float(fraction)
        if not math.isfinite(fraction_value) or fraction_value <= 0:
            raise ValueError("sample fraction must be positive and finite")
        parts.append(f"fraction={fraction_value:.12g}")
    if seed is not None:
        parts.append(f"seed={_normalize_non_negative_int('sample seed', seed)}")
    return ",".join(parts)


def _normalize_merge_target(
    other: "LazyFrame | str",
    *,
    on: object | None,
    left_on: object | None,
    right_on: object | None,
    how: str,
    kwargs: Mapping[str, object],
) -> str:
    normalized_how = _normalize_join_how(how)
    if on is not None and (left_on is not None or right_on is not None):
        raise ValueError("merge accepts either on= or left_on=/right_on=, not both")
    parts = [f"how={normalized_how}"]
    if on is not None:
        parts.append(f"on={_join_columns_for_target('merge on', on)}")
    elif left_on is not None or right_on is not None:
        if left_on is None or right_on is None:
            raise ValueError("merge requires both left_on and right_on when using sided keys")
        parts.append(f"left_on={_join_columns_for_target('merge left_on', left_on)}")
        parts.append(f"right_on={_join_columns_for_target('merge right_on', right_on)}")
    else:
        parts.append("on=implicit_common_columns")
    parts.extend(_normalize_extra_kwargs("merge", kwargs))
    parts.append(_workflow_target_summary(other))
    return ";".join(parts)


def _normalize_concat_target(
    others: "LazyFrame | str | Sequence[LazyFrame | str]",
    *,
    axis: int,
    join: str,
    kwargs: Mapping[str, object],
) -> str:
    if isinstance(axis, bool) or axis not in (0, 1):
        raise ValueError("concat axis must be 0 or 1")
    normalized_join = _require_non_empty("concat join", join).lower()
    if normalized_join not in {"inner", "outer"}:
        raise ValueError("concat join must be inner or outer")
    targets = _normalize_workflow_targets(others)
    parts = [f"axis={axis}", f"join={normalized_join}"]
    parts.extend(_normalize_extra_kwargs("concat", kwargs))
    parts.extend(targets)
    return ";".join(parts)


def _normalize_pivot_target(
    *,
    index: object | None,
    columns: object | None,
    values: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"index={_optional_columns_for_target(index)}",
        f"columns={_optional_columns_for_target(columns)}",
        f"values={_optional_columns_for_target(values)}",
    ]
    parts.extend(_normalize_extra_kwargs("pivot", kwargs))
    return ";".join(parts)


def _normalize_pivot_table_target(
    *,
    values: object | None,
    index: object | None,
    columns: object | None,
    aggfunc: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"index={_optional_columns_for_target(index)}",
        f"columns={_optional_columns_for_target(columns)}",
        f"values={_optional_columns_for_target(values)}",
        f"aggfunc={_require_non_empty('pivot_table aggfunc', aggfunc or 'mean')}",
    ]
    parts.extend(_normalize_extra_kwargs("pivot_table", kwargs))
    return ";".join(parts)


def _normalize_melt_target(
    *,
    id_vars: object | None,
    value_vars: object | None,
    var_name: object | None,
    value_name: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"id_vars={_optional_columns_for_target(id_vars)}",
        f"value_vars={_optional_columns_for_target(value_vars)}",
    ]
    if var_name is not None:
        parts.append(f"var_name={_require_non_empty('melt var_name', var_name)}")
    if value_name is not None:
        parts.append(f"value_name={_require_non_empty('melt value_name', value_name)}")
    parts.extend(_normalize_extra_kwargs("melt", kwargs))
    return ";".join(parts)


def _normalize_rolling_target(
    window: object,
    *,
    min_periods: int | None,
    center: bool,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"window={_require_non_empty('rolling window', window)}"]
    if min_periods is not None:
        parts.append(
            f"min_periods={_normalize_non_negative_int('rolling min_periods', min_periods)}"
        )
    parts.append(f"center={str(bool(center)).lower()}")
    parts.extend(_normalize_extra_kwargs("rolling", kwargs))
    return ";".join(parts)


def _normalize_describe_target(
    columns: Sequence[object],
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"columns={_optional_columns_for_target(columns or None)}"]
    parts.extend(_normalize_extra_kwargs("describe", kwargs))
    return ";".join(parts)


def _normalize_distinct_count_target(
    columns: Sequence[object],
    *,
    dropna: bool,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"columns={_optional_columns_for_target(columns or None)}",
        f"dropna={str(bool(dropna)).lower()}",
    ]
    parts.extend(_normalize_extra_kwargs("nunique", kwargs))
    return ";".join(parts)


def _normalize_value_counts_target(
    columns: Sequence[object],
    *,
    sort: bool,
    dropna: bool,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"columns={_optional_columns_for_target(columns or None)}",
        f"sort={str(bool(sort)).lower()}",
        f"dropna={str(bool(dropna)).lower()}",
    ]
    parts.extend(_normalize_extra_kwargs("value_counts", kwargs))
    return ";".join(parts)


def _normalize_fillna_target(
    value: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"value={_stable_target_value(value)}"]
    parts.extend(_normalize_extra_kwargs("fillna", kwargs))
    return ";".join(parts)


def _normalize_null_mask_target(columns: Sequence[object]) -> str:
    return f"columns={_optional_columns_for_target(columns or None)}"


def _normalize_query_target(expr: object, kwargs: Mapping[str, object]) -> str:
    parts = [f"expr={_require_non_empty('query expression', expr)}"]
    parts.extend(_normalize_extra_kwargs("query", kwargs))
    return ";".join(parts)


def _normalize_dropna_how(value: str) -> str:
    normalized = _require_non_empty("dropna how", value).lower().replace("_", "-")
    if normalized not in {"any", "all"}:
        raise ValueError("dropna how must be 'any' or 'all'")
    return normalized


def _normalize_dropna_target(
    *,
    subset: object | None,
    how: str,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"subset={_optional_columns_for_target(subset)}",
        f"how={_normalize_dropna_how(how)}",
    ]
    parts.extend(_normalize_extra_kwargs("dropna", kwargs))
    return ";".join(parts)


def _normalize_astype_errors(value: str) -> str:
    normalized = _require_non_empty("astype errors", value).lower().replace("_", "-")
    if normalized not in {"raise", "ignore"}:
        raise ValueError("astype errors must be 'raise' or 'ignore'")
    return normalized


def _normalize_astype_dtype_map(
    dtype: object,
    projection_columns: tuple[str, ...],
) -> dict[str, str] | None:
    if isinstance(dtype, Mapping):
        if not dtype:
            return None
        dtype_map: dict[str, str] = {}
        for raw_column, raw_dtype in dtype.items():
            column = _require_non_empty("astype column", raw_column)
            if not _is_sql_identifier(column):
                raise ValueError("astype column names admit only bare SQL identifiers")
            dtype_map[column] = _normalize_cast_dtype(raw_dtype)
        return dtype_map
    normalized_dtype = _normalize_cast_dtype(dtype)
    return {column: normalized_dtype for column in projection_columns}


def _normalize_astype_target(
    *,
    dtype: object,
    errors: str,
    kwargs: Mapping[str, object],
) -> str:
    if isinstance(dtype, Mapping):
        dtype_ref = "{" + ",".join(
            f"{_require_non_empty('astype column', column)}={_normalize_cast_dtype(raw_dtype)}"
            for column, raw_dtype in sorted(dtype.items(), key=lambda item: str(item[0]))
        ) + "}"
    else:
        dtype_ref = _normalize_cast_dtype(dtype)
    parts = [f"dtype={dtype_ref}", f"errors={_normalize_astype_errors(errors)}"]
    parts.extend(_normalize_extra_kwargs("astype", kwargs))
    return ";".join(parts)


def _normalize_top_n_count(operation: str, value: object) -> int:
    _validate_positive_row_count(f"{operation} n", value)
    return int(value)


def _normalize_top_n_keep(operation: str, value: str) -> str:
    normalized = _require_non_empty(f"{operation} keep", value).lower().replace("_", "-")
    if normalized not in {"first", "last", "all"}:
        raise ValueError(f"{operation} keep must be 'first', 'last', or 'all'")
    return normalized


def _normalize_top_n_target(
    *,
    n: int,
    columns: tuple[str, ...],
    keep: str,
) -> str:
    return f"n={n};columns={','.join(columns)};keep={keep}"


def _normalize_duplicated_target(
    *,
    subset: object | None,
    keep: str | bool,
    kwargs: Mapping[str, object],
) -> str:
    if keep is False:
        normalized_keep = "false"
    else:
        normalized_keep = _require_non_empty("duplicate keep", keep).lower().replace("_", "-")
        if normalized_keep not in {"first", "last"}:
            raise ValueError("duplicate keep must be 'first', 'last', or False")
    parts = [
        f"subset={_optional_columns_for_target(subset)}",
        f"keep={normalized_keep}",
    ]
    parts.extend(_normalize_extra_kwargs("duplicated", kwargs))
    return ";".join(parts)


def _normalize_mask_target(
    *,
    cond: object,
    other: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"cond={_require_non_empty('mask condition', cond)}",
        f"other={_stable_target_value(other)}",
    ]
    parts.extend(_normalize_extra_kwargs("mask", kwargs))
    return ";".join(parts)


def _normalize_replace_target(
    *,
    to_replace: object | None,
    value: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"to_replace={_stable_target_value(to_replace)}",
        f"value={_stable_target_value(value)}",
    ]
    parts.extend(_normalize_extra_kwargs("replace", kwargs))
    return ";".join(parts)


def _normalize_index_target(
    context: str,
    *,
    keys: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"keys={_optional_columns_for_target(keys)}"]
    parts.extend(_normalize_extra_kwargs(context, kwargs))
    return ";".join(parts)


def _normalize_sort_index_target(
    *,
    ascending: bool,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"ascending={str(bool(ascending)).lower()}"]
    parts.extend(_normalize_extra_kwargs("sort_index", kwargs))
    return ";".join(parts)


def _sql_fillna_literal(value: object) -> str | None:
    if value is None:
        return None
    try:
        return _sql_literal(value)
    except (TypeError, ValueError):
        return None


def _normalize_callable_transform_target(
    context: str,
    function: object,
    args: Sequence[object],
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"callable={_stable_callable_name(function)}"]
    if args:
        parts.append(f"arg_count={len(args)}")
    parts.extend(_normalize_extra_kwargs(context, kwargs))
    return ";".join(parts)


def _normalize_eval_target(
    expr: object,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"expr={_require_non_empty('eval expression', expr)}"]
    parts.extend(_normalize_extra_kwargs("eval", kwargs))
    return ";".join(parts)


def _stable_callable_name(function: object) -> str:
    if isinstance(function, str):
        return _require_non_empty("callable expression", function)
    name = getattr(function, "__name__", None)
    if isinstance(name, str) and name.strip():
        return name.strip()
    type_name = type(function).__name__
    if type_name:
        return type_name
    return _require_non_empty("callable", function)


def _stable_target_value(value: object | None) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float, str)):
        return str(value)
    if isinstance(value, Mapping):
        items = [
            f"{_require_non_empty('fillna key', key)}={_stable_target_value(item_value)}"
            for key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        ]
        return "{" + ",".join(items) + "}"
    if _is_non_string_sequence(value):
        return "[" + ",".join(_stable_target_value(item) for item in value) + "]"
    return type(value).__name__


def _join_columns_for_target(name: str, value: object) -> str:
    return ",".join(_normalize_columns((value,))) or _require_non_empty(name, value)


def _optional_columns_for_target(value: object | None) -> str:
    columns = _normalize_optional_columns(value)
    return ",".join(columns) if columns else "none"


def _normalize_workflow_targets(
    value: "LazyFrame | str | Sequence[LazyFrame | str]",
) -> tuple[str, ...]:
    if isinstance(value, LazyFrame) or isinstance(value, (str, os.PathLike)):
        targets = (_workflow_target_summary(value),)
    elif _is_non_string_sequence(value):
        targets = tuple(_workflow_target_summary(item) for item in value)
    else:
        raise TypeError("concat others must be a workflow, path, or sequence of workflows/paths")
    if not targets:
        raise ValueError("concat others must not be empty")
    return targets


def _single_lazyframe_target(
    value: "LazyFrame | str | Sequence[LazyFrame | str]",
) -> LazyFrame | None:
    if isinstance(value, LazyFrame):
        return value
    if _is_non_string_sequence(value) and len(value) == 1 and isinstance(value[0], LazyFrame):
        return value[0]
    return None


def _workflow_target_summary(value: "LazyFrame | str | os.PathLike[str]") -> str:
    if isinstance(value, LazyFrame):
        return value.operation_summary
    return _require_non_empty("workflow target", value)


def _normalize_extra_kwargs(context: str, kwargs: Mapping[str, object]) -> tuple[str, ...]:
    normalized: list[str] = []
    for key in sorted(kwargs):
        name = _normalize_output_column_name(key)
        normalized.append(f"{name}={_require_non_empty(f'{context} {name}', kwargs[key])}")
    return tuple(normalized)


def _normalize_optional_columns(columns: object | None) -> tuple[str, ...]:
    if columns is None:
        return ()
    return _normalize_columns((columns,))


def _normalize_window_expressions(expressions: Sequence[object]) -> tuple[str, ...]:
    if len(expressions) == 1 and _is_non_string_sequence(expressions[0]):
        values = [str(expression).strip() for expression in expressions[0]]
    else:
        values = [str(expression).strip() for expression in expressions]
    values = [value for value in values if value]
    if not values:
        raise ValueError("window expressions must not be empty")
    return tuple(values)


def _normalize_join_how(value: object) -> str:
    normalized = _require_non_empty("join how", value).lower().replace("-", "_")
    aliases = {
        "inner": "inner",
        "inner_equi": "inner",
        "left": "left",
        "left_outer": "left",
        "right": "right",
        "right_outer": "right",
        "full": "full",
        "full_outer": "full",
        "outer": "full",
        "semi": "semi",
        "left_semi": "semi",
        "anti": "anti",
        "left_anti": "anti",
        "cross": "cross",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise ValueError(
            "join how must be one of inner, left, right, full, semi, anti, or cross"
        ) from exc


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


def _format_named_aggregate(name: object, expression: object) -> str:
    alias = _normalize_output_column_name(name)
    aggregate_expression = _require_non_empty("aggregate expression", expression)
    return f"{aggregate_expression} AS {alias}"


def _normalize_cast_dtype(value: object) -> str:
    dtype = _require_non_empty("cast dtype", value).lower()
    if dtype == "timestamp":
        dtype = "timestamp_micros"
    if dtype in {"blob", "varbinary"}:
        dtype = "binary"
    decimal_dtype = _normalize_decimal_cast_dtype(dtype)
    if decimal_dtype is not None:
        return decimal_dtype
    if dtype not in {
        "int64",
        "float64",
        "utf8",
        "boolean",
        "date32",
        "timestamp_micros",
        "binary",
    }:
        raise ValueError(
            "cast dtype must be one of ('int64', 'float64', 'utf8', 'boolean', 'date32', 'timestamp_micros', 'binary', 'decimal128(p,s)')"
        )
    return dtype


def _normalize_decimal_cast_dtype(dtype: str) -> str | None:
    compact = "".join(dtype.split())
    names = ("decimal128", "decimal", "numeric")
    name = compact.split("(", 1)[0]
    if name not in names:
        return None
    if "(" not in compact:
        return "decimal128(38,0)"
    if not compact.endswith(")"):
        raise ValueError("decimal cast dtype must use decimal128(precision,scale)")
    args = compact[compact.find("(") + 1 : -1].split(",")
    if len(args) != 2:
        raise ValueError("decimal cast dtype must use decimal128(precision,scale)")
    try:
        precision = int(args[0])
        scale = int(args[1])
    except ValueError as exc:
        raise ValueError("decimal cast precision and scale must be integers") from exc
    if precision < 1 or precision > 38 or scale < 0 or scale > precision:
        raise ValueError(
            "decimal cast precision/scale must satisfy 1 <= precision <= 38 and 0 <= scale <= precision"
        )
    return f"decimal128({precision},{scale})"


def _interval_literal(value: object, unit: str) -> IntervalLiteral:
    return IntervalLiteral(_normalize_interval_integer(value), unit)


def _normalize_interval_unit(unit: object) -> str:
    text = _require_non_empty("interval literal unit", unit).upper()
    if text in _INTERVAL_SECOND_MULTIPLIERS:
        return text
    if text.endswith("S") and text[:-1] in _INTERVAL_SECOND_MULTIPLIERS:
        return text[:-1]
    raise ValueError("interval literal unit must be DAY, HOUR, MINUTE, or SECOND")


def _normalize_interval_integer(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("interval literal value must be a signed integer literal")
    if isinstance(value, int):
        return value
    text = _require_non_empty("interval literal value", value)
    if text in {"+", "-"} or not all(
        ch.isdigit() or (index == 0 and ch in {"+", "-"})
        for index, ch in enumerate(text)
    ):
        raise ValueError("interval literal value must be a signed integer literal")
    return int(text)


def _normalize_date_arithmetic_days(value: object) -> str:
    interval = _coerce_interval_literal(value)
    if interval is not None:
        if interval.unit != "DAY":
            raise ValueError("date arithmetic interval literals admit DAY units only")
        if builtins.abs(interval.value) > MAX_DATE_ARITHMETIC_DAYS:
            raise ValueError("date arithmetic days admits absolute values <= 366000")
        return interval.sql
    if isinstance(value, bool):
        raise ValueError("date arithmetic days must be a signed integer literal")
    if isinstance(value, int):
        days = value
    else:
        text = _require_non_empty("date arithmetic days", value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError("date arithmetic days must be a signed integer literal")
        days = int(text)
    if builtins.abs(days) > MAX_DATE_ARITHMETIC_DAYS:
        raise ValueError("date arithmetic days admits absolute values <= 366000")
    return str(days)


def _normalize_timestamp_arithmetic_seconds(value: object) -> str:
    interval = _coerce_interval_literal(value)
    if interval is not None:
        seconds = interval.value * _INTERVAL_SECOND_MULTIPLIERS[interval.unit]
        if builtins.abs(seconds) > MAX_TIMESTAMP_ARITHMETIC_SECONDS:
            raise ValueError(
                "timestamp arithmetic seconds admits absolute values <= 31622400000"
            )
        return interval.sql
    if isinstance(value, bool):
        raise ValueError("timestamp arithmetic seconds must be a signed integer literal")
    if isinstance(value, int):
        seconds = value
    else:
        text = _require_non_empty("timestamp arithmetic seconds", value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError(
                "timestamp arithmetic seconds must be a signed integer literal"
            )
        seconds = int(text)
    if builtins.abs(seconds) > MAX_TIMESTAMP_ARITHMETIC_SECONDS:
        raise ValueError(
            "timestamp arithmetic seconds admits absolute values <= 31622400000"
        )
    return str(seconds)


def _coerce_interval_literal(value: object) -> IntervalLiteral | None:
    if isinstance(value, IntervalLiteral):
        return value
    if isinstance(value, str):
        return _parse_interval_literal_sql(value)
    return None


def _parse_interval_literal_sql(value: str) -> IntervalLiteral | None:
    text = value.strip()
    lowered = text.lower()
    if not lowered.startswith("interval"):
        return None
    if len(text) > len("interval") and not text[len("interval")].isspace():
        return None
    parts = text.split(maxsplit=2)
    if len(parts) != 3 or parts[0].lower() != "interval":
        raise ValueError(
            "interval SQL literals must use INTERVAL '<signed integer>' DAY|HOUR|MINUTE|SECOND"
        )
    literal = parts[1]
    if len(literal) < 3 or not literal.startswith("'") or not literal.endswith("'"):
        raise ValueError("interval SQL literal value must be single quoted")
    unit = _normalize_interval_unit(parts[2])
    return _interval_literal(literal[1:-1], unit)


def _normalize_interval_unit(value: object) -> str:
    unit = _require_non_empty("interval unit", value).upper()
    aliases = {
        "DAY": "DAY",
        "DAYS": "DAY",
        "HOUR": "HOUR",
        "HOURS": "HOUR",
        "MINUTE": "MINUTE",
        "MINUTES": "MINUTE",
        "SECOND": "SECOND",
        "SECONDS": "SECOND",
    }
    try:
        return aliases[unit]
    except KeyError as exc:
        raise ValueError(
            "interval unit must be one of DAY, HOUR, MINUTE, or SECOND"
        ) from exc


def _sql_temporal_difference_arg(value: object, dtype: str) -> str:
    if isinstance(value, ColumnExpression):
        return value.sql
    if dtype == "date32":
        if isinstance(value, datetime):
            raise TypeError("date_diff_days arguments must be date values or columns")
        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"
    elif dtype == "timestamp_micros":
        if isinstance(value, datetime):
            return f"TIMESTAMP '{_normalize_timestamp_literal(value)}'"
    else:
        raise ValueError("temporal difference dtype must be date32 or timestamp_micros")
    raise TypeError("temporal difference arguments must be shardloom columns or typed literals")


def _sql_string_literal(value: object) -> str:
    text = _require_non_empty("string literal", value)
    return "'" + text.replace("'", "''") + "'"


def _sql_string_function_literal(
    name: str, value: object, *, allow_empty: bool
) -> str:
    text = str(value)
    if not allow_empty and text == "":
        raise ValueError(f"{name} must not be empty")
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
        return f"TIMESTAMP '{_normalize_timestamp_literal(value)}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (bytes, bytearray)):
        return f"X'{bytes(value).hex()}'"
    if isinstance(value, str):
        return _sql_string_literal(value)
    raise TypeError(
        "SQL predicate literals must be bool, int, float, str, bytes, date, datetime, or None"
    )


def _sql_complex_projection_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _sql_numeric_literal(value: object) -> str:
    if isinstance(value, bool):
        raise ValueError("numeric arithmetic literals must be int or finite float values")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("numeric arithmetic float literals must be finite")
        return str(value)
    raise TypeError("numeric arithmetic literals must be int or finite float values")


def _sql_numeric_arithmetic_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    parts = text.split()
    if len(parts) != 3 or parts[1] not in {"+", "-", "*", "/"}:
        raise ValueError(
            "computed with_column currently admits sl.col(...) numeric arithmetic "
            "expressions of the form column (+|-|*|/) literal"
        )
    _normalize_expression_column(parts[0])
    literal = _parse_numeric_literal_token(parts[2])
    _sql_numeric_literal(literal)
    if parts[1] == "/" and literal == 0:
        raise ValueError("numeric arithmetic projection division by zero is not admitted")
    return text


def _sql_generic_expression_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    if not _expression_has_numeric_operator(
        text
    ) and not _expression_has_temporal_difference_call(text):
        raise ValueError(
            "computed with_column generic expressions require a numeric expression tree or temporal difference expression"
        )
    _validate_balanced_expression_parentheses(text)
    return text


def _parenthesize_numeric_operand(value: str) -> str:
    text = _require_non_empty("numeric expression", value)
    if _expression_has_numeric_operator(text) and not (
        text.startswith("(") and text.endswith(")")
    ):
        return f"({text})"
    return text


def _expression_has_numeric_operator(value: str) -> bool:
    in_quote = False
    depth = 0
    for index, char in enumerate(value):
        if char == "'":
            in_quote = not in_quote
            continue
        if in_quote:
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            depth -= 1
            continue
        if char in {"*", "/"}:
            return True
        if char in {"+", "-"} and not _is_unary_numeric_sign(value, index, char):
            return True
    return False


def _expression_has_temporal_difference_call(value: str) -> bool:
    text = value.strip()
    while text.startswith("(") and text.endswith(")"):
        inner = text[1:-1].strip()
        if not inner:
            return False
        text = inner
    upper = text.upper()
    return upper.startswith("DATE_DIFF_DAYS(") or upper.startswith(
        "TIMESTAMP_DIFF_SECONDS("
    )


def _is_unary_numeric_sign(value: str, index: int, char: str) -> bool:
    if char not in {"+", "-"}:
        return False
    before = next(
        (candidate for candidate in reversed(value[:index]) if not candidate.isspace()),
        None,
    )
    after = next(
        (
            candidate
            for candidate in value[index + len(char) :]
            if not candidate.isspace()
        ),
        None,
    )
    return (before is None or before in "(,+-*/") and (
        after is not None and (after.isdigit() or after == ".")
    )


def _validate_balanced_expression_parentheses(value: str) -> None:
    in_quote = False
    depth = 0
    for char in value:
        if char == "'":
            in_quote = not in_quote
            continue
        if in_quote:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("computed with_column expression has unbalanced parentheses")
    if in_quote:
        raise ValueError("computed with_column expression has an unclosed string literal")
    if depth != 0:
        raise ValueError("computed with_column expression has unbalanced parentheses")


def _sql_numeric_abs_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits ABS column expressions")
    function = text[:open_index].strip().upper()
    if function != "ABS":
        raise ValueError("computed with_column currently admits ABS column expressions")
    column = text[open_index + 1 : -1].strip()
    normalized = _normalize_expression_column(column)
    return f"ABS({normalized})"


def _sql_numeric_rounding_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits numeric rounding column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"FLOOR", "CEIL", "ROUND"}:
        raise ValueError(
            "computed with_column currently admits numeric rounding column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    normalized = _normalize_expression_column(column)
    return f"{function}({normalized})"


def _sql_computed_projection_expression(expression: object) -> str:
    parsers = (
        _sql_complex_projection_expression,
        _sql_cast_projection_expression,
        _sql_null_coalesce_projection_expression,
        _sql_nullif_projection_expression,
        _sql_conditional_projection_expression,
        _sql_predicate_projection_expression,
        _sql_numeric_arithmetic_projection_expression,
        _sql_numeric_abs_projection_expression,
        _sql_numeric_rounding_projection_expression,
        _sql_generic_expression_projection_expression,
        _sql_date_arithmetic_projection_expression,
        _sql_timestamp_arithmetic_projection_expression,
        _sql_string_length_projection_expression,
        _sql_string_transform_projection_expression,
        _sql_string_function_projection_expression,
        _sql_binary_helper_projection_expression,
        _sql_binary_byte_length_projection_expression,
        _sql_temporal_extract_projection_expression,
    )
    last_error: TypeError | ValueError | None = None
    for parser in parsers:
        try:
            return parser(expression)
        except (TypeError, ValueError) as error:
            last_error = error
    if last_error is None:
        raise ValueError("computed with_column expression is not admitted")
    raise last_error


def _sql_complex_projection_expression(expression: object) -> str:
    if not isinstance(expression, ComplexProjectionExpression):
        raise TypeError("complex projections require sl.array(...) or sl.struct(...)")
    text = expression.sql.strip()
    if not text:
        raise ValueError("complex projection expression must not be empty")
    return text


def _sql_predicate_projection_expression(expression: object) -> str:
    if not isinstance(expression, PredicateExpression):
        raise TypeError("computed with_column predicate projections require a PredicateExpression")
    text = _predicate_sql(expression).strip()
    if not text:
        raise ValueError("predicate with_column expression must not be empty")
    return text


def _split_cast_source_and_dtype(inner: str, syntax_error: str) -> tuple[str, str]:
    marker_index = _find_top_level_sql_keyword_outside_quotes(inner, "as")
    if marker_index is None:
        raise ValueError(syntax_error)
    source = inner[:marker_index].strip()
    dtype = _normalize_cast_dtype(inner[marker_index + len("as") :].strip())
    if not source:
        raise ValueError(syntax_error)
    return source, dtype


def _sql_cast_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    upper_text = text.upper()
    if not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits CAST/TRY_CAST column expressions"
        )
    if upper_text.startswith("TRY_CAST("):
        function = "TRY_CAST"
        inner = text[len("TRY_CAST(") : -1].strip()
    elif upper_text.startswith("CAST("):
        function = "CAST"
        inner = text[len("CAST(") : -1].strip()
    else:
        raise ValueError(
            "computed with_column currently admits CAST/TRY_CAST column expressions"
        )
    source, dtype = _split_cast_source_and_dtype(
        inner,
        "CAST/TRY_CAST column expressions must use CAST(column AS dtype) syntax",
    )
    if dtype == "binary":
        column, has_source_column = _normalize_string_scalar_expression_sql(source)
        if not has_source_column:
            raise ValueError(
                "binary CAST/TRY_CAST expressions require a source-backed string expression"
            )
    else:
        column = _normalize_expression_column(source)
    return f"{function}({column} AS {dtype})"


def _sql_null_coalesce_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits COALESCE column expressions")
    function = text[:open_index].strip().upper()
    if function != "COALESCE":
        raise ValueError("computed with_column currently admits COALESCE column expressions")
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError("COALESCE with_column expressions require exactly two arguments")
    column = _normalize_nullable_projection_column(args[0])
    fallback = args[1].strip()
    if fallback.upper() == "NULL":
        raise ValueError("COALESCE with_column expressions require a non-NULL fallback")
    return f"COALESCE({column}, {fallback})"


def _sql_nullif_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits NULLIF column expressions")
    function = text[:open_index].strip().upper()
    if function != "NULLIF":
        raise ValueError("computed with_column currently admits NULLIF column expressions")
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError("NULLIF with_column expressions require exactly two arguments")
    column = _normalize_nullable_projection_column(args[0])
    sentinel = args[1].strip()
    if sentinel.upper() == "NULL":
        raise ValueError("NULLIF with_column expressions require a non-NULL sentinel")
    return f"NULLIF({column}, {sentinel})"


def _sql_conditional_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    upper = text.upper()
    if not upper.startswith("CASE "):
        raise ValueError("computed with_column currently admits CASE WHEN expressions")
    when_marker = "WHEN "
    then_marker = " THEN "
    else_marker = " ELSE "
    end_marker = " END"
    when_index = upper.find(when_marker)
    then_index = upper.find(then_marker)
    else_index = upper.find(else_marker)
    end_index = upper.rfind(end_marker)
    if not (0 <= when_index < then_index < else_index < end_index):
        raise ValueError(
            "CASE with_column expressions must use CASE WHEN <predicate> THEN <literal-or-column> ELSE <literal-or-column> END"
        )
    if upper[:when_index].strip() != "CASE" or upper[end_index + len(end_marker) :].strip():
        raise ValueError(
            "CASE with_column expressions must be a single CASE WHEN expression"
        )
    predicate = _predicate_sql(text[when_index + len(when_marker) : then_index].strip())
    then_literal = text[then_index + len(then_marker) : else_index].strip()
    else_literal = text[else_index + len(else_marker) : end_index].strip()
    if not then_literal or not else_literal:
        raise ValueError("CASE with_column expressions require THEN and ELSE branches")
    if then_literal.upper() == "NULL" or else_literal.upper() == "NULL":
        raise ValueError("CASE with_column expressions require non-NULL branch literals")
    return f"CASE WHEN {predicate} THEN {then_literal} ELSE {else_literal} END"


def _sql_case_branch(value: object) -> str:
    if isinstance(value, ColumnExpression):
        text = value.sql.strip()
        if not text:
            raise ValueError("CASE branch column expression must not be empty")
        return text
    if value is None:
        raise ValueError("CASE branch literals must be non-NULL")
    return _sql_literal(value)


def _sql_date_arithmetic_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits DATE_ADD_DAYS/DATE_SUB_DAYS expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"DATE_ADD_DAYS", "DATE_SUB_DAYS"}:
        raise ValueError(
            "computed with_column currently admits DATE_ADD_DAYS/DATE_SUB_DAYS expressions"
        )
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError(
            "date arithmetic with_column expressions require exactly two arguments"
        )
    column = _normalize_temporal_extract_column(args[0], "date32")
    days = _normalize_date_arithmetic_days(args[1])
    return f"{function}({column}, {days})"


def _sql_timestamp_arithmetic_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"TIMESTAMP_ADD_SECONDS", "TIMESTAMP_SUB_SECONDS"}:
        raise ValueError(
            "computed with_column currently admits TIMESTAMP_ADD_SECONDS/TIMESTAMP_SUB_SECONDS expressions"
        )
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 2:
        raise ValueError(
            "timestamp arithmetic with_column expressions require exactly two arguments"
        )
    column = _normalize_temporal_extract_column(args[0], "timestamp_micros")
    seconds = _normalize_timestamp_arithmetic_seconds(args[1])
    return f"{function}({column}, {seconds})"


def _sql_string_transform_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits LOWER/UPPER/TRIM column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"LOWER", "UPPER", "TRIM"}:
        raise ValueError(
            "computed with_column currently admits LOWER/UPPER/TRIM column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    normalized, has_source_column = _normalize_string_scalar_expression_sql(column)
    if not has_source_column:
        raise ValueError(
            "string transform with_column expressions require at least one source column"
        )
    return f"{function}({normalized})"


def _sql_string_length_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError("computed with_column currently admits LENGTH column expressions")
    function = text[:open_index].strip().upper()
    if function != "LENGTH":
        raise ValueError("computed with_column currently admits LENGTH column expressions")
    column = text[open_index + 1 : -1].strip()
    normalized, has_source_column = _normalize_string_scalar_expression_sql(column)
    if not has_source_column:
        raise ValueError("LENGTH with_column expressions require at least one source column")
    return f"LENGTH({normalized})"


def _sql_string_function_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits CONCAT/SUBSTR/LEFT/RIGHT/REPLACE expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"CONCAT", "SUBSTR", "SUBSTRING", "LEFT", "RIGHT", "REPLACE"}:
        raise ValueError(
            "computed with_column currently admits CONCAT/SUBSTR/LEFT/RIGHT/REPLACE expressions"
        )
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if function == "CONCAT":
        if len(args) < 2:
            raise ValueError("CONCAT with_column expressions require at least two arguments")
        normalized_args: list[str] = []
        has_source_column = False
        for arg in args:
            normalized, is_source_column = _normalize_string_scalar_expression_sql(arg)
            normalized_args.append(normalized)
            has_source_column = has_source_column or is_source_column
        if not has_source_column:
            raise ValueError(
                "CONCAT with_column expressions require at least one source column"
            )
        return f"CONCAT({', '.join(normalized_args)})"
    if function in {"SUBSTR", "SUBSTRING"}:
        if len(args) != 3:
            raise ValueError(
                "SUBSTR/SUBSTRING with_column expressions require exactly three arguments"
            )
        value_arg, is_source_column = _normalize_string_scalar_expression_sql(args[0])
        if not is_source_column:
            raise ValueError(
                "SUBSTR/SUBSTRING with_column expressions require a source column argument"
            )
        start = _normalize_substring_bound("substring start", args[1], minimum=1)
        length = _normalize_substring_bound("substring length", args[2], minimum=0)
        return f"SUBSTR({value_arg}, {start}, {length})"
    if function in {"LEFT", "RIGHT"}:
        if len(args) != 2:
            raise ValueError(
                "LEFT/RIGHT with_column expressions require exactly two arguments"
            )
        value_arg, is_source_column = _normalize_string_scalar_expression_sql(args[0])
        if not is_source_column:
            raise ValueError(
                "LEFT/RIGHT with_column expressions require a source column argument"
            )
        count = _normalize_substring_bound("left/right count", args[1], minimum=0)
        return f"{function}({value_arg}, {count})"
    if len(args) != 3:
        raise ValueError("REPLACE with_column expressions require exactly three arguments")
    value_arg, is_source_column = _normalize_string_scalar_expression_sql(args[0])
    if not is_source_column:
        raise ValueError("REPLACE with_column expressions require a source column argument")
    needle = _parse_sql_string_literal_token(args[1])
    if needle == "":
        raise ValueError("REPLACE with_column expressions require a non-empty search literal")
    replacement = _parse_sql_string_literal_token(args[2])
    return (
        f"REPLACE({value_arg}, "
        f"{_sql_string_function_literal('replace search literal', needle, allow_empty=False)}, "
        f"{_sql_string_function_literal('replace replacement literal', replacement, allow_empty=True)})"
    )


def _sql_binary_helper_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits UNHEX/FROM_BASE64 binary helper expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"UNHEX", "FROM_BASE64"}:
        raise ValueError(
            "computed with_column currently admits UNHEX/FROM_BASE64 binary helper expressions"
        )
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 1:
        raise ValueError("binary helper with_column expressions require exactly one argument")
    expression_sql, has_source_column = _normalize_string_scalar_expression_sql(args[0])
    if not has_source_column:
        raise ValueError("binary helper with_column expressions require a source column argument")
    return f"{function}({expression_sql})"


def _sql_binary_byte_length_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits BYTE_LENGTH/OCTET_LENGTH binary expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {"BYTE_LENGTH", "OCTET_LENGTH"}:
        raise ValueError(
            "computed with_column currently admits BYTE_LENGTH/OCTET_LENGTH binary expressions"
        )
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if len(args) != 1:
        raise ValueError("binary byte length with_column expressions require exactly one argument")
    expression_sql, has_source_column = _normalize_binary_scalar_expression_sql(args[0])
    if not has_source_column:
        raise ValueError(
            "binary byte length with_column expressions require a source-backed binary expression"
        )
    return f"{function}({expression_sql})"


def _sql_temporal_extract_projection_expression(expression: object) -> str:
    if not isinstance(expression, ColumnExpression):
        raise TypeError("computed with_column requires a shardloom ColumnExpression")
    text = expression.sql.strip()
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "computed with_column currently admits DATE/TIMESTAMP extract column expressions"
        )
    function = text[:open_index].strip().upper()
    if function not in {
        "DATE_YEAR",
        "DATE_MONTH",
        "DATE_DAY",
        "TIMESTAMP_YEAR",
        "TIMESTAMP_MONTH",
        "TIMESTAMP_DAY",
        "TIMESTAMP_HOUR",
        "TIMESTAMP_MINUTE",
        "TIMESTAMP_SECOND",
    }:
        raise ValueError(
            "computed with_column currently admits DATE/TIMESTAMP extract column expressions"
        )
    column = text[open_index + 1 : -1].strip()
    if function.startswith("DATE_"):
        normalized = _normalize_temporal_extract_column(column, "date32")
    else:
        normalized = _normalize_temporal_extract_column(column, "timestamp_micros")
    return f"{function}({normalized})"


def _split_projection_function_args(expression: str) -> tuple[str, ...]:
    args: list[str] = []
    start = 0
    depth = 0
    in_quote = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if char == "'":
            if in_quote and index + 1 < len(expression) and expression[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
        elif char == "(" and not in_quote:
            depth += 1
        elif char == ")" and not in_quote:
            depth -= 1
            if depth < 0:
                raise ValueError("computed with_column expression has unbalanced parentheses")
        elif char == "," and not in_quote and depth == 0:
            args.append(expression[start:index].strip())
            start = index + 1
        index += 1
    if in_quote:
        raise ValueError("computed with_column expression has an unclosed string literal")
    if depth != 0:
        raise ValueError("computed with_column expression has unbalanced parentheses")
    args.append(expression[start:].strip())
    if any(not arg for arg in args):
        raise ValueError("computed with_column expression has an empty argument")
    return tuple(args)


def _sql_string_function_text_arg(value: object, name: str) -> tuple[str, bool]:
    if isinstance(value, ColumnExpression):
        return _normalize_string_scalar_expression_sql(value.sql)
    return _sql_string_function_literal(name, value, allow_empty=True), False


def _normalize_string_function_text_arg_sql(raw: str) -> tuple[str, bool]:
    text = _require_non_empty("string function argument", raw)
    if text.startswith("'"):
        value = _parse_sql_string_literal_token(text)
        return (
            _sql_string_function_literal(
                "string function literal", value, allow_empty=True
            ),
            False,
        )
    return _normalize_string_scalar_expression_sql(text)


def _normalize_string_scalar_expression_sql(raw: str) -> tuple[str, bool]:
    text = _require_non_empty("string expression", raw)
    if text.startswith("'"):
        value = _parse_sql_string_literal_token(text)
        return (
            _sql_string_function_literal(
                "string function literal", value, allow_empty=True
            ),
            False,
        )
    open_index = text.find("(")
    if open_index < 0:
        return _normalize_expression_column(text), True
    if not text.endswith(")"):
        raise ValueError("string expression function call must be closed")
    function = text[:open_index].strip().upper()
    args = _split_projection_function_args(text[open_index + 1 : -1].strip())
    if function in {"LOWER", "UPPER", "TRIM"}:
        if len(args) != 1:
            raise ValueError("string transform expressions require exactly one argument")
        arg_sql, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        return f"{function}({arg_sql})", has_source_column
    if function == "CONCAT":
        if len(args) < 2:
            raise ValueError("CONCAT string expressions require at least two arguments")
        normalized_args: list[str] = []
        has_source_column = False
        for arg in args:
            arg_sql, arg_has_source = _normalize_string_scalar_expression_sql(arg)
            normalized_args.append(arg_sql)
            has_source_column = has_source_column or arg_has_source
        return f"CONCAT({', '.join(normalized_args)})", has_source_column
    if function in {"SUBSTR", "SUBSTRING"}:
        if len(args) != 3:
            raise ValueError("SUBSTR/SUBSTRING string expressions require exactly three arguments")
        value_arg, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        start = _normalize_substring_bound("substring start", args[1], minimum=1)
        length = _normalize_substring_bound("substring length", args[2], minimum=0)
        return f"SUBSTR({value_arg}, {start}, {length})", has_source_column
    if function in {"LEFT", "RIGHT"}:
        if len(args) != 2:
            raise ValueError("LEFT/RIGHT string expressions require exactly two arguments")
        value_arg, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        count = _normalize_substring_bound("left/right count", args[1], minimum=0)
        return f"{function}({value_arg}, {count})", has_source_column
    if function == "REPLACE":
        if len(args) != 3:
            raise ValueError("REPLACE string expressions require exactly three arguments")
        value_arg, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        needle = _parse_sql_string_literal_token(args[1])
        if needle == "":
            raise ValueError("REPLACE string expressions require a non-empty search literal")
        replacement = _parse_sql_string_literal_token(args[2])
        return (
            f"REPLACE({value_arg}, "
            f"{_sql_string_function_literal('replace search literal', needle, allow_empty=False)}, "
            f"{_sql_string_function_literal('replace replacement literal', replacement, allow_empty=True)})",
            has_source_column,
        )
    raise ValueError(
        "string expressions currently admit columns, string literals, LOWER/UPPER/TRIM, CONCAT, SUBSTR/SUBSTRING, LEFT/RIGHT, and REPLACE"
    )


def _normalize_binary_scalar_expression_sql(raw: str) -> tuple[str, bool]:
    text = _require_non_empty("binary expression", raw)
    open_index = text.find("(")
    if open_index < 0 or not text.endswith(")"):
        raise ValueError(
            "binary byte length expressions admit UNHEX(...), FROM_BASE64(...), or CAST(... AS binary)"
        )
    function = text[:open_index].strip().upper()
    inner = text[open_index + 1 : -1].strip()
    if function in {"UNHEX", "FROM_BASE64"}:
        args = _split_projection_function_args(inner)
        if len(args) != 1:
            raise ValueError("binary helper expressions require exactly one argument")
        expression_sql, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        if not has_source_column:
            raise ValueError(
                "binary helper expressions require a source-backed string expression"
            )
        return f"{function}({expression_sql})", True
    if function in {"CAST", "TRY_CAST"}:
        source, dtype = _split_cast_source_and_dtype(
            inner,
            "binary byte length CAST expressions must use CAST(expression AS binary)",
        )
        if dtype != "binary":
            raise ValueError(
                "binary byte length CAST expressions must target binary, blob, or varbinary"
            )
        expression_sql, has_source_column = _normalize_string_scalar_expression_sql(source)
        if not has_source_column:
            raise ValueError(
                "binary byte length CAST expressions require a source-backed string expression"
            )
        return f"{function}({expression_sql} AS {dtype})", True
    raise ValueError(
        "binary byte length expressions admit UNHEX(...), FROM_BASE64(...), or CAST(... AS binary)"
    )


def _parse_sql_string_literal_token(raw: str) -> str:
    text = raw.strip()
    if not text.startswith("'") or not text.endswith("'") or len(text) < 2:
        raise ValueError("string function literals must be single quoted")
    body = text[1:-1]
    output: list[str] = []
    index = 0
    while index < len(body):
        char = body[index]
        if char == "'":
            if index + 1 < len(body) and body[index + 1] == "'":
                output.append("'")
                index += 2
                continue
            raise ValueError(
                "single quotes inside string function literals must be escaped as doubled quotes"
            )
        output.append(char)
        index += 1
    return "".join(output)


def _normalize_substring_bound(name: str, value: object, *, minimum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer literal")
    if isinstance(value, int):
        parsed = value
    else:
        text = _require_non_empty(name, value)
        if text in {"+", "-"} or not all(
            ch.isdigit() or (index == 0 and ch in {"+", "-"})
            for index, ch in enumerate(text)
        ):
            raise ValueError(f"{name} must be an integer literal")
        parsed = int(text)
    if parsed < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return parsed


def _normalize_temporal_extract_column(expression: str, dtype: str) -> str:
    text = _require_non_empty("temporal extract column expression", expression)
    if text.upper().startswith("CAST("):
        if not text.endswith(")"):
            raise ValueError("temporal extract CAST expression must be closed")
        inner = text[5:-1].strip()
        upper_inner = inner.upper()
        marker = " AS "
        marker_index = upper_inner.find(marker)
        if marker_index < 0:
            raise ValueError("temporal extract CAST expression must use CAST(column AS dtype)")
        column = inner[:marker_index].strip()
        target = inner[marker_index + len(marker) :].strip().lower()
        if target == "timestamp":
            target = "timestamp_micros"
        if target != dtype:
            raise ValueError(f"temporal extract CAST target must be {dtype}")
        return f"CAST({_normalize_expression_column(column)} AS {dtype})"
    return _normalize_expression_column(text)


def _normalize_nullable_projection_column(expression: str) -> str:
    text = _require_non_empty("COALESCE column expression", expression)
    if text.upper().startswith("CAST("):
        if not text.endswith(")"):
            raise ValueError("COALESCE CAST expression must be closed")
        inner = text[5:-1].strip()
        upper_inner = inner.upper()
        marker = " AS "
        marker_index = upper_inner.find(marker)
        if marker_index < 0:
            raise ValueError("COALESCE CAST expression must use CAST(column AS dtype)")
        column = _normalize_expression_column(inner[:marker_index].strip())
        dtype = _normalize_cast_dtype(inner[marker_index + len(marker) :].strip())
        if dtype not in {"date32", "timestamp_micros"}:
            raise ValueError("COALESCE CAST target must be date32 or timestamp_micros")
        return f"CAST({column} AS {dtype})"
    return _normalize_expression_column(text)


def _parse_numeric_literal_token(value: str) -> int | float:
    try:
        return int(value)
    except ValueError:
        try:
            parsed = float(value)
        except ValueError as exc:
            raise ValueError("numeric arithmetic projection literal must be numeric") from exc
        if not math.isfinite(parsed):
            raise ValueError("numeric arithmetic projection float literal must be finite")
        return parsed


def _sql_in_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _normalize_timestamp_literal(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(
            "SQL predicate datetime literals must be timezone-aware; scoped timestamp_micros admits UTC ISO timestamps only"
        )
    value = value.astimezone(timezone.utc)
    if value.microsecond:
        text = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        text = value.strftime("%Y-%m-%dT%H:%M:%SZ")
    return text


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


def _like_escape_clause(escape: object | None) -> str:
    if escape is None:
        return ""
    text = _require_non_empty("LIKE escape character", escape)
    if len(text) != 1:
        raise ValueError("LIKE escape character must be exactly one character")
    return f" ESCAPE {_sql_string_literal(text)}"


def _normalize_in_values(values: tuple[object, ...]) -> tuple[object, ...]:
    if len(values) == 1 and _is_non_string_sequence(values[0]):
        normalized = tuple(values[0])
    else:
        normalized = values
    if not normalized:
        raise ValueError("IN predicates require at least one value")
    if len(normalized) > 32:
        raise ValueError("IN predicates admit at most 32 values")
    return normalized


def _row_value_in_predicate(
    columns: object, rows: object, *, negated: bool
) -> PredicateExpression:
    normalized_columns = _normalize_row_value_columns(columns)
    normalized_rows = _normalize_row_value_in_rows(rows, arity=len(normalized_columns))
    column_sql = ",".join(normalized_columns)
    row_sql = ",".join(
        "(" + ",".join(_sql_in_literal(value) for value in row) + ")"
        for row in normalized_rows
    )
    operator = "NOT IN" if negated else "IN"
    return PredicateExpression(f"({column_sql}) {operator} ({row_sql})")


def _row_value_in_source_predicate(
    columns: object,
    source: object,
    source_columns: object,
    *,
    source_alias: object | None,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
    negated: bool,
) -> PredicateExpression:
    normalized_columns = _normalize_row_value_columns(columns)
    normalized_source_columns = _normalize_row_value_columns(source_columns)
    if len(normalized_source_columns) != len(normalized_columns):
        raise ValueError(
            "row-value IN subquery selected-column arity must match the source column count"
        )
    column_sql = ",".join(normalized_columns)
    source_column_sql = ",".join(normalized_source_columns)
    source_ref = _sql_in_subquery_source(source, source_alias=source_alias)
    tail = _sql_in_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )
    operator = "NOT IN" if negated else "IN"
    return PredicateExpression(
        f"({column_sql}) {operator} (SELECT {source_column_sql} FROM {source_ref}{tail})"
    )


def _exists_source_predicate(
    source: object,
    *,
    source_alias: object | None,
    select: object,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
    negated: bool,
) -> PredicateExpression:
    projection_sql = _normalize_exists_subquery_projection(select)
    source_ref = _sql_local_subquery_source(
        source, "EXISTS subquery source", source_alias=source_alias
    )
    max_limit = 32 if group_by is not None or having is not None else 10_000
    tail = _sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="EXISTS subquery limit",
        max_limit=max_limit,
        positive_limit=False,
    )
    operator = "NOT EXISTS" if negated else "EXISTS"
    return PredicateExpression(
        f"{operator} (SELECT {projection_sql} FROM {source_ref}{tail})"
    )


def _quantified_source_predicate(
    column_sql: str,
    comparison: object,
    quantifier: str,
    source: object,
    source_column: object,
    *,
    source_alias: object | None,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
) -> PredicateExpression:
    operator = _normalize_quantified_comparison_operator(comparison)
    source_column_sql = _normalize_expression_column(source_column)
    source_ref = _sql_local_subquery_source(
        source, "ANY/ALL subquery source", source_alias=source_alias
    )
    tail = _sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="ANY/ALL subquery limit",
        max_limit=32,
        positive_limit=True,
    )
    return PredicateExpression(
        f"{column_sql} {operator} {quantifier} "
        f"(SELECT {source_column_sql} FROM {source_ref}{tail})"
    )


def _normalize_quantified_comparison_operator(operator: object) -> str:
    text = _require_non_empty("ANY/ALL comparison operator", operator).lower()
    operators = {
        "=": "=",
        "==": "=",
        "eq": "=",
        "!=": "!=",
        "<>": "!=",
        "ne": "!=",
        "neq": "!=",
        "<": "<",
        "lt": "<",
        "<=": "<=",
        "le": "<=",
        "lte": "<=",
        ">": ">",
        "gt": ">",
        ">=": ">=",
        "ge": ">=",
        "gte": ">=",
    }
    try:
        return operators[text]
    except KeyError as exc:
        raise ValueError(
            "ANY/ALL comparison operator must be one of =, !=, <>, <, <=, >, >=, "
            "eq, ne, lt, le, gt, or ge"
        ) from exc


def _normalize_exists_subquery_projection(select: object) -> str:
    if isinstance(select, str) and select.strip() == "*":
        return "*"
    if isinstance(select, ColumnExpression):
        return _normalize_expression_column(select.sql)
    if _is_non_string_sequence(select):
        columns = tuple(_normalize_expression_column(item) for item in select)
        if not columns:
            raise ValueError("EXISTS subquery projection columns must not be empty")
        if len(set(columns)) != len(columns):
            raise ValueError("EXISTS subquery projection columns must be unique")
        return ",".join(columns)
    if isinstance(select, str):
        return _normalize_expression_column(select)
    return _sql_literal(select)


def _normalize_row_value_columns(columns: object) -> tuple[str, ...]:
    if _is_non_string_sequence(columns):
        raw_columns = tuple(columns)
    else:
        raw_columns = (columns,)
    normalized = tuple(_normalize_expression_column(column) for column in raw_columns)
    if len(normalized) < 2:
        raise ValueError("row-value IN predicates require at least two columns")
    if len(set(normalized)) != len(normalized):
        raise ValueError("row-value IN predicate columns must be unique")
    return normalized


def _normalize_row_value_in_rows(
    rows: object, *, arity: int
) -> tuple[tuple[object, ...], ...]:
    if not _is_non_string_sequence(rows):
        raise TypeError("row-value IN predicates require a sequence of literal rows")
    normalized_rows: list[tuple[object, ...]] = []
    for row in rows:
        if not _is_non_string_sequence(row):
            raise TypeError("row-value IN literal rows must be sequences")
        normalized_row = tuple(row)
        if len(normalized_row) != arity:
            raise ValueError(
                "row-value IN literal row arity must match the source column count"
            )
        normalized_rows.append(normalized_row)
    if not normalized_rows:
        raise ValueError("row-value IN predicates require at least one literal row")
    if len(normalized_rows) > 32:
        raise ValueError("row-value IN predicates admit at most 32 literal rows")
    return tuple(normalized_rows)


def _sql_in_subquery_source(
    source: object, *, source_alias: object | None = None
) -> str:
    return _sql_local_subquery_source(
        source, "IN subquery source", source_alias=source_alias
    )


def _sql_local_subquery_source(
    source: object, name: str, *, source_alias: object | None = None
) -> str:
    if isinstance(source, LazyFrame):
        source_ref = _quote_sql_local_source_path(source.source.uri)
    else:
        source_ref = _quote_sql_local_source_path(_require_non_empty(name, source))
    if source_alias is None:
        return source_ref
    alias = _normalize_output_column_name(source_alias)
    if alias.lower() == "outer":
        raise ValueError("local subquery source alias 'outer' is reserved")
    return f"{source_ref} AS {alias}"


def _sql_in_subquery_tail(
    *,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
) -> str:
    return _sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="IN subquery limit",
        max_limit=32,
        positive_limit=True,
    )


def _sql_local_subquery_tail(
    *,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
    limit_name: str,
    max_limit: int,
    positive_limit: bool,
) -> str:
    tail = ""
    if where is not None:
        tail = f"{tail} WHERE {_predicate_sql(where)}"
    group_columns = _normalize_in_subquery_order_by(group_by)
    if having is not None and not group_columns:
        raise ValueError("source subquery HAVING requires group_by in this scoped helper")
    if group_columns:
        tail = f"{tail} GROUP BY {','.join(group_columns)}"
    if having is not None:
        tail = f"{tail} HAVING {_predicate_sql(having)}"
    order_columns = _normalize_in_subquery_order_by(order_by)
    if order_columns:
        direction = "desc" if descending else "asc"
        tail = f"{tail}{_format_order_by_clause(order_columns, direction)}"
    if limit is not None:
        if positive_limit:
            normalized_limit = _normalize_positive_int(limit_name, limit)
        else:
            normalized_limit = _normalize_non_negative_int(limit_name, limit)
        if normalized_limit > max_limit:
            raise ValueError(f"{limit_name} admits at most {max_limit} rows")
        tail = f"{tail} LIMIT {normalized_limit}"
    return tail


def _normalize_in_subquery_order_by(value: object | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, ColumnExpression):
        return (_normalize_expression_column(value.sql),)
    if _is_non_string_sequence(value):
        return tuple(_normalize_expression_column(item) for item in value)
    return (_normalize_expression_column(value),)


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
    from_position = _find_sql_keyword_outside_quotes(select_body, "from")
    if from_position is None:
        return False
    source_ref = select_body[from_position + len("from") :].strip().lower()
    clause_positions = tuple(
        position
        for position in (
            _find_sql_keyword_outside_quotes(source_ref, "where"),
            _find_sql_keyword_outside_quotes(source_ref, "limit"),
        )
        if position is not None
    )
    if clause_positions:
        source_ref = source_ref[: min(clause_positions)].strip()
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
        and any(_is_local_source_sql_ref(value) for value in _sql_source_refs(normalized))
    )


def _sql_statement_with_limit(statement: str, count: int) -> str:
    """Return a normalized SQL statement with an explicit LIMIT clause."""

    _validate_positive_row_count("SQL LIMIT", count)
    normalized = statement.strip().rstrip(";").strip()
    if not normalized:
        raise ValueError("SQL statement must not be empty")
    limit_index = _find_top_level_sql_keyword_outside_quotes(normalized, "limit")
    if limit_index is not None:
        return _cap_top_level_sql_limit(normalized, limit_index, count)
    return f"{normalized} LIMIT {count}"


def _cap_top_level_sql_limit(statement: str, limit_index: int, count: int) -> str:
    limit_end = limit_index + len("limit")
    tail = statement[limit_end:].lstrip()
    if not tail:
        return statement
    digit_count = 0
    while digit_count < len(tail) and tail[digit_count].isdigit():
        digit_count += 1
    if digit_count == 0:
        return statement
    existing_limit = int(tail[:digit_count])
    capped_limit = min(existing_limit, count)
    return f"{statement[:limit_index].rstrip()} LIMIT {capped_limit}{tail[digit_count:]}"


def _workflow_has_limit(operations: Sequence[WorkflowOperation]) -> bool:
    """Whether a lazy workflow already carries a limit operation."""

    return any(operation.kind == "limit" for operation in operations)


def _is_local_source_sql_ref(value: str) -> bool:
    lower = value.strip().lower()
    if "://" in lower or lower.startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return False
    return lower.endswith(
        (
            ".csv",
            ".json",
            ".jsonl",
            ".ndjson",
            ".parquet",
            ".arrow",
            ".ipc",
            ".feather",
            ".avro",
            ".orc",
        )
    )


def _is_local_vortex_source_ref(value: str) -> bool:
    lower = value.strip().lower()
    if "://" in lower or lower.startswith(("s3:", "gs:", "abfs:", "abfss:")):
        return False
    return lower.endswith(".vortex")


def _vortex_sql_primitive_shape(
    statement: str,
) -> _VortexSqlPrimitiveWorkflowShape | None:
    normalized = statement.strip().rstrip(";").strip()
    if not _starts_with_sql_keyword(normalized, "select"):
        return None
    refs = _sql_source_refs(normalized)
    if len(refs) != 1 or not _is_local_vortex_source_ref(refs[0]):
        return None
    select_body = normalized[len("select") :].strip()
    from_position = _find_sql_keyword_outside_quotes(select_body, "from")
    if from_position is None:
        return None
    projection = select_body[:from_position].strip()
    from_tail = select_body[from_position + len("from") :].strip()
    parsed_ref = _parse_sql_single_quoted_prefix(from_tail)
    if parsed_ref is None:
        return None
    source_ref, tail = parsed_ref
    if source_ref != refs[0] or not _is_local_vortex_source_ref(source_ref):
        return None
    parsed_tail = _parse_vortex_sql_tail(tail)
    if parsed_tail is None:
        return None
    predicate_sql, limit = parsed_tail
    predicate = None
    if predicate_sql is not None:
        predicate = _vortex_sql_predicate_to_tiny(predicate_sql)
        if predicate is None:
            return None
    count = _is_sql_count_star_projection(projection)
    if count:
        if limit is not None:
            return None
        return _VortexSqlPrimitiveWorkflowShape(
            uri=source_ref,
            predicate=predicate,
            count=True,
        )
    columns: tuple[str, ...] | None
    if projection == "*":
        columns = ("*",)
    else:
        try:
            columns = tuple(
                _normalize_output_column_name(column)
                for column in _split_projection_function_args(projection)
            )
        except ValueError:
            return None
        if not columns:
            return None
    if predicate is None:
        return _VortexSqlPrimitiveWorkflowShape(
            uri=source_ref,
            columns=columns,
            limit=limit,
        )
    return _VortexSqlPrimitiveWorkflowShape(
        uri=source_ref,
        predicate=predicate,
        columns=columns,
        limit=limit,
    )


def _parse_sql_single_quoted_prefix(value: str) -> tuple[str, str] | None:
    if not value.startswith("'"):
        return None
    current: list[str] = []
    index = 1
    while index < len(value):
        char = value[index]
        if char == "'":
            if index + 1 < len(value) and value[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            return "".join(current), value[index + 1 :].strip()
        current.append(char)
        index += 1
    return None


def _parse_vortex_sql_tail(value: str) -> tuple[str | None, int | None] | None:
    tail = value.strip()
    if not tail:
        return None, None
    where_position = _find_sql_keyword_outside_quotes(tail, "where")
    limit_position = _find_sql_keyword_outside_quotes(tail, "limit")
    positions = tuple(
        position
        for position in (where_position, limit_position)
        if position is not None
    )
    if not positions:
        return None
    if where_position is not None and limit_position is not None and limit_position < where_position:
        return None
    if tail[: min(positions)].strip():
        return None
    predicate: str | None = None
    if where_position is not None:
        predicate_end = limit_position if limit_position is not None else len(tail)
        predicate = tail[where_position + len("where") : predicate_end].strip()
        if not predicate:
            return None
    limit: int | None = None
    if limit_position is not None:
        limit_text = tail[limit_position + len("limit") :].strip()
        if not limit_text or not limit_text.isdecimal():
            return None
        limit = _normalize_positive_int("SQL Vortex LIMIT", int(limit_text))
    return predicate, limit


def _is_sql_count_star_projection(value: str) -> bool:
    return "".join(value.split()).lower() == "count(*)"


def _vortex_sql_predicate_to_tiny(value: str) -> str | None:
    predicate = value.strip()
    lower = predicate.lower()
    for suffix, primitive in (
        (" is not null", "is_not_null"),
        (" is null", "is_null"),
    ):
        if lower.endswith(suffix):
            column = predicate[: -len(suffix)].strip()
            try:
                return f"{primitive}:{_normalize_output_column_name(column)}"
            except ValueError:
                return None
    if "!=" in predicate or "<>" in predicate:
        return None
    for operator, primitive in (
        (">=", "gte"),
        ("<=", "lte"),
        ("=", "eq"),
        (">", "gt"),
        ("<", "lt"),
    ):
        position = _find_unquoted_token(predicate, operator)
        if position is None:
            continue
        left = predicate[:position].strip()
        right = predicate[position + len(operator) :].strip()
        try:
            column = _normalize_output_column_name(left)
        except ValueError:
            return None
        literal = _parse_sql_int_literal(right)
        if literal is None:
            return None
        return f"{primitive}:{column}:{literal}"
    return None


def _find_unquoted_token(value: str, token: str) -> int | None:
    in_quote = False
    index = 0
    while index <= len(value) - len(token):
        char = value[index]
        if char == "'":
            if in_quote and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and value.startswith(token, index):
            return index
        index += 1
    return None


def _parse_sql_int_literal(value: str) -> str | None:
    text = value.strip()
    if not text or text in {"+", "-"}:
        return None
    if not all(
        char.isdigit() or (index == 0 and char in {"+", "-"})
        for index, char in enumerate(text)
    ):
        return None
    return str(int(text))


def _is_local_csv_source_ref(value: str) -> bool:
    lower = value.strip().lower()
    return _is_local_source_sql_ref(value) and lower.endswith(".csv")


def _source_format_for_local_source_ref(value: str) -> str | None:
    if not _is_local_source_sql_ref(value):
        return None
    lower = value.strip().lower()
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith((".json", ".jsonl", ".ndjson")):
        return "json"
    if lower.endswith(".parquet"):
        return "parquet"
    if lower.endswith((".arrow", ".ipc", ".feather")):
        return "arrow-ipc"
    if lower.endswith(".avro"):
        return "avro"
    if lower.endswith(".orc"):
        return "orc"
    return None


def _is_local_json_source_ref(value: str) -> bool:
    return _source_format_for_local_source_ref(value) == "json"


def _is_local_parquet_source_ref(value: str) -> bool:
    return _source_format_for_local_source_ref(value) == "parquet"


def _is_local_arrow_ipc_source_ref(value: str) -> bool:
    return _source_format_for_local_source_ref(value) == "arrow-ipc"


def _is_local_avro_source_ref(value: str) -> bool:
    return _source_format_for_local_source_ref(value) == "avro"


def _is_local_orc_source_ref(value: str) -> bool:
    return _source_format_for_local_source_ref(value) == "orc"


def _is_query_builder_local_source(source: WorkflowSource) -> bool:
    if source.source_format == "csv":
        return _is_local_csv_source_ref(source.uri)
    if source.source_format == "json":
        return _is_local_json_source_ref(source.uri)
    if source.source_format == "parquet":
        return _is_local_parquet_source_ref(source.uri)
    if source.source_format == "arrow-ipc":
        return _is_local_arrow_ipc_source_ref(source.uri)
    if source.source_format == "avro":
        return _is_local_avro_source_ref(source.uri)
    if source.source_format == "orc":
        return _is_local_orc_source_ref(source.uri)
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


def _sql_source_refs(statement: str) -> tuple[str, ...]:
    refs: list[str] = []
    lower = statement.lower()
    in_quote = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if in_quote:
            index += 1
            continue
        keyword_len = 0
        for keyword in ("from", "join"):
            if lower.startswith(keyword, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(keyword)
                after = statement[after_index] if after_index < len(statement) else ""
                if not _is_identifier_char(before) and not _is_identifier_char(after):
                    keyword_len = len(keyword)
                    break
        if keyword_len == 0:
            index += 1
            continue
        ref_start = index + keyword_len
        while ref_start < len(statement) and statement[ref_start].isspace():
            ref_start += 1
        if ref_start < len(statement) and statement[ref_start] == "'":
            ref_end = ref_start + 1
            current: list[str] = []
            while ref_end < len(statement):
                if statement[ref_end] == "'":
                    if ref_end + 1 < len(statement) and statement[ref_end + 1] == "'":
                        current.append("'")
                        ref_end += 2
                        continue
                    refs.append("".join(current))
                    index = ref_end + 1
                    break
                current.append(statement[ref_end])
                ref_end += 1
            else:
                index = ref_end
        else:
            index = ref_start
    return tuple(refs)


def _starts_with_sql_keyword(statement: str, keyword: str) -> bool:
    lower = statement.lower()
    needle = keyword.lower()
    if not lower.startswith(needle):
        return False
    if len(statement) == len(needle):
        return True
    return not _is_identifier_char(statement[len(needle)])


def _contains_sql_keyword_outside_quotes(statement: str, keyword: str) -> bool:
    return _find_sql_keyword_outside_quotes(statement, keyword) is not None


def _find_sql_keyword_outside_quotes(statement: str, keyword: str) -> int | None:
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
                return index
        index += 1
    return None


def _find_top_level_sql_keyword_outside_quotes(statement: str, keyword: str) -> int | None:
    lower = statement.lower()
    needle = keyword.lower()
    in_quote = False
    depth = 0
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
        if not in_quote:
            if char == "(":
                depth += 1
                index += 1
                continue
            if char == ")" and depth > 0:
                depth -= 1
                index += 1
                continue
            if depth == 0 and lower.startswith(needle, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(needle)
                after = statement[after_index] if after_index < len(statement) else ""
                if not _is_identifier_char(before) and not _is_identifier_char(after):
                    return index
        index += 1
    return None


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


def _normalize_join_condition(value: object) -> str:
    condition = _predicate_sql(value)
    if ";" in condition:
        raise ValueError("join condition cannot contain statement separators")
    return condition


def _normalize_sort_nulls(value: object | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("sort nulls must be one of 'first' or 'last'")
    normalized = value.strip().lower().replace("_", "-")
    if normalized in {"first", "nulls-first"}:
        return "first"
    if normalized in {"last", "nulls-last"}:
        return "last"
    raise ValueError("sort nulls must be one of 'first' or 'last'")


def _format_sort_operation_values(
    direction: str,
    columns: tuple[str, ...],
    null_ordering: str | None,
) -> tuple[str, ...]:
    if null_ordering is None:
        return (direction, *columns)
    return (direction, f"{_SORT_NULLS_TOKEN_PREFIX}{null_ordering}", *columns)


def _parse_sort_operation_values(
    values: tuple[str, ...],
) -> tuple[str, tuple[str, ...], str | None]:
    direction = values[0]
    if len(values) >= 2 and values[1].startswith(_SORT_NULLS_TOKEN_PREFIX):
        null_ordering = values[1][len(_SORT_NULLS_TOKEN_PREFIX) :]
        return direction, values[2:], null_ordering
    return direction, values[1:], None


def _format_order_by_clause(
    columns: tuple[str, ...],
    direction: str,
    null_ordering: str | None = None,
) -> str:
    if not columns:
        return ""
    direction_label = direction.upper()
    null_clause = "" if null_ordering is None else f" NULLS {null_ordering.upper()}"
    keys = ",".join(f"{column} {direction_label}{null_clause}" for column in columns)
    return f" ORDER BY {keys}"


def _sql_join_keyword(how: str) -> str:
    return {
        "inner": "INNER JOIN",
        "left": "LEFT JOIN",
        "right": "RIGHT JOIN",
        "full": "FULL JOIN",
        "semi": "LEFT SEMI JOIN",
        "anti": "LEFT ANTI JOIN",
        "cross": "CROSS JOIN",
    }[how]


def _optional_sql_where_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" WHERE {predicate}"


def _optional_sql_having_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" HAVING {predicate}"


def _workflow_schema_report(
    workflow: LazyFrame,
    smoke_report: SqlLocalSourceSmokeReport,
) -> WorkflowSchemaReport:
    rows = smoke_report.result_rows
    fields = _infer_workflow_schema_fields(rows, workflow.source.schema)
    return WorkflowSchemaReport(workflow=workflow, smoke_report=smoke_report, fields=fields)


def _infer_workflow_schema_fields(
    rows: tuple[Mapping[str, Any], ...],
    declared_schema: tuple[tuple[str, str], ...],
) -> tuple[WorkflowSchemaField, ...]:
    declared = {name: dtype for name, dtype in declared_schema}
    field_order: list[str] = []
    for name, _dtype in declared_schema:
        if name not in field_order:
            field_order.append(name)
    for row in rows:
        for name in row:
            if name not in field_order:
                field_order.append(name)

    fields: list[WorkflowSchemaField] = []
    for name in field_order:
        values = [row.get(name) for row in rows]
        non_null_values = [value for value in values if value is not None]
        observed_dtype = _merge_observed_dtypes(
            _infer_python_scalar_dtype(value) for value in non_null_values
        )
        declared_dtype = declared.get(name)
        dtype = observed_dtype or _normalize_schema_dtype_token(declared_dtype) or "null"
        null_count = len(rows) - len(non_null_values)
        fields.append(
            WorkflowSchemaField(
                name=name,
                dtype=dtype,
                nullable=null_count > 0,
                declared_dtype=declared_dtype,
                observed_non_null_count=len(non_null_values),
                null_count=null_count,
            )
        )
    return tuple(fields)


def _infer_python_scalar_dtype(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, str):
        return "utf8"
    if value is None:
        return "null"
    return "json"


def _merge_observed_dtypes(dtypes: Sequence[str]) -> str | None:
    unique = tuple(dict.fromkeys(dtype for dtype in dtypes if dtype != "null"))
    if not unique:
        return None
    if len(unique) == 1:
        return unique[0]
    if set(unique) <= {"int64", "float64"}:
        return "float64"
    return "mixed"


def _normalize_schema_dtype_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().lower().replace("-", "_")
    aliases = {
        "boolean": "bool",
        "bool": "bool",
        "int": "int64",
        "integer": "int64",
        "i64": "int64",
        "int64": "int64",
        "long": "int64",
        "float": "float64",
        "double": "float64",
        "f64": "float64",
        "float64": "float64",
        "str": "utf8",
        "string": "utf8",
        "utf8": "utf8",
        "date": "date32",
        "date32": "date32",
        "timestamp": "timestamp_micros",
        "timestamp_micros": "timestamp_micros",
    }
    return aliases.get(normalized, normalized)


def _validate_workflow_schema(
    report: WorkflowSchemaReport,
    expected_schema: tuple[tuple[str, str], ...],
) -> WorkflowSchemaValidationReport:
    observed = report.schema_map
    expected = {
        name: _normalize_schema_dtype_token(dtype) or dtype
        for name, dtype in expected_schema
    }
    missing_fields = tuple(name for name in expected if name not in observed)
    unexpected_fields = tuple(name for name in observed if name not in expected)
    mismatches: list[WorkflowSchemaMismatch] = []
    for name, expected_dtype in expected.items():
        observed_dtype = observed.get(name)
        if observed_dtype is not None and observed_dtype != expected_dtype:
            mismatches.append(
                WorkflowSchemaMismatch(
                    field=name,
                    expected_dtype=expected_dtype,
                    observed_dtype=observed_dtype,
                )
            )
    return WorkflowSchemaValidationReport(
        schema_report=report,
        expected_schema=expected_schema,
        missing_fields=missing_fields,
        unexpected_fields=unexpected_fields,
        dtype_mismatches=tuple(mismatches),
    )


def _parse_data_quality_checks(
    checks: tuple[str, ...],
) -> tuple[_WorkflowDataQualityCheckSpec, ...] | None:
    if not checks:
        raise ValueError("data-quality checks must not be empty")
    parsed: list[_WorkflowDataQualityCheckSpec] = []
    for check in checks:
        parts = check.split(":", 2)
        if len(parts) < 2:
            return None
        kind = parts[0].strip().lower().replace("-", "_").replace(" ", "_")
        column = parts[1].strip()
        if not column:
            return None
        if kind in {"not_null", "non_null", "required"}:
            if len(parts) != 2:
                return None
            parsed.append(_WorkflowDataQualityCheckSpec("not_null", column, check))
        elif kind == "unique":
            if len(parts) != 2:
                return None
            parsed.append(_WorkflowDataQualityCheckSpec("unique", column, check))
        elif kind in {"regex", "matches"}:
            if len(parts) != 3:
                return None
            pattern = parts[2].strip()
            if not pattern:
                return None
            try:
                re.compile(pattern)
            except re.error:
                return None
            parsed.append(_WorkflowDataQualityCheckSpec("regex", column, check, pattern))
        else:
            return None
    return tuple(parsed)


def _workflow_data_quality_report(
    schema_report: WorkflowSchemaReport,
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> WorkflowDataQualityReport:
    rows = schema_report.smoke_report.result_rows
    field_names = set(schema_report.field_names)
    schema_map = schema_report.schema_map
    results: list[WorkflowDataQualityCheckResult] = []
    for spec in checks:
        kind = spec.kind
        column = spec.column
        raw_check = spec.raw
        if column not in field_names:
            results.append(
                WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=False,
                    failing_row_count=len(rows),
                    message=f"column {column!r} was not observed",
                )
            )
            continue
        if kind == "not_null":
            failing = sum(1 for row in rows if row.get(column) is None)
            results.append(
                WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=failing == 0,
                    failing_row_count=failing,
                    message="all rows are non-null" if failing == 0 else "null values observed",
                )
            )
            continue
        if kind == "unique":
            seen: set[str] = set()
            duplicate_count = 0
            for row in rows:
                key = _stable_quality_value_key(row.get(column))
                if key in seen:
                    duplicate_count += 1
                else:
                    seen.add(key)
            results.append(
                WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=duplicate_count == 0,
                    failing_row_count=duplicate_count,
                    message="all values are unique"
                    if duplicate_count == 0
                    else "duplicate values observed",
                )
            )
            continue
        if kind == "regex":
            if schema_map.get(column) != "utf8":
                results.append(
                    WorkflowDataQualityCheckResult(
                        check=raw_check,
                        column=column,
                        passed=False,
                        failing_row_count=len(rows),
                        message="regex data-quality checks require utf8 values",
                    )
                )
                continue
            pattern = re.compile(spec.pattern or "")
            failing = sum(
                1
                for row in rows
                if not isinstance(row.get(column), str) or pattern.search(row[column]) is None
            )
            results.append(
                WorkflowDataQualityCheckResult(
                    check=raw_check,
                    column=column,
                    passed=failing == 0,
                    failing_row_count=failing,
                    message="all values match regex"
                    if failing == 0
                    else "regex mismatches or null values observed",
                )
            )
    return WorkflowDataQualityReport(
        schema_report=schema_report,
        checks=tuple(results),
    )


def _workflow_quarantine_checks(
    schema_report: WorkflowSchemaReport,
    checks: tuple[object, ...],
) -> tuple[_WorkflowDataQualityCheckSpec, ...]:
    if checks:
        normalized_checks = _normalize_columns(checks)
        parsed = _parse_data_quality_checks(normalized_checks)
        if parsed is None:
            raise ValueError(
                "quarantine checks must use supported data-quality forms such as "
                "'not_null:column', 'unique:column', or 'regex:column:pattern'"
            )
        return parsed
    return tuple(
        _WorkflowDataQualityCheckSpec("not_null", field.name, f"not_null:{field.name}")
        for field in schema_report.fields
    )


def _workflow_quarantine_rows(
    schema_report: WorkflowSchemaReport,
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> tuple[Mapping[str, Any], ...]:
    rows = schema_report.smoke_report.result_rows
    if not rows or not checks:
        return ()
    field_names = set(schema_report.field_names)
    unique_value_counts: dict[str, dict[str, int]] = {}
    regex_patterns = {
        spec.raw: re.compile(spec.pattern or "")
        for spec in checks
        if spec.kind == "regex" and spec.pattern is not None
    }
    for spec in checks:
        if spec.kind != "unique" or spec.column not in field_names:
            continue
        counts: dict[str, int] = {}
        for row in rows:
            key = _stable_quality_value_key(row.get(spec.column))
            counts[key] = counts.get(key, 0) + 1
        unique_value_counts[spec.column] = counts

    quarantined: list[Mapping[str, Any]] = []
    for row in rows:
        failed = False
        for spec in checks:
            kind = spec.kind
            column = spec.column
            if column not in field_names:
                failed = True
            elif kind == "not_null":
                failed = row.get(column) is None
            elif kind == "unique":
                key = _stable_quality_value_key(row.get(column))
                failed = unique_value_counts.get(column, {}).get(key, 0) > 1
            elif kind == "regex":
                pattern = regex_patterns.get(spec.raw)
                value = row.get(column)
                failed = (
                    not isinstance(value, str)
                    or pattern is None
                    or pattern.search(value) is None
                )
            if failed:
                quarantined.append(row)
                break
    return tuple(quarantined)


def _quarantine_pushdown_predicate(
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> str | None:
    if not checks or any(spec.kind not in {"not_null", "regex"} for spec in checks):
        return None
    predicates = []
    for spec in checks:
        column = spec.column
        if not _is_sql_identifier(column):
            return None
        if spec.kind == "not_null":
            predicates.append(f"{column} IS NULL")
        elif spec.kind == "regex" and spec.pattern is not None:
            predicates.append(
                f"({column} IS NULL OR {column} NOT RLIKE {_sql_string_literal(spec.pattern)})"
            )
        else:
            return None
    return " OR ".join(predicates) if predicates else None


def _normalize_optional_quarantine_output_format(
    target_uri: str | os.PathLike[str] | None,
    output_format: str | None,
) -> str | None:
    if output_format is not None:
        return _normalize_local_output_format(output_format)
    if target_uri is None:
        return None
    suffix = Path(str(target_uri)).suffix.lower()
    if suffix in {".vortex", ".vtx"}:
        return "vortex"
    if suffix == ".csv":
        return "csv"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".arrow", ".ipc", ".feather"}:
        return "arrow-ipc"
    if suffix == ".avro":
        return "avro"
    if suffix == ".orc":
        return "orc"
    return "jsonl"


def _optional_module(module_name: str) -> object | None:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError:
        return None


def _rows_as_dicts(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _row_field_order(rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(str(key))
    return tuple(fields)


def _rows_to_pandas(rows: Sequence[Mapping[str, Any]], pandas: object) -> object:
    return getattr(pandas, "DataFrame")(_rows_as_dicts(rows))


def _rows_to_arrow_table(rows: Sequence[Mapping[str, Any]], pyarrow: object) -> object:
    table_type = getattr(pyarrow, "Table")
    return table_type.from_pylist(_rows_as_dicts(rows))


def _rows_to_arrow_ipc(rows: Sequence[Mapping[str, Any]], pyarrow: object) -> bytes:
    table = _rows_to_arrow_table(rows, pyarrow)
    sink = getattr(pyarrow, "BufferOutputStream")()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    buffer = sink.getvalue()
    return buffer.to_pybytes()


def _rows_to_numpy(rows: Sequence[Mapping[str, Any]], numpy: object) -> object:
    columns = _row_field_order(rows)
    values = [[row.get(column) for column in columns] for row in rows]
    return getattr(numpy, "asarray")(values)


def _pandas_like_records(dataframe: object) -> Sequence[Mapping[str, object]] | None:
    to_dict = getattr(dataframe, "to_dict", None)
    if not callable(to_dict):
        return None
    try:
        rows = to_dict(orient="records")
    except TypeError:
        rows = to_dict("records")
    return rows if _is_mapping_sequence(rows) else None


def _arrow_table_like_records(table: object) -> Sequence[Mapping[str, object]] | None:
    to_pylist = getattr(table, "to_pylist", None)
    if not callable(to_pylist):
        return None
    rows = to_pylist()
    return rows if _is_mapping_sequence(rows) else None


def _read_arrow_ipc_table(source: object, pyarrow: object) -> object:
    ipc = pyarrow.ipc
    if isinstance(source, (str, os.PathLike)):
        with open(source, "rb") as handle:
            return _read_arrow_ipc_from_seekable(handle, ipc)
    if isinstance(source, (bytes, bytearray, memoryview)):
        reader = pyarrow.BufferReader(bytes(source))
        return _read_arrow_ipc_from_seekable(reader, ipc)
    return ipc.open_stream(source).read_all()


def _read_arrow_ipc_from_seekable(source: object, ipc: object) -> object:
    try:
        return ipc.open_stream(source).read_all()
    except Exception as stream_error:
        seek = getattr(source, "seek", None)
        if callable(seek):
            seek(0)
        open_file = getattr(ipc, "open_file", None)
        if not callable(open_file):
            raise stream_error
        try:
            return open_file(source).read_all()
        except Exception:
            raise stream_error


def _is_mapping_sequence(value: object) -> bool:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes, bytearray))
        and all(isinstance(row, Mapping) for row in value)
    )


def _display_cell(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def _stable_quality_value_key(value: object) -> str:
    return repr(value)


def _normalize_local_output_format(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"jsonl", "json-lines", "ndjson", "inline-jsonl"}:
        return "jsonl"
    if normalized == "csv":
        return "csv"
    if normalized == "parquet":
        return "parquet"
    if normalized in {"arrow", "arrow-ipc", "arrow_ipc", "ipc", "feather"}:
        return "arrow-ipc"
    if normalized == "avro":
        return "avro"
    if normalized == "orc":
        return "orc"
    if normalized in {"vortex", "vtx"}:
        return "vortex"
    raise ValueError(
        "scoped local writes currently support local JSONL, CSV, and feature-gated "
        "Parquet/Arrow IPC/Avro/ORC/Vortex only"
    )


def _public_write_request_for_format(output_format: str) -> str:
    normalized = _normalize_local_output_format(output_format)
    return {
        "jsonl": "write_jsonl",
        "csv": "write_csv",
        "parquet": "write_parquet",
        "arrow-ipc": "write_arrow_ipc",
        "avro": "write_avro",
        "orc": "write_orc",
        "vortex": "write_vortex",
    }[normalized]


def _normalize_fanout_outputs(
    outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
) -> tuple[tuple[str, CommandPart], ...]:
    if isinstance(outputs, Mapping):
        items = outputs.items()
    elif _is_non_string_sequence(outputs):
        items = outputs
    else:
        raise TypeError("fanout outputs must be a mapping or sequence of (format, path) pairs")

    normalized: list[tuple[str, CommandPart]] = []
    for item in items:
        if not _is_non_string_sequence(item) or len(item) != 2:
            raise ValueError("fanout outputs must contain (format, path) pairs")
        output_format, output_path = item
        if not isinstance(output_format, str):
            raise TypeError("fanout output format names must be strings")
        if not isinstance(output_path, (str, os.PathLike)):
            raise TypeError("fanout output paths must be strings or path-like objects")
        normalized.append(
            (
                _normalize_local_output_format(output_format),
                _require_non_empty("fanout output path", output_path),
            )
        )
    if not normalized:
        raise ValueError("fanout outputs must not be empty")
    return tuple(normalized)


def _generated_primary_and_fanout_outputs(
    outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
) -> tuple[CommandPart, str, tuple[tuple[str, CommandPart], ...]]:
    normalized = _normalize_fanout_outputs(outputs)
    output_format, output_path = normalized[0]
    return output_path, output_format, normalized[1:]


def _is_non_string_sequence(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _optional_binary(value: object) -> Binary | None:
    if value is None:
        return None
    return cast(Binary, value)


def _optional_env(value: object) -> Mapping[str, str] | None:
    if value is None:
        return None
    return cast(Mapping[str, str], value)


def _optional_path(value: object) -> str | os.PathLike[str] | None:
    if value is None:
        return None
    return cast(Union[str, os.PathLike[str]], value)


def _optional_profile_order(value: object) -> Sequence[str] | None:
    if value is None:
        return None
    return cast(Sequence[str], value)


def _optional_timeout(value: object) -> float | None:
    if value is None:
        return None
    return cast(float, value)
