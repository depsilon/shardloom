"""Lazy workflow planning helpers for the ShardLoom Python surface."""

from __future__ import annotations

import ast
import builtins
import hashlib
import html
import importlib
import json
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, Union, cast

from ._compat import dataclass
from ._result_schema import ResultType, arrow_table, python_rows, schema_fields
from .errors import ShardLoomProtocolError
from .client import (
    Binary,
    CommandPart,
    DEFAULT_PROFILE_ORDER,
    EngineSelectionPlan,
    PublicWorkflowExecution,
    PublicWorkflowRoute,
    ShardLoomClient,
    _jsonl_object_rows,
    _required_field,
    VortexIngestSmokeReport,
)
from .models import (
    ClaimSummary,
    Diagnostic,
    EvidenceSummary,
    OutputEnvelope,
    RuntimeActivationSummary,
)
from .runtime_defaults import (
    DEFAULT_INTERNAL_SMOKE_MAX_PARALLELISM,
    DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
    DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
)

SUPPORTED_SOURCE_FORMATS = ("vortex", "csv", "json", "parquet", "arrow-ipc", "avro", "orc")
_NATIVE_UNARY_PRIMITIVES = frozenset({
    "distinct", "drop_duplicates", "duplicate_mask", "tail", "sample",
    "expression_project", "melt", "explode", "pivot", "rolling_window",
})
MAX_DATE_ARITHMETIC_DAYS = 366_000
MAX_TIMESTAMP_ARITHMETIC_SECONDS = MAX_DATE_ARITHMETIC_DAYS * 86_400
_INTERVAL_SECOND_MULTIPLIERS = {
    "DAY": 86_400,
    "HOUR": 3_600,
    "MINUTE": 60,
    "SECOND": 1,
}
_SORT_NULLS_TOKEN_PREFIX = "__sort_nulls__:"
_RAW_SQL_FRAGMENT_BREAKOUT_KEYWORDS = (
    "select",
    "from",
    "where",
    "join",
    "group by",
    "having",
    "order by",
    "limit",
    "union",
    "intersect",
    "except",
    "with",
)
_RAW_SQL_FRAGMENT_BREAKOUT_TOKENS = (";", "--", "/*", "*/")


@dataclass(frozen=True, slots=True)
class WorkflowSource:
    """A declared workflow source that is not read during construction."""

    source_format: str
    uri: str
    schema: tuple[tuple[str, str], ...] = ()
    memory_input: tuple[tuple[str, object], ...] = ()

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
    source_bindings: tuple[WorkflowSource, ...] = ()
    right_statement: str | None = None
    projection_sql: tuple[str, ...] | None = None

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
class WorkflowPlanTransform:
    """Caller-declared plan transform accepted by `LazyFrame.pipe(...)`.

    This is a plan-construction helper, not a data UDF. The callable receives a
    `LazyFrame` and must return a `LazyFrame`; terminal execution still goes
    through ShardLoom's normal no-fallback Vortex-prepared/native routes.
    """

    name: str
    function: object

    def apply(self, frame: "LazyFrame", *args: object, **kwargs: object) -> object:
        """Apply the caller-owned transform to a lazy ShardLoom plan."""

        return self.function(frame, *args, **kwargs)  # type: ignore[misc]


@dataclass(frozen=True, slots=True)
class WorkflowColumnTransform:
    """Caller-declared column transform accepted by scoped map/applymap/transform paths."""

    items: tuple[tuple[str, object], ...]


@dataclass(frozen=True, slots=True)
class WorkflowRowTransform:
    """Caller-declared row transform accepted by scoped map_rows paths."""

    items: tuple[tuple[str, object], ...]


@dataclass(frozen=True, slots=True)
class PredicateExpression:
    """A scoped SQL predicate expression for ShardLoom local-source smokes."""

    sql: str
    source_bindings: tuple[WorkflowSource, ...] = ()

    def __str__(self) -> str:
        return self.sql

    def __and__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical AND predicate."""

        return PredicateExpression(
            f"({self.sql} AND {_predicate_sql(other)})", _predicate_sources(self, other)
        )

    def __or__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical OR predicate."""

        return PredicateExpression(
            f"({self.sql} OR {_predicate_sql(other)})", _predicate_sources(self, other)
        )

    def __invert__(self) -> "PredicateExpression":
        """Return a scoped logical NOT predicate."""

        return PredicateExpression(f"NOT {self.sql}", self.source_bindings)


@dataclass(frozen=True, slots=True)
class WindowExpression:
    """A SQL window declaration submitted to the shared native engine."""

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

    def __neg__(self) -> "ColumnExpression":
        """Return a checked native numeric negation expression."""

        return ColumnExpression(f"-({self.sql})")

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

        expression = _sql_computed_projection_expression(self)
        return ColumnExpression(f"UNHEX({expression})")

    def from_base64(self) -> "ColumnExpression":
        """Return a scoped `FROM_BASE64(<utf8-expression>)` binary helper expression."""

        expression = _sql_computed_projection_expression(self)
        return ColumnExpression(f"FROM_BASE64({expression})")

    def byte_length(self) -> "ColumnExpression":
        """Return a scoped `BYTE_LENGTH(<binary-expression>)` byte-count expression."""

        expression = _sql_computed_projection_expression(self)
        return ColumnExpression(f"BYTE_LENGTH({expression})")

    def fill_null(self, value: object) -> "ColumnExpression":
        """Return a `COALESCE` expression with a scalar value or expression."""

        return ColumnExpression(f"COALESCE({self.sql}, {_sql_case_branch(value)})")

    def null_if(self, value: object) -> "ColumnExpression":
        """Return a scoped `NULLIF(column, literal)` null-cleanup expression."""

        return ColumnExpression(f"NULLIF({self.sql}, {_sql_case_branch(value)})")

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
            f"{self.sql} IN (SELECT {source_column} FROM {source_ref}{tail})",
            _predicate_sources(source, where, having),
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
            f"{self.sql} NOT IN (SELECT {source_column} FROM {source_ref}{tail})",
            _predicate_sources(source, where, having),
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


@dataclass(frozen=True, slots=True)
class SqlWorkflow:
    """A scoped SQL workflow entry point over currently admitted ShardLoom SQL paths."""

    statement: str
    client: ShardLoomClient
    input_uri: str | None = None
    input_format: str | None = None
    source_bindings: tuple[WorkflowSource, ...] = ()

    @property
    def operation_summary(self) -> str:
        """Return a deterministic SQL workflow summary."""

        return "sql(statement)"

    def _relation_statement(self) -> str:
        return self.statement.strip().removesuffix(";").strip()

    def _declared_sources(self) -> tuple[WorkflowSource, ...]:
        declared = self.source_bindings
        if self.input_uri is not None and self.input_format is not None:
            primary = WorkflowSource(self.input_format, self.input_uri)
            matching = tuple(source for source in declared if source.uri == self.input_uri)
            if any(
                _public_workflow_input_format(source) != _public_workflow_input_format(primary)
                for source in matching
            ):
                raise ValueError(
                    f"conflicting format or schema declarations for source {self.input_uri!r}"
                )
            if not matching:
                declared = (primary, *declared)
        return declared

    def _compose(self, *operations: WorkflowOperation) -> "SqlWorkflow":
        from ._relational_sql import render_stages

        statement = render_stages(self.statement, operations)
        if statement is None:
            raise ValueError("SQL transformation has no admitted native relational lowering")
        return SqlWorkflow(
            statement, self.client, self.input_uri, self.input_format,
            (*self.source_bindings, *(source for operation in operations
                                     for source in operation.source_bindings)),
        )

    def select(self, *columns: object) -> "SqlWorkflow":
        """Project the complete preceding SQL result without executing it."""
        return self._compose(WorkflowOperation("select", _normalize_columns(columns)))

    def project(self, *columns: object) -> "SqlWorkflow":
        return self.select(*columns)

    def filter(self, predicate: object) -> "SqlWorkflow":
        """Filter the complete preceding result, including its order and limit."""
        return self._compose(WorkflowOperation(
            "filter", (_normalize_raw_or_typed_predicate("filter predicate", predicate),),
            _predicate_sources(predicate),
        ))

    def where(self, predicate: object) -> "SqlWorkflow":
        return self.filter(predicate)

    def distinct(self) -> "SqlWorkflow":
        return self._compose(WorkflowOperation("distinct", ()))

    def sort(
        self, *columns: object, descending: bool = False, nulls: str | None = None,
        check: bool = False,
    ) -> "SqlWorkflow":
        return self._compose(WorkflowOperation("sort", _format_sort_operation_values(
            "desc" if descending else "asc", _normalize_columns(columns), _normalize_sort_nulls(nulls),
        )))

    def order_by(
        self, *columns: object, descending: bool = False, nulls: str | None = None,
        check: bool = False,
    ) -> "SqlWorkflow":
        return self.sort(*columns, descending=descending, nulls=nulls, check=check)

    def window(self, *expressions: object, check: bool = False) -> "SqlWorkflow":
        return self._compose(WorkflowOperation("window", _normalize_window_expressions(expressions)))

    def group_by(self, *columns: object) -> "GroupedLazyFrame":
        return GroupedLazyFrame(self, _normalize_columns(columns))

    def groupby(self, *columns: object) -> "GroupedLazyFrame":
        return self.group_by(*columns)

    def _append_group_by_aggregate(
        self, columns: tuple[str, ...], expressions: tuple[str, ...],
    ) -> "SqlWorkflow":
        return self._compose(
            WorkflowOperation("group_by", columns), WorkflowOperation("aggregate", expressions),
        )

    def aggregate(self, *expressions: object, check: bool = False) -> "SqlWorkflow":
        return self._compose(WorkflowOperation("aggregate", _normalize_columns(expressions)))

    def agg(
        self, *expressions: object, check: bool = False, **named_expressions: object,
    ) -> "SqlWorkflow":
        values = list(_normalize_columns(expressions)) if expressions else []
        values.extend(
            _format_named_aggregate(name, expression)
            for name, expression in named_expressions.items()
        )
        if not values:
            raise ValueError("aggregate expressions must not be empty")
        return self._compose(WorkflowOperation("aggregate", tuple(values)))

    def having(self, predicate: object, *, check: bool = False) -> "SqlWorkflow":
        """Apply HAVING to the current aggregate scope; Rust validates the scope."""
        value = _normalize_raw_or_typed_predicate("HAVING predicate", predicate)
        # HAVING binds aggregate inputs/aliases, so it stays inside this SELECT.
        statement = self._relation_statement()
        if _find_top_level_sql_keyword_outside_quotes(statement, "having") is not None:
            raise ValueError("HAVING is already present; use filter() for another result stage")
        if any(
            _find_top_level_sql_keyword_outside_quotes(statement, keyword) is not None
            for keyword in ("order by", "limit", "union", "intersect", "except")
        ):
            raise ValueError(
                "HAVING must follow its aggregate before ordering, limits or sets; "
                "use filter() for a later result stage"
            )
        return SqlWorkflow(
            f"{statement} HAVING {value}",
            self.client, self.input_uri, self.input_format,
            (*self.source_bindings, *_predicate_sources(predicate)),
        )

    def with_column(
        self, name: str, expression: object, *, check: bool = False,
    ) -> "SqlWorkflow":
        column = _normalize_output_column_name(name)
        try:
            literal = _generated_literal_expression(expression)
            expression_sql = "NULL" if literal is None else _sql_literal(literal)
        except (TypeError, ValueError):
            expression_sql = _sql_computed_projection_expression(expression)
        return self._compose(WorkflowOperation("with_column", (column, expression_sql)))

    def with_columns(
        self, columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
        *, check: bool = False, **named_expressions: object,
    ) -> "SqlWorkflow":
        workflow = self
        for name, expression in _normalize_named_projection_items(
            "with_columns", columns, named_expressions,
        ):
            workflow = workflow.with_column(name, expression, check=check)
        return workflow

    def join(
        self, other: "LazyFrame | SqlWorkflow | str", *, on: str | Sequence[str] | None = None,
        condition: object | None = None, how: str = "inner", check: bool = False,
    ) -> "SqlWorkflow":
        kind = _normalize_join_how(how)
        if on is not None and condition is not None:
            raise ValueError("join() accepts either on= equi keys or condition=, not both")
        keys = () if on is None else tuple(
            _normalize_output_column_name(column) for column in _normalize_columns((on,))
        )
        predicate = "" if condition is None else _normalize_join_condition(condition)
        if kind == "cross" and (keys or predicate):
            raise ValueError("cross joins do not accept keys or condition=; use filter() after join()")
        if kind != "cross" and not keys and not predicate:
            raise ValueError("join() requires on= or condition=")
        right = other._relation_statement() if isinstance(other, (LazyFrame, SqlWorkflow)) else None
        if isinstance(other, (LazyFrame, SqlWorkflow)) and right is None:
            raise ValueError("join input has no admitted native relational lowering")
        uri = "" if right is not None else _require_non_empty("join right source", other)
        return self._compose(WorkflowOperation(
            "join", (uri, ",".join(keys), ",".join(keys), kind, "f", "d", predicate),
            _predicate_sources(other, condition), right_statement=right,
        ))

    def _set_operation(
        self, other: "LazyFrame | SqlWorkflow", *, operation: str, keyword: str, check: bool,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        if (not isinstance(other, (LazyFrame, SqlWorkflow))
                or (right := other._relation_statement()) is None):
            return self._unsupported_operation(operation, str(other), check=check)
        return SqlWorkflow(
            f"SELECT * FROM ({self._relation_statement()}) AS _sl_set_left {keyword} "
            f"SELECT * FROM ({right}) AS _sl_set_right", self.client,
            source_bindings=(*self._declared_sources(), *other._declared_sources()),
        )

    def union(
        self, other: "LazyFrame | SqlWorkflow", *, check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        return self._set_operation(other, operation="union", keyword="UNION", check=check)

    def union_all(
        self, other: "LazyFrame | SqlWorkflow", *, check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        return self._set_operation(other, operation="union-all", keyword="UNION ALL", check=check)

    def intersect(
        self, other: "LazyFrame | SqlWorkflow", *, check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        return self._set_operation(other, operation="intersect", keyword="INTERSECT", check=check)

    def except_(
        self, other: "LazyFrame | SqlWorkflow", *, check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        return self._set_operation(other, operation="except", keyword="EXCEPT", check=check)

    def route(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "vortex_middle",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = False,
    ) -> PublicWorkflowRoute:
        """Return the shared public route envelope for this SQL workflow."""

        normalized_bounded = (
            _find_top_level_sql_keyword_outside_quotes(self.statement.strip(), "limit")
            is not None
            if bounded is None and requested_output == "collect"
            else bounded
        )
        workflow_kwargs = self._declared_or_embedded_vortex_input_kwargs()
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
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
            **workflow_kwargs,
        )

    def run(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str = "vortex_middle",
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Run this SQL workflow through the shared public route facade."""

        normalized_bounded = (
            _find_top_level_sql_keyword_outside_quotes(self.statement.strip(), "limit")
            is not None
            if bounded is None and requested_output == "collect"
            else bounded
        )
        workflow_kwargs = self._declared_or_embedded_vortex_input_kwargs()
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
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
            **workflow_kwargs,
        )

    def collect(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Collect rows or run admitted local Vortex SQL primitives."""

        if limit is not None:
            return self.limit(limit).collect(
                check=check,
                memory_gb=memory_gb,
                max_parallelism=max_parallelism,
                spill=spill,
            )
        envelope = _collect_native_relational(
            self.client, self.statement, surface="sql",
            plan_summary=self.operation_summary,
            input_kwargs=self._declared_or_embedded_vortex_input_kwargs(),
            check=check, memory_gb=memory_gb, max_parallelism=max_parallelism,
            spill=spill,
        )
        return VortexWorkflowExecutionReport(
            workflow=self._report_workflow(), operation="collect", envelope=envelope,
        )

    def limit(self, count: int) -> "SqlWorkflow":
        """Cap this SQL result without expanding an existing limit."""

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("limit count must be an integer")
        if count < 0:
            raise ValueError("limit count must be non-negative")
        if count == 0:
            return self._compose(WorkflowOperation("limit", ("0",)))
        statement = _sql_statement_with_limit(self.statement, count)
        return SqlWorkflow(
            statement=statement,
            client=self.client,
            input_uri=self.input_uri,
            input_format=self.input_format,
            source_bindings=self.source_bindings,
        )

    def _declared_input_kwargs(self) -> dict[str, Any]:
        """Return public workflow input kwargs declared outside the SQL text."""

        payload: dict[str, Any] = {}
        if self.input_uri is not None:
            payload["input_uri"] = self.input_uri
        if self.input_format is not None:
            payload["input_format"] = self.input_format
        if self.source_bindings:
            payload["source_bindings"] = _workflow_source_bindings(self.source_bindings)
        return payload

    def _declared_or_embedded_vortex_input_kwargs(self) -> dict[str, Any]:
        """Return declared input kwargs or a single embedded local Vortex input binding."""

        payload = self._declared_input_kwargs()
        if payload:
            return payload
        embedded = _embedded_vortex_input_uri(self.statement)
        if embedded is None:
            return {}
        return {"input_uri": embedded, "input_format": "vortex"}

    def schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return a bounded schema report for admitted local-source SQL."""

        return self._bounded_schema_report(check=check)

    def describe_schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return detailed bounded schema evidence for admitted local-source SQL."""

        return self._bounded_schema_report(check=check)

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
        report = self._bounded_schema_report(check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _validate_workflow_schema(report, normalized)

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
            report = self._bounded_schema_report(check=check)
            if isinstance(report, UnsupportedWorkflowOperationReport):
                return report
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

        report = self._bounded_schema_report(check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return WorkflowDataQualityReport(schema_report=report)

    def profile(
        self,
        limit: int = 100,
        *,
        check: bool = False,
    ) -> (
        WorkflowProfileReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Return a metadata-first profile for admitted Vortex/prepared SQL."""

        _validate_positive_row_count("profile limit", limit)
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        workflow = self._report_workflow()
        return WorkflowProfileReport(
            workflow=workflow,
            smoke_report=report,
            schema_report=_workflow_schema_report(workflow, report),
            limit=limit,
        )

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
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
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

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded preview through the native Vortex SQL route when admitted."""

        _validate_positive_row_count("preview limit", limit)
        return self.limit(limit).collect(check=check)

    def head(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded SQL preview using familiar DataFrame naming."""

        _validate_positive_row_count("head limit", limit)
        return self.limit(limit).collect(check=check)

    def take(
        self,
        count: int,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded SQL preview for the requested row count."""

        _validate_positive_row_count("take limit", count)
        return self.limit(count).collect(check=check)

    def to_python_objects(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> tuple[Mapping[str, Any], ...] | UnsupportedWorkflowOperationReport:
        """Return bounded Python row objects for admitted local-source SQL."""

        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return report.python_objects

    def to_pandas(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a pandas DataFrame at an explicit bounded materialization boundary."""

        pandas = _optional_module("pandas")
        if pandas is None:
            return self._unsupported_operation(
                "to-pandas",
                "missing optional dependency: pandas",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_pandas(report, pandas)

    def to_arrow(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table at an explicit bounded materialization boundary."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_table(report, pyarrow)

    def to_arrow_table(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table for admitted bounded local-source SQL."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-table",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_table(report, pyarrow)

    def to_arrow_ipc(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> bytes | UnsupportedWorkflowOperationReport:
        """Return Arrow IPC stream bytes for admitted bounded local-source SQL."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-ipc",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_ipc(report, pyarrow)

    def to_numpy(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a NumPy array for admitted bounded local-source SQL rows."""

        numpy = _optional_module("numpy")
        if numpy is None:
            return self._unsupported_operation(
                "to-numpy",
                "missing optional dependency: numpy",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_numpy(report, numpy)

    def display(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> WorkflowNotebookPreview | UnsupportedWorkflowOperationReport:
        """Return a bounded notebook/display preview for admitted local-source SQL."""

        _validate_positive_row_count("display limit", limit)
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return WorkflowNotebookPreview(
            workflow=self._report_workflow(),
            smoke_report=report,
            limit=limit,
        )

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Write an admitted SQL result to a scoped local output."""

        normalized_output_format = _normalize_local_output_format(output_format)
        return self._public_workflow_write_report(
            target_uri,
            requested_output=_public_write_request_for_format(normalized_output_format),
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_json(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Alias for `write(..., output_format="json")` (one JSON array)."""

        return self.write(
            target_uri,
            output_format="json",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="parquet")`.

        Local SQL-source Parquet output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Parquet sink blocker.
        """

        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_parquet",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        Local SQL-source Arrow IPC output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Arrow IPC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="arrow-ipc",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="avro")`.

        Local SQL-source Avro output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic Avro sink blocker.
        """

        return self.write(
            target_uri,
            output_format="avro",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="orc")`.

        Local SQL-source ORC output requires a CLI built with
        `--features universal-format-io`; default binaries return a
        deterministic ORC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="orc",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Alias for `write(..., output_format="vortex")`.

        Source-free SQL can route through the generated-source Vortex sink, exact
        admitted local `.vortex` SQL shapes can route through the native Vortex
        provider sink, and compatibility local-source SQL can route through the
        scoped local-source Vortex sink when the CLI is built with
        `--features vortex-write`. Default binaries return deterministic
        Vortex sink blockers for unsupported shapes.
        """

        return self._public_workflow_write_report(
            target_uri,
            requested_output="write_vortex",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Write an admitted SQL result to primary and fanout local sinks."""

        normalized_outputs = _normalize_fanout_outputs(outputs)
        output_format, output_path = normalized_outputs[0]
        fanout_outputs = normalized_outputs[1:]
        requested_output = _public_write_request_for_format(output_format)
        return self._public_workflow_write_report(
            output_path,
            requested_output=requested_output,
            allow_overwrite=allow_overwrite,
            fanout_outputs=fanout_outputs,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def _public_workflow_write_report(
        self,
        target_uri: str | os.PathLike[str],
        *,
        requested_output: str,
        allow_overwrite: bool,
        check: bool,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        fanout_outputs: Sequence[tuple[str, CommandPart]] | None = None,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        # The CLI owns source preparation and native operator/sink admission.
        # Sending the original statement preserves optimized aggregate/sort paths
        # and avoids rebuilding compatibility inputs in Python for each sink.
        execution = self.client.public_workflow_run(
            "sql",
            sql_statement=self.statement,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=target_uri,
            fanout_outputs=fanout_outputs,
            execution_policy="vortex_middle",
            materialization_policy="bounded",
            evidence_level="production_admitted_local_workflow",
            bounded=True,
            allow_overwrite=allow_overwrite,
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
            check=check,
            **self._declared_or_embedded_vortex_input_kwargs(),
        )
        return VortexWorkflowExecutionReport(
            workflow=self._report_workflow(),
            operation=requested_output,
            envelope=execution.envelope,
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

    def _bounded_schema_report(
        self, *, check: bool,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        report = self._bounded_materialization_report(limit=100, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _workflow_schema_report(self._report_workflow(), report)

    def _bounded_materialization_report(
        self,
        *,
        limit: int | None,
        check: bool,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        if limit is not None:
            _validate_positive_row_count("materialization limit", limit)
        report = self.collect(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        if report.status != "success":
            return UnsupportedWorkflowOperationReport(report.workflow, "collect", report.envelope)
        return report




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
    def activation_summary(self) -> RuntimeActivationSummary:
        """Return compact runtime activation evidence for this unsupported diagnostic."""

        return self.envelope.activation_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return the compact claim summary for this unsupported diagnostic."""

        return self.envelope.claim_summary


@dataclass(frozen=True, slots=True)
class VortexWorkflowExecutionReport:
    """Rows, diagnostics and sink evidence from the shared Vortex-native engine."""

    workflow: "LazyFrame"
    operation: str
    envelope: OutputEnvelope

    @property
    def result_jsonl(self) -> str:
        """Return complete bounded rows when this execution includes a row payload."""

        if self.envelope.field("result_values_json") is not None:
            return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in self.result_rows)
        return _required_field(self.envelope, "result_jsonl", allow_empty=True)

    @property
    def result_rows(self) -> tuple[Mapping[str, Any], ...]:
        """Decode this execution's rows without reading or executing its source again."""

        values = self.envelope.field("result_values_json")
        if values is None:
            return _jsonl_object_rows(self.result_jsonl, field_name="result_jsonl")
        try:
            rows = json.loads(values)
        except ValueError as error:
            raise ShardLoomProtocolError("invalid native result_values_json") from error
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise ShardLoomProtocolError("native result_values_json requires an array of row objects")
        return tuple(rows)

    @property
    def result_schema(self) -> tuple[tuple[str, ResultType], ...]:
        """Return the ordered schema delivered by this native execution."""

        return schema_fields(_required_field(self.envelope, "result_schema_json"),
                             _required_field(self.envelope, "result_schema_format"))

    @property
    def result_columns(self) -> tuple[str, ...]:
        """Return actual output names even when no rows were produced."""

        return tuple(name for name, _ in self.result_schema)

    @property
    def python_objects(self) -> tuple[Mapping[str, Any], ...]:
        """Restore logical binary, decimal and temporal values at this boundary."""

        return tuple(python_rows(self.result_rows, self.result_schema))

    @property
    def first_result_row(self) -> Mapping[str, Any] | None:
        """Return the first collected row, or None for an empty result."""

        rows = self.result_rows
        return rows[0] if rows else None

    @property
    def is_error(self) -> bool:
        return self.envelope.is_error

    @property
    def has_error_diagnostics(self) -> bool:
        return self.envelope.has_error_diagnostics

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        return self.envelope.diagnostics

    @property
    def unsupported_reasons(self) -> tuple[str, ...]:
        reasons = tuple(dict.fromkeys(
            diagnostic.reason or diagnostic.message for diagnostic in self.diagnostics
            if diagnostic.reason or diagnostic.message
        ))
        if not reasons and self.is_error and self.envelope.human_text:
            return (self.envelope.human_text,)
        return reasons

    @property
    def output_path(self) -> str | None:
        """Return the primary artifact path reported by the native sink."""

        return self.envelope.field("native_vortex_result_export_path")

    @property
    def output_format(self) -> str | None:
        return self.envelope.field("native_vortex_result_export_format")

    @property
    def rows_written(self) -> int | None:
        return self.envelope.field_int("native_vortex_result_export_rows_written")

    @property
    def output_row_count(self) -> int | None:
        """Return the engine's complete output count when it was reported."""

        return _first_int_field(self.envelope, (
            "output_row_count", "native_vortex_result_export_rows_written",
            "rows_projected", "local_primitive_rows_selected",
        ))

    @property
    def output_sha256(self) -> str | None:
        """Return the native writer's digest when that writer reports one."""

        return self.envelope.field("native_vortex_array_sink_output_sha256")

    @property
    def output_commit_status(self) -> str | None:
        """Report completion only when every declared sink was committed."""

        committed = self.envelope.field_bool("native_vortex_result_export_all_targets_committed")
        if committed is None:
            return None
        return "committed" if committed else "not_committed"

    @property
    def native_io_certificate_status(self) -> str | None:
        return self.envelope.field("local_primitive_native_io_certificate_status")

    @property
    def output_replay_verified(self) -> bool:
        """A recorded row count or successful commit does not establish replay."""

        return self.envelope.field_bool("native_vortex_result_export_replay_verified", False) is True

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

        return _any_true_field(
            self.envelope,
            (
                "runtime_execution",
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

        return self.output_io_performed or self.envelope.field_bool("write_io", False) is True

    @property
    def output_io_performed(self) -> bool:
        """Whether the primitive/export report wrote a local output artifact."""

        return self.envelope.field_bool("output_io_performed", False) is True

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
    def activation_summary(self) -> RuntimeActivationSummary:
        """Return compact runtime activation evidence for this Vortex workflow."""

        return self.envelope.activation_summary

    @property
    def claim_summary(self) -> ClaimSummary:
        """Return compact claim posture from the backing Vortex primitive."""

        return self.envelope.claim_summary

    @property
    def vortex_ingest_performed(self) -> bool:
        """Whether this native execution performed compatibility input preparation."""

        return self.envelope.field_bool(
            "public_workflow_preparation_vortex_ingest_performed", False
        ) is True

    @property
    def prepared_vortex_path(self) -> str | None:
        """Return the internally prepared Vortex artifact path, when present."""

        return self.envelope.field("public_workflow_local_source_prepared_vortex_path")

    @property
    def source_state_id(self) -> str | None:
        """Return the prepared input SourceState id, when the preparation reported one."""

        return self._preparation_field("source_state_id")

    @property
    def source_state_digest(self) -> str | None:
        """Return the prepared input SourceState digest, when available."""

        return self._preparation_field("source_state_digest")

    @property
    def source_state_contract_schema_version(self) -> str | None:
        """Return the prepared input SourceState contract version, when available."""

        return self._preparation_field("source_state_contract_schema_version")

    @property
    def source_state_read_plan(self) -> str | None:
        """Return the prepared input SourceState read plan, when available."""

        return self._preparation_field("source_state_read_plan")

    @property
    def source_state_projection_pushdown_status(self) -> str | None:
        """Return the prepared input SourceState projection status, when available."""

        return self._preparation_field("source_state_projection_pushdown_status")

    def _preparation_field(self, key: str) -> str | None:
        return self.envelope.field(f"public_workflow_preparation_{key}") or self.envelope.field(key)












@dataclass(frozen=True, slots=True)
class RollingFrame:
    """Scoped source-order rolling builder for admitted native Vortex windows."""

    frame: "LazyFrame"
    window: int
    min_periods: int
    center: bool = False

    def _aggregate(
        self,
        aggregate: str,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        target_ref = _normalize_rolling_aggregate_target(
            aggregate,
            column,
            alias=alias,
            kwargs=kwargs,
        )
        if kwargs:
            return self.frame._unsupported_operation(
                f"rolling-{aggregate}",
                target_ref,
                check=check,
            )
        payload = _vortex_rolling_window_payload(
            column,
            output_column=alias,
            window_size=self.window,
            min_periods=self.min_periods,
            aggregate=aggregate,
            center=self.center,
        )
        if payload is None:
            return self.frame._unsupported_operation(
                f"rolling-{aggregate}",
                target_ref,
                check=check,
            )
        return self.frame._append(WorkflowOperation("rolling_window", (payload,)))

    def sum(
        self,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling sum over one numeric column."""

        return self._aggregate("sum", column, alias=alias, check=check, **kwargs)

    def mean(
        self,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling mean over one numeric column."""

        return self._aggregate("mean", column, alias=alias, check=check, **kwargs)

    def count(
        self,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling row count over one scalar column."""

        return self._aggregate("count", column, alias=alias, check=check, **kwargs)

    def min(
        self,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling minimum over one numeric column."""

        return self._aggregate("min", column, alias=alias, check=check, **kwargs)

    def max(
        self,
        column: object,
        *,
        alias: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling maximum over one numeric column."""

        return self._aggregate("max", column, alias=alias, check=check, **kwargs)




def _strip_index_metadata_operations(
    operations: Sequence[WorkflowOperation],
) -> tuple[WorkflowOperation, ...]:
    return tuple(operation for operation in operations if operation.kind != "set_index")








def _vortex_expression_project_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    columns = decoded.get("columns")
    if isinstance(columns, str):
        values = tuple(column.strip() for column in columns.split(",") if column.strip())
    elif _is_non_string_sequence(columns):
        values = tuple(str(column).strip() for column in columns if str(column).strip())
    else:
        return None
    if not values or any(not _is_sql_identifier(column) for column in values):
        return None
    return values


def _vortex_melt_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    id_columns = _json_column_tuple(decoded.get("id_columns"))
    value_columns = _json_column_tuple(decoded.get("value_columns"))
    if id_columns is None or value_columns is None or not value_columns:
        return None
    columns = (*id_columns, *value_columns)
    if any(not _is_sql_identifier(column) for column in columns):
        return None
    return columns


def _vortex_explode_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    raw_columns = (
        decoded.get("explode_columns")
        or decoded.get("target_columns")
        or decoded.get("columns")
    )
    if raw_columns is None:
        raw_columns = (
            decoded.get("column")
            or decoded.get("explode_column")
            or decoded.get("target_column")
        )
    try:
        columns = _normalize_columns((raw_columns,))
    except (TypeError, ValueError):
        return None
    element_field = decoded.get("element_field") or decoded.get("field")
    if element_field is not None and (
        not isinstance(element_field, str) or not _is_sql_identifier(element_field)
    ):
        return None
    if not columns or any(not _is_sql_identifier(column) for column in columns):
        return None
    return tuple(columns)


def _vortex_explode_projection_payload(columns: Sequence[str]) -> str | None:
    if not columns:
        return None
    if len(columns) == 1:
        dotted = _split_dotted_field_accessor(columns[0])
        if dotted is not None:
            source_column, element_field = dotted
            return json.dumps(
                {
                    "column": source_column,
                    "element_field": element_field,
                    "output_column": element_field,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
    if any(not _is_sql_identifier(column) for column in columns):
        return None
    payload: dict[str, object]
    if len(columns) == 1:
        payload = {"column": columns[0]}
    else:
        payload = {"explode_columns": list(columns)}
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _split_dotted_field_accessor(value: str) -> tuple[str, str] | None:
    parts = value.split(".")
    if len(parts) != 2:
        return None
    source_column, field = (part.strip() for part in parts)
    if not _is_sql_identifier(source_column) or not _is_sql_identifier(field):
        return None
    return source_column, field


def _vortex_pivot_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    index_column = decoded.get("index_column") or decoded.get("index")
    pivot_column = decoded.get("pivot_column") or decoded.get("columns")
    value_column = decoded.get("value_column") or decoded.get("values")
    columns = (index_column, pivot_column, value_column)
    if not all(isinstance(column, str) and _is_sql_identifier(column) for column in columns):
        return None
    if len(set(columns)) != len(columns):
        return None
    aggregate = str(decoded.get("aggregate") or decoded.get("aggfunc") or "").strip().lower()
    if aggregate not in {"first", "first_unique", "sum", "count", "mean", "min", "max"}:
        return None
    return tuple(columns)  # type: ignore[return-value]


def _vortex_pivot_projection_payload(
    *,
    index: object | None,
    columns: object | None,
    values: object | None,
    aggregate: str,
    kwargs: Mapping[str, object],
) -> str | None:
    if index is None or columns is None or values is None:
        return None
    try:
        index_columns = _normalize_columns((index,))
        pivot_columns = _normalize_columns((columns,))
        value_columns = _normalize_columns((values,))
    except (TypeError, ValueError):
        return None
    if len(index_columns) != 1 or len(pivot_columns) != 1 or len(value_columns) != 1:
        return None
    index_column = index_columns[0]
    pivot_column = pivot_columns[0]
    value_column = value_columns[0]
    if (
        not _is_sql_identifier(index_column)
        or not _is_sql_identifier(pivot_column)
        or not _is_sql_identifier(value_column)
        or len({index_column, pivot_column, value_column}) != 3
    ):
        return None
    normalized_aggregate = aggregate.strip().lower().replace("-", "_")
    policy_kwargs = _normalize_pivot_policy_kwargs(kwargs)
    if policy_kwargs is None:
        return None
    aliases = {
        "first": "first_unique",
        "first_unique": "first_unique",
        "unique": "first_unique",
        "sum": "sum",
        "count": "count",
        "mean": "mean",
        "avg": "mean",
        "min": "min",
        "max": "max",
    }
    aggregate_value = aliases.get(normalized_aggregate)
    if aggregate_value is None:
        return None
    if aggregate_value == "first_unique" and bool(policy_kwargs.get("margins", False)):
        return None
    payload = {
        "aggregate": aggregate_value,
        "index_column": index_column,
        "pivot_column": pivot_column,
        "value_column": value_column,
    }
    payload.update(policy_kwargs)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _normalize_pivot_policy_kwargs(kwargs: Mapping[str, object]) -> dict[str, object] | None:
    allowed = {"fill_value", "dropna", "margins", "margins_name"}
    if any(key not in allowed for key in kwargs):
        return None
    policy: dict[str, object] = {}
    if "fill_value" in kwargs:
        fill_value = _json_scalar_policy_value(kwargs["fill_value"])
        if fill_value is None and kwargs["fill_value"] is not None:
            return None
        policy["fill_value"] = fill_value
    if "dropna" in kwargs:
        dropna = kwargs["dropna"]
        if not isinstance(dropna, bool):
            return None
        policy["dropna"] = dropna
    if "margins" in kwargs:
        margins = kwargs["margins"]
        if not isinstance(margins, bool):
            return None
        policy["margins"] = margins
    if "margins_name" in kwargs:
        margins_name = _require_non_empty("pivot_table margins_name", kwargs["margins_name"])
        policy["margins_name"] = margins_name
    return policy


def _json_scalar_policy_value(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, bool | int | str):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return _vortex_exact_scalar_payload(value)


def _normalize_pivot_table_single_aggregate(
    aggfunc: object | None,
    *,
    values: object | None,
) -> str | None:
    if aggfunc is None:
        return "mean"
    if isinstance(aggfunc, str):
        return _require_non_empty("pivot_table aggfunc", aggfunc)
    if isinstance(aggfunc, Mapping):
        if len(aggfunc) != 1 or values is None:
            return None
        try:
            value_columns = _normalize_columns((values,))
        except (TypeError, ValueError):
            return None
        if len(value_columns) != 1:
            return None
        raw_key, raw_value = next(iter(aggfunc.items()))
        if str(raw_key).strip() != value_columns[0]:
            return None
        return _normalize_pivot_table_single_aggregate(raw_value, values=values)
    if _is_non_string_sequence(aggfunc):
        if len(aggfunc) != 1:
            return None
        return _normalize_pivot_table_single_aggregate(aggfunc[0], values=values)
    return None


def _json_column_tuple(value: object) -> tuple[str, ...] | None:
    if isinstance(value, str):
        columns = tuple(column.strip() for column in value.split(",") if column.strip())
    elif _is_non_string_sequence(value):
        columns = tuple(str(column).strip() for column in value if str(column).strip())
    else:
        return None
    if any(not _is_sql_identifier(column) for column in columns):
        return None
    return columns


def _vortex_melt_projection_payload(
    *,
    id_vars: object | None,
    value_vars: object | None,
    var_name: object | None,
    value_name: object | None,
    kwargs: Mapping[str, object],
) -> str | None:
    if kwargs or value_vars is None:
        return None
    try:
        id_columns = _normalize_optional_columns(id_vars)
        value_columns = _normalize_columns((value_vars,))
    except (TypeError, ValueError):
        return None
    variable_column = "variable" if var_name is None else str(var_name).strip()
    value_column = "value" if value_name is None else str(value_name).strip()
    if (
        not value_columns
        or not variable_column
        or not value_column
        or variable_column == value_column
        or any(not _is_sql_identifier(column) for column in id_columns)
        or any(not _is_sql_identifier(column) for column in value_columns)
        or not _is_sql_identifier(variable_column)
        or not _is_sql_identifier(value_column)
        or variable_column in id_columns
        or value_column in id_columns
    ):
        return None
    payload = {
        "id_columns": list(id_columns),
        "value_columns": list(value_columns),
        "variable_column": variable_column,
        "value_column": value_column,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _vortex_rolling_window_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    source_column = decoded.get("source_column") or decoded.get("column") or decoded.get("on")
    if not isinstance(source_column, str) or not _is_sql_identifier(source_column):
        return None
    output_column = decoded.get("output_column") or decoded.get("alias")
    if not isinstance(output_column, str) or not _is_sql_identifier(output_column):
        return None
    if source_column == output_column:
        return None
    window_size = decoded.get("window_size") or decoded.get("window")
    min_periods = decoded.get("min_periods")
    center = decoded.get("center", False)
    if not isinstance(center, bool):
        return None
    aggregate = str(decoded.get("aggregate") or decoded.get("agg") or "").strip().lower()
    try:
        parsed_window = _normalize_positive_int("rolling window", window_size)
        parsed_min_periods = (
            parsed_window
            if min_periods is None
            else _normalize_positive_int("rolling min_periods", min_periods)
        )
    except (TypeError, ValueError):
        return None
    if parsed_min_periods > parsed_window or aggregate not in {
        "sum",
        "mean",
        "count",
        "min",
        "max",
    }:
        return None
    return (source_column,)


def _vortex_rolling_window_payload(
    column: object,
    *,
    output_column: object | None,
    window_size: int,
    min_periods: int,
    aggregate: str,
    center: bool = False,
) -> str | None:
    source_column = _normalize_expression_column(column)
    if "." in source_column:
        return None
    output = (
        _normalize_output_column_name(output_column)
        if output_column is not None
        else _normalize_output_column_name(f"{source_column}_rolling_{aggregate}")
    )
    if output == source_column:
        return None
    normalized_window = _normalize_positive_int("rolling window", window_size)
    normalized_min_periods = _normalize_positive_int("rolling min_periods", min_periods)
    normalized_aggregate = aggregate.strip().lower()
    if normalized_min_periods > normalized_window or normalized_aggregate not in {
        "sum",
        "mean",
        "count",
        "min",
        "max",
    }:
        return None
    return json.dumps(
        {
            "aggregate": normalized_aggregate,
            "min_periods": normalized_min_periods,
            "output_column": output,
            "source_column": source_column,
            "window_size": normalized_window,
            "center": bool(center),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


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
    smoke_report: VortexWorkflowExecutionReport
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
    smoke_report: VortexWorkflowExecutionReport
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
    sink_report: VortexWorkflowExecutionReport | None = None

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
    smoke_report: VortexWorkflowExecutionReport
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

        value = _normalize_raw_or_typed_predicate("filter predicate", predicate)
        if self._can_append_having():
            return self._append(WorkflowOperation("having", (value,), _predicate_sources(predicate)))
        return self._append(WorkflowOperation("filter", (value,), _predicate_sources(predicate)))

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

        value = _normalize_raw_or_typed_predicate("HAVING predicate", predicate)
        if self._can_append_having():
            return self._append(WorkflowOperation("having", (value,), _predicate_sources(predicate)))
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
        axis: object | None = None,
        thresh: int | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped schema-declared non-null filter when admitted."""

        target_ref = _normalize_dropna_target(
            subset=subset,
            how=how,
            axis=axis,
            thresh=thresh,
            kwargs=kwargs,
        )
        predicate = self._schema_declared_dropna_predicate(
            subset,
            how=how,
            axis=axis,
            thresh=thresh,
            kwargs=kwargs,
        )
        if predicate is not None:
            if predicate == "":
                return self
            return self._with_combined_filter_condition(predicate)
        if self.source.source_format == "vortex" and not kwargs:
            normalized_how = _normalize_dropna_how(how)
            if normalized_how == "any" and subset is not None and thresh is None:
                target_columns = _normalize_columns((subset,))
                if target_columns and all(_is_sql_identifier(column) for column in target_columns):
                    return self._with_combined_filter_condition(
                        " AND ".join(f"{column} IS NOT NULL" for column in target_columns)
                    )
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
        if self.source.source_format == "vortex":
            return self._unsupported_operation("native-vortex-cast", target_ref, check=check)
        return self._unsupported_operation("astype", target_ref, check=check)

    def sample(
        self,
        n: int | None = None,
        fraction: float | None = None,
        seed: int | None = None,
        *,
        frac: float | None = None,
        random_state: int | None = None,
        weights: object | None = None,
        replace: bool = False,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped deterministic sample over admitted Vortex-backed rows."""

        effective_fraction = _normalize_sample_fraction_alias(
            fraction=fraction,
            frac=frac,
        )
        effective_seed = _normalize_sample_seed_alias(
            seed=seed,
            random_state=random_state,
        )
        target_ref = _normalize_sample_target(
            n=n,
            fraction=effective_fraction,
            seed=effective_seed,
            weights=weights,
            replace=replace,
        )
        if effective_seed is not None and (
            isinstance(effective_seed, bool) or not isinstance(effective_seed, int)
        ):
            return self._unsupported_operation("sample", target_ref, check=check)
        sample_weight_column = _normalize_sample_weight_column(weights)
        if weights is not None and sample_weight_column is None:
            return self._unsupported_operation("sample", target_ref, check=check)
        sample_seed = (
            0
            if effective_seed is None
            else _normalize_non_negative_int("sample seed", effective_seed)
        )
        if effective_fraction is not None:
            sample_fraction = _normalize_sample_fraction_value(effective_fraction)
            values = ["fraction", f"{sample_fraction:.12g}", str(sample_seed)]
            if sample_weight_column is not None:
                values = [
                    f"fraction={sample_fraction:.12g}",
                    f"seed={sample_seed}",
                    f"weights={sample_weight_column}",
                ]
            if replace is True:
                values.append("replacement")
            return self._append(
                WorkflowOperation(
                    "sample",
                    tuple(values),
                )
            )
        sample_n = 1 if n is None else _normalize_non_negative_int("sample n", n)
        _validate_positive_row_count("sample n", sample_n)
        if sample_weight_column is not None:
            values = [f"n={sample_n}", f"seed={sample_seed}", f"weights={sample_weight_column}"]
            if replace is True:
                values.append("replacement")
            return self._append(WorkflowOperation("sample", tuple(values)))
        if replace is True:
            return self._append(
                WorkflowOperation("sample", (str(sample_n), str(sample_seed), "replacement"))
            )
        return self._append(WorkflowOperation("sample", (str(sample_n), str(sample_seed))))

    def explode(
        self,
        *columns: object,
        ignore_index: bool = False,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped native Vortex list explode plan when explicitly shaped."""

        normalized_columns = _normalize_columns(columns)
        target_ref = ",".join(normalized_columns)
        if ignore_index:
            target_ref = f"{target_ref};ignore_index=true"
        payload = _vortex_explode_projection_payload(normalized_columns)
        if payload is not None:
            return self._append(WorkflowOperation("explode", (payload,)))
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
                    source_bindings=(*self._declared_sources(), *other._declared_sources()),
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped native Vortex pivot plan when explicitly shaped."""

        target_ref = _normalize_pivot_target(
            index=index,
            columns=columns,
            values=values,
            kwargs=kwargs,
        )
        payload = _vortex_pivot_projection_payload(
            index=index,
            columns=columns,
            values=values,
            aggregate="first_unique",
            kwargs=kwargs,
        )
        if payload is not None:
            return self._append(WorkflowOperation("pivot", (payload,)))
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped native Vortex pivot-table plan when explicitly shaped."""

        target_ref = _normalize_pivot_table_target(
            values=values,
            index=index,
            columns=columns,
            aggfunc=aggfunc,
            kwargs=kwargs,
        )
        aggregate = _normalize_pivot_table_single_aggregate(
            aggfunc,
            values=values,
        )
        payload = _vortex_pivot_projection_payload(
            index=index,
            columns=columns,
            values=values,
            aggregate=aggregate or "",
            kwargs=kwargs,
        )
        if payload is not None:
            return self._append(WorkflowOperation("pivot", (payload,)))
        return self._unsupported_operation("pivot-table", target_ref, check=check)

    def melt(
        self,
        *,
        id_vars: object | None = None,
        value_vars: object | None = None,
        var_name: object | None = None,
        value_name: object | None = None,
        ignore_index: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped native Vortex melt plan when explicitly or schema-inferred shaped."""

        target_ref = _normalize_melt_target(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
            ignore_index=ignore_index,
            kwargs=kwargs,
        )
        effective_value_vars = value_vars
        if effective_value_vars is None and not kwargs and ignore_index:
            effective_value_vars = self._schema_declared_melt_value_columns(id_vars)
        if not ignore_index and not kwargs:
            try:
                original_id_columns = _normalize_optional_columns(id_vars)
                value_columns = (
                    _normalize_columns((value_vars,))
                    if value_vars is not None
                    else self._schema_declared_melt_value_columns(id_vars)
                )
            except (TypeError, ValueError):
                value_columns = None
            if value_columns:
                indexed = self.reset_index()
                if isinstance(indexed, LazyFrame):
                    id_columns = ("index", *original_id_columns)
                    payload = _vortex_melt_projection_payload(
                        id_vars=id_columns,
                        value_vars=value_columns,
                        var_name=var_name,
                        value_name=value_name,
                        kwargs=kwargs,
                    )
                    if payload is not None:
                        candidate = indexed._append(WorkflowOperation("melt", (payload,)))
                        if candidate._relation_statement() is not None:
                            return candidate
        if ignore_index and (
            payload := _vortex_melt_projection_payload(
                id_vars=id_vars,
                value_vars=effective_value_vars,
                var_name=var_name,
                value_name=value_name,
                kwargs=kwargs,
            )
        ) is not None:
            return self._append(WorkflowOperation("melt", (payload,)))
        return self._unsupported_operation("melt", target_ref, check=check)

    def rolling(
        self,
        window: object,
        *,
        min_periods: int | None = None,
        center: bool = False,
        check: bool = False,
        **kwargs: object,
    ) -> "RollingFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped source-order rolling builder when admitted."""

        target_ref = _normalize_rolling_target(
            window,
            min_periods=min_periods,
            center=center,
            kwargs=kwargs,
        )
        if kwargs:
            return self._unsupported_operation("rolling", target_ref, check=check)
        try:
            window_size = _normalize_positive_int("rolling window", window)
            min_window_periods = (
                window_size
                if min_periods is None
                else _normalize_positive_int("rolling min_periods", min_periods)
            )
        except (TypeError, ValueError):
            return self._unsupported_operation("rolling", target_ref, check=check)
        if min_window_periods > window_size:
            return self._unsupported_operation("rolling", target_ref, check=check)
        return RollingFrame(
            frame=self,
            window=window_size,
            min_periods=min_window_periods,
            center=bool(center),
        )

    def duplicated(
        self,
        subset: object | None = None,
        *,
        keep: str | bool = "first",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped row-duplicate mask for admitted Vortex-backed rows."""

        target_ref = _normalize_duplicated_target(subset=subset, keep=keep, kwargs=kwargs)
        normalized_keep = _normalize_duplicate_keep_value(keep)
        if not kwargs and normalized_keep in {"first", "last", "false"}:
            columns = self._duplicate_mask_projection_columns(subset)
            if columns is not None:
                return self._append(
                    WorkflowOperation("duplicate_mask", (*columns, f"keep={normalized_keep}"))
                )
        return self._unsupported_operation("duplicated", target_ref, check=check)

    def tail(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> "LazyFrame":
        """Return a lazy source-order tail window over admitted Vortex-backed rows."""

        _validate_positive_row_count("tail limit", limit)
        return self._append(WorkflowOperation("tail", (str(limit),)))

    def describe(
        self,
        *columns: object,
        check: bool = False,
        **kwargs: object,
    ) -> (
        WorkflowProfileReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Return a metadata-first profile for admitted local/Vortex columns."""

        target_ref = _normalize_describe_target(columns, kwargs)
        if not kwargs:
            if not columns:
                return self.profile(check=check)
            normalized_columns = _normalize_columns(columns)
            if self.source.source_format == "vortex":
                projection_columns = self._declared_projection_columns()
                if projection_columns is None:
                    return self._unsupported_operation("describe", target_ref, check=check)
                missing = tuple(
                    column for column in normalized_columns if column not in projection_columns
                )
                if missing:
                    return self._unsupported_operation("describe", target_ref, check=check)
            if all(_is_sql_identifier(column) for column in normalized_columns):
                return self.select(*normalized_columns).profile(check=check)
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
        if normalized_keep in {"first", "last", "all"} and self._can_append_sort(
            normalized_columns
        ):
            sort_values = (
                ("desc" if descending else "asc", *normalized_columns)
                if normalized_keep == "first"
                else (
                    "desc" if descending else "asc",
                    f"keep={normalized_keep}",
                    *normalized_columns,
                )
            )
            sorted_frame = self._append(
                WorkflowOperation(
                    "sort",
                    sort_values,
                )
            )
            return sorted_frame.limit(normalized_n)
        if self.source.source_format == "vortex":
            sort_values = (
                ("desc" if descending else "asc", *normalized_columns)
                if normalized_keep == "first"
                else (
                    "desc" if descending else "asc",
                    f"keep={normalized_keep}",
                    *normalized_columns,
                )
            )
            sorted_frame = self._append(
                WorkflowOperation(
                    "sort",
                    sort_values,
                )
            )
            return sorted_frame.limit(normalized_n)
        return self._unsupported_operation(operation, target_ref, check=check)

    def fillna(
        self,
        value: object | None = None,
        *,
        axis: object | None = None,
        inplace: bool = False,
        method: object | None = None,
        limit: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a bounded schema-declared null-fill projection when admitted."""

        target_ref = _normalize_fillna_target(
            value,
            axis=axis,
            inplace=inplace,
            method=method,
            limit=limit,
            kwargs=kwargs,
        )
        if (
            not kwargs
            and value is None
            and _normalize_dropna_axis(axis) == "rows"
            and not inplace
            and (method_name := _normalize_fill_method(method)) == "ffill"
        ):
            fill_limit = _normalize_optional_positive_int("fillna limit", limit)
            payload = self._forward_fill_null_expression_project_payload(fill_limit)
            if payload is not None:
                return self._append(WorkflowOperation("expression_project", (payload,)))
        if (
            not kwargs
            and method is None
            and limit is None
            and _normalize_dropna_axis(axis) in {"rows", "columns"}
            and not inplace
            and (projection := self._schema_declared_fillna_projection(value))
        ):
            return self._with_rewritten_projection(projection)
        return self._unsupported_operation("fillna", target_ref, check=check)

    def fill_null(
        self,
        value: object | None = None,
        *,
        axis: object | None = None,
        inplace: bool = False,
        method: object | None = None,
        limit: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `fillna(...)` using expression-engine null terminology."""

        return self.fillna(
            value,
            axis=axis,
            inplace=inplace,
            method=method,
            limit=limit,
            check=check,
            **kwargs,
        )

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
        axis: object | None = None,
        inplace: bool = False,
        level: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped scalar conditional rewrite when the native route can admit it."""

        target_ref = _normalize_mask_target(
            cond=cond,
            other=other,
            axis=axis,
            inplace=inplace,
            level=level,
            kwargs=kwargs,
        )
        if (
            not kwargs
            and _normalize_dropna_axis(axis) == "rows"
            and not inplace
            and level is None
        ):
            predicate = _vortex_tiny_predicate_from_sql(_predicate_sql(cond))
            target_column = (
                _vortex_tiny_predicate_column(predicate) if predicate is not None else None
            )
            if target_column is not None:
                payload = self._expression_project_payload(
                    (
                        {
                            "kind": "mask_scalar",
                            "target_column": target_column,
                            "predicate": predicate,
                            "replacement": other,
                        },
                    ),
                    required_columns=(target_column,),
                )
                if payload is not None:
                    return self._append(WorkflowOperation("expression_project", (payload,)))
        return self._unsupported_operation("mask", target_ref, check=check)

    def replace(
        self,
        to_replace: object | None = None,
        value: object | None = None,
        *,
        regex: bool = False,
        inplace: bool = False,
        method: object | None = None,
        limit: object | None = None,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped scalar value rewrite when the native route can admit it."""

        target_ref = _normalize_replace_target(
            to_replace=to_replace,
            value=value,
            regex=regex,
            inplace=inplace,
            method=method,
            limit=limit,
            kwargs=kwargs,
        )
        if (
            not kwargs
            and not inplace
            and method is None
            and limit is None
            and to_replace is not None
        ):
            rewrite_specs = (
                self._regex_replace_rewrite_specs(to_replace, value)
                if regex
                else self._replace_rewrite_specs(to_replace, value)
            )
            if rewrite_specs is not None:
                payload = self._expression_project_payload(
                    rewrite_specs,
                    required_columns=tuple(
                        str(spec["target_column"]) for spec in rewrite_specs
                    ),
                )
                if payload is not None:
                    return self._append(WorkflowOperation("expression_project", (payload,)))
        return self._unsupported_operation("replace", target_ref, check=check)

    def apply(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped ShardLoom plan-transform apply when admitted."""

        if isinstance(function, WorkflowPlanTransform):
            result = function.apply(self, *args, **kwargs)
            if isinstance(result, LazyFrame):
                return result
            return self._unsupported_operation(
                "apply",
                f"plan_transform={function.name};return_type={type(result).__name__}",
                check=check,
            )
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped ShardLoom plan-transform pipe when admitted."""

        if isinstance(function, WorkflowPlanTransform):
            result = function.apply(self, *args, **kwargs)
            if isinstance(result, LazyFrame):
                return result
            return self._unsupported_operation(
                "pipe",
                f"plan_transform={function.name};return_type={type(result).__name__}",
                check=check,
            )
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return scoped column transform mappings when admitted."""

        if isinstance(function, WorkflowColumnTransform) and not args and not kwargs:
            return self.with_columns(function.items, check=check)
        if not args and not kwargs and isinstance(function, Mapping):
            return self.with_columns(function, check=check)
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return scoped column transforms while blocking Python cell callables."""

        if isinstance(function, WorkflowColumnTransform) and not args and not kwargs:
            return self.with_columns(function.items, check=check)
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return scoped column transforms while blocking Python element callables."""

        if isinstance(function, WorkflowColumnTransform) and not args and not kwargs:
            return self.with_columns(function.items, check=check)
        target_ref = _normalize_callable_transform_target("map", function, args, kwargs)
        return self._unsupported_operation("map", target_ref, check=check)

    def map_rows(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return scoped declarative row transforms while blocking Python row callables."""

        if isinstance(function, WorkflowRowTransform) and not args and not kwargs:
            return self.with_columns(function.items, check=check)
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped expression-project assignment when admitted."""

        target_ref = _normalize_eval_target(expr, kwargs)
        if not kwargs:
            rewrite_specs = _eval_numeric_scalar_assignment_rewrites(expr)
            if rewrite_specs is not None:
                target_columns = tuple(
                    str(rewrite_spec["target_column"])
                    for rewrite_spec in rewrite_specs
                )
                payload = self._expression_project_payload(
                    rewrite_specs,
                    required_columns=target_columns,
                )
                if payload is not None:
                    return self._append(WorkflowOperation("expression_project", (payload,)))
        return self._unsupported_operation("eval", target_ref, check=check)

    def distinct(self) -> "LazyFrame":
        """Return a lazy plan with row-level duplicate removal."""

        return self._append(WorkflowOperation("distinct", ()))

    def union(
        self,
        other: "LazyFrame | SqlWorkflow",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION` workflow over two local-source plans."""

        return self._union(other, union_all=False, check=check)

    def union_all(
        self,
        other: "LazyFrame | SqlWorkflow",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION ALL` workflow over two local-source plans."""

        return self._union(other, union_all=True, check=check)

    def intersect(
        self,
        other: "LazyFrame | SqlWorkflow",
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
        other: "LazyFrame | SqlWorkflow",
        *,
        check: bool = False,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `EXCEPT` workflow over two local-source plans."""

        return self.except_rows(other, check=check)

    def except_rows(
        self,
        other: "LazyFrame | SqlWorkflow",
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
        other: "LazyFrame | SqlWorkflow",
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
        """Return scoped retained rows after native Vortex row-key deduplication."""

        if not kwargs:
            normalized_keep = _normalize_duplicate_keep_value(keep)
            projection_columns = self._unary_projection_columns()
            try:
                subset_columns = (
                    projection_columns
                    if subset is None and projection_columns is not None
                    else _normalize_columns((subset,))
                )
            except (TypeError, ValueError):
                subset_columns = ()
            if (
                normalized_keep in {"first", "last", "false"}
                and projection_columns is not None
                and subset_columns
                and (subset_columns == ("*",) or all(_is_sql_identifier(column) for column in subset_columns))
                and all(column == "*" or _is_sql_identifier(column) for column in projection_columns)
                and (projection_columns == ("*",) or all(column in projection_columns for column in subset_columns))
            ):
                return self._append(
                    WorkflowOperation(
                        "drop_duplicates",
                        (*subset_columns, f"keep={normalized_keep}"),
                    )
                )
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
        drop: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Attach scoped index-state metadata without changing encoded row data."""

        target_ref = _normalize_index_target(
            "set_index",
            keys=keys,
            drop=drop,
            kwargs=kwargs,
        )
        index_columns = _normalize_index_columns(keys)
        if drop is False and not kwargs:
            return self._append(WorkflowOperation("set_index", index_columns))
        return self._unsupported_operation("set-index", target_ref, check=check)

    def reset_index(
        self,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Materialize an explicit source-order row number unless `drop=True` is requested."""

        if kwargs == {"drop": True}:
            if _workflow_index_columns(self.operations):
                return LazyFrame(
                    source=self.source,
                    client=self.client,
                    operations=_strip_index_metadata_operations(self.operations),
                    engine_mode=self.engine_mode,
                )
            return self
        if not kwargs or kwargs == {"drop": False}:
            base = LazyFrame(
                source=self.source,
                client=self.client,
                operations=_strip_index_metadata_operations(self.operations),
                engine_mode=self.engine_mode,
            )
            projection_columns = base._expression_project_projection_columns(())
            if projection_columns is not None:
                target_column = (
                    "index"
                    if "index" not in projection_columns
                    else "__shardloom_row_number"
                )
                payload = base._row_number_expression_project_payload(target_column)
                if payload is not None:
                    return base._append(WorkflowOperation("expression_project", (payload,)))
            if _workflow_index_columns(self.operations):
                return base
        target_ref = _normalize_index_target("reset_index", keys=None, kwargs=kwargs)
        return self._unsupported_operation("reset-index", target_ref, check=check)

    def sort_index(
        self,
        *,
        ascending: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Preserve source order when no explicit DataFrame index state exists."""

        if not kwargs:
            index_columns = _workflow_index_columns(self.operations)
            if index_columns:
                return self.sort(*index_columns, descending=not ascending, check=check)
            if ascending is True:
                return self
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
            literal = (
                expression
                if isinstance(expression, (Decimal, bytes, bytearray, date))
                else _generated_literal_expression(expression)
            )
            expression_sql = "NULL" if literal is None else _sql_literal(literal)
        except (TypeError, ValueError):
            try:
                expression_sql = _sql_computed_projection_expression(expression)
            except (TypeError, ValueError):
                expression_text = _require_non_empty("column expression", expression)
                if self.source.source_format == "vortex" and _sql_text_looks_like_cast(
                    expression_text
                ):
                    return self._append(
                        WorkflowOperation("with_column", (column_name, expression_text))
                    )
                return self._unsupported_operation(
                    "with-column",
                    f"{column_name}={expression_text}",
                    check=check,
                )
        if expression_project_payload := self._numeric_scalar_expression_project_payload(
            column_name,
            expression_sql,
        ):
            return self._append(
                self._computed_projection_operation(expression_project_payload, column_name, expression_sql)
            )
        if expression_project_payload := self._string_replace_expression_project_payload(
            column_name,
            expression_sql,
        ):
            return self._append(
                self._computed_projection_operation(expression_project_payload, column_name, expression_sql)
            )
        if (self.source.source_format == "vortex"
                and isinstance(expression, ComplexProjectionExpression)
                and self._can_append_projection_column(column_name, allow_vortex=True)):
            projected = self._append(WorkflowOperation("with_column", (column_name, expression_sql)))
            if projected._has_structured_binary_export_shape():
                return projected
        if self._can_append_projection_column(column_name, allow_vortex=True):
            return self._append(WorkflowOperation("with_column", (column_name, expression_sql)))
        if self.source.source_format == "vortex" and _sql_text_looks_like_cast(expression_sql):
            return self._append(WorkflowOperation("with_column", (column_name, expression_sql)))
        return self._unsupported_operation(
            "with-column",
            f"{column_name}={expression_sql}",
            check=check,
        )

    def _computed_projection_operation(self, payload: str, name: str, expression: str) -> WorkflowOperation:
        from ._relational_sql import computed_projection

        columns = self._expression_project_projection_columns(())
        if columns == ("*",):
            return WorkflowOperation("with_column", (name, expression))
        return WorkflowOperation(
            "expression_project", (payload,),
            projection_sql=computed_projection(columns, name, expression) if columns is not None else None,
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
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = False,
    ) -> PublicWorkflowRoute:
        """Return the shared public route envelope for this lazy workflow."""

        normalized_bounded = (
            _workflow_has_limit(self.operations)
            if bounded is None and requested_output == "collect"
            else bounded
        )
        effective_evidence_level = evidence_level
        if (
            evidence_level == "runtime_smoke"
            and _is_declared_local_source(self.source)
        ):
            effective_evidence_level = "production_admitted_local_workflow"
        effective_execution_policy = (
            _public_workflow_default_execution_policy(self.source)
            if execution_policy is None
            else execution_policy
        )
        relational_statement = self._native_relational_statement()
        return self.client.public_workflow_route(
            "dataframe",
            input_uri=self.source.uri,
            input_format=_public_workflow_input_format(self.source),
            sql_statement=relational_statement,
            source_bindings=_workflow_source_bindings(self._declared_sources()),
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy=effective_execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=effective_evidence_level,
            bounded=normalized_bounded,
            check=check,
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
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
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Run this lazy workflow through the shared public route facade."""

        normalized_bounded = (
            _workflow_has_limit(self.operations)
            if bounded is None and requested_output == "collect"
            else bounded
        )
        effective_execution_policy = (
            _public_workflow_default_execution_policy(self.source)
            if execution_policy is None
            else execution_policy
        )
        relational_statement = self._native_relational_statement()
        return self.client.public_workflow_run(
            "dataframe",
            input_uri=self.source.uri,
            input_format=_public_workflow_input_format(self.source),
            sql_statement=relational_statement,
            source_bindings=_workflow_source_bindings(self._declared_sources()),
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=output_ref,
            execution_policy=effective_execution_policy,
            materialization_policy=materialization_policy,
            evidence_level=evidence_level,
            bounded=normalized_bounded,
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
            check=check,
        )

    def prepare(
        self,
        target_vortex_path: str | os.PathLike[str],
        *,
        evidence_level: str = "production_admitted_local_workflow",
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Prepare this source through the shared public route facade."""

        return self.client.public_workflow_prepare(
            "dataframe",
            input_uri=self.source.uri,
            input_format=_public_workflow_input_format(self.source),
            source_schema=_prepare_vortex_schema_hints(self.source),
            output_ref=target_vortex_path,
            plan_summary=self.operation_summary,
            evidence_level=evidence_level,
            memory_gb=DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
            max_parallelism=DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
            check=check,
        )

    def profile(
        self,
        limit: int = 100,
        *,
        check: bool = False,
    ) -> (
        WorkflowProfileReport
        | VortexWorkflowExecutionReport
        | UnsupportedWorkflowOperationReport
    ):
        """Return a metadata-first profile for admitted Vortex/prepared workflows."""

        _validate_positive_row_count("profile limit", limit)
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return WorkflowProfileReport(
            workflow=self,
            smoke_report=report,
            schema_report=_workflow_schema_report(self, report),
            limit=limit,
        )

    def collect(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Collect the complete bounded result through the shared native engine."""
        if limit is not None:
            return self.limit(limit).collect(check=check, memory_gb=memory_gb,
                                            max_parallelism=max_parallelism, spill=spill)
        statement = self._relation_statement()
        if statement is None:
            return self._unsupported_operation("collect", check=check)
        envelope = _collect_native_relational(
            self.client, statement, surface="dataframe", plan_summary=self.operation_summary,
            input_kwargs={"input_uri": self.source.uri,
                          "input_format": _public_workflow_input_format(self.source),
                          "source_bindings": _workflow_source_bindings(self._declared_sources())},
            check=check, memory_gb=memory_gb, max_parallelism=max_parallelism, spill=spill,
        )
        return VortexWorkflowExecutionReport(self, "collect", envelope)

    def count(
        self,
        *,
        check: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Declare COUNT over this complete input; native planning selects its strategy."""
        if self._relation_statement() is None:
            return self._unsupported_operation("count", check=check)
        return self._append(WorkflowOperation("aggregate", ("COUNT(*) AS count",))).collect(
            check=check, memory_gb=memory_gb, max_parallelism=max_parallelism, spill=spill)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Write this complete native declaration through the requested output adapter."""
        normalized_output_format = _normalize_local_output_format(output_format)
        requested_output = _public_write_request_for_format(normalized_output_format)
        if self._relation_statement() is None:
            return self._unsupported_operation(requested_output, str(target_uri), check=check)
        return self._public_workflow_write_report(
            target_uri, requested_output=requested_output, allow_overwrite=allow_overwrite,
            memory_gb=memory_gb, max_parallelism=max_parallelism, spill=spill, check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="jsonl")`."""

        return self.write(
            target_uri,
            output_format="jsonl",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_json(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="json")` (one JSON array)."""

        return self.write(
            target_uri,
            output_format="json",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="csv")`."""

        return self.write(
            target_uri,
            output_format="csv",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="parquet")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Parquet sink blocker.
        """

        return self.write(
            target_uri,
            output_format="parquet",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Arrow IPC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="arrow-ipc",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="avro")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Avro sink blocker.
        """

        return self.write(
            target_uri,
            output_format="avro",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="orc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic ORC sink blocker.
        """

        return self.write(
            target_uri,
            output_format="orc",
            allow_overwrite=allow_overwrite,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            spill=spill,
            check=check,
        )

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport
    ):
        """Write one declared workflow through the common multi-output owner."""
        normalized_outputs = _normalize_fanout_outputs(outputs)
        output_format, output_path = normalized_outputs[0]
        if self._relation_statement() is None:
            return self._unsupported_operation("fanout", str(output_path), check=check)
        return self._public_workflow_write_report(
            output_path, requested_output=_public_write_request_for_format(output_format),
            allow_overwrite=allow_overwrite, fanout_outputs=normalized_outputs[1:],
            memory_gb=memory_gb, max_parallelism=max_parallelism, spill=spill, check=check,
        )

    def to_pandas(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a pandas DataFrame at an explicit bounded materialization boundary."""

        pandas = _optional_module("pandas")
        if pandas is None:
            return self._unsupported_operation(
                "to-pandas",
                "missing optional dependency: pandas",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_pandas(report, pandas)

    def to_arrow(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table at an explicit bounded materialization boundary."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_table(report, pyarrow)

    def to_arrow_table(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table for admitted bounded local-source workflows."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-table",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_table(report, pyarrow)

    def to_arrow_ipc(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> bytes | UnsupportedWorkflowOperationReport:
        """Return Arrow IPC stream bytes for admitted bounded local-source workflows."""

        pyarrow = _optional_module("pyarrow")
        if pyarrow is None:
            return self._unsupported_operation(
                "to-arrow-ipc",
                "missing optional dependency: pyarrow",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_arrow_ipc(report, pyarrow)

    def to_numpy(
        self,
        *,
        limit: int | None = None,
        check: bool = False,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a NumPy array for admitted bounded local-source workflow rows."""

        numpy = _optional_module("numpy")
        if numpy is None:
            return self._unsupported_operation(
                "to-numpy",
                "missing optional dependency: numpy",
                check=check,
            )
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _result_to_numpy(report, numpy)

    def to_python_objects(
        self,
        *,
        check: bool = False,
    ) -> tuple[Mapping[str, Any], ...] | UnsupportedWorkflowOperationReport:
        """Return bounded Python row objects for admitted local-source workflows."""

        report = self._bounded_materialization_report(limit=None, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return report.python_objects

    def prepare_vortex(
        self,
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        workspace: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        memory_gb: int | None = None,
        max_parallelism: int | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        check: bool = True,
    ) -> VortexIngestSmokeReport | VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Prepare this raw local source into a caller-owned `VortexPreparedState`.

        When `workspace` is supplied without `target_vortex_path`, the target is derived as
        `<workspace>/<source-stem>.vortex`. The real CLI `vortex-prepare` route writes the selected
        single `.vortex` artifact and reports no-sidecar evidence; callers opt into replacing an
        existing artifact with ``allow_overwrite=True`` when the writer admits replacement.
        """

        if self.engine_mode not in {"auto", "batch"}:
            raise ValueError(
                "LazyFrame.prepare_vortex currently supports engine_mode='auto' or 'batch' "
                "for scoped local batch preparation; live/hybrid preparation remains gated"
            )
        if self.source.memory_input:
            target = _generated_prepared_vortex_target_path(
                self.source.uri.rsplit("/", 1)[-1],
                target_vortex_path=target_vortex_path, workspace=workspace,
            )
            return self.write_vortex(
                target, allow_overwrite=allow_overwrite, check=check,
                memory_gb=DEFAULT_LOCAL_RUNTIME_MEMORY_GB if memory_gb is None else memory_gb,
                max_parallelism=(DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM
                                 if max_parallelism is None else max_parallelism),
            )
        if self.source.source_format == "vortex":
            raise ValueError(
                "LazyFrame.prepare_vortex starts from raw compatibility input; "
                "read_vortex(...) sources are already Vortex-native"
            )
        if not _is_declared_local_source(self.source):
            raise ValueError(
                "LazyFrame.prepare_vortex requires a local CSV, JSON/JSONL/NDJSON, Parquet, "
                "Arrow IPC, Avro, or ORC source"
            )
        if self.operations:
            raise ValueError(
                "LazyFrame.prepare_vortex prepares the raw local source before query operators; "
                "call it directly on read_*(...) or use write_vortex(...) for a query-result sink"
            )
        target = _prepared_vortex_target_path(
            self.source.uri,
            target_vortex_path=target_vortex_path,
            workspace=workspace,
        )
        return self.client.vortex_prepare(
            self.source.uri,
            target,
            input_format=input_format,
            schema=_prepare_vortex_schema_hints(self.source),
            allow_overwrite=allow_overwrite,
            certification_level=certification_level,
            memory_gb=memory_gb,
            max_parallelism=max_parallelism,
            check=check,
        )

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Write native Vortex output through the common output adapter contract."""
        return self.write(target_uri, output_format="vortex", allow_overwrite=allow_overwrite,
                          memory_gb=memory_gb, max_parallelism=max_parallelism, spill=spill, check=check)


    def _public_workflow_write_report(
        self,
        target_uri: str | os.PathLike[str],
        *,
        requested_output: str,
        allow_overwrite: bool,
        check: bool,
        memory_gb: int = DEFAULT_LOCAL_RUNTIME_MEMORY_GB,
        max_parallelism: int = DEFAULT_LOCAL_RUNTIME_MAX_PARALLELISM,
        spill: Mapping[str, object] | str | None = None,
        fanout_outputs: Sequence[tuple[str, CommandPart]] | None = None,
    ) -> VortexWorkflowExecutionReport:
        statement = self._relation_statement()
        if statement is None:
            raise ValueError(
                "public workflow write facade requires an admitted local-source statement"
            )
        execution = self.client.public_workflow_run(
            "dataframe",
            input_uri=self.source.uri,
            input_format=_public_workflow_input_format(self.source),
            source_schema=_prepare_vortex_schema_hints(self.source),
            source_bindings=_workflow_source_bindings(self._declared_sources()),
            sql_statement=statement,
            plan_summary=self.operation_summary,
            requested_output=requested_output,
            output_ref=target_uri,
            fanout_outputs=fanout_outputs,
            materialization_policy="bounded",
            evidence_level="production_admitted_local_workflow",
            bounded=True,
            allow_overwrite=allow_overwrite,
            **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
            check=check,
        )
        return VortexWorkflowExecutionReport(
            workflow=self, operation=requested_output, envelope=execution.envelope,
        )

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
        other: "LazyFrame | SqlWorkflow | str",
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
        right_statement: str | None = None
        right_source_local = False
        if isinstance(other, LazyFrame):
            right_uri = other.source.uri
            right_summary = other.operation_summary
            right_operations = other.operations
            if right_operations:
                right_statement = other._relation_statement()
            right_source_local = other._relation_statement() is not None
            right_source_vortex = other.source.source_format == "vortex"
        elif isinstance(other, SqlWorkflow):
            right_uri = ""
            right_summary = other.operation_summary
            right_statement = other._relation_statement()
            right_source_local = True
            right_source_vortex = False
        else:
            right_uri = _require_non_empty("join right source", other)
            right_summary = right_uri
            right_source_local = _source_format_for_local_source_ref(right_uri) is not None
            right_source_vortex = _is_local_vortex_source_ref(right_uri)
        target = f"{normalized_how}:{columns}:{normalized_condition or ''}:{right_summary}"
        if (
            (
                self._relation_statement() is not None
                and (right_source_local or right_source_vortex)
            )
            and (not right_operations or right_statement is not None)
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
                    source_bindings=_predicate_sources(other, condition),
                    right_statement=right_statement,
                )
            )
        if self.source.source_format == "vortex" or right_source_vortex:
            return self._unsupported_operation("native-vortex-join", target, check=check)
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
        if self.source.source_format == "vortex":
            return self._unsupported_operation("native-vortex-aggregate", target, check=check)
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
        if self.source.source_format == "vortex":
            return self._unsupported_operation(
                "native-vortex-aggregate",
                ",".join(target_values),
                check=check,
            )
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
        """Declare native window projections, including SQL frame bounds and exclusions."""

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

        return self._bounded_schema_report(check=check)

    def describe_schema(
        self,
        *,
        check: bool = False,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return detailed bounded schema evidence for admitted local-source workflows."""

        return self._bounded_schema_report(check=check)

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
        report = self._bounded_schema_report(check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _validate_workflow_schema(report, normalized)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Run bounded data-quality checks for admitted local-source workflows."""

        normalized_checks = _normalize_columns(checks)
        parsed_checks = _parse_data_quality_checks(normalized_checks)
        if parsed_checks is not None:
            report = self._bounded_schema_report(check=check)
            if isinstance(report, UnsupportedWorkflowOperationReport):
                return report
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

        report = self._bounded_schema_report(check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return WorkflowDataQualityReport(schema_report=report)

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
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        schema_report = _workflow_schema_report(self, report)
        parsed_checks = parsed_checks or _workflow_quarantine_checks(schema_report, ())
        quality_report = _workflow_data_quality_report(schema_report, parsed_checks)
        rows = _workflow_quarantine_rows(schema_report, parsed_checks)
        normalized_output_format = _normalize_optional_quarantine_output_format(
            target_uri,
            output_format,
        )
        sink_report: VortexWorkflowExecutionReport | None = None
        if target_uri is not None and rows:
            pushdown_statement = self._quarantine_pushdown_statement(
                parsed_checks,
                limit=limit,
            )
            if pushdown_statement is not None and normalized_output_format is not None:
                sink = SqlWorkflow(
                    statement=pushdown_statement,
                    client=self.client,
                    source_bindings=self._declared_sources(),
                ).write(
                    target_uri,
                    output_format=normalized_output_format,
                    allow_overwrite=allow_overwrite,
                    check=check,
                )
                if sink.envelope.status == "success":
                    sink_report = sink
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

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded local preview when admitted, otherwise report unsupported."""

        _validate_positive_row_count("preview limit", limit)
        return self.limit(limit).collect(check=check)

    def head(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report using familiar DataFrame naming."""

        _validate_positive_row_count("head limit", limit)
        return self.limit(limit).collect(check=check)

    def take(
        self,
        count: int,
        *,
        check: bool = False,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report for the requested row count."""

        _validate_positive_row_count("take limit", count)
        return self.limit(count).collect(check=check)

    def display(
        self,
        limit: int = 20,
        *,
        check: bool = False,
    ) -> WorkflowNotebookPreview | UnsupportedWorkflowOperationReport:
        """Return a bounded notebook/display preview for admitted workflows."""

        _validate_positive_row_count("display limit", limit)
        report = self._bounded_materialization_report(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return WorkflowNotebookPreview(
            workflow=self,
            smoke_report=report,
            limit=limit,
        )

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
        return self._append(WorkflowOperation("select", projection))

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
                    source_bindings=operation.source_bindings,
                    right_statement=operation.right_statement,
                    projection_sql=operation.projection_sql,
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
        other: "LazyFrame | SqlWorkflow",
        *,
        union_all: bool,
        check: bool,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        operation = "union-all" if union_all else "union"
        keyword = "UNION ALL" if union_all else "UNION"
        return self._set_operation(other, operation=operation, keyword=keyword, check=check)

    def _set_operation(
        self,
        other: "LazyFrame | SqlWorkflow",
        *,
        operation: str,
        keyword: str,
        check: bool,
    ) -> "SqlWorkflow | UnsupportedWorkflowOperationReport":
        if isinstance(other, SqlWorkflow) and (left := self._relation_statement()) is not None:
            return SqlWorkflow(
                left, self.client, source_bindings=self._declared_sources(),
            )._set_operation(other, operation=operation, keyword=keyword, check=check)
        if isinstance(other, LazyFrame):
            left = self._sql_local_source_union_branch_statement()
            right = other._sql_local_source_union_branch_statement()
            if left is None and (complete_left := self._relation_statement()) is not None:
                left = f"SELECT * FROM ({complete_left}) AS _sl_set_left"
            if right is None and (complete_right := other._relation_statement()) is not None:
                right = f"SELECT * FROM ({complete_right}) AS _sl_set_right"
            if left is not None and right is not None:
                return SqlWorkflow(
                    statement=f"{left} {keyword} {right}",
                    client=self.client,
                    source_bindings=(*self._declared_sources(), *other._declared_sources()),
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
        operation = self._native_vortex_blocker_operation(operation, target_ref)
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

    def _native_vortex_blocker_operation(self, operation: str, target_ref: str | None) -> str:
        if self.source.source_format != "vortex":
            return operation
        normalized = operation.replace("_", "-")
        if normalized.startswith("native-vortex-"):
            return normalized
        if normalized in {
            "write-vortex",
            "write-parquet",
            "write-arrow-ipc",
            "write-arrow",
            "write-ipc",
            "write-avro",
            "write-orc",
            "write-jsonl",
            "write-csv",
            "fanout",
        }:
            return "native-vortex-sink"
        if normalized in {"join", "merge"} or any(
            op.kind == "join" for op in self.operations
        ):
            return "native-vortex-join"
        if normalized in {"aggregate", "aggregation", "agg", "group-by", "groupby"} or any(
            op.kind in {"aggregate", "group_by"} for op in self.operations
        ):
            return "native-vortex-aggregate"
        if normalized in {"nlargest", "nsmallest"} or _workflow_has_top_n_shape(
            self.operations
        ):
            return "native-vortex-top-n"
        if normalized in {"astype", "cast", "try-cast"} or any(
            op.kind == "with_column"
            and any(_sql_text_looks_like_cast(value) for value in op.values)
            for op in self.operations
        ):
            return "native-vortex-cast"
        if target_ref is not None and _sql_text_looks_like_cast(target_ref):
            return "native-vortex-cast"
        if any(
            op.kind == "filter" and _sql_filter_looks_like_substring_contains(op.values[0])
            for op in self.operations
        ):
            return "native-vortex-string-contains"
        return operation



















    def _has_structured_binary_export_shape(self) -> bool:
        if self.source.source_format != "vortex":
            return False
        operations = [op for op in self.operations if op.kind != "set_index"]
        if len(operations) < 1:
            return False
        if operations[-1].kind == "limit":
            try:
                if int(operations[-1].values[0]) <= 0:
                    return False
            except (TypeError, ValueError):
                return False
            operations = operations[:-1]
        if not operations or operations[0].kind != "select":
            return False
        if not all(_is_sql_identifier(column) and column.lower() not in {"null", "true", "false"}
                   for column in operations[0].values):
            return False
        for operation in operations[1:]:
            if operation.kind != "with_column" or len(operation.values) != 2:
                return False
            name, expression = operation.values
            if not _is_sql_identifier(name):
                return False
            expression_text = str(expression).strip()
            if expression_text.startswith("ARRAY[") and expression_text.endswith("]"):
                continue
            if expression_text.startswith("STRUCT(") and expression_text.endswith(")"):
                inner = expression_text[len("STRUCT(") : -1].strip()
                try:
                    columns = _split_projection_function_args(inner)
                except ValueError:
                    return False
                if columns and all(_is_sql_identifier(column) for column in columns):
                    continue
            return False
        return True

    def _can_append_scalar_aggregate(self) -> bool:
        if self._relation_statement() is not None:
            return True
        if not _is_declared_local_source(self.source) and self.source.source_format != "vortex":
            return False
        if self.source.source_format == "vortex" and any(
            operation.kind == "limit" for operation in self.operations
        ):
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by", "sort"}
            for operation in self.operations
        )

    def _can_append_group_by_aggregate(self, columns: tuple[str, ...]) -> bool:
        if self._relation_statement() is not None:
            return True
        if not _is_declared_local_source(self.source):
            return False
        return all(
            operation.kind not in {"select", "aggregate", "group_by", "sort"}
            for operation in self.operations
        )

    def _can_append_value_counts(self, columns: tuple[str, ...]) -> bool:
        if not _is_declared_local_source(self.source):
            return False
        if not columns or any(not _is_sql_identifier(column) for column in columns):
            return False
        filter_count = sum(1 for operation in self.operations if operation.kind == "filter")
        return filter_count <= 1 and all(
            operation.kind == "filter" for operation in self.operations
        )

    def _can_append_nunique(self, column: str) -> bool:
        if not _is_declared_local_source(self.source) or not _is_sql_identifier(column):
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
        if not columns:
            return False
        if len(set(columns)) != len(columns):
            return False
        if self._relation_statement() is not None:
            return True
        if any(operation.kind == "limit" for operation in self.operations):
            return False
        if _is_declared_local_source(self.source):
            return all(operation.kind != "sort" for operation in self.operations)
        if self.source.source_format == "vortex":
            return all(
                operation.kind in {"filter", "select", "set_index", "group_by", "aggregate", "having", "window", "join"}
                for operation in self.operations
            )
        return False

    def _can_append_window(self, expressions: tuple[str, ...]) -> bool:
        if expressions and self._relation_statement() is not None:
            return True
        if (not (_is_declared_local_source(self.source) or self.source.source_format == "vortex")
                or not expressions):
            return False
        for operation in self.operations:
            if operation.kind in {"select", "filter", "window"}:
                continue
            return False
        return True

    def _can_append_having(self) -> bool:
        if self._relation_statement() is not None:
            return bool(self.operations and self.operations[-1].kind == "aggregate")
        if not _is_declared_local_source(self.source) and self.source.source_format != "vortex":
            return False
        saw_aggregate = False
        for operation in self.operations:
            if operation.kind == "aggregate":
                saw_aggregate = True
                continue
            if saw_aggregate and operation.kind in {"filter", "having", "sort", "limit"}:
                return False
        return saw_aggregate

    def _can_append_projection_column(self, column_name: str, *, allow_vortex: bool = False) -> bool:
        if self._relation_statement() is not None:
            return True
        if (not _is_declared_local_source(self.source)
                and not (allow_vortex and self.source.source_format == "vortex")):
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
        if self._relation_statement() is None:
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
            return other._relation_statement() is not None
        return _source_format_for_local_source_ref(str(other)) is not None

    def _unary_projection_columns(self) -> tuple[str, ...] | None:
        from ._relational_sql import frame_stages

        rendered = frame_stages(self)
        if rendered is None:
            return None
        return rendered.columns if rendered.columns is not None else ("*",)

    def _expression_project_projection_columns(
        self,
        required_columns: tuple[str, ...],
    ) -> tuple[str, ...] | None:
        projection_columns = self._unary_projection_columns()
        if projection_columns is None:
            return None
        required = tuple(dict.fromkeys(required_columns))
        if any(not _is_sql_identifier(column) for column in required):
            return None
        if projection_columns != ("*",) and any(column not in projection_columns for column in required):
            return None
        return projection_columns

    def _duplicate_mask_projection_columns(
        self,
        subset: object | None,
    ) -> tuple[str, ...] | None:
        projection_columns = self._unary_projection_columns()
        if projection_columns is None:
            return None
        if subset is None:
            return projection_columns
        subset_columns = _normalize_columns((subset,))
        if not subset_columns or any(not _is_sql_identifier(column) for column in subset_columns):
            return None
        missing = tuple(column for column in subset_columns if column not in projection_columns) if projection_columns != ("*",) else ()
        if missing:
            raise ValueError(
                "duplicated subset referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        return subset_columns

    def _expression_project_payload(
        self,
        rewrite_specs: tuple[Mapping[str, object], ...],
        *,
        required_columns: tuple[str, ...],
    ) -> str | None:
        projection_columns = self._expression_project_projection_columns(required_columns)
        if projection_columns is None:
            return None
        # Declared source types remain hints only while stages preserve those
        # fields. Derived expressions bind their types in the native executor.
        schema = self.source.schema_map if all(
            operation.kind in {"filter", "select", "set_index", "sort", "limit", "tail", "sample", "distinct", "drop_duplicates"}
            and (operation.kind != "select" or all(_is_sql_identifier(value) for value in operation.values))
            for operation in self.operations
        ) else {}
        rewrites: list[dict[str, object]] = []
        for spec in rewrite_specs:
            kind = str(spec.get("kind", "")).strip()
            target_column = str(spec.get("target_column", "")).strip()
            if not kind or not target_column or not _is_sql_identifier(target_column):
                return None
            if kind == "row_number":
                start = spec.get("start", 0)
                if isinstance(start, bool) or not isinstance(start, int) or start < 0:
                    return None
                rewrites.append(
                    {
                        "kind": "row_number",
                        "target_column": target_column,
                        "start": start,
                    }
                )
                continue
            target_dtype = schema.get(target_column)
            if kind == "mask_scalar":
                predicate = str(spec.get("predicate", "")).strip()
                replacement = _vortex_expression_scalar_payload(
                    spec.get("replacement"),
                    target_dtype=target_dtype,
                )
                if not predicate or replacement is None:
                    return None
                rewrites.append(
                    {
                        "kind": "mask_scalar",
                        "target_column": target_column,
                        "predicate": predicate,
                        "replacement": replacement,
                    }
                )
            elif kind == "replace_scalar":
                to_replace = _vortex_expression_scalar_payload(
                    spec.get("to_replace"),
                    target_dtype=target_dtype,
                    allow_null=False,
                )
                replacement = _vortex_expression_scalar_payload(
                    spec.get("replacement"),
                    target_dtype=target_dtype,
                )
                if to_replace is None or replacement is None:
                    return None
                rewrites.append(
                    {
                        "kind": "replace_scalar",
                        "target_column": target_column,
                        "to_replace": to_replace,
                        "replacement": replacement,
                    }
                )
            elif kind == "string_replace_scalar":
                dtype = target_dtype.strip().lower().replace("-", "_") if target_dtype else None
                needle = spec.get("needle")
                replacement = spec.get("replacement")
                if (
                    dtype not in {None, "utf8", "string", "str"}
                    or not isinstance(needle, str)
                    or needle == ""
                    or not isinstance(replacement, str)
                ):
                    return None
                rewrites.append(
                    {
                        "kind": "string_replace_scalar",
                        "target_column": target_column,
                        "needle": needle,
                        "replacement": replacement,
                    }
                )
            elif kind == "regex_replace_scalar":
                dtype = target_dtype.strip().lower().replace("-", "_") if target_dtype else None
                pattern = spec.get("pattern")
                replacement = spec.get("replacement")
                if (
                    dtype not in {None, "utf8", "string", "str"}
                    or not isinstance(pattern, str)
                    or pattern == ""
                    or not isinstance(replacement, str)
                ):
                    return None
                rewrites.append(
                    {
                        "kind": "regex_replace_scalar",
                        "target_column": target_column,
                        "pattern": pattern,
                        "replacement": replacement,
                    }
                )
            elif kind == "numeric_scalar_arithmetic":
                operator = str(spec.get("operator", "")).strip()
                if operator not in {"+", "-", "*", "/"}:
                    return None
                operand = _vortex_expression_scalar_payload(
                    spec.get("operand"),
                    target_dtype=target_dtype,
                    allow_null=False,
                )
                if operand is None:
                    return None
                rewrites.append(
                    {
                        "kind": "numeric_scalar_arithmetic",
                        "target_column": target_column,
                        "operator": operator,
                        "operand": operand,
                    }
                )
            elif kind == "forward_fill_null":
                limit = spec.get("limit")
                rewrite: dict[str, object] = {
                    "kind": "forward_fill_null",
                    "target_column": target_column,
                }
                if limit is not None:
                    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                        return None
                    rewrite["limit"] = limit
                rewrites.append(rewrite)
            else:
                return None
        if not rewrites:
            return None
        payload = {
            "columns": "*" if projection_columns == ("*",) else list(projection_columns),
            "rewrites": rewrites,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _row_number_expression_project_payload(
        self,
        target_column: str,
    ) -> str | None:
        projection_columns = self._expression_project_projection_columns(())
        if projection_columns is None:
            return None
        return self._expression_project_payload(
            (
                {
                    "kind": "row_number",
                    "target_column": target_column,
                    "start": 0,
                },
            ),
            required_columns=(),
        )

    def _forward_fill_null_expression_project_payload(
        self,
        limit: int | None,
    ) -> str | None:
        projection_columns = self._expression_project_projection_columns(())
        if projection_columns is None or projection_columns == ("*",):
            return None
        return self._expression_project_payload(
            tuple(
                {
                    "kind": "forward_fill_null",
                    "target_column": column,
                    "limit": limit,
                }
                if limit is not None
                else {
                    "kind": "forward_fill_null",
                    "target_column": column,
                }
                for column in projection_columns
            ),
            required_columns=projection_columns,
        )

    def _replace_rewrite_specs(
        self,
        to_replace: object,
        value: object,
    ) -> tuple[Mapping[str, object], ...] | None:
        projection_columns = self._expression_project_projection_columns(())
        if projection_columns is None:
            return None
        specs: list[Mapping[str, object]] = []
        if isinstance(to_replace, Mapping):
            if not to_replace:
                return None
            replacement_map = value if isinstance(value, Mapping) else None
            for raw_column, old_value in to_replace.items():
                column = str(raw_column).strip()
                if not _is_sql_identifier(column) or column not in projection_columns:
                    return None
                if isinstance(old_value, Mapping) and value is None:
                    if not old_value:
                        return None
                    for nested_old, nested_new in old_value.items():
                        specs.append(
                            {
                                "kind": "replace_scalar",
                                "target_column": column,
                                "to_replace": nested_old,
                                "replacement": nested_new,
                            }
                        )
                    continue
                if replacement_map is not None:
                    if raw_column not in replacement_map and column not in replacement_map:
                        return None
                    new_value = replacement_map.get(raw_column, replacement_map.get(column))
                else:
                    new_value = value
                specs.append(
                    {
                        "kind": "replace_scalar",
                        "target_column": column,
                        "to_replace": old_value,
                        "replacement": new_value,
                    }
                )
            return tuple(specs)
        if len(projection_columns) != 1:
            return None
        return (
            {
                "kind": "replace_scalar",
                "target_column": projection_columns[0],
                "to_replace": to_replace,
                "replacement": value,
            },
        )

    def _regex_replace_rewrite_specs(
        self,
        to_replace: object,
        value: object,
    ) -> tuple[Mapping[str, object], ...] | None:
        projection_columns = self._expression_project_projection_columns(())
        if projection_columns is None:
            return None
        specs: list[Mapping[str, object]] = []
        if isinstance(to_replace, Mapping):
            if not to_replace:
                return None
            replacement_map = value if isinstance(value, Mapping) else None
            for raw_column, pattern in to_replace.items():
                column = str(raw_column).strip()
                if not _is_sql_identifier(column) or column not in projection_columns:
                    return None
                if not isinstance(pattern, str) or pattern == "":
                    return None
                if replacement_map is not None:
                    if raw_column not in replacement_map and column not in replacement_map:
                        return None
                    replacement = replacement_map.get(raw_column, replacement_map.get(column))
                else:
                    replacement = value
                if not isinstance(replacement, str):
                    return None
                specs.append(
                    {
                        "kind": "regex_replace_scalar",
                        "target_column": column,
                        "pattern": pattern,
                        "replacement": replacement,
                    }
                )
            return tuple(specs)
        if (
            len(projection_columns) != 1
            or not isinstance(to_replace, str)
            or to_replace == ""
            or not isinstance(value, str)
        ):
            return None
        return (
            {
                "kind": "regex_replace_scalar",
                "target_column": projection_columns[0],
                "pattern": to_replace,
                "replacement": value,
            },
        )

    def _string_replace_expression_project_payload(
        self,
        column_name: str,
        expression_sql: str,
    ) -> str | None:
        parsed = _vortex_string_replace_expression_parts(expression_sql)
        if parsed is None:
            return None
        source_column, needle, replacement = parsed
        if source_column != column_name:
            return None
        return self._expression_project_payload(
            (
                {
                    "kind": "string_replace_scalar",
                    "target_column": column_name,
                    "needle": needle,
                    "replacement": replacement,
                },
            ),
            required_columns=(column_name,),
        )

    def _numeric_scalar_expression_project_payload(
        self,
        column_name: str,
        expression_sql: str,
    ) -> str | None:
        rewrite = _numeric_scalar_assignment_rewrite_from_expression(
            column_name,
            expression_sql,
        )
        if rewrite is None:
            return None
        return self._expression_project_payload(
            (rewrite,),
            required_columns=(column_name,),
        )

    def _schema_declared_projection_columns(self) -> tuple[str, ...] | None:
        from ._relational_sql import frame_stages

        rendered = frame_stages(self)
        if rendered is None or not rendered.columns:
            return None
        if any(not _is_sql_identifier(name) for name in rendered.columns):
            return None
        return rendered.columns

    def _projection_columns_for_schema_or_explicit_selection(
        self,
        *,
        allowed_operations: set[str] | None = None,
    ) -> tuple[str, ...] | None:
        if self.source.schema:
            return self._declared_projection_columns(allowed_operations=allowed_operations)
        projection_columns: tuple[str, ...] | None = None
        for operation in self.operations:
            if operation.kind == "select":
                if any(not _is_sql_identifier(value) for value in operation.values):
                    return None
                projection_columns = operation.values
            elif operation.kind == "set_index":
                continue
            elif allowed_operations is not None and operation.kind in allowed_operations:
                continue
            else:
                return None
        if not projection_columns:
            return None
        return projection_columns

    def _declared_projection_columns(
        self,
        *,
        allowed_operations: set[str] | None = None,
    ) -> tuple[str, ...] | None:
        if not self.source.schema:
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
            elif operation.kind == "set_index":
                continue
            elif allowed_operations is not None and operation.kind in allowed_operations:
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
        if projection_columns is None and not columns:
            return None
        target_columns = (
            _normalize_columns(columns)
            if columns
            else projection_columns
        )
        if any(not _is_sql_identifier(column) for column in target_columns):
            return None
        missing = (
            tuple(column for column in target_columns if column not in projection_columns)
            if projection_columns is not None
            else ()
        )
        if missing:
            raise ValueError(
                "null-mask referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        null_operator = "IS NOT NULL" if is_not else "IS NULL"
        return tuple(f"{column} {null_operator} AS {column}" for column in target_columns)

    def _schema_declared_melt_value_columns(
        self,
        id_vars: object | None,
    ) -> tuple[str, ...] | None:
        projection_columns = self._unary_projection_columns()
        if projection_columns is None or projection_columns == ("*",):
            return None
        try:
            id_columns = _normalize_optional_columns(id_vars)
        except (TypeError, ValueError):
            return None
        if any(not _is_sql_identifier(column) for column in id_columns):
            return None
        missing = tuple(column for column in id_columns if column not in projection_columns)
        if missing:
            raise ValueError(
                "melt id_vars referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        value_columns = tuple(
            column for column in projection_columns if column not in set(id_columns)
        )
        if not value_columns:
            return None
        return value_columns

    def _schema_declared_dropna_predicate(
        self,
        subset: object | None,
        *,
        how: str,
        axis: object | None,
        thresh: int | None,
        kwargs: Mapping[str, object],
    ) -> str | None:
        if kwargs:
            return None
        if _normalize_dropna_axis(axis) != "rows":
            return None
        if any(operation.kind == "limit" for operation in self.operations):
            return None
        normalized_how = _normalize_dropna_how(how)
        projection_columns = self._schema_declared_projection_columns()
        if projection_columns is None and subset is None:
            return None
        target_columns = _normalize_columns((subset,)) if subset is not None else projection_columns
        if any(not _is_sql_identifier(column) for column in target_columns):
            return None
        missing = (
            tuple(column for column in target_columns if column not in projection_columns)
            if projection_columns is not None
            else ()
        )
        if missing:
            raise ValueError(
                "dropna referenced unknown declared/projection column(s): "
                + ", ".join(missing)
            )
        if thresh is not None:
            threshold = _normalize_non_negative_int("dropna thresh", thresh)
            if threshold == 0:
                return ""
            return _dropna_threshold_predicate(target_columns, threshold)
        joiner = " AND " if normalized_how == "any" else " OR "
        return joiner.join(f"{column} IS NOT NULL" for column in target_columns)

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

    def _bounded_schema_report(
        self, *, check: bool,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        report = self._bounded_materialization_report(limit=100, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        return _workflow_schema_report(self, report)

    def _bounded_materialization_report(
        self,
        *,
        limit: int | None,
        check: bool,
    ) -> VortexWorkflowExecutionReport | UnsupportedWorkflowOperationReport:
        if limit is not None:
            _validate_positive_row_count("materialization limit", limit)
        report = self.collect(limit=limit, check=check)
        if isinstance(report, UnsupportedWorkflowOperationReport):
            return report
        if report.status != "success":
            return UnsupportedWorkflowOperationReport(report.workflow, "collect", report.envelope)
        return report

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
        )._relation_statement()

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

    def _declared_sources(self) -> tuple[WorkflowSource, ...]:
        return (self.source, *(source for operation in self.operations
                               for source in operation.source_bindings))

    def _native_relational_statement(self) -> str | None:
        """Submit every complete declaration to shared native admission."""
        return self._relation_statement()

    def _relation_statement(self) -> str | None:
        """Render this complete input, including its order and limits, without I/O."""
        from ._relational_sql import flat_order_is_safe, render_frame

        if flat_order_is_safe(self.operations):
            statement = self._sql_local_source_statement(allow_native_source=True, require_limit=False)
            if statement is not None:
                return statement
        return render_frame(self)


    def _sql_local_source_statement(
        self, *, default_limit: int | None = None, allow_native_source: bool = False,
        require_limit: bool = True,
    ) -> str | None:
        if not _is_declared_local_source(self.source) and not (
            allow_native_source and (
                self.source.source_format == "vortex" or _is_declared_local_source(self.source)
            )
        ):
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
                if any(
                    not _sql_fragment_admitted_for_local_source_statement(
                        "select projection", value
                    )
                    for value in operation.values
                ):
                    return None
                projection_list = operation.values
            elif operation.kind == "aggregate" and aggregate_list is None:
                if distinct_requested:
                    return None
                if any(
                    not _sql_fragment_admitted_for_local_source_statement(
                        "aggregate expression", value
                    )
                    for value in operation.values
                ):
                    return None
                aggregate_list = operation.values
            elif operation.kind == "group_by" and group_by_list is None:
                if distinct_requested:
                    return None
                if any(
                    not _sql_fragment_admitted_for_local_source_statement(
                        "GROUP BY column", value
                    )
                    for value in operation.values
                ):
                    return None
                group_by_list = operation.values
            elif operation.kind == "with_column":
                if window_expressions or distinct_requested:
                    return None
                if not _sql_fragment_admitted_for_local_source_statement(
                    "computed-column expression", operation.values[1]
                ):
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
                if any(
                    not _sql_fragment_admitted_for_local_source_statement(
                        "window expression",
                        value,
                        allowed_keywords=("order by",),
                    )
                    for value in operation.values
                ):
                    return None
                window_expressions.extend(operation.values)
            elif operation.kind == "sort" and sort_key is None:
                sort_key = _parse_sort_operation_values(operation.values)
                if any(
                    not _sql_fragment_admitted_for_local_source_statement(
                        "ORDER BY column", value
                    )
                    for value in sort_key[1]
                ):
                    return None
            elif operation.kind == "join" and join_info is None:
                if operation.right_statement is not None:
                    return None
                if (aggregate_list is not None or group_by_list is not None or distinct_requested
                        or predicate is not None or sort_key is not None or limit is not None
                        or projection_list is not None or literal_columns or window_expressions):
                    # This flat SQL renderer cannot move a preceding input stage
                    # across a join; doing so can change rows and null extension.
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
                predicate = _rewrite_predicate_with_computed_columns(
                    operation.values[0],
                    tuple(literal_columns),
                )
            elif operation.kind == "having" and having is None:
                if aggregate_list is None or sort_key is not None or limit is not None:
                    return None
                having = operation.values[0]
            elif operation.kind == "limit" and limit is None:
                limit = operation.values[0]
            else:
                return None
        if limit is None:
            if default_limit is None and require_limit:
                return None
            if default_limit is not None:
                limit = str(default_limit)
        limit_clause = f" LIMIT {limit}" if limit is not None else ""
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
                f"{_optional_sql_having_clause(having)}{order_by_clause}{limit_clause}"
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
            f"{_optional_sql_having_clause(having)}{order_by_clause}{limit_clause}"
        )

    def _sql_local_source_union_branch_statement(self) -> str | None:
        from ._relational_sql import flat_order_is_safe

        if not flat_order_is_safe(self.operations):
            return None
        if any(operation.kind in {"limit", "sort"} for operation in self.operations):
            return None
        statement = self._sql_local_source_statement(default_limit=1, allow_native_source=True)
        suffix = " LIMIT 1"
        if statement is None or not statement.endswith(suffix):
            return None
        return statement[: -len(suffix)]


@dataclass(frozen=True, slots=True)
class GroupedLazyFrame:
    """Grouped lazy workflow handle for scoped aggregation and blockers."""

    workflow: LazyFrame | SqlWorkflow
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
    ) -> "LazyFrame | SqlWorkflow | UnsupportedWorkflowOperationReport":
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
        if isinstance(self.workflow, SqlWorkflow):
            return self.workflow._append_group_by_aggregate(self.columns, tuple(values))
        if self.workflow._can_append_group_by_aggregate(self.columns):
            return self.workflow._append_group_by_aggregate(self.columns, tuple(values))
        if self.workflow.source.source_format == "vortex":
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
    ) -> "LazyFrame | SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Alias for grouped `agg`."""

        return self.agg(*expressions, check=check, **named_expressions)

    def count(
        self,
        *,
        alias: object = "rows",
        check: bool = False,
    ) -> "LazyFrame | SqlWorkflow | UnsupportedWorkflowOperationReport":
        """Return a grouped `count(*)` workflow using a familiar aggregation shortcut."""

        return self.agg(**{_normalize_output_column_name(alias): "count(*)"}, check=check)


def read_vortex(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy native Vortex source."""

    return _read_source(
        "vortex",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def plan_transform(
    function: object | None = None,
    *,
    name: object | None = None,
) -> object:
    """Declare a workflow-level transform admitted by `LazyFrame.pipe(...)`.

    The wrapped callable runs only during lazy plan construction. It must
    return a `LazyFrame`; data execution remains inside ShardLoom's terminal
    Vortex-prepared/native routes.
    """

    if function is None:

        def decorator(candidate: object) -> WorkflowPlanTransform:
            return plan_transform(candidate, name=name)  # type: ignore[return-value]

        return decorator
    if not callable(function):
        raise TypeError("plan_transform requires a callable")
    default_name = getattr(function, "__name__", "plan_transform")
    transform_name = _normalize_plan_transform_name(
        default_name if name is None else name,
    )
    return WorkflowPlanTransform(transform_name, function)


def column_transform(
    columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
    **named_expressions: object,
) -> WorkflowColumnTransform:
    """Declare scoped column rewrites for `map`, `applymap`, or `transform`.

    The wrapper is declarative. It does not execute Python row/cell functions;
    terminal execution still lowers through ShardLoom's native/prepared routes.
    """

    return WorkflowColumnTransform(
        _normalize_named_projection_items(
            "column_transform",
            columns,
            named_expressions,
        )
    )


def row_transform(
    columns: Mapping[str, object] | Sequence[tuple[object, object]] | None = None,
    **named_expressions: object,
) -> WorkflowRowTransform:
    """Declare scoped row-shaped rewrites for `map_rows`.

    The wrapper is declarative and may reference columns through ShardLoom
    expressions. It does not execute Python row functions; terminal execution
    still lowers through native/prepared Vortex expression-project routes.
    """

    return WorkflowRowTransform(
        _normalize_named_projection_items(
            "row_transform",
            columns,
            named_expressions,
        )
    )


def typed_scalar_udf(udf_id: object, column: object) -> ColumnExpression:
    """Return an admitted typed deterministic scalar UDF expression.

    The v1 runtime admits only built-in ShardLoom UDF fixtures with declared
    dtype/null/effect metadata. This helper does not execute Python code; it
    lowers the declared fixture into the native expression-project route.
    """

    normalized_id = _require_non_empty("typed scalar UDF id", udf_id).strip()
    source_column = _normalize_expression_column(
        column.sql if isinstance(column, ColumnExpression) else column
    )
    if normalized_id != "sl_fixture_double_i64":
        raise ValueError(
            "typed scalar UDF must be the admitted built-in sl_fixture_double_i64 fixture"
        )
    return ColumnExpression(f"{source_column} * 2")


def fixture_double_i64(column: object) -> ColumnExpression:
    """Return the admitted `sl_fixture_double_i64` typed scalar UDF expression."""

    return typed_scalar_udf("sl_fixture_double_i64", column)


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
    if suffix in {".vortex", ".vtx", ".vortex-manifest"}:
        return "vortex"
    admitted = ".csv, .json, .jsonl, .ndjson, .parquet, .arrow, .ipc, .feather, .avro, .orc, .vortex, .vtx, .vortex-manifest"
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
        return read_vortex(
            uri,
            schema=schema,
            client=client,
            engine_mode=engine_mode,
            **client_config,
        )
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

    Admitted local Parquet workflows normalize through the same Vortex-prepared
    middle as other compatibility inputs; binaries built without
    `universal-format-io` return ShardLoom's deterministic Parquet adapter
    blocker.
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

    Admitted local Arrow IPC workflows normalize through the same
    Vortex-prepared middle as other compatibility inputs; binaries built
    without `universal-format-io` return ShardLoom's deterministic Arrow IPC
    adapter blocker. This is a local file adapter, not an in-memory Arrow table
    fallback.
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

    Admitted local Avro workflows normalize through the same Vortex-prepared
    middle as other compatibility inputs; binaries built without
    `universal-format-io` return ShardLoom's deterministic Avro adapter
    blocker. This is a local flat scalar file adapter, not broad Avro
    schema-evolution support.
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

    Admitted local ORC workflows normalize through the same Vortex-prepared
    middle as other compatibility inputs; binaries built without
    `universal-format-io` return ShardLoom's deterministic ORC adapter blocker.
    This is a local flat scalar file adapter, not broad ORC stripe/statistics
    runtime support.
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
    schema: Mapping[str, object] | None = None,
    **client_config: object,
) -> LazyFrame:
    """Declare native scalar rows; pass schema for typed empty or all-null columns."""

    return _memory_rows_source(
        rows,
        client=_client_from_config(client, client_config),
        schema=schema,
    )


def literal_table(
    rows: Sequence[Mapping[str, object]],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
    """Declare a literal table for the shared native engine."""

    return from_rows(
        rows,
        schema=schema,
        client=client,
        **client_config,
    )


def dataframe_source_free_projection(
    *expressions: object,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
    """Create a scoped one-row DataFrame-style literal projection.

    Literal values become typed native input. Later expressions and output
    use the same native engine as file-backed DataFrame workflows.
    """

    return _memory_rows_source(
        [_dataframe_source_free_projection_row(expressions)],
        client=_client_from_config(client, client_config),
    )


def dataframe_generated_with_column(
    name: object,
    expression: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
    """Create a scoped one-row generated DataFrame with one literal column.

    This admits the narrow source-free `with_column` helper advertised by the
    generated-output capability matrix. It is not broad DataFrame expression
    execution; source-backed native rows and range expressions still use
    `from_rows(...).with_column(...)` and `range(...).with_column(...)`.
    """

    column = _require_non_empty("generated DataFrame column name", name)
    literal = _generated_literal_expression(expression)
    return _memory_rows_source(
        [{column: literal}],
        client=_client_from_config(client, client_config),
    )


def range(
    start: int,
    end: int,
    *,
    step: int = 1,
    column: str = "value",
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
    """Declare a native integer input with an exclusive end for the shared engine."""

    normalized_start = _require_range_int("start", start)
    normalized_end = _require_range_int("end", end)
    normalized_step = _require_range_int("step", step)
    if normalized_step == 0:
        raise ValueError("range step must not be zero")
    normalized_column = _require_non_empty("range column", column)
    return _native_memory_frame(
        {"kind": "range", "start": normalized_start, "end": normalized_end,
         "step": normalized_step, "column": normalized_column, "inclusive": False},
        schema=((normalized_column, "int64"),),
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
) -> LazyFrame:
    """Alias for the native integer range, including its exclusive end."""

    return range(start, end, step=step, column=column, client=client, **client_config)


def sql_values(
    values_clause: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> SqlWorkflow:
    """Declare SQL VALUES for the shared native engine."""

    statement = _require_non_empty("SQL VALUES clause", values_clause)
    return SqlWorkflow(
        statement=statement,
        client=_client_from_config(client, client_config),
    )


def sql_literal_select(
    expression: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> SqlWorkflow:
    """Declare a source-free SQL projection for the shared native engine."""

    statement = _require_non_empty("SQL literal SELECT expression", expression)
    return SqlWorkflow(
        statement=statement,
        client=_client_from_config(client, client_config),
    )


def sql(
    statement: object,
    *,
    input: str | os.PathLike[str] | None = None,
    input_format: str | None = None,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> SqlWorkflow:
    """Create a scoped SQL workflow over currently admitted ShardLoom SQL paths."""

    input_uri, normalized_input_format = _normalize_sql_workflow_input(
        input,
        input_format,
    )
    return SqlWorkflow(
        statement=_require_non_empty("SQL statement", statement),
        client=_client_from_config(client, client_config),
        input_uri=input_uri,
        input_format=normalized_input_format,
    )


def _normalize_sql_workflow_input(
    input_ref: str | os.PathLike[str] | None,
    input_format: str | None,
) -> tuple[str | None, str | None]:
    """Normalize optional input binding for SQL workflows."""

    if input_ref is None:
        if input_format is not None:
            raise ValueError("input_format requires input")
        return None, None
    input_uri = _require_non_empty("SQL input", os.fspath(input_ref))
    if input_format is None:
        inferred = _source_format_for_sql_input_ref(input_uri)
        if inferred is None:
            raise ValueError(
                "input_format is required when SQL input format cannot be inferred"
            )
        return input_uri, inferred
    normalized = input_format.strip().lower().replace("_", "-")
    admitted_formats = set(SUPPORTED_SOURCE_FORMATS) | {"jsonl", "ndjson"}
    if normalized not in admitted_formats:
        raise ValueError(
            f"input_format must be one of {tuple(sorted(admitted_formats))}; got {input_format!r}"
        )
    return input_uri, normalized


def _source_format_for_sql_input_ref(value: str) -> str | None:
    """Infer source format from a normal Python path or SQL source literal."""

    if _is_local_vortex_source_ref(value):
        return "vortex"
    return _source_format_for_local_source_ref(value) or _source_format_for_local_source_ref(
        repr(value)
    )


def calendar(
    start: str | date,
    end: str | date,
    *,
    column: str = "date",
    include_parts: bool = True,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
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
        **client_config,
    )


def from_pandas(
    dataframe: object,
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> LazyFrame | UnsupportedWorkflowOperationReport:
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
        return _memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-pandas", workflow.uri, check=check)


def from_arrow_table(
    table: object,
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> LazyFrame | UnsupportedWorkflowOperationReport:
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
        return _memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-arrow-table", workflow.uri, check=check)


def from_arrow_ipc(
    source: object,
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> LazyFrame | UnsupportedWorkflowOperationReport:
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
        return _memory_rows_source(
            rows,
            client=resolved_client,
            schema=schema,
            engine_mode=engine_mode,
        )
    except (TypeError, ValueError):
        return workflow._unsupported_operation("from-arrow-ipc", workflow.uri, check=check)


def _memory_rows_source(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient,
    schema: Mapping[str, object] | None = None,
    engine_mode: str = "auto",
) -> LazyFrame:
    """Declare bounded nullable scalar rows without executing any expressions."""
    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise TypeError("rows must be a sequence of mappings")
    if len(rows) > 65_536:
        raise ValueError("native row input exceeds 65,536 rows")
    declared = _normalize_schema(schema)
    if declared:
        columns = tuple(name for name, _ in declared)
    elif rows and isinstance(rows[0], Mapping):
        columns = tuple(rows[0])
    elif rows:
        raise TypeError("rows must contain mappings")
    else:
        raise ValueError("empty row input requires an explicit schema")
    if not columns or len(columns) > 64:
        raise ValueError("native row input requires 1..=64 columns")
    if any(not isinstance(name, str) for name in columns):
        raise TypeError("row column names must be strings")
    if any(not name or len(name.encode("utf-8")) > 256 for name in columns):
        raise ValueError("row column names must contain 1..=256 UTF8 bytes")
    keys = set(columns)
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise TypeError(f"row {index} is not a mapping")
        if set(row) != keys:
            raise ValueError("all rows must match the declared column names")
    if declared:
        aliases = {"int": "int64", "integer": "int64", "float": "float64", "double": "float64",
                   "boolean": "bool", "str": "utf8", "string": "utf8"}
        kinds = tuple(aliases.get(str(dtype).lower(), str(dtype).lower()) for _, dtype in declared)
        if any(kind not in {"int64", "float64", "bool", "utf8"} for kind in kinds):
            raise ValueError("native row schema admits int64, float64, bool and utf8")
    else:
        kinds = tuple(_infer_memory_column_type(row[name] for row in rows) for name in columns)
    schema_fields = tuple(zip(columns, kinds))
    encoded_rows = tuple(tuple(None if row[name] is None else _memory_value(kind, row[name])
                               for name, kind in schema_fields) for row in rows)
    return _native_memory_frame(
        {"kind": "rows", "schema": schema_fields, "rows": encoded_rows},
        schema=schema_fields, client=client, engine_mode=engine_mode,
    )


def _infer_memory_column_type(values: Iterable[object]) -> str:
    kinds = {_memory_value_type(value) for value in values if value is not None}
    if not kinds:
        return "bool"
    if kinds <= {"int64", "float64"}:
        return "float64" if "float64" in kinds else "int64"
    if len(kinds) == 1:
        return next(iter(kinds))
    raise TypeError("row values in a column must share one scalar type")


def _native_memory_frame(
    declaration: Mapping[str, object], *, schema: tuple[tuple[str, str], ...],
    client: ShardLoomClient, engine_mode: str = "auto",
) -> LazyFrame:
    payload = json.dumps(declaration, sort_keys=True, ensure_ascii=False).encode("utf-8")
    if len(payload) > 8 * 1024 * 1024:
        raise ValueError("native memory declaration exceeds 8 MiB")
    uri = "memory://input/" + hashlib.sha256(payload).hexdigest()
    return LazyFrame(
        source=WorkflowSource("memory", uri, schema, tuple(declaration.items())),
        client=client, engine_mode=_normalize_engine_mode(engine_mode),
    )


def _memory_value_type(value: object) -> str:
    if value is None or isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError("native row integers must fit int64")
        return "int64"
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("float native row values must be finite")
        return "float64"
    if isinstance(value, str):
        return "utf8"
    raise TypeError(
        "native row values must be None, bool, int, float, or str"
    )


def _memory_value(value_type: str, value: object) -> str:
    if value_type == "bool":
        if not isinstance(value, bool):
            raise TypeError("native bool columns must contain only bool values")
        return "true" if value else "false"
    if value_type == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("native int64 columns must contain only int values")
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError("native row integers must fit int64")
        return str(value)
    if value_type == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("native float64 columns must contain only numeric values")
        if isinstance(value, int) and builtins.abs(value) > (1 << 53):
            raise ValueError("integer-to-float row conversion exceeds the exact integer range")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("float native row values must be finite")
        return str(numeric)
    if value_type == "utf8":
        if not isinstance(value, str):
            raise TypeError("native utf8 columns must contain only str values")
        return value
    raise ValueError(f"unsupported native input value type {value_type!r}")


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
            _memory_value_type(raw_value)
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
            _memory_value_type(value)
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
    _memory_value_type(value)
    return _normalize_output_column_name(alias_node.value), value


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
            return None
        try:
            parsed = ast.literal_eval(inner)
        except (SyntaxError, ValueError) as exc:
            raise ValueError(
                "lit(...) expression must contain a bool, int, float, or quoted string"
            ) from exc
        _memory_value_type(parsed)
        return parsed
    _memory_value_type(expression)
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
    weights: object | None = None,
    replace: bool = False,
) -> str:
    if n is not None and fraction is not None:
        raise ValueError("sample accepts either n or fraction, not both")
    if not isinstance(replace, bool):
        raise TypeError("sample replace must be boolean")
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
        if isinstance(seed, bool) or not isinstance(seed, int):
            parts.append(f"seed={_stable_target_value(seed)}")
        else:
            parts.append(f"seed={_normalize_non_negative_int('sample seed', seed)}")
    if weights is not None:
        parts.append(f"weights={_stable_target_value(weights)}")
    if replace:
        parts.append("replace=true")
    return ",".join(parts)


def _normalize_sample_fraction_alias(
    *,
    fraction: float | None,
    frac: float | None,
) -> float | None:
    if fraction is not None and frac is not None:
        raise ValueError("sample accepts either fraction or frac, not both")
    return fraction if fraction is not None else frac


def _normalize_sample_fraction_value(fraction: float) -> float:
    if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
        raise TypeError("sample fraction must be numeric")
    value = float(fraction)
    if not math.isfinite(value) or value <= 0 or value > 1:
        raise ValueError("sample fraction must be finite and in the range (0, 1]")
    return value


def _normalize_sample_weight_column(weights: object | None) -> str | None:
    if weights is None:
        return None
    if not isinstance(weights, str):
        return None
    column = weights.strip()
    if not _is_sql_identifier(column):
        return None
    return column


def _sample_operation_parts(
    values: tuple[str, ...],
) -> tuple[int | None, float | None, int, bool, str | None] | None:
    """One declaration parser for standalone and composed native sampling."""
    sample_count = None
    sample_fraction = None
    sample_seed = 0
    sample_with_replacement = False
    sample_weight_column = None
    try:
        if any("=" in value for value in values):
            parsed_sample: int | None = None
            parsed_fraction: float | None = None
            parsed_seed = 0
            parsed_replacement = False
            parsed_weight_column: str | None = None
            for value in values:
                if value in {"replacement", "replace=true"}:
                    if parsed_replacement:
                        return None
                    parsed_replacement = True
                elif value.startswith("n="):
                    if parsed_sample is not None:
                        return None
                    parsed_sample = int(value.removeprefix("n="))
                elif value.startswith("fraction=") or value.startswith("frac="):
                    if parsed_fraction is not None:
                        return None
                    _, raw_fraction = value.split("=", 1)
                    parsed_fraction = float(raw_fraction)
                elif value.startswith("seed=") or value.startswith("random_state="):
                    _, raw_seed = value.split("=", 1)
                    parsed_seed = int(raw_seed)
                elif value.startswith("weights=") or value.startswith("weight="):
                    if parsed_weight_column is not None:
                        return None
                    _, raw_weight_column = value.split("=", 1)
                    if not _is_sql_identifier(raw_weight_column):
                        return None
                    parsed_weight_column = raw_weight_column
                else:
                    return None
            if (parsed_sample is None) == (parsed_fraction is None):
                return None
            if parsed_sample is not None and parsed_sample <= 0:
                return None
            if parsed_fraction is not None and (
                not math.isfinite(parsed_fraction)
                or parsed_fraction <= 0
                or parsed_fraction > 1
            ):
                return None
            if parsed_seed < 0:
                return None
            sample_count = parsed_sample
            sample_fraction = parsed_fraction
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
            sample_weight_column = parsed_weight_column
        elif values and values[0] == "fraction":
            if len(values) < 2:
                return None
            parsed_fraction = float(values[1])
            parsed_seed = int(values[2]) if len(values) > 2 else 0
            parsed_replacement = (
                len(values) > 3 and values[3] == "replacement"
            )
            if len(values) > (4 if parsed_replacement else 3):
                return None
            if (
                not math.isfinite(parsed_fraction)
                or parsed_fraction <= 0
                or parsed_fraction > 1
                or parsed_seed < 0
            ):
                return None
            sample_fraction = parsed_fraction
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
        else:
            parsed_sample = int(values[0])
            parsed_seed = int(values[1]) if len(values) > 1 else 0
            parsed_replacement = len(values) > 2 and values[2] == "replacement"
            if len(values) > (3 if parsed_replacement else 2):
                return None
            if parsed_sample <= 0 or parsed_seed < 0:
                return None
            sample_count = parsed_sample
            sample_seed = parsed_seed
            sample_with_replacement = parsed_replacement
    except (ValueError, TypeError, IndexError):
        return None
    return sample_count, sample_fraction, sample_seed, sample_with_replacement, sample_weight_column


def _normalize_sample_seed_alias(
    *,
    seed: int | None,
    random_state: int | None,
) -> int | None:
    if seed is None:
        return random_state
    if random_state is None:
        return seed
    normalized_seed = _normalize_non_negative_int("sample seed", seed)
    normalized_random_state = _normalize_non_negative_int(
        "sample random_state",
        random_state,
    )
    if normalized_seed != normalized_random_state:
        raise ValueError("sample seed and random_state must match when both are provided")
    return normalized_seed


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
    ignore_index: bool,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"id_vars={_optional_columns_for_target(id_vars)}",
        f"value_vars={_optional_columns_for_target(value_vars)}",
        f"ignore_index={str(bool(ignore_index)).lower()}",
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


def _normalize_rolling_aggregate_target(
    aggregate: str,
    column: object,
    *,
    alias: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"aggregate={_require_non_empty('rolling aggregate', aggregate)}",
        f"column={_require_non_empty('rolling column', column)}",
    ]
    if alias is not None:
        parts.append(f"alias={_require_non_empty('rolling alias', alias)}")
    parts.extend(_normalize_extra_kwargs("rolling aggregate", kwargs))
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
    *,
    axis: object | None,
    inplace: bool,
    method: object | None,
    limit: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"value={_stable_target_value(value)}",
        f"axis={_normalize_dropna_axis(axis)}",
        f"inplace={str(bool(inplace)).lower()}",
    ]
    if method is not None:
        parts.append(f"method={_stable_target_value(method)}")
    if limit is not None:
        parts.append(f"limit={_stable_target_value(limit)}")
    parts.extend(_normalize_extra_kwargs("fillna", kwargs))
    return ";".join(parts)


def _normalize_fill_method(method: object | None) -> str | None:
    if method is None:
        return None
    normalized = _require_non_empty("fillna method", method).lower().replace("-", "_")
    if normalized in {"ffill", "pad", "forward_fill", "forwardfill"}:
        return "ffill"
    if normalized in {"bfill", "backfill", "backward_fill", "backwardfill"}:
        return "bfill"
    raise ValueError("fillna method must be 'ffill'/'pad' or 'bfill'/'backfill'")


def _normalize_optional_positive_int(name: str, value: object | None) -> int | None:
    if value is None:
        return None
    return _normalize_positive_int(name, value)


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


def _normalize_dropna_axis(value: object | None) -> str:
    if value is None:
        return "rows"
    if value in (0, "0", "index", "row", "rows"):
        return "rows"
    if value in (1, "1", "columns", "column", "cols"):
        return "columns"
    raise ValueError("dropna axis must be 0/'index' or 1/'columns'")


def _dropna_threshold_predicate(columns: tuple[str, ...], threshold: int) -> str | None:
    if not columns:
        return None
    if threshold > len(columns):
        column = columns[0]
        return f"{column} IS NOT NULL AND {column} IS NULL"
    if threshold == 1:
        return " OR ".join(f"{column} IS NOT NULL" for column in columns)
    if threshold == len(columns):
        return " AND ".join(f"{column} IS NOT NULL" for column in columns)
    if math.comb(len(columns), threshold) > 128:
        return None
    clauses = []
    for group in combinations(columns, threshold):
        clauses.append(
            "(" + " AND ".join(f"{column} IS NOT NULL" for column in group) + ")"
        )
    return " OR ".join(clauses)


def _normalize_dropna_target(
    *,
    subset: object | None,
    how: str,
    axis: object | None,
    thresh: int | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"subset={_optional_columns_for_target(subset)}",
        f"how={_normalize_dropna_how(how)}",
        f"axis={_normalize_dropna_axis(axis)}",
    ]
    if thresh is not None:
        parts.append(f"thresh={_normalize_non_negative_int('dropna thresh', thresh)}")
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
    normalized_keep = _normalize_duplicate_keep_value(keep)
    parts = [
        f"subset={_optional_columns_for_target(subset)}",
        f"keep={normalized_keep}",
    ]
    parts.extend(_normalize_extra_kwargs("duplicated", kwargs))
    return ";".join(parts)


def _normalize_duplicate_keep_value(keep: str | bool) -> str:
    if keep is False:
        return "false"
    normalized_keep = _require_non_empty("duplicate keep", keep).lower().replace("_", "-")
    if normalized_keep not in {"first", "last"}:
        raise ValueError("duplicate keep must be 'first', 'last', or False")
    return normalized_keep


def _normalize_mask_target(
    *,
    cond: object,
    other: object | None,
    axis: object | None,
    inplace: bool,
    level: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"cond={_require_non_empty('mask condition', cond)}",
        f"other={_stable_target_value(other)}",
        f"axis={_normalize_dropna_axis(axis)}",
        f"inplace={str(bool(inplace)).lower()}",
        f"level={_stable_target_value(level)}",
    ]
    parts.extend(_normalize_extra_kwargs("mask", kwargs))
    return ";".join(parts)


def _normalize_replace_target(
    *,
    to_replace: object | None,
    value: object | None,
    regex: bool,
    inplace: bool,
    method: object | None,
    limit: object | None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [
        f"to_replace={_stable_target_value(to_replace)}",
        f"value={_stable_target_value(value)}",
        f"regex={str(bool(regex)).lower()}",
        f"inplace={str(bool(inplace)).lower()}",
        f"method={_stable_target_value(method)}",
        f"limit={_stable_target_value(limit)}",
    ]
    parts.extend(_normalize_extra_kwargs("replace", kwargs))
    return ";".join(parts)


def _normalize_index_target(
    context: str,
    *,
    keys: object | None,
    drop: bool | None = None,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"keys={_optional_columns_for_target(keys)}"]
    if drop is not None:
        parts.append(f"drop={str(bool(drop)).lower()}")
    parts.extend(_normalize_extra_kwargs(context, kwargs))
    return ";".join(parts)


def _normalize_index_columns(keys: object) -> tuple[str, ...]:
    columns = _normalize_columns((keys,))
    if any(not _is_sql_identifier(column) for column in columns):
        raise ValueError("set_index keys admit only bare SQL identifiers")
    if len(set(columns)) != len(columns):
        raise ValueError("set_index keys must be unique")
    return columns


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


def _normalize_plan_transform_name(value: object) -> str:
    text = _require_non_empty("plan transform name", value)
    if any(char in text for char in (";", "\n", "\r", "\t")):
        raise ValueError("plan transform name must be a compact label")
    return text


def _normalize_eval_target(
    expr: object,
    kwargs: Mapping[str, object],
) -> str:
    parts = [f"expr={_require_non_empty('eval expression', expr)}"]
    parts.extend(_normalize_extra_kwargs("eval", kwargs))
    return ";".join(parts)


def _eval_numeric_scalar_assignment_rewrite(expr: object) -> dict[str, object] | None:
    rewrites = _eval_numeric_scalar_assignment_rewrites(expr)
    return rewrites[0] if rewrites and len(rewrites) == 1 else None


def _eval_numeric_scalar_assignment_rewrites(
    expr: object,
) -> tuple[dict[str, object], ...] | None:
    if not isinstance(expr, str):
        return None
    try:
        parsed = ast.parse(_require_non_empty("eval expression", expr), mode="exec")
    except SyntaxError:
        return None
    if not parsed.body:
        return None
    rewrites: list[dict[str, object]] = []
    target_columns: set[str] = set()
    for statement in parsed.body:
        if not isinstance(statement, ast.Assign):
            return None
        if len(statement.targets) != 1 or not isinstance(statement.targets[0], ast.Name):
            return None
        target_column = _normalize_output_column_name(statement.targets[0].id)
        if target_column in target_columns:
            return None
        target_columns.add(target_column)
        value = statement.value
        if not isinstance(value, ast.BinOp) or not isinstance(value.left, ast.Name):
            return None
        if _normalize_output_column_name(value.left.id) != target_column:
            return None
        rewrite = _numeric_scalar_assignment_rewrite_from_binop(target_column, value)
        if rewrite is None:
            return None
        rewrites.append(rewrite)
    return tuple(rewrites) if rewrites else None


def _numeric_scalar_assignment_rewrite_from_expression(
    target_column: str,
    expression_sql: str,
) -> dict[str, object] | None:
    target_column = _normalize_output_column_name(target_column)
    try:
        parsed = ast.parse(
            _require_non_empty("numeric assignment expression", expression_sql),
            mode="eval",
        )
    except SyntaxError:
        return None
    value = parsed.body
    if not isinstance(value, ast.BinOp) or not isinstance(value.left, ast.Name):
        return None
    if _normalize_output_column_name(value.left.id) != target_column:
        return None
    return _numeric_scalar_assignment_rewrite_from_binop(target_column, value)


def _numeric_scalar_assignment_rewrite_from_binop(
    target_column: str,
    value: ast.BinOp,
) -> dict[str, object] | None:
    operator = _eval_numeric_scalar_operator(value.op)
    if operator is None:
        return None
    operand = _eval_numeric_scalar_literal(value.right)
    if operand is None:
        return None
    if operator == "/" and float(operand) == 0.0:
        return None
    return {
        "kind": "numeric_scalar_arithmetic",
        "target_column": target_column,
        "operator": operator,
        "operand": operand,
    }


def _eval_numeric_scalar_operator(operator: ast.operator) -> str | None:
    if isinstance(operator, ast.Add):
        return "+"
    if isinstance(operator, ast.Sub):
        return "-"
    if isinstance(operator, ast.Mult):
        return "*"
    if isinstance(operator, ast.Div):
        return "/"
    return None


def _eval_numeric_scalar_literal(node: ast.AST) -> int | float | None:
    if isinstance(node, ast.Constant) and not isinstance(node.value, bool):
        if isinstance(node.value, (int, float)) and math.isfinite(float(node.value)):
            return node.value
        return None
    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.USub)
        and isinstance(node.operand, ast.Constant)
        and not isinstance(node.operand.value, bool)
        and isinstance(node.operand.value, (int, float))
    ):
        value = -node.operand.value
        return value if math.isfinite(float(value)) else None
    return None


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
        "uint64",
        "float64",
        "utf8",
        "boolean",
        "date32",
        "timestamp_micros",
        "binary",
    }:
        raise ValueError(
            "cast dtype must be one of ('int64', 'uint64', 'float64', 'utf8', 'boolean', 'date32', 'timestamp_micros', 'binary', 'decimal128(p,s)')"
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
    if isinstance(value, ColumnExpression):
        return _sql_computed_projection_expression(value)
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
    if not -(1 << 63) <= days < (1 << 64):
        raise ValueError("date arithmetic days must fit int64 or uint64")
    return str(days)


def _normalize_timestamp_arithmetic_seconds(value: object) -> str:
    if isinstance(value, ColumnExpression):
        return _sql_computed_projection_expression(value)
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
    if not -(1 << 63) <= seconds < (1 << 64):
        raise ValueError("timestamp arithmetic seconds must fit int64 or uint64")
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
    if isinstance(value, Decimal):
        return _sql_decimal_literal(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{_normalize_timestamp_literal(value)}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (bytes, bytearray)):
        return f"X'{bytes(value).hex()}'"
    if isinstance(value, str):
        return _sql_string_literal(value)
    raise TypeError(
        "SQL predicate literals must be bool, int, float, Decimal, str, bytes, date, datetime, or None"
    )


def _sql_decimal_literal(value: Decimal) -> str:
    """Carry exact declared decimal values; the native kernel owns evaluation."""
    precision, scale = _decimal_literal_metadata(value)
    return f"CAST('{format(value, 'f')}' AS decimal128({precision},{scale}))"


def _decimal_literal_metadata(value: Decimal) -> tuple[int, int]:
    if not value.is_finite():
        raise ValueError("Decimal literals must be finite")
    _, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    scale = max(-exponent, 0)
    precision = max(len(digits) + max(exponent, 0), scale, 1)
    if precision > 38 or scale > 38:
        raise ValueError("Decimal literals must fit decimal128 precision and scale <= 38")
    # Check metadata first: formatting a huge exponent must never allocate a
    # correspondingly huge SQL string. This formatted value is at most 41 bytes.
    return precision, scale


def _vortex_exact_scalar_payload(value: object) -> dict[str, object] | None:
    """Declare exact literal bytes or text; native binding owns all conversion."""
    if isinstance(value, Decimal):
        precision, scale = _decimal_literal_metadata(value)
        return {"type": f"decimal128({precision},{scale})", "value": format(value, "f")}
    if isinstance(value, datetime):
        return {"type": "timestamp_micros", "value": _normalize_timestamp_literal(value)}
    if isinstance(value, date):
        return {"type": "date32", "value": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "binary", "value": bytes(value).hex()}
    return None


def _vortex_expression_scalar_payload(
    value: object,
    *,
    target_dtype: str | None,
    allow_null: bool = True,
) -> dict[str, object] | None:
    if target_dtype is None:
        # Literal typing is independent of input schema; native binding and
        # checked coercion remain authoritative for the produced column.
        if value is None:
            return {"type": "null", "value": None} if allow_null else None
        if isinstance(value, bool):
            return {"type": "boolean", "value": value}
        if isinstance(value, int) and -(1 << 63) <= value < (1 << 64):
            return {"type": "int64" if value < (1 << 63) else "uint64", "value": value}
        if isinstance(value, float) and math.isfinite(value):
            return {"type": "float64", "value": value}
        if isinstance(value, str):
            return {"type": "utf8", "value": value}
        return _vortex_exact_scalar_payload(value)
    dtype = target_dtype.strip().lower().replace("-", "_")
    decimal_dtype = _normalize_decimal_cast_dtype(dtype)
    supported_dtype = dtype in {
        "bool",
        "boolean",
        "int64",
        "int",
        "integer",
        "uint64",
        "uint",
        "unsigned",
        "float64",
        "float",
        "double",
        "utf8",
        "string",
        "str",
        "binary",
        "date32",
        "timestamp_micros",
    } or decimal_dtype is not None
    if value is None:
        if not allow_null:
            return None
        return {"type": "null", "value": None} if supported_dtype else None
    if decimal_dtype is not None:
        if isinstance(value, Decimal):
            return _vortex_exact_scalar_payload(value)
        if isinstance(value, int) and not isinstance(value, bool) and -(1 << 63) <= value < (1 << 64):
            return {"type": "int64" if value < (1 << 63) else "uint64", "value": value}
        return None
    if dtype in {"binary", "date32", "timestamp_micros"}:
        literal = _vortex_exact_scalar_payload(value)
        return literal if literal is not None and literal["type"] == dtype else None
    if dtype in {"bool", "boolean"}:
        if isinstance(value, bool):
            return {"type": "boolean", "value": value}
        return None
    if dtype in {"int64", "int", "integer"}:
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return {"type": "int64", "value": value}
    if dtype in {"uint64", "uint", "unsigned"}:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        return {"type": "uint64", "value": value}
    if dtype in {"float64", "float", "double"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        parsed = float(value)
        if not math.isfinite(parsed):
            return None
        return {"type": "float64", "value": parsed}
    if dtype in {"utf8", "string", "str"}:
        if isinstance(value, str):
            return {"type": "utf8", "value": value}
        return None
    return None




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
    if isinstance(value, Decimal):
        return _sql_decimal_literal(value)
    raise TypeError("numeric arithmetic literals must be int, finite float, or finite Decimal values")


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
    function = text.split("(", 1)[0].strip().upper() if "(" in text else ""
    scalar_call = function in {
        "CAST", "TRY_CAST", "ABS", "FLOOR", "CEIL", "CEILING", "ROUND",
        "LOWER", "UPPER", "TRIM", "LENGTH", "CONCAT", "SUBSTR", "SUBSTRING",
        "LEFT", "RIGHT", "REPLACE", "BYTE_LENGTH", "OCTET_LENGTH", "UNHEX",
        "FROM_BASE64", "COALESCE", "NULLIF", "DATE_YEAR", "DATE_MONTH", "DATE_DAY",
        "YEAR", "MONTH", "DAY", "TIMESTAMP_YEAR", "TIMESTAMP_MONTH", "TIMESTAMP_DAY",
        "TIMESTAMP_HOUR", "TIMESTAMP_MINUTE", "TIMESTAMP_SECOND", "DATE_ADD_DAYS",
        "DATE_SUB_DAYS", "TIMESTAMP_ADD_SECONDS", "TIMESTAMP_SUB_SECONDS",
        "DATE_DIFF_DAYS", "TIMESTAMP_DIFF_SECONDS",
    }
    if not (
        _expression_has_numeric_operator(text)
        or scalar_call
        or text.upper().startswith(("-", "+", "CASE "))
    ):
        raise ValueError(
            "computed with_column requires an admitted scalar expression"
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
    if isinstance(expression, ColumnExpression):
        try:
            return _normalize_expression_column(expression.sql)
        except (TypeError, ValueError):
            pass
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
    _validate_balanced_expression_parentheses(source)
    return f"{function}({source} AS {dtype})"


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
    return f"CASE WHEN {predicate} THEN {then_literal} ELSE {else_literal} END"


def _sql_case_branch(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, ColumnExpression):
        text = value.sql.strip()
        if not text:
            raise ValueError("CASE branch column expression must not be empty")
        return text
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


def _vortex_string_replace_expression_parts(
    expression_sql: str,
) -> tuple[str, str, str] | None:
    text = expression_sql.strip()
    upper = text.upper()
    if not upper.startswith("REPLACE(") or not text.endswith(")"):
        return None
    args = _split_projection_function_args(text[len("REPLACE(") : -1].strip())
    if len(args) != 3:
        return None
    try:
        column, has_source_column = _normalize_string_scalar_expression_sql(args[0])
        if not has_source_column or not _is_sql_identifier(column):
            return None
        needle = _parse_sql_string_literal_token(args[1])
        replacement = _parse_sql_string_literal_token(args[2])
    except (TypeError, ValueError):
        return None
    if needle == "":
        return None
    return column, needle, replacement


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
    return _normalize_raw_or_typed_predicate("predicate expression", value)


def _predicate_sources(*values: object) -> tuple[WorkflowSource, ...]:
    sources: list[WorkflowSource] = []
    for value in values:
        if isinstance(value, PredicateExpression):
            sources.extend(value.source_bindings)
        elif isinstance(value, (LazyFrame, SqlWorkflow)):
            sources.extend(value._declared_sources())
    return tuple(sources)


def _normalize_raw_or_typed_predicate(name: str, value: object) -> str:
    if isinstance(value, PredicateExpression):
        return value.sql
    text = str(value).strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    _validate_raw_sql_fragment(name, text)
    return text


def _validate_raw_sql_fragment(
    name: str,
    value: str,
    *,
    allowed_keywords: tuple[str, ...] = (),
) -> None:
    _validate_sql_fragment_quotes(name, value)
    for token in _RAW_SQL_FRAGMENT_BREAKOUT_TOKENS:
        if _contains_sql_token_outside_quotes(value, token):
            raise ValueError(
                f"{name} must stay inside a scoped expression; SQL statement separators "
                "and comments are not admitted"
            )
    for keyword in _RAW_SQL_FRAGMENT_BREAKOUT_KEYWORDS:
        if keyword in allowed_keywords:
            continue
        if _contains_sql_keyword_outside_quotes(value, keyword):
            raise ValueError(
                f"{name} must stay inside a scoped expression; SQL clause keyword "
                f"{keyword!r} is not admitted in raw fragments"
            )


def _sql_fragment_admitted_for_local_source_statement(
    name: str,
    value: str,
    *,
    allowed_keywords: tuple[str, ...] = (),
) -> bool:
    try:
        _validate_raw_sql_fragment(
            name,
            value,
            allowed_keywords=allowed_keywords,
        )
    except ValueError:
        return False
    return True


def _validate_sql_fragment_quotes(name: str, value: str) -> None:
    in_quote = False
    index = 0
    while index < len(value):
        if value[index] == "'":
            if in_quote and index + 1 < len(value) and value[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
        index += 1
    if in_quote:
        raise ValueError(f"{name} SQL string literal is not closed")


def _contains_sql_token_outside_quotes(statement: str, token: str) -> bool:
    in_quote = False
    index = 0
    while index <= len(statement) - len(token):
        char = statement[index]
        if char == "'":
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if not in_quote and statement.startswith(token, index):
            return True
        index += 1
    return False


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
        f"({column_sql}) {operator} (SELECT {source_column_sql} FROM {source_ref}{tail})",
        _predicate_sources(source, where, having),
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
    tail = _sql_local_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
        limit_name="EXISTS subquery limit",
        positive_limit=False,
    )
    operator = "NOT EXISTS" if negated else "EXISTS"
    return PredicateExpression(
        f"{operator} (SELECT {projection_sql} FROM {source_ref}{tail})",
        _predicate_sources(source, where, having),
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
        positive_limit=True,
    )
    return PredicateExpression(
        f"{column_sql} {operator} {quantifier} "
        f"(SELECT {source_column_sql} FROM {source_ref}{tail})",
        _predicate_sources(source, where, having),
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
    if isinstance(source, SqlWorkflow):
        source_ref = f"({source._relation_statement()})"
        if source_alias is None:
            source_alias = "_sl_subquery"
    elif isinstance(source, LazyFrame):
        if source.operations:
            statement = source._relation_statement()
            if statement is None:
                raise ValueError("source subquery contains an operation without native relational lowering")
            source_ref = f"({statement})"
            if source_alias is None:
                source_alias = "_sl_subquery"
        else:
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
    """Whether a lazy workflow already carries a finite collect bound."""

    return any(operation.kind in {"limit", "tail", "sample"} for operation in operations)


def _workflow_has_index_metadata(operations: Sequence[WorkflowOperation]) -> bool:
    """Whether a lazy workflow carries explicit index-state metadata."""

    return any(operation.kind == "set_index" for operation in operations)


def _workflow_index_columns(operations: Sequence[WorkflowOperation]) -> tuple[str, ...]:
    """Return the explicit ShardLoom index columns, if the lazy plan records them."""

    for operation in reversed(operations):
        if operation.kind == "set_index":
            return operation.values
    return ()


def _duplicate_mask_operation_parts(
    values: Sequence[str],
) -> tuple[tuple[str, ...] | None, str]:
    if not values:
        return None, "first"
    keep = "first"
    columns: list[str] = []
    for value in values:
        if value.startswith("keep="):
            keep = value.removeprefix("keep=")
        else:
            columns.append(value)
    if keep not in {"first", "last", "false"}:
        return None, "first"
    if not columns or (columns != ["*"] and any(not _is_sql_identifier(column) for column in columns)):
        return None, keep
    return tuple(columns), keep


def _workflow_has_top_n_shape(operations: Sequence[WorkflowOperation]) -> bool:
    """Whether a lazy workflow has the sort+limit shape used for global top-N."""

    has_sort = False
    for operation in operations:
        if operation.kind == "sort":
            has_sort = True
        elif operation.kind == "limit" and has_sort:
            return True
    return False


def _sql_normalized(value: str) -> str:
    return "".join(value.strip().lower().split())


def _sql_text_looks_like_cast(value: str) -> bool:
    normalized = value.upper()
    return "CAST(" in normalized or "TRY_CAST(" in normalized


def _sql_filter_looks_like_substring_contains(predicate: str) -> bool:
    normalized = predicate.upper()
    return (
        (" LIKE " in normalized or " NOT LIKE " in normalized)
        and "'%" in predicate
        and "%'" in predicate
    )


def _sql_filter_requires_native_vortex_expression_route(predicate: str) -> bool:
    return _sql_text_looks_like_cast(predicate) or _sql_filter_looks_like_substring_contains(
        predicate
    )


_VORTEX_TINY_NULL_PREDICATE_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL\s*$",
    re.IGNORECASE,
)
_VORTEX_TINY_INT_COMPARE_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(=|>=|<=|>|<)\s*(-?\d+)\s*$"
)
_VORTEX_TINY_COMPACT_RE = re.compile(
    r"^\s*(is_null|is_not_null):([A-Za-z_][A-Za-z0-9_]*)\s*$"
    r"|^\s*(eq|gt|gte|lt|lte):([A-Za-z_][A-Za-z0-9_]*):(-?\d+)\s*$"
)


def _vortex_tiny_predicate_from_sql(predicate: str) -> str | None:
    """Lower a scoped SQL predicate into the native Vortex primitive predicate format."""

    text = predicate.strip()
    if not text:
        return None
    if _VORTEX_TINY_COMPACT_RE.fullmatch(text):
        return text
    null_match = _VORTEX_TINY_NULL_PREDICATE_RE.fullmatch(text)
    if null_match:
        column = null_match.group(1)
        return f"is_not_null:{column}" if null_match.group(2) else f"is_null:{column}"
    compare_match = _VORTEX_TINY_INT_COMPARE_RE.fullmatch(text)
    if compare_match is None:
        return None
    column, operator, value = compare_match.groups()
    op = {
        "=": "eq",
        ">": "gt",
        ">=": "gte",
        "<": "lt",
        "<=": "lte",
    }[operator]
    return f"{op}:{column}:{value}"


def _vortex_tiny_predicate_column(predicate: str | None) -> str | None:
    if predicate is None:
        return None
    text = predicate.strip()
    null_match = _VORTEX_TINY_NULL_PREDICATE_RE.fullmatch(text)
    if null_match:
        return null_match.group(1)
    compact_match = _VORTEX_TINY_COMPACT_RE.fullmatch(text)
    if compact_match:
        return compact_match.group(2) or compact_match.group(4)
    compare_match = _VORTEX_TINY_INT_COMPARE_RE.fullmatch(text)
    if compare_match:
        return compare_match.group(1)
    return None


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
    return lower.endswith((".vortex", ".vtx", ".vortex-manifest"))


def _embedded_vortex_input_uri(statement: str) -> str | None:
    """Return the single embedded local Vortex SQL source ref, when unambiguous."""

    refs = tuple(_sql_source_refs(statement))
    if not refs or any(not _is_local_vortex_source_ref(ref) for ref in refs):
        return None
    unique_refs = tuple(dict.fromkeys(refs))
    if len(unique_refs) != 1:
        return None
    return unique_refs[0]




def _find_top_level_sql_phrase_span_outside_quotes(
    statement: str,
    phrase: str,
) -> tuple[int, int] | None:
    tokens = phrase.lower().split()
    if not tokens:
        return None
    lower = statement.lower()
    in_quote = False
    depth = 0
    index = 0
    first = tokens[0]
    while index <= len(statement) - len(first):
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
            if depth == 0 and lower.startswith(first, index):
                before = statement[index - 1] if index > 0 else ""
                after_index = index + len(first)
                after = statement[after_index] if after_index < len(statement) else ""
                if _is_identifier_char(before) or _is_identifier_char(after):
                    index += 1
                    continue
                cursor = after_index
                matched = True
                for token in tokens[1:]:
                    if cursor >= len(statement) or not statement[cursor].isspace():
                        matched = False
                        break
                    while cursor < len(statement) and statement[cursor].isspace():
                        cursor += 1
                    if not lower.startswith(token, cursor):
                        matched = False
                        break
                    token_end = cursor + len(token)
                    next_char = statement[token_end] if token_end < len(statement) else ""
                    if _is_identifier_char(next_char):
                        matched = False
                        break
                    cursor = token_end
                if matched:
                    return index, cursor
        index += 1
    return None
















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


def _public_workflow_input_format(source: WorkflowSource) -> str:
    """Return the input format to pass to the public workflow CLI facade."""

    if source.source_format == "json":
        lower = source.uri.strip().lower()
        if lower.endswith(".jsonl"):
            return "jsonl"
        if lower.endswith(".ndjson"):
            return "ndjson"
    return source.source_format


def _workflow_source_bindings(sources: Sequence[WorkflowSource]) -> dict[str, dict[str, object]]:
    """Retain each source's adapter contract across relational SQL lowering."""
    bindings: dict[str, dict[str, object]] = {}
    for source in sources:
        binding: dict[str, object] = {"input_format": _public_workflow_input_format(source)}
        if source.memory_input:
            binding["memory_input"] = dict(source.memory_input)
        elif schema := _prepare_vortex_schema_hints(source):
            binding["source_schema"] = schema
        previous = bindings.setdefault(source.uri, binding)
        if previous != binding:
            raise ValueError(f"conflicting format or schema declarations for source {source.uri!r}")
    return bindings


def _prepare_vortex_schema_hints(source: WorkflowSource) -> Mapping[str, object] | None:
    """Return CLI schema hints only for text adapters that accept them."""

    if source.source_format in {"csv", "json"}:
        return source.schema or None
    return None


def _public_workflow_default_execution_policy(source: WorkflowSource) -> str:
    """Return the public workflow policy implied by the source boundary."""

    return "native_vortex" if source.source_format == "vortex" else "vortex_middle"


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


def _is_declared_local_source(source: WorkflowSource) -> bool:
    uri = source.uri.strip().lower()
    return (
        source.source_format in {"csv", "json", "parquet", "arrow-ipc", "avro", "orc"}
        and "://" not in uri
        and not uri.startswith(("s3:", "gs:", "abfs:", "abfss:"))
    )




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








def _terminal_resource_kwargs(
    memory_gb: int,
    max_parallelism: int,
    spill: Mapping[str, object] | str | None = None,
) -> dict[str, object]:
    """Build terminal runtime kwargs while leaving spill policy opaque."""

    kwargs: dict[str, object] = {
        "memory_gb": _normalize_positive_int("memory_gb", memory_gb),
        "max_parallelism": _normalize_positive_int("max_parallelism", max_parallelism),
    }
    if spill is not None:
        kwargs["spill"] = spill
    return kwargs


def _collect_native_relational(
    client: ShardLoomClient, statement: str, *, surface: str,
    plan_summary: str, input_kwargs: Mapping[str, Any], check: bool,
    memory_gb: int, max_parallelism: int,
    spill: Mapping[str, object] | str | None = None,
) -> OutputEnvelope:
    return client.public_workflow_run(
        surface, sql_statement=statement, plan_summary=plan_summary,
        requested_output="collect", execution_policy="vortex_middle",
        materialization_policy="bounded", evidence_level="production_admitted_local_workflow",
        bounded=True,
        **_terminal_resource_kwargs(memory_gb, max_parallelism, spill),
        check=check, **input_kwargs,
    ).envelope


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
    return _sql_string_literal(path)


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
    index = 1
    null_ordering = None
    if len(values) > index and values[index].startswith(_SORT_NULLS_TOKEN_PREFIX):
        null_ordering = values[index][len(_SORT_NULLS_TOKEN_PREFIX) :]
        index += 1
    columns = tuple(value for value in values[index:] if not value.startswith("keep="))
    return direction, columns, null_ordering


def _parse_sort_keep_policy(values: tuple[str, ...]) -> str:
    keep_values = tuple(value.split("=", 1)[1] for value in values if value.startswith("keep="))
    if not keep_values:
        return "first"
    if len(keep_values) != 1:
        return "first"
    keep = keep_values[0].strip().lower().replace("_", "-")
    return keep if keep in {"first", "last", "all"} else "first"




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


def _rewrite_predicate_with_computed_columns(
    predicate: str,
    computed_columns: tuple[tuple[str, str], ...],
) -> str:
    expanded_columns: dict[str, str] = {}
    for alias, expression in computed_columns:
        expanded = expression
        for prior_alias, prior_expression in expanded_columns.items():
            expanded = _replace_sql_identifier_outside_quotes(
                expanded,
                prior_alias,
                f"({prior_expression})",
            )
        expanded_columns[alias] = expanded

    rewritten = predicate
    for alias, expression in expanded_columns.items():
        rewritten = _replace_sql_identifier_outside_quotes(
            rewritten,
            alias,
            f"({expression})",
        )
    return rewritten


def _replace_sql_identifier_outside_quotes(
    statement: str,
    identifier: str,
    replacement: str,
) -> str:
    if not identifier or identifier not in statement:
        return statement
    rewritten: list[str] = []
    in_quote = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if char == "'":
            rewritten.append(char)
            if in_quote and index + 1 < len(statement) and statement[index + 1] == "'":
                rewritten.append(statement[index + 1])
                index += 2
                continue
            in_quote = not in_quote
            index += 1
            continue
        if (
            not in_quote
            and statement.startswith(identifier, index)
            and _sql_identifier_rewrite_boundary(statement, index, len(identifier))
        ):
            rewritten.append(replacement)
            index += len(identifier)
            continue
        rewritten.append(char)
        index += 1
    return "".join(rewritten)


def _sql_identifier_rewrite_boundary(
    statement: str,
    start: int,
    length: int,
) -> bool:
    before = statement[start - 1] if start > 0 else ""
    after_index = start + length
    after = statement[after_index] if after_index < len(statement) else ""
    return (
        not _is_identifier_char(before)
        and not _is_identifier_char(after)
        and before != "."
        and after != "."
    )


def _optional_sql_having_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" HAVING {predicate}"


def _workflow_schema_report(
    workflow: LazyFrame,
    smoke_report: VortexWorkflowExecutionReport,
) -> WorkflowSchemaReport:
    rows = smoke_report.result_rows
    declared = dict(workflow.source.schema)
    fields = tuple(
        WorkflowSchemaField(
            name=name, dtype=dtype.label, nullable=dtype.nullable,
            declared_dtype=declared.get(name),
            observed_non_null_count=builtins.sum(row[name] is not None for row in rows),
            null_count=builtins.sum(row[name] is None for row in rows),
        ) for name, dtype in smoke_report.result_schema
    )
    return WorkflowSchemaReport(workflow=workflow, smoke_report=smoke_report, fields=fields)




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


def _result_to_pandas(report: VortexWorkflowExecutionReport, pandas: object) -> object:
    # Object columns preserve NULL and full-width integers without float coercion.
    return getattr(pandas, "DataFrame")(report.python_objects, columns=report.result_columns, dtype=object)


def _result_to_arrow_table(report: VortexWorkflowExecutionReport, pyarrow: object) -> object:
    return arrow_table(report.result_rows, report.result_schema, pyarrow)


def _result_to_arrow_ipc(report: VortexWorkflowExecutionReport, pyarrow: object) -> bytes:
    table = _result_to_arrow_table(report, pyarrow)
    sink = getattr(pyarrow, "BufferOutputStream")()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _result_to_numpy(report: VortexWorkflowExecutionReport, numpy: object) -> object:
    rows, columns = report.python_objects, report.result_columns
    # Assign whole cells to keep mixed scalars and nested values in two axes.
    values = getattr(numpy, "empty")((len(rows), len(columns)), dtype=object)
    for index, row in enumerate(rows):
        for column_index, column in enumerate(columns):
            values[index, column_index] = row[column]
    return values


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
    if normalized in {"json", "json-array"}:
        return "json"
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
        "scoped local writes currently support local JSON arrays, JSONL, CSV, and feature-gated "
        "Parquet/Arrow IPC/Avro/ORC/Vortex only"
    )


def _public_write_request_for_format(output_format: str) -> str:
    normalized = _normalize_local_output_format(output_format)
    return {
        "json": "write_json",
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
