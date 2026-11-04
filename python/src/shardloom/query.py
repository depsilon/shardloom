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
from dataclasses import field, replace as _dataclass_replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, Union, cast

from ._compat import dataclass
from ._native_backend import NativePlanState
from ._result_schema import ResultType, arrow_table, python_rows, schema_fields
from ._temporal import parse_temporal_declaration
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
from .execution_resources import (
    ExecutionResourceLimits, ExecutionResources, merge_resource_limits,
    optional_resources, resolve_resources,
)

SUPPORTED_SOURCE_FORMATS = ("vortex", "csv", "tsv", "json", "jsonl", "parquet", "arrow-ipc", "avro", "orc", "iceberg", "delta", "hudi")
_NATIVE_UNARY_PRIMITIVES = frozenset({
    "distinct", "drop_duplicates", "duplicate_mask", "tail", "sample",
    "expression_project", "melt", "explode", "pivot", "rolling_window",
})
_NATIVE_USIZE_MAX = (1 << 64) - 1
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
class SqlRelation:
    """Immutable SQL syntax, typed values and named relation snapshots."""

    statement: str
    source_bindings: tuple["WorkflowSource", ...] = ()
    relations: tuple[tuple[str, "LazyFrame"], ...] = ()
    parameters: tuple[tuple[str, object], ...] = ()
    input_uri: str | None = None
    input_format: str | None = None


@dataclass(frozen=True, slots=True)
class WorkflowSource:
    """A declared workflow source that is not read during construction."""

    source_format: str
    uri: str
    schema: tuple[tuple[str, str], ...] = ()
    memory_input: tuple[tuple[str, object], ...] = ()
    batch_input: object | None = None
    native_input: object | None = field(default=None, repr=False, compare=False)
    _bound_frame: "LazyFrame | None" = field(default=None, repr=False, compare=False)
    relation: SqlRelation | None = None
    _session: object | None = field(default=None, repr=False, compare=False)
    _native_plan: NativePlanState = field(default_factory=NativePlanState, init=False, repr=False, compare=False)
    table_snapshot: tuple[tuple[str, object], ...] = ()

    @property
    def schema_map(self) -> dict[str, str]:
        """Return the optional declared schema as a dict."""

        return dict(self.schema)

    def to_summary(self) -> str:
        """Return a deterministic source summary for CLI explain/estimate calls."""

        if self.relation is not None:
            return "sql(statement)"
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
    resource_limits: ExecutionResourceLimits | None = None
    right_frame: "LazyFrame | None" = field(default=None, repr=False, compare=False)
    grouping_clause: str | None = None
    batch_function: object | None = field(default=None, repr=False)

    def to_summary(self) -> str:
        """Return a deterministic operation summary."""
        from ._query_declarations import WorkflowOperation_to_summary as _impl
        return _impl(self)


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

        result = self.function(frame, *args, **kwargs)  # type: ignore[misc]
        if isinstance(result, LazyFrame) and result.client is not frame.client:
            raise ValueError("plan transform returned a DataFrame from another context")
        return result


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
    resource_limits: ExecutionResourceLimits | None = None

    def __str__(self) -> str:
        return self.sql

    def alias(self, name: str) -> "ColumnExpression":
        """Project this native Boolean expression under an explicit field name."""
        return ColumnExpression(f"{self.sql} AS {_normalize_output_column_name(name)}",
            self.source_bindings, resource_limits=self.resource_limits)

    def __and__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical AND predicate."""

        return PredicateExpression(
            f"({self.sql} AND {_predicate_sql(other)})", _predicate_sources(self, other),
            resource_limits=_predicate_limits(self, other),
        )

    def __or__(self, other: object) -> "PredicateExpression":
        """Return a scoped logical OR predicate."""

        return PredicateExpression(
            f"({self.sql} OR {_predicate_sql(other)})", _predicate_sources(self, other),
            resource_limits=_predicate_limits(self, other),
        )

    def __invert__(self) -> "PredicateExpression":
        """Return a scoped logical NOT predicate."""

        return PredicateExpression(f"NOT {self.sql}", self.source_bindings, resource_limits=self.resource_limits)


@dataclass(frozen=True, slots=True)
class WindowExpression:
    """A SQL window declaration submitted to the shared native engine."""

    sql: str
    source_bindings: tuple[WorkflowSource, ...] = ()
    resource_limits: ExecutionResourceLimits | None = None

    def __str__(self) -> str:
        return self.sql

    def alias(self, name: str) -> "WindowExpression":
        return WindowExpression(f"{self.sql} AS {_normalize_output_column_name(name)}",
            self.source_bindings, self.resource_limits)


@dataclass(frozen=True, slots=True)
class IntervalLiteral:
    """A scoped ANSI interval literal for admitted temporal helper functions."""

    value: int
    unit: str

    def __post_init__(self) -> None:
        from ._query_expression_methods import IntervalLiteral___post_init__ as _impl
        return _impl(self)

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
    source_bindings: tuple[WorkflowSource, ...] = ()
    resource_limits: ExecutionResourceLimits | None = None

    def __str__(self) -> str:
        return self.sql

    def alias(self, name: str) -> "ColumnExpression":
        """Name a native projection without evaluating the expression."""
        return ColumnExpression(f"{self.sql} AS {_normalize_output_column_name(name)}",
            self.source_bindings, self.resource_limits)

    def over(self, specification) -> WindowExpression:
        """Apply an immutable WindowSpec through the shared native binder."""
        from ._window import WindowSpec
        if not isinstance(specification, WindowSpec):
            raise TypeError("over() requires a WindowSpec")
        return WindowExpression(f"{self.sql} OVER ({specification.sql})",
            _predicate_sources(self, specification), _predicate_limits(self, specification))

    def forward_fill(self) -> "ColumnExpression":
        """Declare native forward filling for use with :meth:`over`."""
        return ColumnExpression(
            f"FORWARD_FILL({self.sql})", self.source_bindings,
            resource_limits=self.resource_limits,
        )

    def backward_fill(self) -> "ColumnExpression":
        """Declare native backward filling for use with :meth:`over`."""
        return ColumnExpression(
            f"BACKWARD_FILL({self.sql})", self.source_bindings,
            resource_limits=self.resource_limits,
        )

    def interpolate_linear(self) -> "ColumnExpression":
        """Declare native linear interpolation for use with :meth:`over`."""
        return ColumnExpression(
            f"INTERPOLATE_LINEAR({self.sql})", self.source_bindings,
            resource_limits=self.resource_limits,
        )

    def decimal_rescale(self, precision, scale, rounding) -> "ColumnExpression":
        """Declare native decimal rescaling with explicit precision and rounding."""
        precision, scale, rounding = _decimal_controls(precision, scale, rounding)
        return ColumnExpression(
            f"DECIMAL_RESCALE({self.sql}, {precision}, {scale}, {_sql_string_literal(rounding)})",
            self.source_bindings, resource_limits=self.resource_limits,
        )

    def _decimal_binary(self, name, other, precision, scale, rounding) -> "ColumnExpression":
        precision, scale, rounding = _decimal_controls(precision, scale, rounding)
        rhs = other if isinstance(other, ColumnExpression) else col(other)
        return ColumnExpression(
            f"{name}({self.sql},{rhs.sql},{precision},{scale},{_sql_string_literal(rounding)})",
            _predicate_sources(self, rhs), resource_limits=_predicate_limits(self, rhs),
        )

    def decimal_add(self, other, precision, scale, rounding) -> "ColumnExpression":
        """Declare native decimal addition with explicit output controls."""
        return self._decimal_binary("DECIMAL_ADD", other, precision, scale, rounding)

    def decimal_subtract(self, other, precision, scale, rounding) -> "ColumnExpression":
        """Declare native decimal subtraction with explicit output controls."""
        return self._decimal_binary("DECIMAL_SUBTRACT", other, precision, scale, rounding)

    def decimal_multiply(self, other, precision, scale, rounding) -> "ColumnExpression":
        """Declare native decimal multiplication with explicit output controls."""
        return self._decimal_binary("DECIMAL_MULTIPLY", other, precision, scale, rounding)

    def decimal_divide(self, other, precision, scale, rounding) -> "ColumnExpression":
        """Declare native decimal division with explicit output controls."""
        return self._decimal_binary("DECIMAL_DIVIDE", other, precision, scale, rounding)

    def filter(self, predicate) -> "ColumnExpression":
        """Declare aggregate-local FILTER; false and NULL predicate values exclude rows."""
        if not isinstance(predicate, (PredicateExpression, ColumnExpression)):
            raise TypeError("aggregate filter requires a PredicateExpression or ColumnExpression")
        return ColumnExpression(
            f"{self.sql} FILTER (WHERE {predicate.sql})",
            _predicate_sources(self, predicate),
            resource_limits=_predicate_limits(self, predicate),
        )

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
        return PredicateExpression(f"{self.sql} {operator} {rhs}", _predicate_sources(self, value), resource_limits=_predicate_limits(self, value))

    def _numeric_binary(self, operator: str, value: object) -> "ColumnExpression":
        from ._query_expression_methods import ColumnExpression__numeric_binary as _impl
        return _impl(self, operator, value)

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

    def __mod__(self, value: object) -> "ColumnExpression":
        """Declare native remainder, retaining SQL sign and null semantics."""

        return self._numeric_binary("%", value)

    def __abs__(self) -> "ColumnExpression":
        """Return a scoped `ABS(column)` numeric absolute-value expression."""

        return self.abs()

    def __neg__(self) -> "ColumnExpression":
        """Return a checked native numeric negation expression."""

        return ColumnExpression(f"-({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def abs(self) -> "ColumnExpression":
        """Return a scoped `ABS(column)` numeric absolute-value expression."""

        return ColumnExpression(f"ABS({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def floor(self) -> "ColumnExpression":
        """Return a scoped `FLOOR(column)` numeric rounding expression."""

        return ColumnExpression(f"FLOOR({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def ceil(self) -> "ColumnExpression":
        """Return a scoped `CEIL(column)` numeric rounding expression."""

        return ColumnExpression(f"CEIL({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def round(self) -> "ColumnExpression":
        """Return a scoped `ROUND(column)` numeric rounding expression."""

        return ColumnExpression(f"ROUND({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def is_null(self) -> PredicateExpression:
        """Return a scoped `IS NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NULL", self.source_bindings, resource_limits=self.resource_limits)

    def is_nan(self) -> PredicateExpression:
        """Classify floating NaN values, propagating NULL validity."""
        return PredicateExpression(f"IS_NAN({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def is_infinite(self) -> PredicateExpression:
        """Classify either floating infinity, propagating NULL validity."""
        return PredicateExpression(f"IS_INFINITE({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def is_finite(self) -> PredicateExpression:
        """Classify finite floating values, propagating NULL validity."""
        return PredicateExpression(f"IS_FINITE({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def signbit(self) -> PredicateExpression:
        """Observe a floating value's sign bit, including zero and NaN."""
        return PredicateExpression(f"SIGNBIT({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def is_not_null(self) -> PredicateExpression:
        """Return a scoped `IS NOT NULL` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT NULL", self.source_bindings, resource_limits=self.resource_limits)

    def is_distinct_from(self, value: object) -> PredicateExpression:
        """Return a scoped SQL `IS DISTINCT FROM` null-safe comparison."""

        return PredicateExpression(
            f"{self.sql} IS DISTINCT FROM {self._null_safe_comparison_rhs(value)}",
            _predicate_sources(self, value),
            resource_limits=_predicate_limits(self, value),
        )

    def is_not_distinct_from(self, value: object) -> PredicateExpression:
        """Return a scoped SQL `IS NOT DISTINCT FROM` null-safe comparison."""

        return PredicateExpression(
            f"{self.sql} IS NOT DISTINCT FROM {self._null_safe_comparison_rhs(value)}",
            _predicate_sources(self, value),
            resource_limits=_predicate_limits(self, value),
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

        return PredicateExpression(f"{self.sql} IS TRUE", self.source_bindings, resource_limits=self.resource_limits)

    def is_false(self) -> PredicateExpression:
        """Return a scoped SQL boolean false predicate."""

        return PredicateExpression(f"{self.sql} IS FALSE", self.source_bindings, resource_limits=self.resource_limits)

    def is_not_true(self) -> PredicateExpression:
        """Return a scoped SQL `IS NOT TRUE` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT TRUE", self.source_bindings, resource_limits=self.resource_limits)

    def is_not_false(self) -> PredicateExpression:
        """Return a scoped SQL `IS NOT FALSE` predicate."""

        return PredicateExpression(f"{self.sql} IS NOT FALSE", self.source_bindings, resource_limits=self.resource_limits)

    def like(self, pattern: object, *, escape: object | None = None) -> PredicateExpression:
        """Return a scoped SQL LIKE predicate.

        The runtime admits scoped UTF-8 SQL LIKE patterns with `%` and `_`
        wildcards and optional single-character ESCAPE clauses. Locale-aware
        collation and case-folding semantics remain outside this helper's claim
        boundary.
        """

        return PredicateExpression(
            f"{self.sql} LIKE {_sql_string_literal(pattern)}{_like_escape_clause(escape)}",
            self.source_bindings,
            resource_limits=self.resource_limits,
        )

    def not_like(self, pattern: object, *, escape: object | None = None) -> PredicateExpression:
        """Return a scoped SQL NOT LIKE predicate."""

        return PredicateExpression(
            f"{self.sql} NOT LIKE {_sql_string_literal(pattern)}{_like_escape_clause(escape)}",
            self.source_bindings,
            resource_limits=self.resource_limits,
        )

    def rlike(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex predicate lowered to SQL `RLIKE`."""

        return PredicateExpression(f"{self.sql} RLIKE {_sql_string_literal(pattern)}", self.source_bindings, resource_limits=self.resource_limits)

    def not_rlike(self, pattern: object) -> PredicateExpression:
        """Return a scoped UTF-8 regex negation lowered to SQL `NOT RLIKE`."""

        return PredicateExpression(f"{self.sql} NOT RLIKE {_sql_string_literal(pattern)}", self.source_bindings, resource_limits=self.resource_limits)

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

        return ColumnExpression(f"LOWER({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def upper(self) -> "ColumnExpression":
        """Return a scoped `UPPER(column)` UTF-8 transform expression."""

        return ColumnExpression(f"UPPER({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def trim(self) -> "ColumnExpression":
        """Return a scoped `TRIM(column)` UTF-8 transform expression."""

        return ColumnExpression(f"TRIM({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def url_domain(self) -> "ColumnExpression":
        """Return the native URL-domain extraction expression for this column."""

        return ColumnExpression(f"URL_DOMAIN({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def regex_domain(self) -> "ColumnExpression":
        """Apply the fixed HTTP(S) domain-replacement contract, not arbitrary regex."""

        return ColumnExpression(f"REGEX_DOMAIN({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def length(self) -> "ColumnExpression":
        """Return a scoped `LENGTH(column)` UTF-8 length expression."""

        return ColumnExpression(f"LENGTH({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def concat(self, *parts: object) -> "ColumnExpression":
        """Return a scoped `CONCAT(column-or-string-literal, ...)` expression."""

        return concat(self, *parts)

    def substr(self, start: object, length: object) -> "ColumnExpression":
        """Return a scoped 1-based `SUBSTR(column, start, length)` expression."""
        from ._query_expression_methods import ColumnExpression_substr as _impl
        return _impl(self, start, length)

    def substring(self, start: object, length: object) -> "ColumnExpression":
        """Alias for `substr(...)`."""

        return self.substr(start, length)

    def left(self, count: object) -> "ColumnExpression":
        """Return a scoped `LEFT(column, count)` UTF-8 expression."""

        column = self.sql
        normalized_count = _normalize_substring_bound("left count", count, minimum=0)
        return ColumnExpression(f"LEFT({column}, {normalized_count})", self.source_bindings, resource_limits=self.resource_limits)

    def right(self, count: object) -> "ColumnExpression":
        """Return a scoped `RIGHT(column, count)` UTF-8 expression."""

        column = self.sql
        normalized_count = _normalize_substring_bound("right count", count, minimum=0)
        return ColumnExpression(f"RIGHT({column}, {normalized_count})", self.source_bindings, resource_limits=self.resource_limits)

    def replace(self, needle: object, replacement: object) -> "ColumnExpression":
        """Return a scoped `REPLACE(column, needle, replacement)` expression."""
        from ._query_expression_methods import ColumnExpression_replace as _impl
        return _impl(self, needle, replacement)

    def unhex(self) -> "ColumnExpression":
        """Return a scoped `UNHEX(<utf8-expression>)` binary helper expression."""

        return ColumnExpression(f"UNHEX({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def from_base64(self) -> "ColumnExpression":
        """Return a scoped `FROM_BASE64(<utf8-expression>)` binary helper expression."""

        return ColumnExpression(f"FROM_BASE64({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def byte_length(self) -> "ColumnExpression":
        """Return a scoped `BYTE_LENGTH(<binary-expression>)` byte-count expression."""

        return ColumnExpression(f"BYTE_LENGTH({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def fill_null(self, value: object) -> "ColumnExpression":
        """Return a `COALESCE` expression with a scalar value or expression."""

        return ColumnExpression(
            f"COALESCE({self.sql}, {_sql_case_branch(value)})",
            _predicate_sources(self, value),
            resource_limits=_predicate_limits(self, value),
        )

    def null_if(self, value: object) -> "ColumnExpression":
        """Return a scoped `NULLIF(column, literal)` null-cleanup expression."""

        return ColumnExpression(
            f"NULLIF({self.sql}, {_sql_case_branch(value)})",
            _predicate_sources(self, value),
            resource_limits=_predicate_limits(self, value),
        )

    def isin(self, *values: object) -> PredicateExpression:
        """Return a scoped bounded `IN (...)` predicate."""

        normalized = _normalize_in_values(values)
        joined = ",".join(_sql_in_literal(value) for value in normalized)
        return PredicateExpression(f"{self.sql} IN ({joined})", self.source_bindings, resource_limits=self.resource_limits)

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
        from ._query_expression_methods import ColumnExpression_isin_source as _impl
        return _impl(self, source, column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)

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
        from ._query_expression_methods import ColumnExpression_any_source as _impl
        return _impl(self, comparison, source, column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)

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
        from ._query_expression_methods import ColumnExpression_all_source as _impl
        return _impl(self, comparison, source, column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)

    def not_in(self, *values: object) -> PredicateExpression:
        """Return a scoped bounded `NOT IN (...)` predicate."""

        normalized = _normalize_in_values(values)
        joined = ",".join(_sql_in_literal(value) for value in normalized)
        return PredicateExpression(f"{self.sql} NOT IN ({joined})", self.source_bindings, resource_limits=self.resource_limits)

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
        from ._query_expression_methods import ColumnExpression_not_in_source as _impl
        return _impl(self, source, column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)

    def between(self, lower: object, upper: object) -> PredicateExpression:
        """Return a scoped inclusive range predicate.

        The expression lowers to an admitted `>=` / `<=` predicate pair so the
        CLI remains responsible for runtime admission, evidence, and blockers.
        """

        return PredicateExpression(
            f"({self.sql} >= {_sql_literal(lower)} AND {self.sql} <= {_sql_literal(upper)})",
            _predicate_sources(self, lower, upper),
            resource_limits=_predicate_limits(self, lower, upper),
        )

    def cast(self, dtype: object) -> "ColumnExpression":
        """Return a scoped `CAST(column AS dtype)` expression for comparisons."""

        normalized_dtype = _normalize_cast_dtype(dtype)
        return ColumnExpression(f"CAST({self.sql} AS {normalized_dtype})", self.source_bindings, resource_limits=self.resource_limits)

    def try_cast(self, dtype: object) -> "ColumnExpression":
        """Return a scoped `TRY_CAST(column AS dtype)` expression for dirty values."""

        normalized_dtype = _normalize_cast_dtype(dtype)
        return ColumnExpression(f"TRY_CAST({self.sql} AS {normalized_dtype})", self.source_bindings, resource_limits=self.resource_limits)

    def date_add_days(self, days: object) -> "ColumnExpression":
        """Return a scoped typed-date day-add expression; native admission checks its domain."""

        normalized_days = _normalize_date_arithmetic_days(days)
        return ColumnExpression(f"DATE_ADD_DAYS({self.sql}, {normalized_days})", _predicate_sources(self, days), resource_limits=_predicate_limits(self, days))

    def date_sub_days(self, days: object) -> "ColumnExpression":
        """Return a scoped typed-date day-subtract expression; native admission checks its domain."""

        normalized_days = _normalize_date_arithmetic_days(days)
        return ColumnExpression(f"DATE_SUB_DAYS({self.sql}, {normalized_days})", _predicate_sources(self, days), resource_limits=_predicate_limits(self, days))

    def timestamp_add_seconds(self, seconds: object) -> "ColumnExpression":
        """Return a scoped typed-timestamp second-add expression; native admission checks its domain."""

        normalized_seconds = _normalize_timestamp_arithmetic_seconds(seconds)
        return ColumnExpression(
            f"TIMESTAMP_ADD_SECONDS({self.sql}, {normalized_seconds})", _predicate_sources(self, seconds),
            resource_limits=_predicate_limits(self, seconds),
        )

    def timestamp_sub_seconds(self, seconds: object) -> "ColumnExpression":
        """Return a scoped typed-timestamp second-subtract expression; native admission checks its domain."""

        normalized_seconds = _normalize_timestamp_arithmetic_seconds(seconds)
        return ColumnExpression(
            f"TIMESTAMP_SUB_SECONDS({self.sql}, {normalized_seconds})", _predicate_sources(self, seconds),
            resource_limits=_predicate_limits(self, seconds),
        )

    def date_diff_days(self, other: object) -> "ColumnExpression":
        """Return a scoped typed-date day-difference expression."""

        return ColumnExpression(
            f"DATE_DIFF_DAYS({self.sql}, {_sql_temporal_difference_arg(other, 'date32')})",
            _predicate_sources(self, other),
            resource_limits=_predicate_limits(self, other),
        )

    def timestamp_diff_seconds(self, other: object) -> "ColumnExpression":
        """Return a scoped typed-timestamp second-difference expression."""

        return ColumnExpression(
            f"TIMESTAMP_DIFF_SECONDS({self.sql}, {_sql_temporal_difference_arg(other, 'timestamp_micros')})",
            _predicate_sources(self, other),
            resource_limits=_predicate_limits(self, other),
        )

    def date_year(self) -> "ColumnExpression":
        """Return a scoped typed-date year-extract expression for predicates."""

        return ColumnExpression(f"DATE_YEAR({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def date_month(self) -> "ColumnExpression":
        """Return a scoped typed-date month-extract expression for predicates."""

        return ColumnExpression(f"DATE_MONTH({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def date_day(self) -> "ColumnExpression":
        """Return a scoped typed-date day-of-month extract expression for predicates."""

        return ColumnExpression(f"DATE_DAY({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_year(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp year-extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_YEAR({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_month(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp month-extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_MONTH({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_day(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp day-of-month extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_DAY({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_hour(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp hour extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_HOUR({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_minute(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp minute extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_MINUTE({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def extract_minute(self) -> "ColumnExpression":
        """Extract a minute from integer epoch seconds or timestamp text.

        TimestampMicros values should use :meth:`timestamp_minute`.
        """

        return ColumnExpression(f"EXTRACT_MINUTE({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def date_trunc_minute(self) -> "ColumnExpression":
        """Truncate integer epoch seconds or timestamp text to the minute."""

        return ColumnExpression(f"DATE_TRUNC_MINUTE({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def timestamp_second(self) -> "ColumnExpression":
        """Return a scoped typed-timestamp second extract expression for predicates."""

        return ColumnExpression(f"TIMESTAMP_SECOND({self.sql})", self.source_bindings, resource_limits=self.resource_limits)

    def temporal_extract(self, part: str) -> "ColumnExpression":
        """Declare extraction of a literal calendar part; native admission validates support."""

        return self._temporal_call("temporal_extract", _temporal_control_literal("part", part))

    def temporal_trunc(self, part: str, *, ambiguity: str = "reject") -> "ColumnExpression":
        """Declare truncation to a calendar part with an explicit ambiguity policy."""

        return self._temporal_call(
            "temporal_trunc", _temporal_control_literal("part", part),
            _temporal_control_literal("ambiguity", ambiguity),
        )

    def temporal_bucket(
        self, every: int, unit: str, *, origin: int | None = None,
        month_end: str = "reject", ambiguity: str = "reject",
    ) -> "ColumnExpression":
        """Declare an anchored bucket; all calendar and tick arithmetic is native.

        Units are s, ms, us, ns, calendar_days or calendar_months. Origin is
        exact source-domain ticks; None uses epoch zero for elapsed grids or
        local 1970-01-01 midnight for calendar grids. Calendar policies reject
        ambiguous times and missing month-end dates unless explicitly changed.
        """
        from ._query_expression_methods import ColumnExpression_temporal_bucket as _impl
        return _impl(self, every, unit, origin=origin, month_end=month_end, ambiguity=ambiguity)

    def calendar_add_days(self, days: object, *, ambiguity: str = "reject") -> "ColumnExpression":
        """Declare local calendar-day arithmetic with explicit ambiguity policy."""

        return self._temporal_call(
            "calendar_add_days", _temporal_value_sql(days),
            _temporal_control_literal("ambiguity", ambiguity), sources=(days,),
        )

    def calendar_add_months(
        self, months: object, *, month_end: str = "reject", ambiguity: str = "reject"
    ) -> "ColumnExpression":
        """Declare calendar-month arithmetic with explicit month-end and ambiguity policies."""

        return self._temporal_call(
            "calendar_add_months", _temporal_value_sql(months),
            _temporal_control_literal("month_end", month_end),
            _temporal_control_literal("ambiguity", ambiguity), sources=(months,),
        )

    def temporal_add_duration(self, other: object) -> "ColumnExpression":
        """Declare typed temporal addition by a fixed duration."""

        return self._temporal_call("temporal_add_duration", _temporal_value_sql(other), sources=(other,))

    def temporal_sub_duration(self, other: object) -> "ColumnExpression":
        """Declare typed temporal subtraction by a fixed duration."""

        return self._temporal_call("temporal_sub_duration", _temporal_value_sql(other), sources=(other,))

    def timestamp_difference(self, other: object) -> "ColumnExpression":
        """Declare a typed difference between timestamps."""

        return self._temporal_call("timestamp_difference", _temporal_value_sql(other), sources=(other,))

    def timestamp_assign_timezone(
        self, timezone: str, *, ambiguity: str = "reject"
    ) -> "ColumnExpression":
        """Declare timezone assignment with explicit ambiguity policy."""

        return self._temporal_call(
            "timestamp_assign_timezone", _temporal_control_literal("timezone", timezone),
            _temporal_control_literal("ambiguity", ambiguity),
        )

    def timestamp_convert_timezone(self, timezone: str) -> "ColumnExpression":
        """Change a zoned instant's presentation zone while preserving its ticks."""

        return self._temporal_call(
            "timestamp_convert_timezone", _temporal_control_literal("timezone", timezone)
        )

    def timestamp_remove_timezone(self) -> "ColumnExpression":
        """Remove a timestamp's zone while preserving its local civil fields."""

        return self._temporal_call("timestamp_remove_timezone")

    def _temporal_call(
        self, name: str, *arguments: str, sources: tuple[object, ...] = ()
    ) -> "ColumnExpression":
        sql = f"{name.upper()}({', '.join((self.sql, *arguments))})"
        values = (self, *sources)
        return ColumnExpression(sql, _predicate_sources(*values), resource_limits=_predicate_limits(*values))


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
class Result:
    """Completed data and its existing native owner, or a completed write receipt.

    Collection does not export Python rows. ``to_rows`` and the optional
    integration conversions are explicit allocation boundaries and never rerun
    the query. Process results retain their delivered typed payload instead.
    """

    workflow: "LazyFrame"
    operation: str
    envelope: OutputEnvelope

    @property
    def schema(self) -> tuple[tuple[str, ResultType], ...]:
        """Ordered exact result types; available for empty collected results too."""
        return self.result_schema

    @property
    def details(self) -> OutputEnvelope:
        """Typed execution details, validation reports and diagnostics from this call."""
        return self.envelope

    def to_rows(self) -> tuple[Mapping[str, Any], ...]:
        """Explicitly allocate exact logical Python values for the complete result.

        Python objects occupy caller memory outside the engine's retained
        allocation. Use ``iter_batches`` on a plan for incremental delivery.
        """
        return self.python_objects

    @property
    def native_result(self) -> object | None:
        """Return the retained in-process owner, when this execution has one."""

        return self.envelope._native_result

    @property
    def result_jsonl(self) -> str:
        """Return complete bounded rows when this execution includes a row payload."""

        if self.native_result is not None or self.envelope.field("result_values_json") is not None:
            return "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in self.result_rows)
        return _required_field(self.envelope, "result_jsonl", allow_empty=True)

    @property
    def result_rows(self) -> tuple[Mapping[str, Any], ...]:
        """Decode this execution's rows without reading or executing its source again."""
        from ._query_result_methods import Result_result_rows as _impl
        return _impl(self)

    @property
    def result_schema(self) -> tuple[tuple[str, ResultType], ...]:
        """Return the ordered schema delivered by this native execution."""

        if self.native_result is not None:
            return schema_fields(self.native_result.schema_json, "vortex.dtype.serde.v1")
        return schema_fields(_required_field(self.envelope, "result_schema_json"),
                             _required_field(self.envelope, "result_schema_format"))

    @property
    def result_columns(self) -> tuple[str, ...]:
        """Return actual output names even when no rows were produced."""

        return tuple(name for name, _ in self.result_schema)

    @property
    def python_objects(self) -> tuple[Mapping[str, Any], ...]:
        """Restore logical binary, decimal and temporal values at this boundary."""

        if self.native_result is not None:
            return tuple(self.native_result.row_export()[0])
        return tuple(python_rows(self.result_rows, self.result_schema))

    @property
    def observations(self) -> Mapping[str, Mapping[str, Any]]:
        """Complete typed metrics from this action, without another input pass.

        Failed or cancelled actions expose no complete observations. A write
        exposes them only after publication; use its receipt for output status.
        """
        from ._query_result_methods import Result_observations as _impl
        return _impl(self)

    def to_arrow(self):
        """Export completed data without rerunning its query."""

        import pyarrow
        return _result_to_arrow_table(self, pyarrow)

    def to_pandas(self):
        """Construct the existing exact object-column pandas representation."""

        import pandas
        return _result_to_pandas(self, pandas)

    def to_numpy(self):
        """Construct the existing exact two-dimensional object representation."""

        import numpy
        return _result_to_numpy(self, numpy)

    def __arrow_c_stream__(self, requested_schema=None):
        if self.native_result is not None:
            return self.native_result.__arrow_c_stream__(requested_schema)
        return self.to_arrow().__arrow_c_stream__(requested_schema)

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
        from ._query_result_methods import Result_result_known as _impl
        return _impl(self)

    @property
    def rows_scanned(self) -> int | None:
        """Return the reported local Vortex rows scanned, when present."""
        from ._query_result_methods import Result_rows_scanned as _impl
        return _impl(self)

    @property
    def rows_selected(self) -> int | None:
        """Return the reported selected row count, when present."""
        from ._query_result_methods import Result_rows_selected as _impl
        return _impl(self)

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
        from ._query_result_methods import Result_projected_columns as _impl
        return _impl(self)

    @property
    def fallback_attempted(self) -> bool:
        """Whether this primitive path attempted fallback execution."""
        from ._query_result_methods import Result_fallback_attempted as _impl
        return _impl(self)

    @property
    def external_engine_invoked(self) -> bool:
        """Whether this primitive path invoked an external execution engine."""

        return self.envelope.field_bool("external_engine_invoked", False) is True

    @property
    def runtime_execution(self) -> bool:
        """Whether the report represents actual local Vortex runtime execution."""
        from ._query_result_methods import Result_runtime_execution as _impl
        return _impl(self)

    @property
    def data_read(self) -> bool:
        """Whether the primitive read Vortex data."""
        from ._query_result_methods import Result_data_read as _impl
        return _impl(self)

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
        from ._query_result_methods import Result_claim_gate_status as _impl
        return _impl(self)

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
        """Whether this execution used the source-preparation path.

        Inspect the preparation details to distinguish newly loaded data from
        reused data; this flag alone does not establish a fresh import.
        """

        return self.envelope.field_bool(
            "public_workflow_preparation_vortex_ingest_performed", False
        ) is True

    @property
    def prepared_vortex_path(self) -> str | None:
        """Return the Vortex file path for prepared source data, when present."""

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












# Compatibility name: retained until an announced breaking release.
VortexWorkflowExecutionReport = Result


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
        from ._query_composition_methods import RollingFrame__aggregate as _impl
        return _impl(self, aggregate, column, alias=alias, check=check, **kwargs)

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
    from ._query_reshape_arguments import _vortex_expression_project_columns_from_payload as _impl
    return _impl(payload)


def _vortex_melt_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    from ._query_reshape_arguments import _vortex_melt_projection_columns_from_payload as _impl
    return _impl(payload)


def _vortex_explode_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    from ._query_reshape_arguments import _vortex_explode_projection_columns_from_payload as _impl
    return _impl(payload)


def _vortex_explode_projection_payload(columns: Sequence[str]) -> str | None:
    from ._query_reshape_arguments import _vortex_explode_projection_payload as _impl
    return _impl(columns)


def _split_dotted_field_accessor(value: str) -> tuple[str, str] | None:
    parts = value.split(".")
    if len(parts) != 2:
        return None
    source_column, field = (part.strip() for part in parts)
    if not _is_sql_identifier(source_column) or not _is_sql_identifier(field):
        return None
    return source_column, field


def _vortex_pivot_projection_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    from ._query_reshape_arguments import _vortex_pivot_projection_columns_from_payload as _impl
    return _impl(payload)


def _vortex_pivot_projection_payload(
    *,
    index: object | None,
    columns: object | None,
    values: object | None,
    aggregate: str,
    kwargs: Mapping[str, object],
) -> str | None:
    from ._query_reshape_arguments import _vortex_pivot_projection_payload as _impl
    return _impl(index=index, columns=columns, values=values, aggregate=aggregate, kwargs=kwargs)


def _normalize_pivot_policy_kwargs(kwargs: Mapping[str, object]) -> dict[str, object] | None:
    from ._query_reshape_arguments import _normalize_pivot_policy_kwargs as _impl
    return _impl(kwargs)


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
    from ._query_reshape_arguments import _normalize_pivot_table_single_aggregate as _impl
    return _impl(aggfunc, values=values)


def _json_column_tuple(value: object) -> tuple[str, ...] | None:
    from ._query_reshape_arguments import _json_column_tuple as _impl
    return _impl(value)


def _vortex_melt_projection_payload(
    *,
    id_vars: object | None,
    value_vars: object | None,
    var_name: object | None,
    value_name: object | None,
    kwargs: Mapping[str, object],
) -> str | None:
    from ._query_reshape_arguments import _vortex_melt_projection_payload as _impl
    return _impl(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name, kwargs=kwargs)


def _vortex_rolling_window_columns_from_payload(payload: str) -> tuple[str, ...] | None:
    from ._query_reshape_arguments import _vortex_rolling_window_columns_from_payload as _impl
    return _impl(payload)


def _vortex_rolling_window_payload(
    column: object,
    *,
    output_column: object | None,
    window_size: int,
    min_periods: int,
    aggregate: str,
    center: bool = False,
) -> str | None:
    from ._query_reshape_arguments import _vortex_rolling_window_payload as _impl
    return _impl(column, output_column=output_column, window_size=window_size, min_periods=min_periods, aggregate=aggregate, center=center)


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
        from ._query_report_views import WorkflowNotebookPreview_to_html as _impl
        return _impl(self)

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
    resources: ExecutionResources | None = None
    resource_limits: ExecutionResourceLimits | None = None
    _native_plan: NativePlanState = field(default_factory=NativePlanState, init=False, repr=False, compare=False)

    @classmethod
    def from_sql(
        cls, statement: str, client: ShardLoomClient, input_uri: str | None = None,
        input_format: str | None = None, source_bindings: tuple[WorkflowSource, ...] = (),
        *, relations: Mapping[str, "LazyFrame"] | None = None,
        parameters: Mapping[str, object] | None = None,
        resources: ExecutionResources | None = None,
        resource_limits: ExecutionResourceLimits | None = None,
    ) -> "LazyFrame":
        """Declare SQL using the same lazy relation and source owners as readers."""
        from ._query_declarations import LazyFrame_from_sql as _impl
        return _impl(cls, statement, client, input_uri, input_format, source_bindings, relations=relations, parameters=parameters, resources=resources, resource_limits=resource_limits)

    @property
    def statement(self) -> str:
        """Return the SQL description; bound values and relations remain separate."""
        statement = self._relation_statement()
        if statement is None:
            raise ValueError("this declaration has no SQL description")
        return statement

    def _declared_input_kwargs(self) -> dict[str, Any]:
        from ._query_declarations import LazyFrame__declared_input_kwargs as _impl
        return _impl(self)

    def _has_declared_limit(self) -> bool:
        if self.source.relation is None:
            return _workflow_has_limit(self.operations)
        statement = self._relation_statement()
        return statement is not None and _find_top_level_sql_keyword_outside_quotes(statement, "limit") is not None

    @property
    def _surface(self) -> str:
        return "sql" if self.source.relation is not None else "dataframe"

    def __post_init__(self) -> None:
        from ._query_declarations import LazyFrame___post_init__ as _impl
        return _impl(self)

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
        def describe():
            parts = [self.source.to_summary()]
            parts.extend(operation.to_summary() for operation in self.operations)
            return " -> ".join(parts)
        return self._native_plan.describe("summary", describe) if type(self) is LazyFrame else describe()

    def with_engine(self, engine_mode: str) -> "LazyFrame":
        """Return this lazy workflow with a different requested engine mode."""

        return LazyFrame(
            source=self.source,
            client=self.client,
            operations=self.operations,
            engine_mode=_normalize_engine_mode(engine_mode),
            resources=self.resources, resource_limits=self.resource_limits,
        )

    def filter(self, predicate: object) -> "LazyFrame":
        """Return a lazy plan with an added filter predicate."""

        value = _normalize_raw_or_typed_predicate("filter predicate", predicate)
        if self._can_append_having():
            return self._append(WorkflowOperation("having", (value,), _predicate_sources(predicate), resource_limits=_predicate_limits(predicate)))
        return self._append(WorkflowOperation("filter", (value,), _predicate_sources(predicate), resource_limits=_predicate_limits(predicate)))

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
            return self._append(WorkflowOperation("having", (value,), _predicate_sources(predicate), resource_limits=_predicate_limits(predicate)))
        raise ValueError("HAVING must follow the aggregate stage; use filter() after ordering, limit or set operations")

    def select(self, *columns: object) -> "LazyFrame":
        """Return a lazy plan with an added projection."""

        return self._append(WorkflowOperation(
            "select", _normalize_columns(columns), _predicate_sources(columns),
            resource_limits=_predicate_limits(columns),
        ))

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
        from ._query_transform_methods import LazyFrame_dropna as _impl
        return _impl(self, subset=subset, how=how, axis=axis, thresh=thresh, check=check, **kwargs)

    def astype(
        self,
        dtype: object,
        *,
        errors: str = "raise",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped schema-declared cast projection when admitted."""
        from ._query_transform_methods import LazyFrame_astype as _impl
        return _impl(self, dtype, errors=errors, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_sample as _impl
        return _impl(self, n, fraction, seed, frac=frac, random_state=random_state, weights=weights, replace=replace, check=check)

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
        from ._query_transform_methods import LazyFrame_merge as _impl
        return _impl(self, other, on=on, left_on=left_on, right_on=right_on, how=how, check=check, **kwargs)

    def concat(
        self,
        others: "LazyFrame | str | Sequence[LazyFrame | str]",
        *,
        axis: int = 0,
        join: str = "outer",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped row-wise concat over matching projected local-source branches."""
        from ._query_transform_methods import LazyFrame_concat as _impl
        return _impl(self, others, axis=axis, join=join, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_pivot as _impl
        return _impl(self, index=index, columns=columns, values=values, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_pivot_table as _impl
        return _impl(self, values=values, index=index, columns=columns, aggfunc=aggfunc, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_melt as _impl
        return _impl(self, id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name, ignore_index=ignore_index, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_rolling as _impl
        return _impl(self, window, min_periods=min_periods, center=center, check=check, **kwargs)

    def duplicated(
        self,
        subset: object | None = None,
        *,
        keep: str | bool = "first",
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped row-duplicate mask for admitted Vortex-backed rows."""
        from ._query_transform_methods import LazyFrame_duplicated as _impl
        return _impl(self, subset, keep=keep, check=check, **kwargs)

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
        WorkflowProfileReport | UnsupportedWorkflowOperationReport
    ):
        """Return a metadata-first profile for admitted local/Vortex columns."""
        from ._query_report_views import LazyFrame_describe as _impl
        return _impl(self, *columns, check=check, **kwargs)

    def nunique(
        self,
        *columns: object,
        dropna: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped one-column count-distinct aggregate when admitted."""
        from ._query_transform_methods import LazyFrame_nunique as _impl
        return _impl(self, *columns, dropna=dropna, check=check, **kwargs)

    def value_counts(
        self,
        *columns: object,
        sort: bool = True,
        dropna: bool = True,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped grouped `count(*)` workflow when admitted."""
        from ._query_transform_methods import LazyFrame_value_counts as _impl
        return _impl(self, *columns, sort=sort, dropna=dropna, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame__top_n_by_columns as _impl
        return _impl(self, operation, n, columns, descending=descending, keep=keep, check=check)

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
        from ._query_transform_methods import LazyFrame_fillna as _impl
        return _impl(self, value, axis=axis, inplace=inplace, method=method, limit=limit, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_fill_null as _impl
        return _impl(self, value, axis=axis, inplace=inplace, method=method, limit=limit, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_mask as _impl
        return _impl(self, cond, other, axis=axis, inplace=inplace, level=level, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_replace as _impl
        return _impl(self, to_replace, value, regex=regex, inplace=inplace, method=method, limit=limit, check=check, **kwargs)

    def apply(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped ShardLoom plan-transform apply when admitted."""
        from ._query_transform_methods import LazyFrame_apply as _impl
        return _impl(self, function, *args, check=check, **kwargs)

    def pipe(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped ShardLoom plan-transform pipe when admitted."""
        from ._query_transform_methods import LazyFrame_pipe as _impl
        return _impl(self, function, *args, check=check, **kwargs)

    def transform(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Apply an explicit plan transform or native column-expression mapping."""
        from ._query_transform_methods import LazyFrame_transform as _impl
        return _impl(self, function, *args, check=check, **kwargs)

    def applymap(
        self,
        function: object,
        *args: object,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return scoped column transforms while blocking Python cell callables."""
        from ._query_transform_methods import LazyFrame_applymap as _impl
        return _impl(self, function, *args, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_map_rows as _impl
        return _impl(self, function, *args, check=check, **kwargs)

    def eval(
        self,
        expr: object,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped expression-project assignment when admitted."""
        from ._query_transform_methods import LazyFrame_eval as _impl
        return _impl(self, expr, check=check, **kwargs)

    def distinct(self) -> "LazyFrame":
        """Return a lazy plan with row-level duplicate removal."""

        return self._append(WorkflowOperation("distinct", ()))

    def union(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION` workflow over two local-source plans."""

        return self._union(other, union_all=False, check=check)

    def union_all(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `UNION ALL` workflow over two local-source plans."""

        return self._union(other, union_all=True, check=check)

    def intersect(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped SQL `EXCEPT` workflow over two local-source plans."""

        return self.except_rows(other, check=check)

    def except_rows(
        self,
        other: "LazyFrame",
        *,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
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
        from ._query_transform_methods import LazyFrame_drop_duplicates as _impl
        return _impl(self, subset, keep=keep, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_set_index as _impl
        return _impl(self, keys, drop=drop, check=check, **kwargs)

    def reset_index(
        self,
        *,
        check: bool = False,
        **kwargs: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Materialize an explicit source-order row number unless `drop=True` is requested."""
        from ._query_transform_methods import LazyFrame_reset_index as _impl
        return _impl(self, check=check, **kwargs)

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
        from ._query_transform_methods import LazyFrame_with_column as _impl
        return _impl(self, name, expression, check=check)

    def _computed_projection_operation(
        self, payload: str, name: str, expression: str,
        *, source_bindings: tuple[WorkflowSource, ...] = (),
        resource_limits: ExecutionResourceLimits | None = None,
    ) -> WorkflowOperation:
        from ._query_transform_methods import LazyFrame__computed_projection_operation as _impl
        return _impl(self, payload, name, expression, source_bindings=source_bindings, resource_limits=resource_limits)

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
        from ._query_transform_methods import LazyFrame_with_columns as _impl
        return _impl(self, columns, check=check, **named_expressions)

    def assign(
        self,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Alias for `with_columns(...)` using pandas-style naming."""

        return self.with_columns(**named_expressions)

    def limit(self, count: int, *, offset: int = 0) -> "LazyFrame":
        """Return a lazy plan with an added count and optional row offset."""

        count, offset = _normalize_limit_range(count, offset)
        values = (str(count),) if offset == 0 else (str(count), str(offset))
        return self._append(WorkflowOperation("limit", values))


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
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = False,
    ) -> PublicWorkflowRoute:
        """Return the shared public route envelope for this lazy workflow."""
        from ._query_action_methods import LazyFrame_route as _impl
        return _impl(self, requested_output=requested_output, output_ref=output_ref, execution_policy=execution_policy, materialization_policy=materialization_policy, evidence_level=evidence_level, bounded=bounded, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def run(
        self,
        *,
        requested_output: str = "collect",
        output_ref: str | os.PathLike[str] | None = None,
        execution_policy: str | None = None,
        materialization_policy: str = "bounded",
        evidence_level: str = "runtime_smoke",
        bounded: bool | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Execute this lazy workflow with the ShardLoom engine."""
        from ._query_action_methods import LazyFrame_run as _impl
        return _impl(self, requested_output=requested_output, output_ref=output_ref, execution_policy=execution_policy, materialization_policy=materialization_policy, evidence_level=evidence_level, bounded=bounded, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def prepare(
        self,
        target_vortex_path: str | os.PathLike[str],
        *,
        evidence_level: str = "production_admitted_local_workflow",
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
        check: bool = True,
    ) -> PublicWorkflowExecution:
        """Load or reuse this source in a Vortex file without executing the query."""
        from ._query_action_methods import LazyFrame_prepare as _impl
        return _impl(self, target_vortex_path, evidence_level=evidence_level, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources, check=check)

    def profile(
        self,
        limit: int = 100,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> (
        WorkflowProfileReport
        | Result
        | UnsupportedWorkflowOperationReport
    ):
        """Execute one bounded sample and inspect its values/schema in Python.

        This is a data-consuming diagnostic, not a metadata-only explain call.
        """
        from ._query_report_views import LazyFrame_profile as _impl
        return _impl(self, limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def collect(
        self,
        *,
        limit: int | None = None,
        reuse: bool = True,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
    ) -> Result:
        """Collect the complete bounded result through the shared native engine."""
        if limit is not None:
            return self.limit(limit).collect(check=check, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources,
                                            max_parallelism=max_parallelism, spill=spill)
        statement = self._relation_statement()
        if statement is None:
            resolve_resources(memory_gb=memory_gb, memory_bytes=memory_bytes,
                              max_parallelism=max_parallelism, resources=resources,
                              inherited=self.resources, limits=self.resource_limits)
            report = self._unsupported_operation("collect", check=check)
            return Result(self, "collect", report.envelope)
        def execute():
            envelope = _collect_native_relational(
                self.client, statement, surface=self._surface, plan_summary=self.operation_summary,
                input_kwargs=self._declared_input_kwargs(),
                check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill,
                input_batches=_workflow_batch_inputs(self._declared_sources()),
                inherited=self.resources, limits=self.resource_limits,
                native_plan=self._native_plan,
                native_frame=self,
            )
            return Result(self, "collect", envelope)
        return _frame_action(self, "collect", execute, reuse=reuse)

    def iter_batches(
        self, *, batch_rows: int = 2048,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        output_window_bytes: int = 0,
    ):
        """Consume native batches incrementally; use a with block for early close.

        Opt into native two-batch delivery with ``output_window_bytes=4194304``;
        zero keeps ordinary demand. Other plans keep ordinary delivery when the
        native memory scan/projection policy does not admit a window.

        The final iterator report establishes completion. Retained native batch
        buffers remain charged to the original grant until their last owner is
        released; separate Python and consumer allocations use caller memory.
        Supported local files use the shared direct readers for batch consumption.
        """
        from ._query_action_methods import LazyFrame_iter_batches as _impl
        return _impl(self, batch_rows=batch_rows, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, output_window_bytes=output_window_bytes)

    def count(
        self,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
    ) -> (
        Result | UnsupportedWorkflowOperationReport
    ):
        """Declare COUNT over this complete input; native planning selects its strategy."""
        if self._relation_statement() is None:
            return self._unsupported_operation("count", check=check)
        return self._append(WorkflowOperation("aggregate", ("COUNT(*) AS count",))).collect(
            check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill)

    def write(
        self,
        target_uri: str | os.PathLike[str],
        *,
        output_format: str = "jsonl",
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        Result | UnsupportedWorkflowOperationReport
    ):
        """Write this complete native declaration through the requested output adapter."""
        normalized_output_format = _normalize_local_output_format(output_format)
        requested_output = _public_write_request_for_format(normalized_output_format)
        if self._relation_statement() is None:
            return self._unsupported_operation(requested_output, str(target_uri), check=check)
        return self._public_workflow_write_report(
            target_uri, requested_output=requested_output, allow_overwrite=allow_overwrite, reuse=reuse,
            memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check,
        )

    def write_jsonl(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="jsonl")`."""
        from ._query_action_methods import LazyFrame_write_jsonl as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_json(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="json")` (one JSON array)."""
        from ._query_action_methods import LazyFrame_write_json as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_csv(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="csv")`."""
        from ._query_action_methods import LazyFrame_write_csv as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_parquet(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="parquet")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Parquet sink blocker.
        """
        from ._query_action_methods import LazyFrame_write_parquet as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_arrow_ipc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="arrow-ipc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Arrow IPC sink blocker.
        """
        from ._query_action_methods import LazyFrame_write_arrow_ipc as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_avro(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="avro")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic Avro sink blocker.
        """
        from ._query_action_methods import LazyFrame_write_avro as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def write_orc(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Alias for `write(..., output_format="orc")`.

        The CLI must be built with `--features universal-format-io`; default
        binaries return ShardLoom's deterministic ORC sink blocker.
        """
        from ._query_action_methods import LazyFrame_write_orc as _impl
        return _impl(self, target_uri, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def fanout(
        self,
        outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> (
        Result | UnsupportedWorkflowOperationReport
    ):
        """Write one declared workflow through the common multi-output owner."""
        from ._query_action_methods import LazyFrame_fanout as _impl
        return _impl(self, outputs, allow_overwrite=allow_overwrite, reuse=reuse, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)

    def to_pandas(
        self,
        *,
        limit: int | None = None,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a pandas DataFrame at an explicit bounded materialization boundary."""
        from ._query_action_methods import LazyFrame_to_pandas as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def to_arrow(
        self,
        *,
        limit: int | None = None,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table at an explicit bounded materialization boundary."""
        from ._query_action_methods import LazyFrame_to_arrow as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def to_arrow_table(
        self,
        *,
        limit: int | None = None,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a PyArrow table for admitted bounded local-source workflows."""
        from ._query_action_methods import LazyFrame_to_arrow_table as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def to_arrow_ipc(
        self,
        *,
        limit: int | None = None,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> bytes | UnsupportedWorkflowOperationReport:
        """Return Arrow IPC stream bytes for admitted bounded local-source workflows."""
        from ._query_action_methods import LazyFrame_to_arrow_ipc as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def to_numpy(
        self,
        *,
        limit: int | None = None,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> object | UnsupportedWorkflowOperationReport:
        """Return a NumPy array for admitted bounded local-source workflow rows."""
        from ._query_action_methods import LazyFrame_to_numpy as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def to_python_objects(
        self,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> tuple[Mapping[str, Any], ...] | UnsupportedWorkflowOperationReport:
        """Return bounded Python row objects for admitted local-source workflows."""
        from ._query_action_methods import LazyFrame_to_python_objects as _impl
        return _impl(self, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def prepare_vortex(
        self,
        target_vortex_path: str | os.PathLike[str] | None = None,
        *,
        workspace: str | os.PathLike[str] | None = None,
        input_format: str | None = None,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        allow_overwrite: bool = False,
        certification_level: str = "ingest_certified",
        check: bool = True,
    ) -> VortexIngestSmokeReport | Result | UnsupportedWorkflowOperationReport:
        """Prepare this raw local source into a caller-owned `VortexPreparedState`.

        When `workspace` is supplied without `target_vortex_path`, the target is derived as
        `<workspace>/<source-stem>.vortex`. The real CLI `vortex-prepare` route writes the selected
        single `.vortex` artifact and reports no-sidecar evidence; callers opt into replacing an
        existing artifact with ``allow_overwrite=True`` when the writer admits replacement.
        """
        from ._query_action_methods import LazyFrame_prepare_vortex as _impl
        return _impl(self, target_vortex_path, workspace=workspace, input_format=input_format, memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, allow_overwrite=allow_overwrite, certification_level=certification_level, check=check)

    def write_vortex(
        self,
        target_uri: str | os.PathLike[str],
        *,
        allow_overwrite: bool = False,
        reuse: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        check: bool = True,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Write native Vortex output through the common output adapter contract."""
        return self.write(target_uri, output_format="vortex", allow_overwrite=allow_overwrite, reuse=reuse,
                          memory_gb=memory_gb, memory_bytes=memory_bytes, resources=resources, max_parallelism=max_parallelism, spill=spill, check=check)


    def _public_workflow_write_report(
        self,
        target_uri: str | os.PathLike[str],
        *,
        requested_output: str,
        allow_overwrite: bool,
        check: bool,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        resources: ExecutionResources | None = None,
        max_parallelism: int | None = None,
        spill: Mapping[str, object] | str | None = None,
        fanout_outputs: Sequence[tuple[str, CommandPart]] | None = None,
        reuse: bool = True,
    ) -> Result:
        statement = self._relation_statement()
        if statement is None:
            raise ValueError(
                "public workflow write facade requires an admitted local-source statement"
            )
        def execute():
            execution = self.client.public_workflow_run(
                self._surface,
                **_native_plan_kwargs(self),
                input_batches=_workflow_batch_inputs(self._declared_sources()),
                **self._declared_input_kwargs(),
                source_schema=_prepare_vortex_schema_hints(self.source),
                sql_statement=statement,
                plan_summary=self.operation_summary,
                requested_output=requested_output,
                output_ref=target_uri,
                fanout_outputs=fanout_outputs,
                materialization_policy="bounded",
                evidence_level="production_admitted_local_workflow",
                bounded=True,
                allow_overwrite=allow_overwrite,
                **_terminal_resource_kwargs(memory_gb, max_parallelism, spill, inherited=self.resources, limits=self.resource_limits, memory_bytes=memory_bytes, resources=resources),
                check=check,
            )
            return Result(
                workflow=self, operation=requested_output, envelope=execution.envelope,
            )
        return _frame_action(self, "fanout" if fanout_outputs is not None else "write", execute, reuse=reuse)

    def sql(
        self,
        statement: str,
        *,
        name: str = "this",
        parameters: Mapping[str, object] | None = None,
        check: bool = False,
    ) -> "LazyFrame":
        """Query this lazy relation by name, without executing or copying it."""
        frame = LazyFrame.from_sql(statement, self.client, relations={name: self}, parameters=parameters,
            resources=self.resources, resource_limits=self.resource_limits)
        if self.source._session is not None:
            from .session import _session_frame
            frame = _session_frame(self.source._session, frame)
        return frame

    def join_asof(
        self,
        other: "LazyFrame",
        *,
        on: str | None = None,
        left_on: str | None = None,
        right_on: str | None = None,
        by: str | Sequence[str] | None = None,
        left_by: str | Sequence[str] | None = None,
        right_by: str | Sequence[str] | None = None,
        strategy: str = "backward",
        tolerance: int | None = None,
        allow_exact_matches: bool = True,
    ) -> "LazyFrame":
        """Match one right row per left row, preserving left order.

        Inputs may be unsorted. ``by`` keys use ordinary SQL NULL semantics.
        Tolerance is inclusive in ordering-key ticks (date32 days, date64
        milliseconds, otherwise the type's unit). Temporal metadata must agree.
        Backward chooses the last equal right key; forward the first. Nearest
        prefers backward on a distance tie. Output names use ``f.`` and ``d.``
        for the left and right fields, as in :meth:`join`.
        """
        if not isinstance(other, LazyFrame):
            raise TypeError("join_asof() requires another LazyFrame")
        if other.client is not self.client:
            raise ValueError("joined relation belongs to another context")
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("join_asof() accepts on= or left_on=/right_on=")
            left_on = right_on = on
        if left_on is None or right_on is None:
            raise ValueError("join_asof() requires on= or both left_on= and right_on=")
        left_key = _normalize_output_column_name(left_on)
        right_key = _normalize_output_column_name(right_on)
        if by is not None:
            if left_by is not None or right_by is not None:
                raise ValueError("join_asof() accepts by= or left_by=/right_by=")
            left_by = right_by = by
        def keys(value: str | Sequence[str] | None) -> tuple[str, ...]:
            return () if value is None else tuple(
                _normalize_output_column_name(name) for name in _normalize_columns((value,))
            )
        left_keys, right_keys = keys(left_by), keys(right_by)
        if len(left_keys) != len(right_keys):
            raise ValueError("join_asof() requires equally many left and right partition keys")
        if strategy not in {"backward", "forward", "nearest"}:
            raise ValueError("as-of strategy must be backward, forward or nearest")
        if type(allow_exact_matches) is not bool:
            raise TypeError("allow_exact_matches must be a bool")
        if tolerance is not None and (type(tolerance) is not int or not 0 <= tolerance <= (1 << 64) - 1):
            raise ValueError("as-of tolerance must be a nonnegative uint64 number of ordering-key ticks")
        if self._relation_statement() is None or other._relation_statement() is None:
            raise ValueError("join_asof() requires two admitted native relations")
        right_statement = (
            f"SELECT * FROM {other._bound_relation_source().uri}"
            if self.client.transport == "native" else other._relation_statement()
        )
        return self._append(WorkflowOperation(
            "join_asof",
            (other.source.uri, left_key, right_key, ",".join(left_keys), ",".join(right_keys),
             strategy, "" if tolerance is None else str(tolerance), "true" if allow_exact_matches else "false"),
            source_bindings=_predicate_sources(other), right_statement=right_statement,
            resource_limits=_predicate_limits(other), right_frame=other,
        ))

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
        from ._query_composition_methods import LazyFrame_join as _impl
        return _impl(self, other, on=on, condition=condition, how=how, check=check)

    def group_by(self, *columns: object) -> "GroupedLazyFrame":
        """Return a grouped lazy workflow handle for scoped aggregation."""

        return GroupedLazyFrame(
            workflow=self,
            columns=_normalize_columns(columns),
            source_bindings=_predicate_sources(columns),
            resource_limits=_predicate_limits(columns),
        )

    def resample(
        self,
        time: object,
        every: int,
        unit: str,
        *,
        by: str | Iterable[str] = (),
        origin: int | None = None,
        month_end: str = "reject",
        ambiguity: str = "reject",
    ) -> "GroupedLazyFrame":
        """Group rows by a declared temporal bucket and optional partition keys.

        Bucket calculation and aggregation are performed by the native temporal
        and grouping owners when the workflow is executed.
        """
        from ._query_composition_methods import LazyFrame_resample as _impl
        return _impl(self, time, every, unit, by=by, origin=origin, month_end=month_end, ambiguity=ambiguity)

    def gap_fill(
        self,
        time: object,
        every: int,
        unit: str,
        *,
        by: str | Iterable[str] = (),
        start: int | None = None,
        end: int | None = None,
        origin: int | None = None,
        month_end: str = "reject",
        ambiguity: str = "reject",
    ) -> "LazyFrame":
        """Declare native insertion of missing points on an aligned time grid.

        This records literal controls only; it does not inspect or load source
        rows. Bounds and origin are signed 64-bit ticks in the source domain.
        """
        from ._query_composition_methods import LazyFrame_gap_fill as _impl
        return _impl(self, time, every, unit, by=by, start=start, end=end, origin=origin, month_end=month_end, ambiguity=ambiguity)

    def groupby(self, *columns: object) -> "GroupedLazyFrame":
        """Alias for `group_by(...)` using pandas-style naming."""

        return self.group_by(*columns)

    def grouping_sets(self, *sets: object, distinct: bool = False) -> "GroupedLazyFrame":
        """Group over explicit key sets; an empty tuple/list denotes the total.

        Example: frame.grouping_sets(["region", "currency"], ["region"], []).agg(...).
        Duplicate sets are retained unless distinct=True.
        """
        return _grouping_frame(self, "GROUPING SETS", sets, distinct=distinct)

    def rollup(self, *columns: object) -> "GroupedLazyFrame":
        """Group over key prefixes; a tuple/list of keys is one rollup unit."""
        return _grouping_frame(self, "ROLLUP", columns)

    def cube(self, *columns: object) -> "GroupedLazyFrame":
        """Group over key subsets; expansion is admitted by the native memory owner."""
        return _grouping_frame(self, "CUBE", columns)

    def aggregate(
        self,
        *expressions: object,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scalar aggregate workflow when admitted, otherwise report unsupported."""
        from ._query_composition_methods import LazyFrame_aggregate as _impl
        return _impl(self, *expressions, check=check)

    def map_batches(self, function) -> "LazyFrame":
        """Apply a registered rowwise function at this exact plan position.

        Register with ``context.batch_functions.register``. The immutable handle
        captures typed, trusted code; no callback runs during plan construction.
        Functions receive a RecordBatch and cooperative invocation context, and
        must preserve row count/order. This requires the native transport.
        """
        from ._batch_function import BatchFunction
        if not isinstance(function, BatchFunction):
            raise TypeError("map_batches requires a registered BatchFunction handle")
        function._for_client(self.client)
        frame = self._append(WorkflowOperation("application_batch", (function.name, function.version), batch_function=function))
        self.client._native_backend_for().declaration(frame)
        return frame

    def observe(self, name: str, *expressions: object, **named_expressions: object) -> "LazyFrame":
        """Measure COUNT, SUM, MIN and MAX here during the eventual native action.

        The returned frame retains the same rows. Names must be unique across
        the action, and metrics become available on its completed Result.
        Scalar expressions inside aggregates use native type and null rules.
        """
        from ._query_composition_methods import LazyFrame_observe as _impl
        return _impl(self, name, *expressions, **named_expressions)

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scalar aggregate workflow for positional expressions when admitted."""
        from ._query_composition_methods import LazyFrame_agg as _impl
        return _impl(self, *expressions, check=check, **named_expressions)

    def sort(
        self,
        *columns: object,
        descending: bool = False,
        nulls: str | None = None,
        check: bool = False,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped sort workflow when admitted, otherwise report unsupported."""
        from ._query_composition_methods import LazyFrame_sort as _impl
        return _impl(self, *columns, descending=descending, nulls=nulls, check=check)

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
            return self._append(WorkflowOperation(
                "window", values, _predicate_sources(expressions),
                resource_limits=_predicate_limits(expressions),
            ))
        return self._unsupported_operation("window", target, check=check)

    def schema_contract(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Alias for exact bounded schema validation over admitted local-source workflows."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        return self.validate_schema(schema, check=check, resources=allocation)

    def schema(
        self,
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> tuple[tuple[str, ResultType], ...]:
        """Inspect typed native metadata without sampling rows or opening producers.

        Data-dependent schemas are available on the completed Result. Explicit
        sampled diagnostics remain available through describe_schema().
        """
        from ._query_report_views import LazyFrame_schema as _impl
        return _impl(self, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def describe_schema(
        self,
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        """Return detailed bounded schema evidence for admitted local-source workflows."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        return self._bounded_schema_report(check=check, resources=allocation)

    def validate_schema(
        self,
        schema: Mapping[str, object],
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowSchemaValidationReport | UnsupportedWorkflowOperationReport:
        """Validate an expected schema against admitted local-source rows."""
        from ._query_report_views import LazyFrame_validate_schema as _impl
        return _impl(self, schema, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def data_quality_check(
        self,
        *checks: object,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Run bounded data-quality checks for admitted local-source workflows."""
        from ._query_report_views import LazyFrame_data_quality_check as _impl
        return _impl(self, *checks, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def data_quality(
        self,
        *checks: object,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Alias for bounded data-quality checks."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        return self.data_quality_check(*checks, check=check, resources=allocation)

    def data_quality_summary(
        self,
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowDataQualityReport | UnsupportedWorkflowOperationReport:
        """Return bounded null-count and schema summary for admitted workflows."""
        from ._query_report_views import LazyFrame_data_quality_summary as _impl
        return _impl(self, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def quarantine(
        self,
        target_uri: str | os.PathLike[str] | None = None,
        *checks: object,
        output_format: str | None = None,
        limit: int = 100,
        allow_overwrite: bool = False,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowQuarantineReport | UnsupportedWorkflowOperationReport:
        """Return bounded quarantine evidence for admitted local-source workflows."""
        from ._query_report_views import LazyFrame_quarantine as _impl
        return _impl(self, target_uri, *checks, output_format=output_format, limit=limit, allow_overwrite=allow_overwrite, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def preview(
        self,
        limit: int = 20,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Return a bounded local preview when admitted, otherwise report unsupported."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        _validate_positive_row_count("preview limit", limit)
        return self.limit(limit).collect(check=check, resources=allocation)

    def head(
        self,
        limit: int = 20,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report using familiar DataFrame naming."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        _validate_positive_row_count("head limit", limit)
        return self.limit(limit).collect(check=check, resources=allocation)

    def take(
        self,
        count: int,
        *,
        check: bool = True,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> Result | UnsupportedWorkflowOperationReport:
        """Return a bounded preview report for the requested row count."""

        allocation = resolve_resources(
            memory_gb=memory_gb, memory_bytes=memory_bytes,
            max_parallelism=max_parallelism, resources=resources,
            inherited=self.resources, limits=self.resource_limits,
        )
        _validate_positive_row_count("take limit", count)
        return self.limit(count).collect(check=check, resources=allocation)

    def display(
        self,
        limit: int = 20,
        *,
        check: bool = False,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowNotebookPreview | UnsupportedWorkflowOperationReport:
        """Return a bounded notebook/display preview for admitted workflows."""
        from ._query_composition_methods import LazyFrame_display as _impl
        return _impl(self, limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def certify(self, *, check: bool = False) -> WorkflowCertificationReport:
        """Return validation reports for this workflow without executing it."""

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
            resources=self.resources, resource_limits=self.resource_limits,
        )

    def _with_rewritten_projection(self, projection: tuple[str, ...]) -> "LazyFrame":
        return self._append(WorkflowOperation("select", projection))

    def _with_filter_condition(self, predicate: str) -> "LazyFrame":
        # The predicate observes this stage's values and evaluation domain.
        # Rewriting an earlier filter can cross callbacks, projections or
        # observations, changing both results and which failures are observed.
        # Keep placement explicit and leave admitted rewrites to native planning.
        return self._append(WorkflowOperation("filter", (predicate,)))

    def _union(
        self,
        other: "LazyFrame",
        *,
        union_all: bool,
        check: bool,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
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
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        from ._query_composition_methods import LazyFrame__set_operation as _impl
        return _impl(self, other, operation=operation, keyword=keyword, check=check)

    def _unsupported_operation(
        self,
        operation: str,
        target_ref: str | None = None,
        *,
        check: bool,
    ) -> UnsupportedWorkflowOperationReport:
        from ._query_composition_methods import LazyFrame__unsupported_operation as _impl
        return _impl(self, operation, target_ref, check=check)

    def _native_vortex_blocker_operation(self, operation: str, target_ref: str | None) -> str:
        from ._query_composition_methods import LazyFrame__native_vortex_blocker_operation as _impl
        return _impl(self, operation, target_ref)



















    def _has_structured_binary_export_shape(self) -> bool:
        from ._query_legacy_lowering import LazyFrame__has_structured_binary_export_shape as _impl
        return _impl(self)

    def _can_append_scalar_aggregate(self) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_scalar_aggregate as _impl
        return _impl(self)

    def _can_append_group_by_aggregate(self, columns: tuple[str, ...]) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_group_by_aggregate as _impl
        return _impl(self, columns)

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
        from ._query_plan_rules import LazyFrame__explicit_set_operation_projection_columns as _impl
        return _impl(self)

    def _can_append_sort(self, columns: tuple[str, ...]) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_sort as _impl
        return _impl(self, columns)

    def _can_append_window(self, expressions: tuple[str, ...]) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_window as _impl
        return _impl(self, expressions)

    def _can_append_having(self) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_having as _impl
        return _impl(self)

    def _can_append_projection_column(self, column_name: str, *, allow_vortex: bool = False) -> bool:
        from ._query_plan_rules import LazyFrame__can_append_projection_column as _impl
        return _impl(self, column_name, allow_vortex=allow_vortex)

    def _can_lower_merge_to_join(
        self,
        other: "LazyFrame | str",
        *,
        on: object,
        how: str,
    ) -> bool:
        from ._query_plan_rules import LazyFrame__can_lower_merge_to_join as _impl
        return _impl(self, other, on=on, how=how)

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
        from ._query_legacy_lowering import LazyFrame__expression_project_projection_columns as _impl
        return _impl(self, required_columns)

    def _duplicate_mask_projection_columns(
        self,
        subset: object | None,
    ) -> tuple[str, ...] | None:
        from ._query_legacy_lowering import LazyFrame__duplicate_mask_projection_columns as _impl
        return _impl(self, subset)

    def _expression_project_payload(
        self,
        rewrite_specs: tuple[Mapping[str, object], ...],
        *,
        required_columns: tuple[str, ...],
    ) -> str | None:
        from ._query_legacy_lowering import LazyFrame__expression_project_payload as _impl
        return _impl(self, rewrite_specs, required_columns=required_columns)

    def _row_number_expression_project_payload(
        self,
        target_column: str,
    ) -> str | None:
        from ._query_legacy_lowering import LazyFrame__row_number_expression_project_payload as _impl
        return _impl(self, target_column)

    def _forward_fill_null_expression_project_payload(
        self,
        limit: int | None,
    ) -> str | None:
        from ._query_legacy_lowering import LazyFrame__forward_fill_null_expression_project_payload as _impl
        return _impl(self, limit)

    def _replace_rewrite_specs(
        self,
        to_replace: object,
        value: object,
    ) -> tuple[Mapping[str, object], ...] | None:
        from ._query_legacy_lowering import LazyFrame__replace_rewrite_specs as _impl
        return _impl(self, to_replace, value)

    def _regex_replace_rewrite_specs(
        self,
        to_replace: object,
        value: object,
    ) -> tuple[Mapping[str, object], ...] | None:
        from ._query_legacy_lowering import LazyFrame__regex_replace_rewrite_specs as _impl
        return _impl(self, to_replace, value)

    def _string_replace_expression_project_payload(
        self,
        column_name: str,
        expression_sql: str,
    ) -> str | None:
        from ._query_legacy_lowering import LazyFrame__string_replace_expression_project_payload as _impl
        return _impl(self, column_name, expression_sql)

    def _numeric_scalar_expression_project_payload(
        self,
        column_name: str,
        expression_sql: str,
    ) -> str | None:
        from ._query_legacy_lowering import LazyFrame__numeric_scalar_expression_project_payload as _impl
        return _impl(self, column_name, expression_sql)

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
        from ._query_report_views import LazyFrame__projection_columns_for_schema_or_explicit_selection as _impl
        return _impl(self, allowed_operations=allowed_operations)

    def _declared_projection_columns(
        self,
        *,
        allowed_operations: set[str] | None = None,
    ) -> tuple[str, ...] | None:
        from ._query_legacy_lowering import LazyFrame__declared_projection_columns as _impl
        return _impl(self, allowed_operations=allowed_operations)

    def _schema_declared_rename_projection(
        self,
        items: tuple[tuple[str, str], ...],
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_rename_projection as _impl
        return _impl(self, items)

    def _schema_declared_drop_projection(
        self,
        columns: tuple[str, ...],
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_drop_projection as _impl
        return _impl(self, columns)

    def _schema_declared_fillna_projection(
        self,
        value: object | None,
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_fillna_projection as _impl
        return _impl(self, value)

    def _schema_declared_null_mask_projection(
        self,
        columns: tuple[object, ...],
        *,
        is_not: bool,
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_null_mask_projection as _impl
        return _impl(self, columns, is_not=is_not)

    def _schema_declared_melt_value_columns(
        self,
        id_vars: object | None,
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_melt_value_columns as _impl
        return _impl(self, id_vars)

    def _schema_declared_dropna_predicate(
        self,
        subset: object | None,
        *,
        how: str,
        axis: object | None,
        thresh: int | None,
        kwargs: Mapping[str, object],
    ) -> str | None:
        from ._query_report_views import LazyFrame__schema_declared_dropna_predicate as _impl
        return _impl(self, subset, how=how, axis=axis, thresh=thresh, kwargs=kwargs)

    def _schema_declared_astype_projection(
        self,
        dtype: object,
        *,
        errors: str,
        kwargs: Mapping[str, object],
    ) -> tuple[str, ...] | None:
        from ._query_report_views import LazyFrame__schema_declared_astype_projection as _impl
        return _impl(self, dtype, errors=errors, kwargs=kwargs)

    def _bounded_schema_report(
        self, *, check: bool,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> WorkflowSchemaReport | UnsupportedWorkflowOperationReport:
        from ._query_report_views import LazyFrame__bounded_schema_report as _impl
        return _impl(self, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def _bounded_materialization_report(
        self,
        *,
        limit: int | None,
        check: bool,
        memory_gb: int | None = None,
        memory_bytes: int | None = None,
        max_parallelism: int | None = None,
        resources: ExecutionResources | None = None,
    ) -> Result | UnsupportedWorkflowOperationReport:
        from ._query_legacy_lowering import LazyFrame__bounded_materialization_report as _impl
        return _impl(self, limit=limit, check=check, memory_gb=memory_gb, memory_bytes=memory_bytes, max_parallelism=max_parallelism, resources=resources)

    def _quarantine_pushdown_statement(
        self,
        checks: tuple[_WorkflowDataQualityCheckSpec, ...],
        *,
        limit: int,
    ) -> str | None:
        from ._query_report_views import LazyFrame__quarantine_pushdown_statement as _impl
        return _impl(self, checks, limit=limit)

    def _append_group_by_aggregate(
        self,
        columns: tuple[str, ...],
        expressions: tuple[str, ...],
        *,
        source_bindings: tuple[WorkflowSource, ...] = (),
        resource_limits: ExecutionResourceLimits | None = None,
        grouping_clause: str | None = None,
    ) -> "LazyFrame":
        from ._query_plan_rules import LazyFrame__append_group_by_aggregate as _impl
        return _impl(self, columns, expressions, source_bindings=source_bindings, resource_limits=resource_limits, grouping_clause=grouping_clause)

    def _declared_sources(self) -> tuple[WorkflowSource, ...]:
        def describe():
            root = self.source.relation.source_bindings if self.source.relation is not None else (self.source,)
            return (*root, *(source for operation in self.operations for source in operation.source_bindings))
        return self._native_plan.describe("sources", describe) if type(self) is LazyFrame else describe()

    def _bound_relation_source(self) -> WorkflowSource:
        # The retained frame keeps this opaque, process-local identifier unique.
        # It is resolved as a relation by Rust and is never a filesystem path.
        return WorkflowSource("relation", f"__sl_expression_{id(self):x}", _bound_frame=self)

    def _native_relational_statement(self) -> str | None:
        """Submit every complete declaration to shared native admission."""
        return self._relation_statement()

    def _relation_statement(self) -> str | None:
        """Render this complete input, including its order and limits, without I/O."""
        from ._query_statement import LazyFrame__relation_statement as _impl
        if type(self) is LazyFrame:
            return self._native_plan.describe("statement", lambda: _impl(self))
        return _impl(self)


    def _sql_local_source_statement(
        self, *, default_limit: int | None = None, allow_native_source: bool = False,
        require_limit: bool = True,
    ) -> str | None:
        from ._query_statement import LazyFrame__sql_local_source_statement as _impl
        return _impl(self, default_limit=default_limit, allow_native_source=allow_native_source, require_limit=require_limit)

    def _sql_local_source_union_branch_statement(self) -> str | None:
        from ._query_statement import LazyFrame__sql_local_source_union_branch_statement as _impl
        return _impl(self)


@dataclass(frozen=True, slots=True)
class GroupedLazyFrame:
    """Grouped lazy workflow handle for scoped aggregation and blockers."""

    workflow: LazyFrame
    columns: tuple[str, ...]
    source_bindings: tuple[WorkflowSource, ...] = ()
    resource_limits: ExecutionResourceLimits | None = None
    grouping_clause: str | None = None

    @property
    def operation_summary(self) -> str:
        """Return the grouped workflow summary."""

        return f"{self.workflow.operation_summary} -> group_by({self.grouping_clause or ','.join(self.columns)})"

    def agg(
        self,
        *expressions: object,
        check: bool = False,
        **named_expressions: object,
    ) -> "LazyFrame | UnsupportedWorkflowOperationReport":
        """Return a scoped grouped aggregate workflow when admitted."""
        from ._query_composition_methods import GroupedLazyFrame_agg as _impl
        return _impl(self, *expressions, check=check, **named_expressions)

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


def _grouping_frame(frame: LazyFrame, kind: str, units: tuple[object, ...], *, distinct: bool = False) -> GroupedLazyFrame:
    from ._query_expression_builders import _grouping_frame as _impl
    return _impl(frame, kind, units, distinct=distinct)


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
    from ._query_expression_builders import typed_scalar_udf as _impl
    return _impl(udf_id, column)


def fixture_double_i64(column: object) -> ColumnExpression:
    """Return the admitted `sl_fixture_double_i64` typed scalar UDF expression."""

    return typed_scalar_udf("sl_fixture_double_i64", column)


def col(name: object) -> ColumnExpression:
    """Return a scoped column expression for local ShardLoom predicates."""

    return ColumnExpression(_normalize_expression_column(name))


def outer(column: object) -> ColumnExpression:
    """Return the reserved outer-row column expression for correlated source predicates."""

    return ColumnExpression(f"outer.{_normalize_output_column_name(column)}")


def scalar_subquery(source: LazyFrame) -> ColumnExpression:
    """Declare a scalar query expression without executing its source.

    A scalar subquery with zero rows yields NULL, one row yields its value, and
    multiple rows produce the native engine's cardinality error. Source binding
    and type admission are validated by the native engine.
    """

    if not isinstance(source, LazyFrame):
        raise TypeError("scalar_subquery requires a LazyFrame")
    statement = (f"SELECT * FROM {source._bound_relation_source().uri}"
                 if source.client.transport == "native" else source._relation_statement())
    if statement is None or not statement.strip():
        raise ValueError("scalar_subquery source has no native relational declaration")
    return ColumnExpression(f"({statement})", _predicate_sources(source), resource_limits=_predicate_limits(source))


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
    from ._query_expression_builders import row_in_source as _impl
    return _impl(columns, source, source_columns, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_expression_builders import row_not_in_source as _impl
    return _impl(columns, source, source_columns, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_expression_builders import any_source as _impl
    return _impl(column, comparison, source, source_column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_expression_builders import all_source as _impl
    return _impl(column, comparison, source, source_column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_expression_builders import exists_source as _impl
    return _impl(source, source_alias=source_alias, select=select, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_expression_builders import not_exists_source as _impl
    return _impl(source, source_alias=source_alias, select=select, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


def row_number(
    *,
    order_by: object = None,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "row_number",
) -> WindowExpression | ColumnExpression:
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
    order_by: object = None,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "rank",
) -> WindowExpression | ColumnExpression:
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
    order_by: object = None,
    partition_by: object | None = None,
    descending: bool = False,
    alias: object = "dense_rank",
) -> WindowExpression | ColumnExpression:
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
) -> WindowExpression | ColumnExpression:
    """Return a scoped ranking window expression."""
    from ._query_expression_builders import _ranking_window_expression as _impl
    return _impl(function_name, order_by=order_by, partition_by=partition_by, descending=descending, alias=alias)


def case_when(predicate: object, then_value: object, else_value: object) -> ColumnExpression:
    """Return a scoped single-branch `CASE WHEN` computed-column expression."""

    then_branch = _sql_case_branch(then_value)
    else_branch = _sql_case_branch(else_value)
    return ColumnExpression(
        f"CASE WHEN {_predicate_sql(predicate)} THEN {then_branch} ELSE {else_branch} END",
        _predicate_sources(predicate, then_value, else_value),
        resource_limits=_predicate_limits(predicate, then_value, else_value),
    )


def count_distinct(column_expression: object) -> ColumnExpression:
    """Declare exact COUNT DISTINCT through the shared native aggregate binder."""

    if isinstance(column_expression, ColumnExpression):
        column_sql = column_expression.sql
    else:
        column_sql = _normalize_expression_column(column_expression)
    sql = f"count(DISTINCT {column_sql})"
    sources = _predicate_sources(column_expression)
    limits = _predicate_limits(column_expression)
    return ColumnExpression(sql, sources, limits)


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
    from ._query_expression_builders import concat as _impl
    return _impl(*parts)


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
    from ._query_expression_builders import struct as _impl
    return _impl(*columns)


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
    from ._query_expression_builders import _source_kind_from_path as _impl
    return _impl(uri)


def _source_suffix_for_local_ref(uri: str | os.PathLike[str]) -> str:
    """Return the local source suffix, stripping only admitted text compression."""
    from ._query_sources import _source_suffix_for_local_ref as _impl
    return _impl(uri)


def read(
    uri: str | os.PathLike[str],
    *,
    input_format: str | None = None,
    schema: Mapping[str, object] | None = None,
    snapshot_id: int | None = None,
    version: int | None = None,
    instant: str | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy local source, with an explicit format for folders or manifests.

    Omit ``input_format`` to infer the adapter from a file extension. Use
    ``input_format="jsonl"`` for JSON Lines collections whose path does not
    declare record framing. Local table sources require an explicit format:
    ``iceberg`` binds a metadata JSON file with optional ``snapshot_id``;
    ``delta`` binds a table root with optional ``version``; ``hudi`` binds a
    table root with optional completed write ``instant``. Without a selector,
    query admission freezes the current table state. Source declaration does
    not read data.
    """
    from ._query_sources import read as _impl
    return _impl(uri, input_format=input_format, schema=schema, snapshot_id=snapshot_id, version=version, instant=instant, client=client, engine_mode=engine_mode, **client_config)


def read_csv(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy CSV source; no source data is loaded by this call."""

    return _read_source(
        "csv",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def read_tsv(
    uri: str | os.PathLike[str],
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Declare a lazy quoted TAB-delimited source; no source data is loaded by this call."""

    return _read_source(
        "tsv",
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
    """Declare a lazy flat JSON, JSONL, or NDJSON source without loading data."""

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
    """Declare a lazy Parquet source without loading data.

    On execution, supported local workflows read the original file through
    ShardLoom's shared native source owner. A build with `universal-format-io` is required;
    other builds report that the Parquet reader is unavailable.
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
    """Declare a lazy local Arrow IPC file or stream source without loading data.

    On execution, supported local workflows read the original file through
    ShardLoom's shared native source owner. A build with `universal-format-io` is required;
    other builds report that the Arrow IPC reader is unavailable. This reader
    accepts local files, not in-memory Arrow tables.
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
    """Declare a lazy Avro source without loading data.

    On execution, supported local workflows read the original file through
    ShardLoom's shared native source owner. A build with `universal-format-io` is required;
    other builds report that the Avro reader is unavailable. The embedded schema
    must be representable; general Avro schema evolution is unsupported.
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
    """Declare a lazy ORC source without loading data.

    On execution, supported local workflows read the original file through
    ShardLoom's shared native source owner. A build with `universal-format-io` is required;
    other builds report that the ORC reader is unavailable. The embedded schema
    and compression must be supported by the local adapter.
    """

    return _read_source(
        "orc",
        uri,
        schema=schema,
        client=client,
        engine_mode=engine_mode,
        **client_config,
    )


def from_batches(
    batches: object, *, schema: Mapping[str, object],
    streaming: bool = False, client: ShardLoomClient | None = None, **client_config: object,
) -> LazyFrame:
    """Declare native input in batches of up to 2,048 rows.

    Each batch is a sequence of row mappings. Schema is explicit and supports
    exact numeric widths, decimal, binary, temporal and nested native types.
    Input is pulled at execution;
    pass a factory for repeated calls, or an iterable for one execution. Total
    input has no fixed batch-count or field-count ceiling. Schema metadata and
    native payloads share the query memory grant; each transport frame remains
    bounded. The default resident mode retains all native input under that grant.
    ``streaming=True`` admits distinct finite batch sources through
    filters/projections, sorting, aggregation, joins, analytic windows, set
    operations, dynamic pivot and limits, alongside ordinary file or resident
    sources. Existing unary stages also compose:
    distinct, deduplication, duplicate masks, tail, sampling, scalar rewrites,
    melt, explode and rolling.
    Incremental results, bounded small collection and all eight existing local
    writers (Vortex, Parquet, Arrow IPC, Avro, ORC, JSON, JSONL and CSV) are
    admitted under each writer's existing type, fidelity and feature limits. A
    single output streams directly. Multiple outputs consume each producer once
    and execute the plan once, then use credited native result replay under the
    same memory grant and explicit spill policy. This is distinct from
    repeated-source replay. All producers must end, and all writers and owned
    spill cleanup must succeed before publication. Local multi-file publication
    is not group-atomic and retains existing partial-publication diagnostics.
    Limits drain and validate every source. Stateful work shares the query grant;
    ordering, aggregation, joins, windows, pivot, distinct set membership and
    relational distinct/deduplication/duplicate-mask state can use explicit
    native spill. Future-dependent duplicate masks can spill row identities.
    Unknown-bound fractional sampling without replacement and fixed/fractional
    sampling with replacement can spill their weighted or unweighted populations.
    Relational IN, EXISTS and quantified ANY/ALL subquery membership/index state
    can also use explicit native spill, including correlated parameters. These
    state owners are separate from source and result replay. Fixed-count and
    known-bound fractional sampling without replacement remain resident under
    credits. Oversized unsupported state fails explicitly.
    Unsupported operator families reject before consumption. A factory is still
    required for repeated executions.
    Results remain provisional until every producer ends and final validation
    succeeds.
    """
    from ._query_sources import from_batches as _impl
    return _impl(batches, schema=schema, streaming=streaming, client=client, **client_config)


def from_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient | None = None,
    schema: Mapping[str, object] | None = None,
    **client_config: object,
) -> LazyFrame:
    """Declare native rows; use an explicit schema for rich types or empty columns."""
    from ._query_sources import from_rows as _impl
    return _impl(rows, client=client, schema=schema, **client_config)


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
    resolved_client, resources, resource_limits = _source_configuration(client, client_config, required=True)

    return _memory_rows_source(
        [_dataframe_source_free_projection_row(expressions)],
        client=resolved_client,
        resources=resources,
        resource_limits=resource_limits,
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
    from ._query_sources import dataframe_generated_with_column as _impl
    return _impl(name, expression, client=client, **client_config)


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
    from ._query_sources import range as _impl
    return _impl(start, end, step=step, column=column, client=client, **client_config)


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
) -> LazyFrame:
    """Declare SQL VALUES for the shared native engine."""
    from ._query_sources import sql_values as _impl
    return _impl(values_clause, client=client, **client_config)


def sql_literal_select(
    expression: object,
    *,
    client: ShardLoomClient | None = None,
    **client_config: object,
) -> LazyFrame:
    """Declare a source-free SQL projection for the shared native engine."""
    from ._query_sources import sql_literal_select as _impl
    return _impl(expression, client=client, **client_config)


def sql(
    statement: object,
    *,
    input: str | os.PathLike[str] | None = None,
    input_format: str | None = None,
    client: ShardLoomClient | None = None,
    parameters: Mapping[str, object] | None = None,
    relations: Mapping[str, LazyFrame] | None = None,
    **client_config: object,
) -> LazyFrame:
    """Create a scoped SQL workflow over currently admitted ShardLoom SQL paths."""
    from ._query_sources import sql as _impl
    return _impl(statement, input=input, input_format=input_format, client=client, parameters=parameters, relations=relations, **client_config)


def _normalize_sql_workflow_input(
    input_ref: str | os.PathLike[str] | None,
    input_format: str | None,
) -> tuple[str | None, str | None]:
    """Normalize optional input binding for SQL workflows."""
    from ._query_sources import _normalize_sql_workflow_input as _impl
    return _impl(input_ref, input_format)


def _source_format_for_sql_input_ref(value: str) -> str | None:
    """Infer source format from a normal Python path or SQL source literal."""
    from ._query_sources import _source_format_for_sql_input_ref as _impl
    return _impl(value)


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
    mirroring `range(start, end)`. ShardLoom writes the generated data to the
    selected local output. No input dataset is read, so the execution details
    contain no source-read validation report.
    """
    from ._query_sources import calendar as _impl
    return _impl(start, end, column=column, include_parts=include_parts, client=client, **client_config)


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
    from ._query_sources import from_pandas as _impl
    return _impl(dataframe, schema=schema, client=client, engine_mode=engine_mode, check=check, **client_config)


def _native_input_frame(owner, *, client, engine_mode, resources, resource_limits, schema=None):
    from ._query_sources import _native_input_frame as _impl
    return _impl(owner, client=client, engine_mode=engine_mode, resources=resources, resource_limits=resource_limits, schema=schema)


def from_vortex_ipc(
    array_message: bytes,
    *,
    dtype_message: bytes,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Import one IPC record for the pinned Vortex provider using native transport.

    Pass complete framed array and dtype messages as immutable bytes. Their
    combined size must not exceed 64 MiB. Intake validates the supported schema
    and encoding tree, discards wire statistics and copies into the receiving
    allocation. The resulting source can serve multiple native queries.
    """
    from ._query_sources import from_vortex_ipc as _impl
    return _impl(array_message, dtype_message=dtype_message, client=client, engine_mode=engine_mode, **client_config)


def from_result(
    result: object,
    *,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    **client_config: object,
) -> LazyFrame:
    """Admit completed ShardLoom data without an Arrow or row round trip in process.

    Native owners and credits are preserved by the existing native admission
    boundary. Moving a process result uses an explicit typed Python payload
    conversion; no cross-process zero-copy claim is made.
    """
    from ._query_sources import from_result as _impl
    return _impl(result, client=client, engine_mode=engine_mode, **client_config)


def from_arrow_table(
    table: object,
    *,
    schema: Mapping[str, object] | None = None,
    client: ShardLoomClient | None = None,
    engine_mode: str = "auto",
    check: bool = False,
    **client_config: object,
) -> LazyFrame | UnsupportedWorkflowOperationReport:
    """Create a source from an Arrow-compatible input or retained native result.

    Native transport preserves columnar ownership through the shared input
    adapter; subprocess transport uses the explicit bounded row conversion.
    """
    from ._query_sources import from_arrow_table as _impl
    return _impl(table, schema=schema, client=client, engine_mode=engine_mode, check=check, **client_config)


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
    from ._query_sources import from_arrow_ipc as _impl
    return _impl(source, schema=schema, client=client, engine_mode=engine_mode, check=check, **client_config)


def _memory_rows_source(
    rows: Sequence[Mapping[str, object]],
    *,
    client: ShardLoomClient,
    schema: Mapping[str, object] | None = None,
    engine_mode: str = "auto",
    resources: ExecutionResources | None = None,
    resource_limits: ExecutionResourceLimits | None = None,
) -> LazyFrame:
    """Declare bounded native rows without executing any expressions."""
    from ._query_sources import _memory_rows_source as _impl
    return _impl(rows, client=client, schema=schema, engine_mode=engine_mode, resources=resources, resource_limits=resource_limits)


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
    resources: ExecutionResources | None = None,
    resource_limits: ExecutionResourceLimits | None = None,
) -> LazyFrame:
    from ._query_sources import _native_memory_frame as _impl
    return _impl(declaration, schema=schema, client=client, engine_mode=engine_mode, resources=resources, resource_limits=resource_limits)


def _memory_value_type(value: object) -> str:
    from ._query_sources import _memory_value_type as _impl
    return _impl(value)


def _memory_value(value_type: str, value: object) -> str:
    from ._query_sources import _memory_value as _impl
    return _impl(value_type, value)


def _dataframe_source_free_projection_row(
    expressions: tuple[object, ...],
) -> dict[str, object]:
    from ._query_sources import _dataframe_source_free_projection_row as _impl
    return _impl(expressions)


def _dataframe_source_free_projection_item(expression: object) -> tuple[str, object]:
    from ._query_sources import _dataframe_source_free_projection_item as _impl
    return _impl(expression)


def _parse_dataframe_literal_alias_expression(expression: str) -> tuple[str, object]:
    from ._query_sources import _parse_dataframe_literal_alias_expression as _impl
    return _impl(expression)


def _generated_literal_expression(expression: object) -> object:
    from ._query_sources import _generated_literal_expression as _impl
    return _impl(expression)


def _normalize_date(name: str, value: str | date) -> date:
    from ._query_sources import _normalize_date as _impl
    return _impl(name, value)


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


def _normalize_limit_range(count: object, offset: object) -> tuple[int, int]:
    """Validate a DataFrame LIMIT range against the native 64-bit usize bound."""
    from ._query_sources import _normalize_limit_range as _impl
    return _impl(count, offset)


def _limit_operation_parts(values: Sequence[str]) -> tuple[str, str] | None:
    """Return canonical count/offset strings for one- or two-field limit ops."""
    from ._query_sources import _limit_operation_parts as _impl
    return _impl(values)


def _workflow_has_offset(frame: LazyFrame) -> bool:
    return any(
        operation.kind == "limit"
        and (parts := _limit_operation_parts(operation.values)) is not None
        and parts[1] != "0"
        for operation in frame.operations
    )


def _normalize_positive_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _first_string_field(envelope: OutputEnvelope, keys: Sequence[str]) -> str | None:
    from ._query_sources import _first_string_field as _impl
    return _impl(envelope, keys)


def _first_int_field(envelope: OutputEnvelope, keys: Sequence[str]) -> int | None:
    from ._query_sources import _first_int_field as _impl
    return _impl(envelope, keys)


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
    table_snapshot: tuple[tuple[str, object], ...] = (),
    **client_config: object,
) -> LazyFrame:
    from ._query_sources import _read_source as _impl
    return _impl(source_format, uri, schema=schema, client=client, engine_mode=engine_mode, table_snapshot=table_snapshot, **client_config)


def _materialized_boundary_workflow(
    source_format: str,
    uri: str,
    *,
    client: ShardLoomClient | None,
    engine_mode: str,
    **client_config: object,
) -> LazyFrame:
    from ._query_sources import _materialized_boundary_workflow as _impl
    return _impl(source_format, uri, client=client, engine_mode=engine_mode, **client_config)


def _python_object_boundary_ref(kind: str, value: object) -> str:
    value_type = type(value)
    return f"{kind}:{value_type.__module__}.{value_type.__qualname__}"


def _source_configuration(
    client: ShardLoomClient | None,
    client_config: Mapping[str, object],
    *, required: bool,
) -> tuple[ShardLoomClient, ExecutionResources | None, ExecutionResourceLimits | None]:
    """Resolve plan-bound configuration without modifying a shared transport."""
    from ._query_sources import _source_configuration as _impl
    return _impl(client, client_config, required=required)


def _client_from_config(
    client: ShardLoomClient | None,
    client_config: Mapping[str, object],
) -> ShardLoomClient:
    from ._query_transform_arguments import _client_from_config as _impl
    return _impl(client, client_config)


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
    from ._query_transform_arguments import _prepared_vortex_target_path as _impl
    return _impl(source_uri, target_vortex_path=target_vortex_path, workspace=workspace)






def _generated_prepared_vortex_target_path(
    stem: str,
    *,
    target_vortex_path: str | os.PathLike[str] | None,
    workspace: str | os.PathLike[str] | None,
) -> str | os.PathLike[str]:
    from ._query_transform_arguments import _generated_prepared_vortex_target_path as _impl
    return _impl(stem, target_vortex_path=target_vortex_path, workspace=workspace)


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
    from ._query_transform_arguments import _normalize_named_projection_items as _impl
    return _impl(context, columns, named_expressions)


def _normalize_rename_items(
    context: str,
    columns: Mapping[str, object] | Sequence[tuple[object, object]] | None,
    named_columns: Mapping[str, object],
) -> tuple[tuple[str, str], ...]:
    """Normalize ordered source/target column pairs for rename diagnostics."""
    from ._query_transform_arguments import _normalize_rename_items as _impl
    return _impl(context, columns, named_columns)


def _normalize_drop_columns(
    labels: Sequence[object],
    columns: object | None,
) -> tuple[str, ...]:
    from ._query_transform_arguments import _normalize_drop_columns as _impl
    return _impl(labels, columns)


def _normalize_sample_target(
    *,
    n: int | None,
    fraction: float | None,
    seed: int | None,
    weights: object | None = None,
    replace: bool = False,
) -> str:
    from ._query_transform_arguments import _normalize_sample_target as _impl
    return _impl(n=n, fraction=fraction, seed=seed, weights=weights, replace=replace)


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
    from ._query_transform_arguments import _sample_operation_parts as _impl
    return _impl(values)


def _normalize_sample_seed_alias(
    *,
    seed: int | None,
    random_state: int | None,
) -> int | None:
    from ._query_transform_arguments import _normalize_sample_seed_alias as _impl
    return _impl(seed=seed, random_state=random_state)


def _normalize_merge_target(
    other: "LazyFrame | str",
    *,
    on: object | None,
    left_on: object | None,
    right_on: object | None,
    how: str,
    kwargs: Mapping[str, object],
) -> str:
    from ._query_transform_arguments import _normalize_merge_target as _impl
    return _impl(other, on=on, left_on=left_on, right_on=right_on, how=how, kwargs=kwargs)


def _normalize_concat_target(
    others: "LazyFrame | str | Sequence[LazyFrame | str]",
    *,
    axis: int,
    join: str,
    kwargs: Mapping[str, object],
) -> str:
    from ._query_transform_arguments import _normalize_concat_target as _impl
    return _impl(others, axis=axis, join=join, kwargs=kwargs)


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
    from ._query_transform_arguments import _normalize_melt_target as _impl
    return _impl(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name, ignore_index=ignore_index, kwargs=kwargs)


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
    from ._query_transform_arguments import _normalize_fillna_target as _impl
    return _impl(value, axis=axis, inplace=inplace, method=method, limit=limit, kwargs=kwargs)


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
    from ._query_transform_arguments import _dropna_threshold_predicate as _impl
    return _impl(columns, threshold)


def _normalize_dropna_target(
    *,
    subset: object | None,
    how: str,
    axis: object | None,
    thresh: int | None,
    kwargs: Mapping[str, object],
) -> str:
    from ._query_transform_arguments import _normalize_dropna_target as _impl
    return _impl(subset=subset, how=how, axis=axis, thresh=thresh, kwargs=kwargs)


def _normalize_astype_errors(value: str) -> str:
    normalized = _require_non_empty("astype errors", value).lower().replace("_", "-")
    if normalized not in {"raise", "ignore"}:
        raise ValueError("astype errors must be 'raise' or 'ignore'")
    return normalized


def _normalize_astype_dtype_map(
    dtype: object,
    projection_columns: tuple[str, ...],
) -> dict[str, str] | None:
    from ._query_transform_arguments import _normalize_astype_dtype_map as _impl
    return _impl(dtype, projection_columns)


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
    from ._query_transform_arguments import _normalize_mask_target as _impl
    return _impl(cond=cond, other=other, axis=axis, inplace=inplace, level=level, kwargs=kwargs)


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
    from ._query_transform_arguments import _normalize_replace_target as _impl
    return _impl(to_replace=to_replace, value=value, regex=regex, inplace=inplace, method=method, limit=limit, kwargs=kwargs)


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
    *,
    allow_zero_division: bool = False,
) -> tuple[dict[str, object], ...] | None:
    from ._query_transform_arguments import _eval_numeric_scalar_assignment_rewrites as _impl
    return _impl(expr, allow_zero_division=allow_zero_division)


def _numeric_scalar_assignment_rewrite_from_expression(
    target_column: str,
    expression_sql: str,
) -> dict[str, object] | None:
    from ._query_transform_arguments import _numeric_scalar_assignment_rewrite_from_expression as _impl
    return _impl(target_column, expression_sql)


def _numeric_scalar_assignment_rewrite_from_binop(
    target_column: str,
    value: ast.BinOp,
    *,
    allow_zero_division: bool = False,
) -> dict[str, object] | None:
    from ._query_transform_arguments import _numeric_scalar_assignment_rewrite_from_binop as _impl
    return _impl(target_column, value, allow_zero_division=allow_zero_division)


def _eval_numeric_scalar_operator(operator: ast.operator) -> str | None:
    from ._query_transform_arguments import _eval_numeric_scalar_operator as _impl
    return _impl(operator)


def _eval_numeric_scalar_literal(node: ast.AST) -> int | float | None:
    from ._query_transform_arguments import _eval_numeric_scalar_literal as _impl
    return _impl(node)


def _stable_callable_name(function: object) -> str:
    from ._query_transform_arguments import _stable_callable_name as _impl
    return _impl(function)


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
    from ._query_transform_arguments import _normalize_workflow_targets as _impl
    return _impl(value)


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
    from ._query_transform_arguments import _normalize_join_how as _impl
    return _impl(value)


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


def _normalize_temporal_grid_keys(by: object, time_column: str) -> tuple[str, ...]:
    from ._query_transform_arguments import _normalize_temporal_grid_keys as _impl
    return _impl(by, time_column)


def _normalize_temporal_grid_column(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"temporal grid {name} must be a string identifier")
    return _normalize_output_column_name(value)


def _normalize_temporal_grid_int(
    name: str, value: object | None, *, required: bool = False,
) -> int | None:
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"temporal grid {name} requires signed 64-bit integer ticks")
    if not -(1 << 63) <= value <= (1 << 63) - 1:
        raise ValueError(f"temporal grid {name} must fit signed 64-bit ticks")
    return value


def _normalize_temporal_grid_unit(unit: object) -> str:
    if not isinstance(unit, str):
        raise TypeError("temporal grid unit must be a string literal")
    if unit not in {"s", "ms", "us", "ns", "calendar_days", "calendar_months"}:
        raise ValueError("temporal grid unit is not supported")
    return unit


def _normalize_temporal_grid_policies(
    month_end: object, ambiguity: object,
) -> tuple[str, str]:
    from ._query_transform_arguments import _normalize_temporal_grid_policies as _impl
    return _impl(month_end, ambiguity)


def _format_named_aggregate(name: object, expression: object) -> str:
    alias = _normalize_output_column_name(name)
    aggregate_expression = _require_non_empty("aggregate expression", expression)
    return f"{aggregate_expression} AS {alias}"


def _normalize_cast_dtype(value: object) -> str:
    from ._query_transform_arguments import _normalize_cast_dtype as _impl
    return _impl(value)


def _normalize_decimal_cast_dtype(dtype: str) -> str | None:
    from ._query_literals import _normalize_decimal_cast_dtype as _impl
    return _impl(dtype)


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
    from ._query_literals import _normalize_interval_integer as _impl
    return _impl(value)


def _normalize_date_arithmetic_days(value: object) -> str:
    from ._query_literals import _normalize_date_arithmetic_days as _impl
    return _impl(value)


def _normalize_timestamp_arithmetic_seconds(value: object) -> str:
    from ._query_literals import _normalize_timestamp_arithmetic_seconds as _impl
    return _impl(value)


def _coerce_interval_literal(value: object) -> IntervalLiteral | None:
    if isinstance(value, IntervalLiteral):
        return value
    if isinstance(value, str):
        return _parse_interval_literal_sql(value)
    return None


def _parse_interval_literal_sql(value: str) -> IntervalLiteral | None:
    from ._query_literals import _parse_interval_literal_sql as _impl
    return _impl(value)


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
    from ._query_literals import _sql_temporal_difference_arg as _impl
    return _impl(value, dtype)


def _sql_string_literal(value: object) -> str:
    return _sql_string_function_literal("string literal", value, allow_empty=False)


def _decimal_controls(precision, scale, rounding):
    from ._query_literals import _decimal_controls as _impl
    return _impl(precision, scale, rounding)


def _sql_string_function_literal(
    name: str, value: object, *, allow_empty: bool
) -> str:
    text = str(value)
    if not allow_empty and text == "":
        raise ValueError(f"{name} must not be empty")
    return "'" + text.replace("'", "''") + "'"


def _sql_literal(value: object) -> str:
    from ._query_literals import _sql_literal as _impl
    return _impl(value)


def _temporal_control_literal(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"temporal {name} must be a string literal")
    return _sql_literal(value)


def _temporal_value_sql(value: object) -> str:
    if isinstance(value, ColumnExpression):
        if not value.sql.strip():
            raise ValueError("temporal expression must not be empty")
        return value.sql
    return _sql_literal(value)


def _sql_decimal_literal(value: Decimal) -> str:
    """Carry exact declared decimal values; the native kernel owns evaluation."""
    precision, scale = _decimal_literal_metadata(value)
    return f"CAST('{format(value, 'f')}' AS decimal128({precision},{scale}))"


def _decimal_literal_metadata(value: Decimal) -> tuple[int, int]:
    from ._query_literals import _decimal_literal_metadata as _impl
    return _impl(value)


def _vortex_exact_scalar_payload(value: object) -> dict[str, object] | None:
    """Declare exact literal bytes or text; native binding owns all conversion."""
    from ._query_literals import _vortex_exact_scalar_payload as _impl
    return _impl(value)


def _vortex_expression_scalar_payload(
    value: object,
    *,
    target_dtype: str | None,
    allow_null: bool = True,
) -> dict[str, object] | None:
    from ._query_literals import _vortex_expression_scalar_payload as _impl
    return _impl(value, target_dtype=target_dtype, allow_null=allow_null)




def _sql_complex_projection_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _sql_numeric_literal(value: object) -> str:
    from ._query_scalar_sql import _sql_numeric_literal as _impl
    return _impl(value)


def _sql_numeric_arithmetic_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_numeric_arithmetic_projection_expression as _impl
    return _impl(expression)


def _sql_generic_expression_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_generic_expression_projection_expression as _impl
    return _impl(expression)


def _is_scalar_subquery_expression(text: str) -> bool:
    if re.match(r"^\(\s*SELECT\b", text, re.IGNORECASE) is None or not text.endswith(")"):
        return False
    _validate_balanced_expression_parentheses(text)
    return True


def _parenthesize_numeric_operand(value: str) -> str:
    text = _require_non_empty("numeric expression", value)
    if _expression_has_numeric_operator(text) and not (
        text.startswith("(") and text.endswith(")")
    ):
        return f"({text})"
    return text


def _expression_has_numeric_operator(value: str) -> bool:
    from ._query_scalar_sql import _expression_has_numeric_operator as _impl
    return _impl(value)


def _expression_has_temporal_difference_call(value: str) -> bool:
    from ._query_scalar_sql import _expression_has_temporal_difference_call as _impl
    return _impl(value)


def _is_unary_numeric_sign(value: str, index: int, char: str) -> bool:
    from ._query_scalar_sql import _is_unary_numeric_sign as _impl
    return _impl(value, index, char)


def _validate_balanced_expression_parentheses(value: str) -> None:
    from ._query_scalar_sql import _validate_balanced_expression_parentheses as _impl
    return _impl(value)


def _sql_numeric_abs_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_numeric_abs_projection_expression as _impl
    return _impl(expression)


def _sql_numeric_rounding_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_numeric_rounding_projection_expression as _impl
    return _impl(expression)


def _sql_computed_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_computed_projection_expression as _impl
    return _impl(expression)


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
    from ._query_scalar_sql import _sql_cast_projection_expression as _impl
    return _impl(expression)


def _sql_null_coalesce_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_null_coalesce_projection_expression as _impl
    return _impl(expression)


def _sql_nullif_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_nullif_projection_expression as _impl
    return _impl(expression)


def _sql_conditional_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_conditional_projection_expression as _impl
    return _impl(expression)


def _sql_case_branch(value: object) -> str:
    from ._query_scalar_sql import _sql_case_branch as _impl
    return _impl(value)


def _sql_date_arithmetic_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_date_arithmetic_projection_expression as _impl
    return _impl(expression)


def _sql_timestamp_arithmetic_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_timestamp_arithmetic_projection_expression as _impl
    return _impl(expression)


def _sql_string_transform_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_string_transform_projection_expression as _impl
    return _impl(expression)


def _sql_string_length_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_string_length_projection_expression as _impl
    return _impl(expression)


def _sql_string_function_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_string_function_projection_expression as _impl
    return _impl(expression)


def _vortex_string_replace_expression_parts(
    expression_sql: str,
) -> tuple[str, str, str] | None:
    from ._query_scalar_sql import _vortex_string_replace_expression_parts as _impl
    return _impl(expression_sql)


def _sql_binary_helper_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_binary_helper_projection_expression as _impl
    return _impl(expression)


def _sql_binary_byte_length_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_binary_byte_length_projection_expression as _impl
    return _impl(expression)


def _sql_temporal_extract_projection_expression(expression: object) -> str:
    from ._query_scalar_sql import _sql_temporal_extract_projection_expression as _impl
    return _impl(expression)


def _split_projection_function_args(expression: str) -> tuple[str, ...]:
    from ._query_scalar_sql import _split_projection_function_args as _impl
    return _impl(expression)


def _sql_string_function_text_arg(value: object, name: str) -> tuple[str, bool]:
    if isinstance(value, ColumnExpression):
        return _require_non_empty(name, value.sql), True
    return _sql_string_function_literal(name, value, allow_empty=True), False


def _normalize_string_function_text_arg_sql(raw: str) -> tuple[str, bool]:
    from ._query_scalar_sql import _normalize_string_function_text_arg_sql as _impl
    return _impl(raw)


def _normalize_string_scalar_expression_sql(raw: str) -> tuple[str, bool]:
    from ._query_scalar_sql import _normalize_string_scalar_expression_sql as _impl
    return _impl(raw)


def _normalize_binary_scalar_expression_sql(raw: str) -> tuple[str, bool]:
    from ._query_scalar_sql import _normalize_binary_scalar_expression_sql as _impl
    return _impl(raw)


def _parse_sql_string_literal_token(raw: str) -> str:
    from ._query_scalar_sql import _parse_sql_string_literal_token as _impl
    return _impl(raw)


def _normalize_substring_bound(name: str, value: object, *, minimum: int) -> int:
    from ._query_predicate_sql import _normalize_substring_bound as _impl
    return _impl(name, value, minimum=minimum)


def _normalize_temporal_extract_column(expression: str, dtype: str) -> str:
    from ._query_predicate_sql import _normalize_temporal_extract_column as _impl
    return _impl(expression, dtype)


def _normalize_nullable_projection_column(expression: str) -> str:
    from ._query_predicate_sql import _normalize_nullable_projection_column as _impl
    return _impl(expression)


def _parse_numeric_literal_token(value: str) -> int | float:
    from ._query_predicate_sql import _parse_numeric_literal_token as _impl
    return _impl(value)


def _sql_in_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return _sql_literal(value)


def _normalize_timestamp_literal(value: datetime) -> str:
    from ._query_predicate_sql import _normalize_timestamp_literal as _impl
    return _impl(value)


def _predicate_sql(value: object) -> str:
    if isinstance(value, PredicateExpression):
        return value.sql
    return _normalize_raw_or_typed_predicate("predicate expression", value)


def _predicate_sources(*values: object) -> tuple[WorkflowSource, ...]:
    from ._query_predicate_sql import _predicate_sources as _impl
    return _impl(*values)


def _predicate_limits(*values: object) -> ExecutionResourceLimits | None:
    """Retain every participating workflow ceiling through lazy composition.

    Ceilings intersect; resource grants keep the receiving workflow's explicit
    inheritance. This also covers source-free scalar subqueries, which carry no
    source bindings but still have an execution policy.
    """
    from ._query_predicate_sql import _predicate_limits as _impl
    return _impl(*values)


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
    allow_nested_queries: bool = False,
) -> None:
    from ._query_predicate_sql import _validate_raw_sql_fragment as _impl
    return _impl(name, value, allowed_keywords=allowed_keywords, allow_nested_queries=allow_nested_queries)


def _sql_fragment_admitted_for_local_source_statement(
    name: str,
    value: str,
    *,
    allowed_keywords: tuple[str, ...] = (),
) -> bool:
    from ._query_predicate_sql import _sql_fragment_admitted_for_local_source_statement as _impl
    return _impl(name, value, allowed_keywords=allowed_keywords)


def _validate_sql_fragment_quotes(name: str, value: str) -> None:
    from ._query_predicate_sql import _validate_sql_fragment_quotes as _impl
    return _impl(name, value)


def _contains_sql_token_outside_quotes(statement: str, token: str) -> bool:
    from ._query_predicate_sql import _contains_sql_token_outside_quotes as _impl
    return _impl(statement, token)


def _like_needle(name: str, value: object) -> str:
    text = str(value)
    if not text:
        raise ValueError(f"{name} must not be empty")
    if "%" in text or "_" in text:
        raise ValueError(f"{name} must not contain SQL LIKE wildcard characters")
    return text


def _like_escape_clause(escape: object | None) -> str:
    if escape is None:
        return ""
    text = str(escape)
    if len(text) != 1:
        raise ValueError("LIKE escape character must be exactly one character")
    return f" ESCAPE {_sql_string_literal(text)}"


def _normalize_in_values(values: tuple[object, ...]) -> tuple[object, ...]:
    from ._query_predicate_sql import _normalize_in_values as _impl
    return _impl(values)


def _row_value_in_predicate(
    columns: object, rows: object, *, negated: bool
) -> PredicateExpression:
    from ._query_predicate_sql import _row_value_in_predicate as _impl
    return _impl(columns, rows, negated=negated)


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
    from ._query_predicate_sql import _row_value_in_source_predicate as _impl
    return _impl(columns, source, source_columns, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit, negated=negated)


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
    from ._query_predicate_sql import _exists_source_predicate as _impl
    return _impl(source, source_alias=source_alias, select=select, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit, negated=negated)


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
    from ._query_predicate_sql import _quantified_source_predicate as _impl
    return _impl(column_sql, comparison, quantifier, source, source_column, source_alias=source_alias, where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


def _normalize_quantified_comparison_operator(operator: object) -> str:
    from ._query_predicate_sql import _normalize_quantified_comparison_operator as _impl
    return _impl(operator)


def _normalize_exists_subquery_projection(select: object) -> str:
    from ._query_predicate_sql import _normalize_exists_subquery_projection as _impl
    return _impl(select)


def _normalize_row_value_columns(columns: object) -> tuple[str, ...]:
    from ._query_predicate_sql import _normalize_row_value_columns as _impl
    return _impl(columns)


def _normalize_row_value_in_rows(
    rows: object, *, arity: int
) -> tuple[tuple[object, ...], ...]:
    from ._query_predicate_sql import _normalize_row_value_in_rows as _impl
    return _impl(rows, arity=arity)


def _sql_in_subquery_source(
    source: object, *, source_alias: object | None = None
) -> str:
    return _sql_local_subquery_source(
        source, "IN subquery source", source_alias=source_alias
    )


def _sql_local_subquery_source(
    source: object, name: str, *, source_alias: object | None = None
) -> str:
    from ._query_predicate_sql import _sql_local_subquery_source as _impl
    return _impl(source, name, source_alias=source_alias)


def _sql_in_subquery_tail(
    *,
    where: object | None,
    group_by: object | None,
    having: object | None,
    order_by: object | None,
    descending: bool,
    limit: int | None,
) -> str:
    from ._query_predicate_sql import _sql_in_subquery_tail as _impl
    return _impl(where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit)


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
    from ._query_predicate_sql import _sql_local_subquery_tail as _impl
    return _impl(where=where, group_by=group_by, having=having, order_by=order_by, descending=descending, limit=limit, limit_name=limit_name, positive_limit=positive_limit)


def _normalize_in_subquery_order_by(value: object | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, ColumnExpression):
        return (_normalize_expression_column(value.sql),)
    if _is_non_string_sequence(value):
        return tuple(_normalize_expression_column(item) for item in value)
    return (_normalize_expression_column(value),)


def _is_source_free_sql_statement(statement: str) -> bool:
    from ._query_predicate_sql import _is_source_free_sql_statement as _impl
    return _impl(statement)


def _is_source_free_sql_generator_statement(statement: str) -> bool:
    from ._query_predicate_sql import _is_source_free_sql_generator_statement as _impl
    return _impl(statement)


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
    from ._query_sql_text import _cap_top_level_sql_limit as _impl
    return _impl(statement, limit_index, count)


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
    from ._query_sql_text import _duplicate_mask_operation_parts as _impl
    return _impl(values)


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
    from ._query_sql_text import _vortex_tiny_predicate_from_sql as _impl
    return _impl(predicate)


def _vortex_tiny_predicate_column(predicate: str | None) -> str | None:
    from ._query_sql_text import _vortex_tiny_predicate_column as _impl
    return _impl(predicate)


def _is_local_source_sql_ref(value: str) -> bool:
    from ._query_sql_text import _is_local_source_sql_ref as _impl
    return _impl(value)


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
    from ._query_sql_text import _find_top_level_sql_phrase_span_outside_quotes as _impl
    return _impl(statement, phrase)
















def _is_local_csv_source_ref(value: str) -> bool:
    return _is_local_source_sql_ref(value) and _source_suffix_for_local_ref(value) == ".csv"


def _source_format_for_local_source_ref(value: str) -> str | None:
    from ._query_bindings import _source_format_for_local_source_ref as _impl
    return _impl(value)


def _public_workflow_input_format(source: WorkflowSource) -> str | None:
    """Return the input format to pass to the public workflow CLI facade."""
    from ._query_bindings import _public_workflow_input_format as _impl
    return _impl(source)


def _workflow_source_bindings(sources: Sequence[WorkflowSource]) -> dict[str, dict[str, object]]:
    """Retain each source's adapter contract across relational SQL lowering."""
    from ._query_bindings import _workflow_source_bindings as _impl
    return _impl(sources)


def _workflow_batch_inputs(sources: Sequence[WorkflowSource]) -> dict[str, object]:
    inputs = {}
    for source in sources:
        if source.batch_input is not None:
            prior = inputs.setdefault(source.uri, source.batch_input)
            if prior is not source.batch_input:
                raise ValueError("conflicting batch providers for the same source")
    return inputs


def _prepare_vortex_schema_hints(source: WorkflowSource) -> Mapping[str, object] | None:
    """Return CLI schema hints only for text adapters that accept them."""

    if source.source_format in {"csv", "tsv", "json", "jsonl"}:
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
        source.source_format in {"csv", "tsv", "json", "jsonl", "parquet", "arrow-ipc", "avro", "orc", "iceberg", "delta", "hudi"}
        and "://" not in uri
        and not uri.startswith(("s3:", "gs:", "abfs:", "abfss:"))
    )




def _single_quoted_sql_strings(statement: str) -> tuple[str, ...]:
    from ._query_bindings import _single_quoted_sql_strings as _impl
    return _impl(statement)








def _terminal_resource_kwargs(
    memory_gb: int | None,
    max_parallelism: int | None,
    spill: Mapping[str, object] | str | None = None,
    *, memory_bytes: int | None = None, resources: ExecutionResources | None = None,
    inherited: ExecutionResources | None = None,
    limits: ExecutionResourceLimits | None = None, required: bool = True,
) -> dict[str, object]:
    """Validate allocation before dispatch; keep descriptions resource-free."""
    from ._query_bindings import _terminal_resource_kwargs as _impl
    return _impl(memory_gb, max_parallelism, spill, memory_bytes=memory_bytes, resources=resources, inherited=inherited, limits=limits, required=required)


def _frame_action(frame, operation, execute, *, reuse=True):
    from ._query_bindings import _frame_action as _impl
    return _impl(frame, operation, execute, reuse=reuse)


def _native_plan_kwargs(workflow) -> dict[str, object]:
    if getattr(workflow.client, "transport", "subprocess") == "native":
        return {"_native_plan": workflow._native_plan, "_native_frame": workflow}
    return {}


def _collect_native_relational(
    client: ShardLoomClient, statement: str, *, surface: str,
    plan_summary: str, input_kwargs: Mapping[str, Any], check: bool,
    memory_gb: int | None, max_parallelism: int | None,
    memory_bytes: int | None = None, resources: ExecutionResources | None = None,
    inherited: ExecutionResources | None = None,
    limits: ExecutionResourceLimits | None = None,
    spill: Mapping[str, object] | str | None = None,
    input_batches: Mapping[str, object] | None = None,
    native_plan: NativePlanState | None = None,
    native_frame: LazyFrame | None = None,
) -> OutputEnvelope:
    from ._query_bindings import _collect_native_relational as _impl
    return _impl(client, statement, surface=surface, plan_summary=plan_summary, input_kwargs=input_kwargs, check=check, memory_gb=memory_gb, max_parallelism=max_parallelism, memory_bytes=memory_bytes, resources=resources, inherited=inherited, limits=limits, spill=spill, input_batches=input_batches, native_plan=native_plan, native_frame=native_frame)


def _sql_source_refs(statement: str) -> tuple[str, ...]:
    from ._query_sql_text import _sql_source_refs as _impl
    return _impl(statement)


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
    from ._query_sql_text import _find_sql_keyword_outside_quotes as _impl
    return _impl(statement, keyword)


def _find_top_level_sql_keyword_outside_quotes(statement: str, keyword: str) -> int | None:
    from ._query_sql_text import _find_top_level_sql_keyword_outside_quotes as _impl
    return _impl(statement, keyword)


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
    from ._query_sql_text import _normalize_sort_nulls as _impl
    return _impl(value)


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
    from ._query_sql_text import _sql_join_keyword as _impl
    return _impl(how)


def _optional_sql_where_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" WHERE {predicate}"


def _rewrite_predicate_with_computed_columns(
    predicate: str,
    computed_columns: tuple[tuple[str, str], ...],
) -> str:
    from ._query_sql_text import _rewrite_predicate_with_computed_columns as _impl
    return _impl(predicate, computed_columns)


def _replace_sql_identifier_outside_quotes(
    statement: str,
    identifier: str,
    replacement: str,
) -> str:
    from ._query_sql_text import _replace_sql_identifier_outside_quotes as _impl
    return _impl(statement, identifier, replacement)


def _sql_identifier_rewrite_boundary(
    statement: str,
    start: int,
    length: int,
) -> bool:
    from ._query_report_values import _sql_identifier_rewrite_boundary as _impl
    return _impl(statement, start, length)


def _optional_sql_having_clause(predicate: str | None) -> str:
    if predicate is None:
        return ""
    return f" HAVING {predicate}"


def _workflow_schema_report(
    workflow: LazyFrame,
    smoke_report: Result,
) -> WorkflowSchemaReport:
    from ._query_report_values import _workflow_schema_report as _impl
    return _impl(workflow, smoke_report)




def _infer_python_scalar_dtype(value: object) -> str:
    from ._query_report_values import _infer_python_scalar_dtype as _impl
    return _impl(value)




def _normalize_schema_dtype_token(value: str | None) -> str | None:
    from ._query_report_values import _normalize_schema_dtype_token as _impl
    return _impl(value)


def _validate_workflow_schema(
    report: WorkflowSchemaReport,
    expected_schema: tuple[tuple[str, str], ...],
) -> WorkflowSchemaValidationReport:
    from ._query_report_values import _validate_workflow_schema as _impl
    return _impl(report, expected_schema)


def _parse_data_quality_checks(
    checks: tuple[str, ...],
) -> tuple[_WorkflowDataQualityCheckSpec, ...] | None:
    from ._query_report_values import _parse_data_quality_checks as _impl
    return _impl(checks)


def _workflow_data_quality_report(
    schema_report: WorkflowSchemaReport,
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> WorkflowDataQualityReport:
    from ._query_report_values import _workflow_data_quality_report as _impl
    return _impl(schema_report, checks)


def _workflow_quarantine_checks(
    schema_report: WorkflowSchemaReport,
    checks: tuple[object, ...],
) -> tuple[_WorkflowDataQualityCheckSpec, ...]:
    from ._query_conversion import _workflow_quarantine_checks as _impl
    return _impl(schema_report, checks)


def _workflow_quarantine_rows(
    schema_report: WorkflowSchemaReport,
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> tuple[Mapping[str, Any], ...]:
    from ._query_conversion import _workflow_quarantine_rows as _impl
    return _impl(schema_report, checks)


def _quarantine_pushdown_predicate(
    checks: tuple[_WorkflowDataQualityCheckSpec, ...],
) -> str | None:
    from ._query_conversion import _quarantine_pushdown_predicate as _impl
    return _impl(checks)


def _normalize_optional_quarantine_output_format(
    target_uri: str | os.PathLike[str] | None,
    output_format: str | None,
) -> str | None:
    from ._query_conversion import _normalize_optional_quarantine_output_format as _impl
    return _impl(target_uri, output_format)


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


def _result_to_pandas(report: Result, pandas: object) -> object:
    # Object columns preserve NULL and full-width integers without float coercion.
    return getattr(pandas, "DataFrame")(report.python_objects, columns=report.result_columns, dtype=object)


def _result_to_arrow_table(report: Result, pyarrow: object) -> object:
    if getattr(report, "native_result", None) is not None:
        return pyarrow.RecordBatchReader.from_stream(report.native_result).read_all()
    return arrow_table(report.result_rows, report.result_schema, pyarrow)


def _result_to_arrow_ipc(report: Result, pyarrow: object) -> bytes:
    table = _result_to_arrow_table(report, pyarrow)
    sink = getattr(pyarrow, "BufferOutputStream")()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _result_to_numpy(report: Result, numpy: object) -> object:
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
    from ._query_conversion import _read_arrow_ipc_from_seekable as _impl
    return _impl(source, ipc)


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
    from ._query_conversion import _normalize_local_output_format as _impl
    return _impl(value)


def _public_write_request_for_format(output_format: str) -> str:
    from ._query_conversion import _public_write_request_for_format as _impl
    return _impl(output_format)


def _normalize_fanout_outputs(
    outputs: Mapping[str, CommandPart] | Sequence[tuple[str, CommandPart]],
) -> tuple[tuple[str, CommandPart], ...]:
    from ._query_conversion import _normalize_fanout_outputs as _impl
    return _impl(outputs)


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
