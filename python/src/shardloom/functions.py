# SPDX-License-Identifier: Apache-2.0
"""Native expression declarations; Python never evaluates data operations.

Names, type/null rules and execution are admitted by the same SQL/expression
binder as context.sql(). Strings in column positions name columns; use lit()
for scalar values. Existing constructors are shared with the top-level API.
"""

from __future__ import annotations

from .query import (
    ColumnExpression, PredicateExpression, all_source, any_source, array, case_when,
    col, column, count_distinct, dense_rank, exists_source,
    interval_days, interval_hours, interval_minutes, interval_seconds,
    not_exists_source, outer, rank,
    row_in, row_in_source, row_not_in, row_not_in_source, row_number, scalar_subquery,
    struct, typed_scalar_udf,
)

__all__ = (
    "abs", "all_source", "any_source", "array", "avg", "byte_length", "case_when",
    "cast", "catalog", "ceil", "coalesce", "col", "column", "concat", "contains", "count",
    "count_distinct", "date_add_days", "date_day", "date_diff_days", "date_month",
    "date_sub_days", "date_trunc_minute", "date_year", "dense_rank", "ends_with",
    "exists_source", "extract_minute", "first_value", "floor", "from_base64", "interval_days",
    "interval_hours", "interval_minutes", "interval_seconds", "is_null", "is_not_null",
    "json_extract", "lag", "last_value", "lead", "left", "length", "lit", "lower",
    "max", "mean", "min", "not_exists_source", "not_regex_match", "nth_value", "null_if",
    "outer", "rank", "regex_domain", "regex_match", "regexp_replace", "replace", "right", "round", "row_in",
    "row_in_source", "row_not_in", "row_not_in_source", "row_number", "scalar_subquery",
    "starts_with", "strptime", "struct", "substr", "substring", "sum", "timestamp_add_seconds",
    "timestamp_day", "timestamp_diff_seconds", "timestamp_hour", "timestamp_minute", "timestamp_month",
    "timestamp_second", "timestamp_sub_seconds", "timestamp_year", "try_cast", "try_strptime",
    "trim", "typed_scalar_udf", "unhex", "upper", "url_domain",
)


def _column(value):
    return value if isinstance(value, ColumnExpression) else col(value)


def catalog() -> tuple[dict, ...]:
    """Inspect native scalar names, aliases, argument counts and type/null rules.

    This reads inert definitions without creating a context or opening inputs.
    Aggregates, window functions and nested constructors have separate contracts.
    """
    from ._native_backend import load_native
    return tuple(load_native().scalar_function_catalog())


def _call(name, *expressions):
    from .query import _predicate_sources, _predicate_limits
    return ColumnExpression(f"{name}({','.join(str(value) for value in expressions)})",
        _predicate_sources(expressions), resource_limits=_predicate_limits(expressions))


def lit(value) -> ColumnExpression:
    """Declare a scalar literal, including null, decimal and naive microsecond datetime values."""
    from datetime import datetime, timezone
    from .query import _sql_literal
    if isinstance(value, datetime) and value.tzinfo is None:
        # Naive TimestampMicros fields use the native UTC epoch representation.
        # Reuse the SQL literal formatter while preserving every supplied field.
        value = value.replace(tzinfo=timezone.utc)
    return ColumnExpression("NULL" if value is None else _sql_literal(value))


def abs(value) -> ColumnExpression:
    """Declare checked absolute value over a column or expression."""
    return _column(value).abs()


def floor(value) -> ColumnExpression:
    """Declare rounding toward negative infinity."""
    return _column(value).floor()


def ceil(value) -> ColumnExpression:
    """Declare rounding toward positive infinity."""
    return _column(value).ceil()


def round(value) -> ColumnExpression:
    """Declare integer rounding with halfway values away from zero."""
    return _column(value).round()


def length(value) -> ColumnExpression:
    """Count Unicode code points in a column or expression."""
    return _column(value).length()


def byte_length(value) -> ColumnExpression:
    """Count bytes in a UTF8 or binary column or expression."""
    return _column(value).byte_length()


def unhex(value) -> ColumnExpression:
    """Declare hexadecimal text decoding to binary."""
    return _column(value).unhex()


def from_base64(value) -> ColumnExpression:
    """Declare Base64 text decoding to binary."""
    return _column(value).from_base64()


def concat(*values) -> ColumnExpression:
    """Concatenate columns or expressions; use lit() for string constants."""
    return _call("CONCAT", *(_column(value) for value in values))


def substr(value, start, length) -> ColumnExpression:
    """Take a Unicode substring with SQL start/length semantics."""
    return _column(value).substr(start, length)


substring = substr


def left(value, count) -> ColumnExpression:
    """Take a UTF8 prefix by code-point count."""
    return _column(value).left(count)


def right(value, count) -> ColumnExpression:
    """Take a UTF8 suffix by code-point count."""
    return _column(value).right(count)


def replace(value, needle, replacement) -> ColumnExpression:
    """Replace literal substrings in a column or expression."""
    return _column(value).replace(needle, replacement)


def null_if(value, other) -> ColumnExpression:
    """Return null when a column/expression equals another expression or literal."""
    return _column(value).null_if(other)


def try_cast(value, dtype) -> ColumnExpression:
    """Declare native conversion that maps malformed values to null."""
    return _column(value).try_cast(dtype)


def sum(value) -> ColumnExpression:
    """Declare native SUM over a column or expression, excluding null values."""
    return _call("SUM", _column(value))


def min(value) -> ColumnExpression:
    """Declare native MIN over non-null values."""
    return _call("MIN", _column(value))


def max(value) -> ColumnExpression:
    """Declare native MAX over non-null values."""
    return _call("MAX", _column(value))


def avg(value) -> ColumnExpression:
    """Declare native AVG over non-null values."""
    return _call("AVG", _column(value))


mean = avg


def count(value=None) -> ColumnExpression:
    """Count all rows with no argument; otherwise count non-null values."""
    return _call("COUNT", ColumnExpression("*") if value is None or isinstance(value, str) and value == "*" else _column(value))


def coalesce(*values) -> ColumnExpression:
    """Declare the first non-null expression; use lit() for scalar constants."""
    if not values:
        raise ValueError("coalesce requires at least one expression")
    return _call("COALESCE", *(_column(value) for value in values))


def cast(value, dtype) -> ColumnExpression:
    """Declare a checked native cast to a supported type."""
    return _column(value).cast(dtype)


def lower(value) -> ColumnExpression:
    """Declare native lowercase conversion."""
    return _column(value).lower()


def upper(value) -> ColumnExpression:
    """Declare native uppercase conversion."""
    return _column(value).upper()


def trim(value) -> ColumnExpression:
    """Declare native whitespace trimming."""
    return _column(value).trim()


def is_null(value):
    """Declare a boolean null check."""
    return _column(value).is_null()


def is_not_null(value):
    """Declare a boolean non-null check."""
    return _column(value).is_not_null()


def first_value(value) -> ColumnExpression:
    """Select the first value of the frame supplied with over()."""
    return _call("FIRST_VALUE", _column(value))


def last_value(value) -> ColumnExpression:
    """Select the last value of the frame supplied with over()."""
    return _call("LAST_VALUE", _column(value))


def nth_value(value, position: int) -> ColumnExpression:
    """Select a value by its one-based position in the frame supplied with over()."""
    if type(position) is not int or not 1 <= position <= (1 << 64) - 1:
        raise ValueError("nth_value position must be a positive UInt64")
    return _call("NTH_VALUE", _column(value), lit(position))


def lag(value, offset: int = 1) -> ColumnExpression:
    """Select an earlier ordered value; admitted offsets are 1 through 50000."""
    return _offset_value("LAG", value, offset)


def lead(value, offset: int = 1) -> ColumnExpression:
    """Select a later ordered value; admitted offsets are 1 through 50000."""
    return _offset_value("LEAD", value, offset)


def regex_match(value, pattern) -> PredicateExpression:
    """Match UTF8 with a literal native regular expression."""
    return _predicate("REGEX_MATCH", value, pattern)


def not_regex_match(value, pattern) -> PredicateExpression:
    """Return a scoped UTF-8 regex negation."""
    return ~regex_match(value, pattern)


def contains(value, needle) -> PredicateExpression:
    """Match a literal UTF8 substring, including percent and underscore characters."""
    return _predicate("CONTAINS", value, needle)


def starts_with(value, prefix) -> PredicateExpression:
    """Match a literal UTF8 prefix."""
    return _predicate("STARTS_WITH", value, prefix)


def ends_with(value, suffix) -> PredicateExpression:
    """Match a literal UTF8 suffix."""
    return _predicate("ENDS_WITH", value, suffix)


def _predicate(name, value, pattern):
    expression = _call(name, _column(value), lit(pattern))
    return PredicateExpression(expression.sql, expression.source_bindings,
        resource_limits=expression.resource_limits)


def regexp_replace(value, pattern, replacement) -> ColumnExpression:
    """Apply the admitted fixed HTTP(S) domain pattern and capture replacement."""
    return _call("REGEXP_REPLACE", _column(value), lit(pattern), lit(replacement))


def url_domain(value) -> ColumnExpression:
    """Return the native URL-domain extraction expression."""
    return _column(value).url_domain()


def regex_domain(value) -> ColumnExpression:
    """Apply the fixed HTTP(S) domain-replacement expression."""
    return _column(value).regex_domain()


def date_add_days(value, days) -> ColumnExpression:
    """Return a scoped Date32 day-add expression."""
    return _column(value).date_add_days(days)


def date_sub_days(value, days) -> ColumnExpression:
    """Return a scoped Date32 day-subtract expression."""
    return _column(value).date_sub_days(days)


def timestamp_add_seconds(value, seconds) -> ColumnExpression:
    """Return a scoped timestamp second-add expression."""
    return _column(value).timestamp_add_seconds(seconds)


def timestamp_sub_seconds(value, seconds) -> ColumnExpression:
    """Return a scoped timestamp second-subtract expression."""
    return _column(value).timestamp_sub_seconds(seconds)


def date_diff_days(value, other) -> ColumnExpression:
    """Return a scoped Date32 day-difference expression."""
    return _column(value).date_diff_days(other)


def timestamp_diff_seconds(value, other) -> ColumnExpression:
    """Return a scoped timestamp second-difference expression."""
    return _column(value).timestamp_diff_seconds(other)


def date_year(value) -> ColumnExpression:
    """Return a scoped Date32 year-extract expression."""
    return _column(value).date_year()


def date_month(value) -> ColumnExpression:
    """Return a scoped Date32 month-extract expression."""
    return _column(value).date_month()


def date_day(value) -> ColumnExpression:
    """Return a scoped Date32 day-of-month extract expression."""
    return _column(value).date_day()


def timestamp_year(value) -> ColumnExpression:
    """Return a scoped timestamp year-extract expression."""
    return _column(value).timestamp_year()


def timestamp_month(value) -> ColumnExpression:
    """Return a scoped timestamp month-extract expression."""
    return _column(value).timestamp_month()


def timestamp_day(value) -> ColumnExpression:
    """Return a scoped timestamp day-of-month extract expression."""
    return _column(value).timestamp_day()


def timestamp_hour(value) -> ColumnExpression:
    """Return a scoped timestamp hour-extract expression."""
    return _column(value).timestamp_hour()


def timestamp_minute(value) -> ColumnExpression:
    """Return a scoped timestamp minute-extract expression."""
    return _column(value).timestamp_minute()


def timestamp_second(value) -> ColumnExpression:
    """Return a scoped timestamp second-extract expression."""
    return _column(value).timestamp_second()


def extract_minute(value) -> ColumnExpression:
    """Extract a minute from integer epoch seconds or timestamp text."""
    return _column(value).extract_minute()


def date_trunc_minute(value) -> ColumnExpression:
    """Truncate integer epoch seconds or timestamp text to the minute."""
    return _column(value).date_trunc_minute()


def json_extract(value, path) -> ColumnExpression:
    """Extract JSON using a native literal path."""
    return _call("JSON_EXTRACT", _column(value), lit(path))


def strptime(value, format) -> ColumnExpression:
    """Parse timestamp text using a native literal format."""
    return _call("STRPTIME", _column(value), lit(format))


def try_strptime(value, format) -> ColumnExpression:
    """Parse timestamp text with a native literal format, yielding null on parse failure."""
    return _call("TRY_STRPTIME", _column(value), lit(format))


def _offset_value(name, value, offset):
    if type(offset) is not int or not 1 <= offset <= 50_000:
        raise ValueError("LAG/LEAD window offset must be between 1 and 50000")
    return _call(name, _column(value), lit(offset))
