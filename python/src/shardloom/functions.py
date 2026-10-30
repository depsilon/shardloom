# SPDX-License-Identifier: Apache-2.0
"""Native expression declarations; Python never evaluates data operations.

Names, type/null rules and execution are admitted by the same SQL/expression
binder as context.sql(). Strings in column positions name columns; use lit()
for scalar values. Existing constructors are shared with the top-level API.
"""

from __future__ import annotations

import math
from collections.abc import Iterable

from .query import (
    _decimal_controls,
    ColumnExpression, PredicateExpression, all_source, any_source, array, case_when,
    col, column, count_distinct, dense_rank, exists_source,
    interval_days, interval_hours, interval_minutes, interval_seconds,
    not_exists_source, outer, rank,
    row_in, row_in_source, row_not_in, row_not_in_source, row_number, scalar_subquery,
    struct, typed_scalar_udf,
)

__all__ = (
    "grouping", "grouping_id",
    "decimal_rescale", "decimal_add", "decimal_subtract", "decimal_multiply", "decimal_divide",
    "abs", "all_source", "any_source", "array", "array_agg", "avg", "byte_length", "case_when",
    "cast", "catalog", "ceil", "coalesce", "col", "column", "concat", "contains", "count",
    "count_distinct", "date_add_days", "date_day", "date_diff_days", "date_month",
    "date_sub_days", "date_trunc_minute", "date_year", "dense_rank", "ends_with",
    "arg_max", "arg_min", "corr", "covar_pop", "covar_samp", "exists_source", "extract_minute", "first", "first_value", "floor", "from_base64", "interval_days",
    "interval_hours", "interval_minutes", "interval_seconds", "is_null", "is_not_null", "is_nan", "is_infinite", "is_finite", "signbit",
    "json_extract", "lag", "last_value", "lead", "left", "length", "list", "lit", "lower",
    "last", "max", "mean", "mean_distinct", "median", "min", "not_exists_source", "not_regex_match", "nth_value", "null_if",
    "outer", "rank", "regex_domain", "regex_match", "regexp_replace", "replace", "right", "round", "row_in",
    "row_in_source", "row_not_in", "row_not_in_source", "row_number", "scalar_subquery",
    "starts_with", "std", "stddev", "stddev_pop", "stddev_samp", "string_agg", "strptime", "struct", "substr", "substring", "sum", "timestamp_add_seconds",
    "timestamp_day", "timestamp_diff_seconds", "timestamp_hour", "timestamp_minute", "timestamp_month",
    "quantile_cont", "quantile_disc", "timestamp_second", "timestamp_sub_seconds", "timestamp_year", "try_cast", "try_strptime",
    "sum_distinct", "avg_distinct", "trim", "typed_scalar_udf", "unhex", "upper", "url_domain", "var", "var_pop", "var_samp", "variance",
)


def _column(value):
    return value if isinstance(value, ColumnExpression) else col(value)


def decimal_rescale(value, precision, scale, rounding) -> ColumnExpression:
    """Declare native decimal rescaling with explicit precision and rounding."""
    return _column(value).decimal_rescale(precision, scale, rounding)


def decimal_add(left, right, precision, scale, rounding) -> ColumnExpression:
    """Declare native decimal addition with explicit output controls."""
    return _column(left).decimal_add(_column(right), precision, scale, rounding)


def decimal_subtract(left, right, precision, scale, rounding) -> ColumnExpression:
    """Declare native decimal subtraction with explicit output controls."""
    return _column(left).decimal_subtract(_column(right), precision, scale, rounding)


def decimal_multiply(left, right, precision, scale, rounding) -> ColumnExpression:
    """Declare native decimal multiplication with explicit output controls."""
    return _column(left).decimal_multiply(_column(right), precision, scale, rounding)


def decimal_divide(left, right, precision, scale, rounding) -> ColumnExpression:
    """Declare native decimal division with explicit output controls."""
    return _column(left).decimal_divide(_column(right), precision, scale, rounding)


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


def grouping(*columns) -> ColumnExpression:
    """Declare the UInt64 grouping omission mask, with the last key in the low bit."""
    if not 1 <= len(columns) <= 64:
        raise ValueError("grouping requires between one and 64 grouping keys")
    return _call("GROUPING", *(_column(value) for value in columns))


def grouping_id(*columns) -> ColumnExpression:
    """Alias for the same grouping omission mask."""
    if not 1 <= len(columns) <= 64:
        raise ValueError("grouping_id requires between one and 64 grouping keys")
    return _call("GROUPING_ID", *(_column(value) for value in columns))


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


def _decimal_arguments(precision, scale, rounding):
    controls = (precision, scale, rounding)
    if all(value is None for value in controls):
        return ()
    if any(value is None for value in controls):
        raise ValueError("decimal output requires precision, scale and rounding together")
    return tuple(lit(value) for value in _decimal_controls(*controls))


def _aggregate(name, value, distinct: bool, precision=None, scale=None, rounding=None) -> ColumnExpression:
    if not isinstance(distinct, bool):
        raise TypeError("distinct must be a bool")
    expression = _column(value)
    arguments = (expression, *_decimal_arguments(precision, scale, rounding))
    result = _call(name, *arguments)
    if not distinct:
        return result
    return ColumnExpression(
        f"{name}(DISTINCT {', '.join(str(argument) for argument in arguments)})",
        result.source_bindings,
        resource_limits=result.resource_limits,
    )


def sum(value, *, distinct: bool = False, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare native SUM, ignoring nulls and optionally using exact distinct membership."""
    return _aggregate("SUM", value, distinct, precision, scale, rounding)


def sum_distinct(value, *, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare native SUM over distinct non-null values."""
    return sum(value, distinct=True, precision=precision, scale=scale, rounding=rounding)


def min(value) -> ColumnExpression:
    """Declare native MIN over non-null values."""
    return _call("MIN", _column(value))


def max(value) -> ColumnExpression:
    """Declare native MAX over non-null values."""
    return _call("MAX", _column(value))


def _normalize_aggregate_order_by(order_by) -> tuple[tuple[ColumnExpression, bool, str | None], ...]:
    if isinstance(order_by, (str, ColumnExpression)):
        entries = (order_by,)
    else:
        if not isinstance(order_by, Iterable):
            raise TypeError("order_by must be a column, expression, or iterable of order keys")
        entries = tuple(order_by)

    normalized = []
    for entry in entries:
        if isinstance(entry, (str, ColumnExpression)):
            normalized.append((_column(entry), False, None))
            continue
        if type(entry) is not tuple or len(entry) != 3:
            raise TypeError(
                "order_by entries must be columns/expressions or exact (column, descending, nulls) triples"
            )
        column, descending, nulls = entry
        if not isinstance(column, (str, ColumnExpression)):
            raise TypeError("aggregate order key column must be a string or ColumnExpression")
        if type(descending) is not bool:
            raise TypeError("aggregate order key direction must be bool")
        if nulls is not None and (type(nulls) is not str or nulls not in {"first", "last"}):
            raise ValueError("aggregate order key nulls must be None, 'first', or 'last'")
        normalized.append((_column(column), descending, nulls))
    return tuple(normalized)


def _ordered_aggregate(name, arguments, order_by) -> ColumnExpression:
    expressions = tuple(_column(value) for value in arguments)
    ordering = _normalize_aggregate_order_by(order_by)
    body = ",".join(str(value) for value in expressions)
    if ordering:
        terms = []
        for column, descending, nulls in ordering:
            term = f"{column} {'DESC' if descending else 'ASC'}"
            if nulls is not None:
                term += f" NULLS {nulls.upper()}"
            terms.append(term)
        body += " ORDER BY " + ",".join(terms)
    from .query import _predicate_limits, _predicate_sources
    dependencies = (*expressions, *(column for column, _, _ in ordering))
    return ColumnExpression(
        f"{name}({body})",
        _predicate_sources(dependencies),
        resource_limits=_predicate_limits(dependencies),
    )


def first(value, *, order_by=()) -> ColumnExpression:
    """Select the first value, retaining NULL; equal order keys follow input order."""
    return _ordered_aggregate("FIRST", (value,), order_by)


def last(value, *, order_by=()) -> ColumnExpression:
    """Select the last value, retaining NULL; equal order keys follow input order."""
    return _ordered_aggregate("LAST", (value,), order_by)


def arg_min(value, key, *, order_by=()) -> ColumnExpression:
    """Select the value at the minimum key, skipping NULL pairs; ties use declared then input order."""
    return _ordered_aggregate("ARG_MIN", (value, key), order_by)


def arg_max(value, key, *, order_by=()) -> ColumnExpression:
    """Select the value at the maximum key, skipping NULL pairs; ties use declared then input order."""
    return _ordered_aggregate("ARG_MAX", (value, key), order_by)


def array_agg(value, *, order_by=()) -> ColumnExpression:
    """Collect original typed values, including NULLs and duplicates; an empty domain yields NULL."""
    return _ordered_aggregate("ARRAY_AGG", (value,), order_by)


list = array_agg


def string_agg(value, separator: str, *, order_by=()) -> ColumnExpression:
    """Join non-NULL UTF-8 values with a literal separator; an empty domain yields NULL."""
    if not isinstance(separator, str):
        raise TypeError("string_agg separator must be a literal string")
    return _ordered_aggregate("STRING_AGG", (value, lit(separator)), order_by)


def median(value, *, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare continuous median (quantile 0.5) over non-null values."""
    return _call("MEDIAN", _column(value), *_decimal_arguments(precision, scale, rounding))


def _quantile(name, value, level, precision=None, scale=None, rounding=None) -> ColumnExpression:
    if type(level) not in (int, float):
        raise TypeError("quantile level must be an int or float")
    try:
        level = float(level)
    except OverflowError as error:
        raise ValueError("quantile level must be finite and between 0 and 1 inclusive") from error
    if not math.isfinite(level) or not 0 <= level <= 1:
        raise ValueError("quantile level must be finite and between 0 and 1 inclusive")
    if level == 0.0:
        level = 0.0
    return _call(name, _column(value), lit(level), *_decimal_arguments(precision, scale, rounding))


def quantile_cont(value, level, *, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare continuous quantile with linear interpolation over ordered non-null values."""
    return _quantile("QUANTILE_CONT", value, level, precision, scale, rounding)


def quantile_disc(value, level) -> ColumnExpression:
    """Declare discrete quantile returning an original typed selected value."""
    return _quantile("QUANTILE_DISC", value, level)


def avg(value, *, distinct: bool = False, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare native AVG, ignoring nulls and optionally using exact distinct membership."""
    return _aggregate("AVG", value, distinct, precision, scale, rounding)


mean = avg


def avg_distinct(value, *, precision=None, scale=None, rounding=None) -> ColumnExpression:
    """Declare native AVG over distinct non-null values."""
    return avg(value, distinct=True, precision=precision, scale=scale, rounding=rounding)


mean_distinct = avg_distinct


def var_pop(value) -> ColumnExpression:
    """Declare population variance over primitive numeric values, ignoring nulls; a singleton yields zero."""
    return _call("VAR_POP", _column(value))


def var_samp(value) -> ColumnExpression:
    """Declare sample variance over primitive numeric values, ignoring nulls; fewer than two yields null."""
    return _call("VAR_SAMP", _column(value))


variance = var_samp
var = var_samp


def stddev_pop(value) -> ColumnExpression:
    """Declare population standard deviation over primitive numeric values, ignoring nulls; a singleton yields zero."""
    return _call("STDDEV_POP", _column(value))


def stddev_samp(value) -> ColumnExpression:
    """Declare sample standard deviation over primitive numeric values, ignoring nulls; fewer than two yields null."""
    return _call("STDDEV_SAMP", _column(value))


stddev = stddev_samp
std = stddev_samp


def covar_pop(left, right) -> ColumnExpression:
    """Declare population covariance for primitive numeric pairs, dropping rows where either input is null; a singleton yields zero."""
    return _call("COVAR_POP", _column(left), _column(right))


def covar_samp(left, right) -> ColumnExpression:
    """Declare sample covariance for primitive numeric pairs, dropping rows where either input is null; fewer than two yields null."""
    return _call("COVAR_SAMP", _column(left), _column(right))


def corr(left, right) -> ColumnExpression:
    """Declare correlation for primitive numeric pairs, dropping rows where either input is null; zero variance yields null."""
    return _call("CORR", _column(left), _column(right))


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


def is_nan(value) -> PredicateExpression:
    """Classify floating NaN values, propagating NULL validity."""
    return _column(value).is_nan()


def is_infinite(value) -> PredicateExpression:
    """Classify either floating infinity, propagating NULL validity."""
    return _column(value).is_infinite()


def is_finite(value) -> PredicateExpression:
    """Classify finite floating values, propagating NULL validity."""
    return _column(value).is_finite()


def signbit(value) -> PredicateExpression:
    """Observe a floating sign bit, including zero and NaN, with NULL propagation."""
    return _column(value).signbit()


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
