"""Deferred implementations for query expression methods.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def IntervalLiteral___post_init__(self):
    interval_value = _api._normalize_interval_integer(self.value)
    unit = _api._normalize_interval_unit(self.unit)
    multiplier = _api._INTERVAL_SECOND_MULTIPLIERS[unit]
    if _api.builtins.abs(interval_value * multiplier) > _api.MAX_TIMESTAMP_ARITHMETIC_SECONDS:
        raise ValueError(
            "interval literal admits absolute values within the scoped temporal arithmetic bound"
        )
    object.__setattr__(self, "value", interval_value)
    object.__setattr__(self, "unit", unit)


def ColumnExpression__numeric_binary(self, operator, value):
    rhs = (
        _api._parenthesize_numeric_operand(value.sql)
        if isinstance(value, _api.ColumnExpression)
        else _api._sql_numeric_literal(value)
    )
    return _api.ColumnExpression(
        f"{_api._parenthesize_numeric_operand(self.sql)} {operator} {rhs}",
        _api._predicate_sources(self, value),
        resource_limits=_api._predicate_limits(self, value),
    )


def ColumnExpression_substr(self, start, length):

    column = self.sql
    normalized_start = _api._normalize_substring_bound("substring start", start, minimum=1)
    normalized_length = _api._normalize_substring_bound(
        "substring length", length, minimum=0
    )
    return _api.ColumnExpression(
        f"SUBSTR({column}, {normalized_start}, {normalized_length})",
        self.source_bindings,
        resource_limits=self.resource_limits,
    )


def ColumnExpression_replace(self, needle, replacement):

    column = self.sql
    needle_literal = _api._sql_string_function_literal(
        "replace search literal", needle, allow_empty=False
    )
    replacement_literal = _api._sql_string_function_literal(
        "replace replacement literal", replacement, allow_empty=True
    )
    return _api.ColumnExpression(
        f"REPLACE({column}, {needle_literal}, {replacement_literal})",
        self.source_bindings,
        resource_limits=self.resource_limits,
    )


def ColumnExpression_isin_source(self, source, column, *, source_alias, where, group_by, having, order_by, descending, limit):

    source_column = _api._normalize_expression_column(column)
    source_ref = _api._sql_in_subquery_source(source, source_alias=source_alias)
    tail = _api._sql_in_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )
    return _api.PredicateExpression(
        f"{self.sql} IN (SELECT {source_column} FROM {source_ref}{tail})",
        _api._predicate_sources(self, source, where, having),
        resource_limits=_api._predicate_limits(self, source, where, having),
    )


def ColumnExpression_any_source(self, comparison, source, column, *, source_alias, where, group_by, having, order_by, descending, limit):

    predicate = _api._quantified_source_predicate(
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
    return _api.PredicateExpression(predicate.sql, _api._predicate_sources(self, predicate), resource_limits=_api._predicate_limits(self, predicate))


def ColumnExpression_all_source(self, comparison, source, column, *, source_alias, where, group_by, having, order_by, descending, limit):

    predicate = _api._quantified_source_predicate(
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
    return _api.PredicateExpression(predicate.sql, _api._predicate_sources(self, predicate), resource_limits=_api._predicate_limits(self, predicate))


def ColumnExpression_not_in_source(self, source, column, *, source_alias, where, group_by, having, order_by, descending, limit):

    source_column = _api._normalize_expression_column(column)
    source_ref = _api._sql_in_subquery_source(source, source_alias=source_alias)
    tail = _api._sql_in_subquery_tail(
        where=where,
        group_by=group_by,
        having=having,
        order_by=order_by,
        descending=descending,
        limit=limit,
    )
    return _api.PredicateExpression(
        f"{self.sql} NOT IN (SELECT {source_column} FROM {source_ref}{tail})",
        _api._predicate_sources(self, source, where, having),
        resource_limits=_api._predicate_limits(self, source, where, having),
    )


def ColumnExpression_temporal_bucket(self, every, unit, *, origin, month_end, ambiguity):

    if isinstance(every, bool) or not isinstance(every, int) or every <= 0:
        raise ValueError("temporal bucket spacing requires a positive integer")
    if origin is not None and (isinstance(origin, bool) or not isinstance(origin, int)):
        raise TypeError("temporal bucket origin requires integer ticks or None")
    return self._temporal_call(
        "temporal_bucket", str(every), _api._temporal_control_literal("unit", unit),
        "NULL" if origin is None else str(origin),
        _api._temporal_control_literal("month_end", month_end),
        _api._temporal_control_literal("ambiguity", ambiguity),
    )
