"""Deferred implementations for query expression builders.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _grouping_frame(frame, kind, units, *, distinct):
    if not isinstance(distinct, bool):
        raise TypeError("distinct must be a Boolean")
    if kind == "GROUPING SETS" and not units:
        raise ValueError("grouping_sets requires at least one set; use [] for the total")
    keys: list[str] = []
    rendered: list[str] = []
    for unit in units:
        values = (unit,) if isinstance(unit, (str, _api.ColumnExpression)) else tuple(unit)
        normalized = tuple(dict.fromkeys(_api._normalize_columns(values))) if values else ()
        keys.extend(name for name in normalized if name not in keys)
        rendered.append("(" + ",".join(normalized) + ")" if kind == "GROUPING SETS" or len(normalized) != 1 else normalized[0])
    clause = ("DISTINCT " if distinct else "") + f"{kind}({','.join(rendered)})"
    return _api.GroupedLazyFrame(frame, tuple(keys), _api._predicate_sources(units), _api._predicate_limits(units), clause)


def typed_scalar_udf(udf_id, column):

    normalized_id = _api._require_non_empty("typed scalar UDF id", udf_id).strip()
    source_column = _api._normalize_expression_column(
        column.sql if isinstance(column, _api.ColumnExpression) else column
    )
    if normalized_id != "sl_fixture_double_i64":
        raise ValueError(
            "typed scalar UDF must be the admitted built-in sl_fixture_double_i64 fixture"
        )
    return _api.ColumnExpression(f"{source_column} * 2", _api._predicate_sources(column), resource_limits=_api._predicate_limits(column))


def row_in_source(columns, source, source_columns, *, source_alias, where, group_by, having, order_by, descending, limit):

    return _api._row_value_in_source_predicate(
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


def row_not_in_source(columns, source, source_columns, *, source_alias, where, group_by, having, order_by, descending, limit):

    return _api._row_value_in_source_predicate(
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


def any_source(column, comparison, source, source_column, *, source_alias, where, group_by, having, order_by, descending, limit):

    return _api._quantified_source_predicate(
        _api._normalize_expression_column(column),
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


def all_source(column, comparison, source, source_column, *, source_alias, where, group_by, having, order_by, descending, limit):

    return _api._quantified_source_predicate(
        _api._normalize_expression_column(column),
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


def exists_source(source, *, source_alias, select, where, group_by, having, order_by, descending, limit):

    return _api._exists_source_predicate(
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


def not_exists_source(source, *, source_alias, select, where, group_by, having, order_by, descending, limit):

    return _api._exists_source_predicate(
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


def _ranking_window_expression(function_name, *, order_by, partition_by, descending, alias):

    if order_by is None:
        if partition_by is not None or descending or alias != function_name.lower():
            raise ValueError("ranking options require order_by; otherwise use over(WindowSpec).alias(name)")
        return _api.ColumnExpression(f"{function_name}()")
    order_columns = _api._normalize_columns((order_by,))
    partition_columns = _api._normalize_optional_columns(partition_by)
    direction = "DESC" if descending else "ASC"
    order_clause = ",".join(f"{column} {direction}" for column in order_columns)
    partition_clause = (
        "" if not partition_columns else f"PARTITION BY {','.join(partition_columns)} "
    )
    output_alias = _api._normalize_output_column_name(alias)
    return _api.WindowExpression(
        f"{function_name}() OVER ({partition_clause}ORDER BY {order_clause}) AS {output_alias}",
        _api._predicate_sources(order_by, partition_by),
        resource_limits=_api._predicate_limits(order_by, partition_by),
    )


def concat(*parts):

    if len(parts) < 2:
        raise ValueError("concat requires at least two arguments")
    sql_parts: list[str] = []
    has_source_column = False
    for index, part in enumerate(parts):
        sql, is_source_column = _api._sql_string_function_text_arg(
            part, f"concat argument {index + 1}"
        )
        sql_parts.append(sql)
        has_source_column = has_source_column or is_source_column
    if not has_source_column:
        raise ValueError("concat requires at least one shardloom column expression")
    return _api.ColumnExpression(f"CONCAT({', '.join(sql_parts)})", _api._predicate_sources(parts), resource_limits=_api._predicate_limits(parts))


def struct(*columns):

    if len(columns) == 1 and _api._is_non_string_sequence(columns[0]):
        raw_columns = tuple(columns[0])
    else:
        raw_columns = columns
    if not raw_columns:
        raise ValueError("struct projection requires at least one source column")
    normalized = tuple(_api._normalize_expression_column(column) for column in raw_columns)
    if len(set(normalized)) != len(normalized):
        raise ValueError("struct projection source columns must be unique")
    return _api.ComplexProjectionExpression(f"STRUCT({', '.join(normalized)})")


def _source_kind_from_path(uri):
    suffix = _api._source_suffix_for_local_ref(uri)
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
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
    admitted = ".csv, .tsv, .json, .jsonl, .ndjson, .parquet, .arrow, .ipc, .feather, .avro, .orc, .vortex, .vtx, .vortex-manifest"
    raise ValueError(
        f"ShardLoom cannot infer a local source adapter for {uri!s}; "
        f"admitted local source extensions are {admitted}"
    )
