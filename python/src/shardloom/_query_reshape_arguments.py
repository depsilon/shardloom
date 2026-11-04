"""Deferred implementations for query reshape arguments.

Public classes, annotations and signatures remain in the canonical module.
Module-qualified lookups preserve its current shared helpers and patches.
"""

from __future__ import annotations

from . import query as _api


def _vortex_expression_project_columns_from_payload(payload):
    try:
        decoded = _api.json.loads(payload)
    except _api.json.JSONDecodeError:
        return None
    if not isinstance(decoded, _api.Mapping):
        return None
    columns = decoded.get("columns")
    if isinstance(columns, str):
        values = tuple(column.strip() for column in columns.split(",") if column.strip())
    elif _api._is_non_string_sequence(columns):
        values = tuple(str(column).strip() for column in columns if str(column).strip())
    else:
        return None
    if not values or any(not _api._is_sql_identifier(column) for column in values):
        return None
    return values


def _vortex_melt_projection_columns_from_payload(payload):
    try:
        decoded = _api.json.loads(payload)
    except _api.json.JSONDecodeError:
        return None
    if not isinstance(decoded, _api.Mapping):
        return None
    id_columns = _api._json_column_tuple(decoded.get("id_columns"))
    value_columns = _api._json_column_tuple(decoded.get("value_columns"))
    if id_columns is None or value_columns is None or not value_columns:
        return None
    columns = (*id_columns, *value_columns)
    if any(not _api._is_sql_identifier(column) for column in columns):
        return None
    return columns


def _vortex_explode_projection_columns_from_payload(payload):
    try:
        decoded = _api.json.loads(payload)
    except _api.json.JSONDecodeError:
        return None
    if not isinstance(decoded, _api.Mapping):
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
        columns = _api._normalize_columns((raw_columns,))
    except (TypeError, ValueError):
        return None
    element_field = decoded.get("element_field") or decoded.get("field")
    if element_field is not None and (
        not isinstance(element_field, str) or not _api._is_sql_identifier(element_field)
    ):
        return None
    if not columns or any(not _api._is_sql_identifier(column) for column in columns):
        return None
    return tuple(columns)


def _vortex_explode_projection_payload(columns):
    if not columns:
        return None
    if len(columns) == 1:
        dotted = _api._split_dotted_field_accessor(columns[0])
        if dotted is not None:
            source_column, element_field = dotted
            return _api.json.dumps(
                {
                    "column": source_column,
                    "element_field": element_field,
                    "output_column": element_field,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
    if any(not _api._is_sql_identifier(column) for column in columns):
        return None
    payload: dict[str, object]
    if len(columns) == 1:
        payload = {"column": columns[0]}
    else:
        payload = {"explode_columns": list(columns)}
    return _api.json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _vortex_pivot_projection_columns_from_payload(payload):
    try:
        decoded = _api.json.loads(payload)
    except _api.json.JSONDecodeError:
        return None
    if not isinstance(decoded, _api.Mapping):
        return None
    index_column = decoded.get("index_column") or decoded.get("index")
    pivot_column = decoded.get("pivot_column") or decoded.get("columns")
    value_column = decoded.get("value_column") or decoded.get("values")
    columns = (index_column, pivot_column, value_column)
    if not all(isinstance(column, str) and _api._is_sql_identifier(column) for column in columns):
        return None
    if len(set(columns)) != len(columns):
        return None
    aggregate = str(decoded.get("aggregate") or decoded.get("aggfunc") or "").strip().lower()
    if aggregate not in {"first", "first_unique", "sum", "count", "mean", "min", "max"}:
        return None
    return tuple(columns)  # type: ignore[return-value]


def _vortex_pivot_projection_payload(*, index, columns, values, aggregate, kwargs):
    if index is None or columns is None or values is None:
        return None
    try:
        index_columns = _api._normalize_columns((index,))
        pivot_columns = _api._normalize_columns((columns,))
        value_columns = _api._normalize_columns((values,))
    except (TypeError, ValueError):
        return None
    if len(index_columns) != 1 or len(pivot_columns) != 1 or len(value_columns) != 1:
        return None
    index_column = index_columns[0]
    pivot_column = pivot_columns[0]
    value_column = value_columns[0]
    if (
        not _api._is_sql_identifier(index_column)
        or not _api._is_sql_identifier(pivot_column)
        or not _api._is_sql_identifier(value_column)
        or len({index_column, pivot_column, value_column}) != 3
    ):
        return None
    normalized_aggregate = aggregate.strip().lower().replace("-", "_")
    policy_kwargs = _api._normalize_pivot_policy_kwargs(kwargs)
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
    return _api.json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _normalize_pivot_policy_kwargs(kwargs):
    allowed = {"fill_value", "dropna", "margins", "margins_name"}
    if any(key not in allowed for key in kwargs):
        return None
    policy: dict[str, object] = {}
    if "fill_value" in kwargs:
        fill_value = _api._json_scalar_policy_value(kwargs["fill_value"])
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
        margins_name = _api._require_non_empty("pivot_table margins_name", kwargs["margins_name"])
        policy["margins_name"] = margins_name
    return policy


def _normalize_pivot_table_single_aggregate(aggfunc, *, values):
    if aggfunc is None:
        return "mean"
    if isinstance(aggfunc, str):
        return _api._require_non_empty("pivot_table aggfunc", aggfunc)
    if isinstance(aggfunc, _api.Mapping):
        if len(aggfunc) != 1 or values is None:
            return None
        try:
            value_columns = _api._normalize_columns((values,))
        except (TypeError, ValueError):
            return None
        if len(value_columns) != 1:
            return None
        raw_key, raw_value = next(iter(aggfunc.items()))
        if str(raw_key).strip() != value_columns[0]:
            return None
        return _api._normalize_pivot_table_single_aggregate(raw_value, values=values)
    if _api._is_non_string_sequence(aggfunc):
        if len(aggfunc) != 1:
            return None
        return _api._normalize_pivot_table_single_aggregate(aggfunc[0], values=values)
    return None


def _json_column_tuple(value):
    if isinstance(value, str):
        columns = tuple(column.strip() for column in value.split(",") if column.strip())
    elif _api._is_non_string_sequence(value):
        columns = tuple(str(column).strip() for column in value if str(column).strip())
    else:
        return None
    if any(not _api._is_sql_identifier(column) for column in columns):
        return None
    return columns


def _vortex_melt_projection_payload(*, id_vars, value_vars, var_name, value_name, kwargs):
    if kwargs or value_vars is None:
        return None
    try:
        id_columns = _api._normalize_optional_columns(id_vars)
        value_columns = _api._normalize_columns((value_vars,))
    except (TypeError, ValueError):
        return None
    variable_column = "variable" if var_name is None else str(var_name).strip()
    value_column = "value" if value_name is None else str(value_name).strip()
    if (
        not value_columns
        or not variable_column
        or not value_column
        or variable_column == value_column
        or any(not _api._is_sql_identifier(column) for column in id_columns)
        or any(not _api._is_sql_identifier(column) for column in value_columns)
        or not _api._is_sql_identifier(variable_column)
        or not _api._is_sql_identifier(value_column)
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
    return _api.json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _vortex_rolling_window_columns_from_payload(payload):
    try:
        decoded = _api.json.loads(payload)
    except _api.json.JSONDecodeError:
        return None
    if not isinstance(decoded, _api.Mapping):
        return None
    source_column = decoded.get("source_column") or decoded.get("column") or decoded.get("on")
    if not isinstance(source_column, str) or not _api._is_sql_identifier(source_column):
        return None
    output_column = decoded.get("output_column") or decoded.get("alias")
    if not isinstance(output_column, str) or not _api._is_sql_identifier(output_column):
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
        parsed_window = _api._normalize_positive_int("rolling window", window_size)
        parsed_min_periods = (
            parsed_window
            if min_periods is None
            else _api._normalize_positive_int("rolling min_periods", min_periods)
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


def _vortex_rolling_window_payload(column, *, output_column, window_size, min_periods, aggregate, center):
    source_column = _api._normalize_expression_column(column)
    if "." in source_column:
        return None
    output = (
        _api._normalize_output_column_name(output_column)
        if output_column is not None
        else _api._normalize_output_column_name(f"{source_column}_rolling_{aggregate}")
    )
    if output == source_column:
        return None
    normalized_window = _api._normalize_positive_int("rolling window", window_size)
    normalized_min_periods = _api._normalize_positive_int("rolling min_periods", min_periods)
    normalized_aggregate = aggregate.strip().lower()
    if normalized_min_periods > normalized_window or normalized_aggregate not in {
        "sum",
        "mean",
        "count",
        "min",
        "max",
    }:
        return None
    return _api.json.dumps(
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
