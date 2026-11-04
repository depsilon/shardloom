# SPDX-License-Identifier: Apache-2.0
"""Bounded, independently checkable window declarations for corpus expansion.

This module is an adapter packet, not a runtime provider. Each declaration
uses the public ShardLoom SQL, typed-relation, or DataFrame surface directly.
"""

from __future__ import annotations

from typing import Iterable, Mapping

from shardloom._result_schema import ResultType

from .specification import Relation


PARTITIONS = {"group_key", "category", "nullable_flag", "skew_key"}
ARGUMENTS = {"signed_value", "optional_value", "mostly_null"}
FUNCTIONS = {"row_number", "lag", "lead", "rolling_sum"}
PARTITION_TYPES = {
    "group_key": ResultType("int64", False),
    "category": ResultType("utf8", False),
    "nullable_flag": ResultType("bool", True),
    "skew_key": ResultType("int64", False),
}


def window(
    source: Relation, *, function: str, partition: str, argument: str, descending: bool, distance: int
) -> Relation:
    """Declare one deterministic per-partition window over unique ``row_id``.

    Input schemas encode the corpus contract: non-null Int64 row_id, one
    admitted partition, and one admitted Int64 argument. Nullable partition
    values form the ordinary SQL NULL partition.
    """
    if not isinstance(source, Relation):
        raise TypeError("window source must be a Relation")
    if not isinstance(function, str) or function not in FUNCTIONS:
        raise ValueError(f"unsupported window function: {function!r}")
    if not isinstance(partition, str) or partition not in PARTITIONS:
        raise ValueError(f"unsupported window partition: {partition!r}")
    if not isinstance(argument, str) or argument not in ARGUMENTS:
        raise ValueError(f"unsupported window argument: {argument!r}")
    if type(descending) is not bool:
        raise TypeError("descending must be a bool")
    if type(distance) is not int:
        raise TypeError("distance must be an integer")
    if function == "row_number":
        if distance != 0:
            raise ValueError("row_number requires distance=0")
    elif not 1 <= distance <= 16:
        raise ValueError(f"{function} requires distance in 1..16")

    schema = dict(source.schema)
    row_id = schema.get("row_id")
    if row_id != ResultType("int64", False):
        raise ValueError("window input requires non-null int64 row_id")
    if schema.get(partition) != PARTITION_TYPES[partition]:
        raise ValueError(f"window input has an incompatible partition column {partition!r}")
    if schema.get(argument) != ResultType("int64", argument != "signed_value"):
        raise ValueError(f"window input has an incompatible argument column {argument!r}")
    result_type = (
        ResultType("int64", False)
        if function == "row_number"
        else ResultType("float64", True)
        if function == "rolling_sum"
        else ResultType("int64", True)
    )
    return Relation(
        "window_case",
        (("row_id", row_id), ("value", result_type)),
        (source,),
        (function, partition, argument, descending, distance),
    )


def _parts(node: Relation):
    if not isinstance(node, Relation) or node.op != "window_case" or len(node.inputs) != 1:
        raise ValueError("expected a window_case relation")
    function, partition, argument, descending, distance = node.args
    return node.inputs[0], function, partition, argument, descending, distance


def sql(node: Relation, child_sql: str) -> str:
    """Render a standalone SELECT from an already-rendered child request."""
    _, function, partition, argument, descending, distance = _parts(node)
    order = "DESC" if descending else "ASC"
    if function == "row_number":
        expression = "ROW_NUMBER()"
        frame = ""
    elif function in {"lag", "lead"}:
        expression = f"{function.upper()}(q.{argument}, {distance})"
        frame = ""
    else:
        expression = f"SUM(q.{argument})"
        frame = f" ROWS BETWEEN {distance} PRECEDING AND CURRENT ROW"
    return (
        f"SELECT q.row_id AS row_id, {expression} OVER (PARTITION BY q.{partition} "
        f"ORDER BY q.row_id {order} NULLS LAST{frame}) AS value "
        f"FROM ({child_sql}) AS q"
    )


def native_plan(node: Relation, child, native):
    """Build the declared window with the public typed Relation API."""
    _, function, partition, argument, descending, distance = _parts(node)
    if function == "row_number":
        measure = native.WindowExpression(
            "row_number", "value", partition_by=[partition], order_by=[("row_id", descending, "last")]
        )
    elif function in {"lag", "lead"}:
        measure = native.WindowExpression(
            function,
            "value",
            source_column=argument,
            argument=distance,
            partition_by=[partition],
            order_by=[("row_id", descending, "last")],
        )
    else:
        frame = native.WindowFrame(
            "rows",
            ("preceding", native.FrameOffset.number(native.Expression.literal(distance))),
            ("current_row", None),
        )
        measure = native.WindowExpression(
            "sum",
            "value",
            source_column=argument,
            partition_by=[partition],
            order_by=[("row_id", descending, "last")],
            frame=frame,
        )
    expanded = child.window(["row_id", partition, argument], [measure])
    return expanded.project(
        [
            ("row_id", native.Expression.column("row_id")),
            ("value", native.Expression.column("value")),
        ]
    )


def dataframe(node: Relation, child, sl):
    """Build the matching DataFrame expression using public window APIs."""
    _, function, partition, argument, descending, distance = _parts(node)
    from shardloom import functions as F

    spec = sl.Window.partition_by(partition).order_by("row_id", descending=descending, nulls="last")
    if function == "row_number":
        value = sl.row_number().over(spec)
    elif function == "lag":
        value = F.lag(argument, distance).over(spec)
    elif function == "lead":
        value = F.lead(argument, distance).over(spec)
    else:
        value = F.sum(argument).over(spec.rows_between(-distance, 0))
    return child.select("row_id", value.alias("value"))


def polars_plan(node: Relation, child, pl):
    """Build a Polars lazy comparison plan, ordered by unique row_id first."""
    _, function, partition, argument, descending, distance = _parts(node)
    source = child.sort("row_id", descending=descending, nulls_last=True)
    if function == "row_number":
        value = pl.col("row_id").rank(method="ordinal", descending=descending).over(partition)
        output_type = pl.Int64
    elif function in {"lag", "lead"}:
        offset = distance if function == "lag" else -distance
        value = pl.col(argument).shift(offset).over(partition)
        output_type = pl.Int64
    else:
        value = pl.col(argument).rolling_sum(window_size=distance + 1, min_samples=1).over(partition)
        output_type = pl.Float64
    return source.select(pl.col("row_id"), value.cast(output_type).alias("value"))


def reference_rows(node: Relation, input_rows: Iterable[Mapping[str, object]]):
    """Independent standard-library reference with SQL NULL partitioning."""
    _, function, partition, argument, descending, distance = _parts(node)
    grouped: dict[object, list[Mapping[str, object]]] = {}
    for row in input_rows:
        grouped.setdefault(row[partition], []).append(row)
    result = []
    for rows in grouped.values():
        ordered = sorted(rows, key=lambda row: row["row_id"], reverse=descending)
        for index, row in enumerate(ordered):
            if function == "row_number":
                value = index + 1
            elif function == "lag":
                source_index = index - distance
                value = ordered[source_index][argument] if source_index >= 0 else None
            elif function == "lead":
                source_index = index + distance
                value = ordered[source_index][argument] if source_index < len(ordered) else None
            else:
                values = [
                    item[argument]
                    for item in ordered[max(0, index - distance) : index + 1]
                    if item[argument] is not None
                ]
                value = float(sum(values)) if values else None
            result.append({"row_id": row["row_id"], "value": value})
    return result
