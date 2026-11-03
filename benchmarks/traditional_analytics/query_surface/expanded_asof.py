# SPDX-License-Identifier: Apache-2.0
"""Bounded as-of query declaration and independent reference oracle.

This is benchmark-only workload description. Polars is an external comparison
surface and is never used to execute ShardLoom work.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right

from shardloom._result_schema import ResultType
from .specification import Relation, identifier


def asof(
    left: Relation,
    right: Relation,
    *,
    left_key: str,
    right_key: str,
    direction: str,
    exact: bool,
    tolerance: int | None,
) -> Relation:
    """Declare a backward/forward match from facts to a sparse calendar."""
    if not isinstance(left, Relation) or not isinstance(right, Relation):
        raise TypeError("asof inputs must be typed Relations")
    identifier(left_key)
    identifier(right_key)
    if direction not in {"backward", "forward"}:
        raise ValueError("as-of direction must be backward or forward")
    if type(exact) is not bool:
        raise TypeError("exact must be a bool")
    if tolerance is not None and (type(tolerance) is not int or tolerance < 0):
        raise ValueError("tolerance must be a nonnegative integer or None")
    left_schema, right_schema = dict(left.schema), dict(right.schema)
    if left_key not in left_schema or "row_id" not in left_schema:
        raise ValueError("left input must declare row_id and its as-of key")
    if right_key not in right_schema or "day_index" not in right_schema or "week_index" not in right_schema:
        raise ValueError("right input must declare its key, day_index and week_index")
    if left_schema["row_id"].name != "int64":
        raise ValueError("left row_id must be int64")
    if left_schema[left_key].name != "int64" or right_schema[right_key].name != "int64":
        raise ValueError("as-of keys must both be int64")
    if right_schema[right_key].nullable:
        raise ValueError("right as-of key must be nonnullable")
    if right_schema["day_index"].name != "int64" or right_schema["week_index"].name != "int64":
        raise ValueError("right day_index and week_index must be int64")
    return Relation(
        "asof_case",
        (
            ("row_id", left_schema["row_id"]),
            ("matched_day_index", ResultType("int64", True)),
            ("week_index", ResultType("int64", True)),
        ),
        (left, right),
        (left_key, right_key, direction, exact, tolerance),
    )


def _declaration(node):
    if (
        not isinstance(node, Relation)
        or node.op != "asof_case"
        or len(node.inputs) != 2
        or len(node.args) != 5
    ):
        raise ValueError("expected a typed asof_case declaration")
    left_key, right_key, direction, exact, tolerance = node.args
    if direction not in {"backward", "forward"} or type(exact) is not bool:
        raise ValueError("invalid as-of declaration")
    return left_key, right_key, direction, exact, tolerance


def sql(node: Relation, children: tuple[str, str]) -> str:
    """Render a true ShardLoom SQL ASOF LEFT JOIN declaration."""
    left_key, right_key, direction, exact, tolerance = _declaration(node)
    if len(children) != 2:
        raise ValueError("as-of SQL declaration needs two child statements")
    operator = ">" if direction == "backward" else "<"
    if exact:
        operator += "="
    bound = "" if tolerance is None else f" TOLERANCE {tolerance}"
    return (
        "SELECT f.row_id AS row_id, d.day_index AS matched_day_index, "
        "d.week_index AS week_index FROM (" + children[0] + ") AS f "
        "ASOF LEFT JOIN (" + children[1] + ") AS d MATCH_CONDITION "
        f"(f.{left_key} {operator} d.{right_key}){bound}"
    )


def _columns(right_key):
    return [
        ("left", "row_id", "row_id"),
        ("right", "day_index", "matched_day_index"),
        ("right", "week_index", "week_index"),
    ]


def native_plan(node: Relation, children: tuple[object, object], native):
    """Declare the benchmark through the native typed Relation API."""
    left_key, right_key, direction, exact, tolerance = _declaration(node)
    if len(children) != 2:
        raise ValueError("as-of native declaration needs two child relations")
    return children[0].join_asof(
        children[1],
        (left_key, right_key),
        _columns(right_key),
        strategy=direction,
        tolerance=tolerance,
        allow_exact_matches=exact,
    )


def dataframe(node: Relation, children: tuple[object, object], sl):
    """Declare the benchmark through ShardLoom's public DataFrame API."""
    left_key, right_key, direction, exact, tolerance = _declaration(node)
    if len(children) != 2:
        raise ValueError("as-of DataFrame declaration needs two child frames")
    return (
        children[0]
        .join_asof(
            children[1],
            left_on=left_key,
            right_on=right_key,
            strategy=direction,
            tolerance=tolerance,
            allow_exact_matches=exact,
        )
        .select("f.row_id AS row_id", "d.day_index AS matched_day_index", "d.week_index AS week_index")
    )


def polars_plan(node: Relation, children: tuple[object, object], pl):
    """Build a Polars lazy sorted as-of join, preserving null-left rows."""
    left_key, right_key, direction, exact, tolerance = _declaration(node)
    if len(children) != 2:
        raise ValueError("as-of Polars declaration needs two lazy frames")
    left, right = children
    left_valid = left.filter(pl.col(left_key).is_not_null()).sort(left_key)
    # Rename the selected right key before joining. Polars may coalesce equal
    # join-key columns; the alias keeps the matched key observable either way.
    right_sorted = right.select(pl.col(right_key).alias("matched_day_index"), pl.col("week_index")).sort(
        "matched_day_index"
    )
    matched = left_valid.join_asof(
        right_sorted,
        left_on=left_key,
        right_on="matched_day_index",
        strategy=direction,
        tolerance=tolerance,
        allow_exact_matches=exact,
    ).select(pl.col("row_id"), pl.col("matched_day_index"), pl.col("week_index"))
    null_left = left.filter(pl.col(left_key).is_null()).select(
        pl.col("row_id"),
        pl.lit(None, dtype=pl.Int64).alias("matched_day_index"),
        pl.lit(None, dtype=pl.Int64).alias("week_index"),
    )
    return pl.concat((matched, null_left))


def reference_rows(node: Relation, left_rows, right_rows):
    """Return independent expected rows using sorted integer keys and bisection."""
    left_key, right_key, direction, exact, tolerance = _declaration(node)
    right_index = {}
    for row in right_rows:
        key = row[right_key]
        if type(key) is not int or not 0 <= key <= 730:
            raise ValueError("right calendar key must be an integer in 0..730")
        if key in right_index:
            raise ValueError("right calendar key must be unique")
        right_index[key] = row
    keys = sorted(right_index)
    result = []
    for row in left_rows:
        key = row[left_key]
        selected = None
        if key is not None:
            if type(key) is not int or not -(1 << 63) <= key < (1 << 63):
                raise ValueError("left as-of key must be a signed int64 or None")
            if direction == "backward":
                pos = (bisect_right(keys, key) if exact else bisect_left(keys, key)) - 1
            else:
                pos = bisect_left(keys, key) if exact else bisect_right(keys, key)
            if pos >= 0 and pos < len(keys):
                candidate = keys[pos]
                distance = abs(candidate - key)
                if tolerance is None or distance <= tolerance:
                    selected = right_index[candidate]
        result.append(
            {
                "row_id": row["row_id"],
                "matched_day_index": None if selected is None else selected["day_index"],
                "week_index": None if selected is None else selected["week_index"],
            }
        )
    return result
