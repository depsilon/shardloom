# SPDX-License-Identifier: Apache-2.0
"""Explicitly shared and bounded recursive workloads with a finite frontier."""

from __future__ import annotations

from dataclasses import replace
from .specification import Relation


def reused(source, threshold):
    """All rows plus a second consumer selecting high-valued rows."""
    source.col("value")
    if type(threshold) is not int:
        raise ValueError("shared consumer threshold must be an integer")
    return Relation("materialized_case", source.schema, (source,), (threshold,))


def recursive(source, step, depth):
    """Duplicate-preserving bounded expansion of each real source seed."""
    if not 1 <= depth <= 12 or type(step) is not int or step == 0:
        raise ValueError("invalid bounded recurrence")
    source.col("value")
    # Recursive frontier fields have a nullable schema even when every
    # concrete anchor is valid (the public recursive-composition contract).
    schema = tuple((name, replace(dtype, nullable=True)) for name, dtype in source.schema)
    return Relation("recursive_case", schema, (source,), (step, depth))


def sql(node, children):
    names = ",".join(name for name, _ in node.schema)
    if node.op == "materialized_case":
        return (
            f"WITH shared AS MATERIALIZED ({children[0]}) "
            f"SELECT {names} FROM shared UNION ALL SELECT {names} FROM shared "
            f"WHERE value > {node.args[0]}"
        )
    step, depth = node.args
    updates = ",".join(f"value + ({step})" if name == "value" else name for name, _ in node.schema)
    return (
        f"WITH RECURSIVE steps({names},generation) AS ("
        f"SELECT {names},0 AS generation FROM ({children[0]}) AS seed UNION ALL "
        f"SELECT {updates},generation+1 FROM steps WHERE generation < {depth}) "
        f"SELECT {names} FROM steps"
    )


def polars_plan(node, children, pl):
    source = children[0].cache()
    if node.op == "materialized_case":
        return pl.concat([source, source.filter(pl.col("value") > node.args[0])])
    step, depth = node.args
    # Exactly the declared number of frontiers, each derived from the original
    # data. Polars has no recursive CTE operator in this adapter. Record this
    # bounded expansion as a different physical implementation of the relation.
    frontiers = [source]
    for _ in range(depth):
        frontiers.append(frontiers[-1].with_columns((pl.col("value") + step).alias("value")))
    return pl.concat(frontiers)
