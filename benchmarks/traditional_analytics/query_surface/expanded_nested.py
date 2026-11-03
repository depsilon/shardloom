# SPDX-License-Identifier: Apache-2.0
"""Bounded explode and scalar-membership declarations for corpus expansion.

These are benchmark adapter declarations and independent expected-value
helpers. They do not add runtime operators or external-engine execution paths.
"""

from __future__ import annotations

import json
from typing import Iterable, Mapping

from shardloom._result_schema import ResultType

from .specification import Relation, identifier


def explode(source: Relation) -> Relation:
    """Declare expansion of one nullable ``list<int64>`` ``items`` column."""
    if not isinstance(source, Relation):
        raise TypeError("explode source must be a Relation")
    schema = dict(source.schema)
    if "row_id" not in schema:
        raise ValueError("explode source must include row_id")
    dtype = schema.get("items")
    if (
        dtype is None
        or dtype.name != "list"
        or not dtype.nullable
        or len(dtype.parameters) != 1
        or dtype.parameters[0].name != "int64"
    ):
        raise ValueError("explode source items must be a nullable list<int64>")
    output = tuple(
        (name, ResultType("int64", True) if name == "items" else field_type)
        for name, field_type in source.schema
    )
    return Relation("explode_case", output, (source,), ())


def membership(left: Relation, right: Relation, *, left_key: str, right_key: str, negated: bool) -> Relation:
    """Declare SQL ``WHERE left_key [NOT] IN (SELECT right_key ...)``."""
    if not isinstance(left, Relation) or not isinstance(right, Relation):
        raise TypeError("membership inputs must be typed Relations")
    identifier(left_key)
    identifier(right_key)
    left_schema, right_schema = dict(left.schema), dict(right.schema)
    if "row_id" not in left_schema or left_key not in left_schema or right_key not in right_schema:
        raise ValueError("membership requires left row_id and both declared key columns")
    if left_schema[left_key].name != "int64" or right_schema[right_key].name != "int64":
        raise ValueError("membership keys must both be int64")
    if type(negated) is not bool:
        raise TypeError("negated must be a bool")
    return Relation("membership_case", left.schema, (left, right), (left_key, right_key, negated))


def _explode_declaration(node: Relation) -> Relation:
    if (
        not isinstance(node, Relation)
        or node.op != "explode_case"
        or len(node.inputs) != 1
        or node.args != ()
    ):
        raise ValueError("expected an explode_case relation")
    return node.inputs[0]


def _membership_declaration(node: Relation):
    if (
        not isinstance(node, Relation)
        or node.op != "membership_case"
        or len(node.inputs) != 2
        or len(node.args) != 3
    ):
        raise ValueError("expected a membership_case relation")
    left_key, right_key, negated = node.args
    if type(negated) is not bool:
        raise ValueError("invalid membership declaration")
    return left_key, right_key, negated


def sql(node: Relation, children: tuple[str, ...]) -> str:
    """Render the exact native EXPLODE form or a true IN-subquery predicate."""
    if node.op == "explode_case":
        _explode_declaration(node)
        if len(children) != 1:
            raise ValueError("explode SQL declaration needs one child statement")
        payload = json.dumps({"column": "items"}, separators=(",", ":"), sort_keys=True)
        return f"SELECT * FROM EXPLODE(({children[0]}), '{payload}') AS explode_case"
    left_key, right_key, negated = _membership_declaration(node)
    if len(children) != 2:
        raise ValueError("membership SQL declaration needs two child statements")
    projection = ",".join(f"l.{name}" for name, _ in node.schema)
    operator = "NOT IN" if negated else "IN"
    return (
        f"SELECT {projection} FROM ({children[0]}) AS l WHERE l.{left_key} {operator} "
        f"(SELECT r.{right_key} FROM ({children[1]}) AS r)"
    )


def native_plan(node: Relation, children: tuple[object, ...], native):
    """Build the declared operation through ShardLoom's typed native Relation API."""
    if node.op == "explode_case":
        _explode_declaration(node)
        if len(children) != 1:
            raise ValueError("explode native declaration needs one child relation")
        options = json.dumps({"column": "items"}, separators=(",", ":"), sort_keys=True)
        return children[0].unary("explode", options)
    left_key, right_key, negated = _membership_declaration(node)
    if len(children) != 2:
        raise ValueError("membership native declaration needs two child relations")
    flag = "__membership_match"
    matched = (
        children[0]
        .subquery(
            children[1],
            flag,
            kind="in",
            keys=[(left_key, right_key)],
            negated=negated,
        )
        .filter(native.Expression.column(flag))
    )
    return matched.project([(name, native.Expression.column(name)) for name, _ in node.schema])


def dataframe(node: Relation, children: tuple[object, ...], sl):
    """Build the public DataFrame declarations using native plan operations."""
    if node.op == "explode_case":
        _explode_declaration(node)
        if len(children) != 1:
            raise ValueError("explode DataFrame declaration needs one child frame")
        return children[0].explode("items")
    left_key, right_key, negated = _membership_declaration(node)
    if len(children) != 2:
        raise ValueError("membership DataFrame declaration needs two child frames")
    # A native LazyFrame source binds its complete child relation to the client.
    predicate = sl.col(left_key).isin_source(children[1], right_key)
    if negated:
        predicate = ~predicate
    return children[0].filter(predicate).select(*(name for name, _ in node.schema))


def polars_plan(node: Relation, children: tuple[object, ...], pl):
    """Build benchmark-only Polars lazy plans with explicit SQL null behavior."""
    if node.op == "explode_case":
        _explode_declaration(node)
        if len(children) != 1:
            raise ValueError("explode Polars declaration needs one lazy frame")
        child = children[0]
        # Polars may emit an empty-list row as a null on explode. Remove empty
        # lists first; retain null lists, whose native/SQL contract is one NULL.
        nonempty_or_null = pl.col("items").is_null() | (pl.col("items").list.len() > 0)
        return child.filter(nonempty_or_null).explode("items")

    left_key, right_key, negated = _membership_declaration(node)
    if len(children) != 2:
        raise ValueError("membership Polars declaration needs two lazy frames")
    left, right = children
    matched_right = right.select(pl.col(right_key)).unique().with_columns(pl.lit(True).alias("__rhs_match"))
    joined = left.join(matched_right, left_on=left_key, right_on=right_key, how="left")
    summary = right.select(
        (pl.len() > 0).alias("__rhs_nonempty"),
        pl.col(right_key).is_null().any().fill_null(False).alias("__rhs_has_null"),
    )
    joined = joined.join(summary, how="cross")
    matched = pl.col("__rhs_match").fill_null(False)
    if negated:
        keep = (~pl.col("__rhs_nonempty")) | (
            pl.col(left_key).is_not_null() & ~matched & ~pl.col("__rhs_has_null")
        )
    else:
        keep = matched
    return joined.filter(keep).select(*(name for name, _ in node.schema))


def reference_rows(node: Relation, *input_iters: Iterable[Mapping[str, object]]):
    """Return independently computed explode rows or SQL WHERE membership rows."""
    if node.op == "explode_case":
        _explode_declaration(node)
        if len(input_iters) != 1:
            raise ValueError("explode reference needs one input iterator")
        result = []
        for source in input_iters[0]:
            values = source["items"]
            for value in [None] if values is None else values:
                row = dict(source)
                row["items"] = value
                result.append(row)
        return result

    left_key, right_key, negated = _membership_declaration(node)
    if len(input_iters) != 2:
        raise ValueError("membership reference needs two input iterators")
    left_rows, right_rows = list(input_iters[0]), list(input_iters[1])
    rhs_values = [row[right_key] for row in right_rows]
    if any(value is not None and type(value) is not int for value in rhs_values):
        raise ValueError("membership RHS values must be int64 or None")
    result = []
    for row in left_rows:
        value = row[left_key]
        if value is not None and type(value) is not int:
            raise ValueError("membership LHS values must be int64 or None")
        if value is not None and value in rhs_values:
            predicate = True
        elif not rhs_values:
            predicate = False
        elif value is None or any(candidate is None for candidate in rhs_values):
            predicate = None
        else:
            predicate = False
        if negated and predicate is not None:
            predicate = not predicate
        if predicate is True:
            result.append(dict(row))
    return result
