# SPDX-License-Identifier: Apache-2.0
"""Finite native optimization declarations with independent complete answers.

This module builds test intent and values only. It never imports a native engine
or derives expected results from an engine response.
"""
from collections import Counter

from .corpus import case
from .semantics import ResultType, TypedResult
from .specification import Measure, Order, Relation

I64 = ResultType("int64", False)
U64 = ResultType("uint64", False)
NULLABLE_I64 = ResultType("int64", True)
TEXT = ResultType("utf8", False)
SCHEMAS = {
    "counts": (("id", I64), ("key", I64), ("value", I64),
               ("optional", NULLABLE_I64), ("payload", TEXT)),
    "probe": (("id", I64), ("key", NULLABLE_I64)),
    "build": (("id", I64), ("key", NULLABLE_I64)),
    **{name: (("id", I64), ("x", NULLABLE_I64), ("y", NULLABLE_I64))
       for name in ("r", "s", "t")},
}


def tables():
    counts = [dict(id=n, key=n % 16, value=5 * n,
                   optional=None if n % 13 == 0 else n, payload=f"unused-{n}")
              for n in range(4096)]
    probe_keys = [7, 100, None, *range(10_000, 10_512)]
    build_keys = [*range(128), *([7] * 69), None]
    return {
        "counts": counts,
        "probe": [dict(id=n, key=value) for n, value in enumerate(probe_keys)],
        "build": [dict(id=n, key=value) for n, value in enumerate(build_keys)],
        # Each cyclic source must meet the existing 128-row admission floor.
        "r": [dict(id=n, x=x, y=y) for n, (x, y) in enumerate(
            [(0, 0), (1, 1), (2, 0), (3, 3), (None, 0), (0, None)] * 22)],
        "s": [dict(id=n, x=x, y=y) for n, (x, y) in enumerate(
            [(0, 1), (0, 1), (1, 2), (3, None), (None, 1)] * 26)],
        "t": [dict(id=n, x=x, y=y) for n, (x, y) in enumerate(
            [(0, 1), (0, 1), (1, 2), (2, 9), (3, None), (None, 1)] * 22)],
    }


def scan(name):
    return Relation("scan", SCHEMAS[name], args=(name,))


def equal(left, right):
    return left is not None and right is not None and left == right


def join_rows(left, right, kind):
    result, matched = [], set()
    for row in left:
        matches = [other for other in right if equal(row["key"], other["key"])]
        if kind in {"semi", "anti"}:
            if bool(matches) == (kind == "semi"):
                result.append({"left_id": row["id"]})
            continue
        for other in matches:
            matched.add(other["id"])
            result.append({"left_id": row["id"], "right_id": other["id"]})
        if not matches and kind in {"left", "full"}:
            result.append({"left_id": row["id"], "right_id": None})
    if kind in {"right", "full"}:
        result.extend({"left_id": None, "right_id": row["id"]}
                      for row in right if row["id"] not in matched)
    return result


def declarations():
    """Return (Case, independent TypedResult, activation requirements) tuples."""
    data, result = tables(), []

    def add(name, family, relation, rows, requirement, *, ordered=False):
        declaration = case("activation_" + name, family, relation, ordered=ordered)
        result.append((declaration, TypedResult(relation.schema, rows), requirement))

    facts = scan("counts")
    filtered = facts.where(facts.col("key").binary("eq", 3))
    projection = filtered.project(("id", filtered.col("id"))).order(Order("id"))
    add("narrow_predicate", "projection", projection,
        [{"id": row["id"]} for row in data["counts"] if row["key"] == 3],
        "narrow_scan", ordered=True)
    empty = facts.where(facts.col("id").binary("lt", -1))
    empty = empty.aggregate((), (Measure("count", None, "rows", U64),))
    add("pruned_count", "global_aggregation", empty, [{"rows": 0}], "pruned_count")
    metadata = facts.aggregate((), (Measure("count", None, "rows", U64),))
    add("metadata_count", "global_aggregation", metadata,
        [{"rows": len(data["counts"])}], "metadata_count")

    # The computed group expression makes this an ordinary shared Aggregate;
    # source-specific aggregate-kernel selection is an independent mechanism.
    projected = facts.project(("key", facts.col("key").binary("add", 0)),
                              ("value", facts.col("value")),
                              ("optional", facts.col("optional")))
    for name, argument, requirement, ordered in (
        ("count_column", "value", "count_partial", True),
        ("count_star", None, "count_partial", True),
        ("count_nullable", "optional", "count_excluded", True),
        ("count_source_order", "value", "count_excluded", False),
    ):
        aggregate = projected.aggregate(("key",),
            (Measure("count", projected.col(argument) if argument else None, "rows", U64),))
        if ordered:
            aggregate = aggregate.order(Order("key"))
        expected = Counter(row["key"] for row in data["counts"]
                           if argument is None or row[argument] is not None)
        add(name, "grouped_aggregation", aggregate,
            [{"key": key, "rows": count} for key, count in sorted(expected.items())],
            requirement, ordered=ordered)
    unique = facts.project(("key", facts.col("id").binary("add", 0)))
    unique = unique.aggregate(("key",), (Measure("count", None, "rows", U64),)).order(Order("key"))
    add("count_unique", "grouped_aggregation", unique,
        [{"key": row["id"], "rows": 1} for row in data["counts"]],
        "count_unique", ordered=True)

    probe, build = scan("probe"), scan("build")
    for kind in ("inner", "left", "right", "full", "semi", "anti"):
        columns = [("left", "id", "left_id")]
        if kind not in {"semi", "anti"}:
            columns.append(("right", "id", "right_id"))
        relation = probe.join(build, (("key", "key"),), columns, kind)
        add("join_" + kind, "joins", relation, join_rows(data["probe"], data["build"], kind),
            "filter_only" if kind in {"semi", "anti"} else "filter_and_skew")
    small = build.take(17)
    relation = probe.join(small, (("key", "key"),),
                          (("left", "id", "left_id"), ("right", "id", "right_id")))
    add("join_small", "joins", relation, join_rows(data["probe"], data["build"][:17], "inner"),
        "filter_small")

    inner = scan("s").join(scan("t"), (("y", "y"),),
                            (("left", "x", "witness_b"), ("right", "x", "witness_a")))
    for kind in ("semi", "anti"):
        relation = scan("r").join(inner, (("y", "witness_b"), ("x", "witness_a")),
                                  (("left", "id", "outer_id"),), kind)
        expected = []
        for row in data["r"]:
            witness = any(equal(row["y"], middle["x"]) and equal(row["x"], last["x"])
                          and equal(middle["y"], last["y"])
                          for middle in data["s"] for last in data["t"])
            if witness == (kind == "semi"):
                expected.append({"outer_id": row["id"]})
        add("cyclic_" + kind, "multiway_joins", relation, expected,
            "cyclic" if kind == "semi" else "cyclic_excluded")
    return result
