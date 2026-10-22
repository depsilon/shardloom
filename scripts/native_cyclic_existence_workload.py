#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Deterministic sources and independent references for a finite join investigation.

Python constructs fixtures and correctness references only. ``plan`` returns a
typed declaration; the shared native runtime must perform every measured query.
"""
from collections import defaultdict
import hashlib
import json


CASES = (
    "abundant", "rare", "absent", "skew_wide", "null_duplicates",
    "semi_small", "empty_inner", "binary_semi", "binary_anti",
    "star_control", "binary_control", "cyclic_dense", "cyclic_small",
)
TARGETS = frozenset(CASES[:4])
EXISTENCE = frozenset(CASES[:7])
GRANT = 128 << 20


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode()


def digest_rows(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(canonical(row) + b"\n")
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def sources(case):
    if case not in CASES:
        raise ValueError("unknown cyclic-existence fixture")
    if case in {"abundant", "rare", "absent", "skew_wide", "empty_inner"}:
        r = [(n % 64, 7 if case == "skew_wide" else n // 64) for n in range(4096)]
        s = ([(7, n % 64) for n in range(8192)] if case == "skew_wide"
             else [(n % 64, n // 64) for n in range(4096)])
        t = [(n % 64 + (1024 if case == "absent" or (case == "rare" and n // 64 < 63) else 0),
              n // 64) for n in range(4096)]
        if case == "empty_inner":
            s = []
    elif case == "null_duplicates":
        r = [((n // 2) % 32, (n // 64) % 8) for n in range(512)]
        s = [((n // 2) % 8, (n // 16) % 32) for n in range(512)]
        t = [((n // 4) % 32, (7 * ((n // 2) % 2) + n // 4) % 32) for n in range(128)]
        r = [(None if n % 37 == 0 else x, None if n % 41 == 0 else y) for n, (x, y) in enumerate(r)]
        s = [(None if n % 43 == 0 else x, None if n % 47 == 0 else y) for n, (x, y) in enumerate(s)]
        t = [(None if n % 53 == 0 else x, None if n % 59 == 0 else y) for n, (x, y) in enumerate(t)]
    elif case in {"semi_small", "cyclic_small", "cyclic_dense"}:
        side = 32 if case == "cyclic_dense" else 4
        r = s = t = [(n % side, n // side) for n in range(side * side)]
    elif case == "star_control":
        r = [(n % 64, (n // 64) % 64) for n in range(4096)]
        s = [(n, 17 * n - 5) for n in range(64)]
        t = [(n, 19 * n - 7) for n in range(64)]
    else:
        r = [(17 * n - 3, n) for n in range(8192)]
        s = [((2 * n if case == "binary_anti" else n), 19 * n - 5) for n in range(8192)]
        t = None
    result = {}
    for name, pairs in (("r", r), ("s", s), ("t", t)):
        if pairs is not None:
            result[name] = [dict(x=x, y=y, id=n) for n, (x, y) in enumerate(pairs)]
    if case == "skew_wide":
        for row in result["r"]:
            row.update({f"p{column}": row["id"] * 17 - column * 3 for column in range(8)})
    return result


def source_fields(case, relation):
    return [(name, {"Primitive": ["i64", name in {"x", "y"}]})
            for name in ["x", "y", "id", *([f"p{i}" for i in range(8)]
                                          if case == "skew_wide" and relation == "r" else [])]]


def output_columns(case):
    if case in EXISTENCE or case in {"binary_semi", "binary_anti"}:
        return [("outer_id", "r", "id"), ("b", "r", "y"), ("a", "r", "x"),
                *[(f"payload_{i}", "r", f"p{i}") for i in range(8) if case == "skew_wide"]]
    return [(alias, relation, column) for alias, relation, column in (
        ("ra", "r", "x"), ("rb", "r", "y"), ("rid", "r", "id"),
        ("sb", "s", "x"), ("sc", "s", "y"), ("sid", "s", "id"),
        ("ta", "t", "x"), ("tc", "t", "y"), ("tid", "t", "id"))
        if relation != "t" or case != "binary_control"]


def output_schema(case):
    return {"Struct": [{"names": [name for name, _, _ in output_columns(case)],
                        "dtypes": [{"Primitive": ["i64", column in {"x", "y"}]}
                                   for _, _, column in output_columns(case)]}, False]}


def uri(case, relation):
    return f"memory://cyclic-existence/{case}/{relation}"


def inner_plan(native, case):
    return native.Relation.scan(uri(case, "s")).join(
        native.Relation.scan(uri(case, "t")), [("y", "y")],
        [("left", "x", "witness_b"), ("right", "x", "witness_a")])


def plan(native, case):
    r = native.Relation.scan(uri(case, "r"))
    s = native.Relation.scan(uri(case, "s"))
    if case in EXISTENCE or case in {"binary_semi", "binary_anti"}:
        right = inner_plan(native, case) if case in EXISTENCE else s
        keys = [("y", "witness_b"), ("x", "witness_a")] if case in EXISTENCE else [("y", "x")]
        return r.join(right, keys, [("left", column, alias) for alias, _, column in output_columns(case)],
                      kind="left_anti" if case == "binary_anti" else "left_semi")
    inner = r.join(s, [("x" if case == "star_control" else "y", "x")],
                   [("left" if relation == "r" else "right", column, alias)
                    for alias, relation, column in output_columns("binary_control")])
    if case == "binary_control":
        return inner
    keys = [("rb", "x")] if case == "star_control" else [("ra", "x"), ("sc", "y")]
    return inner.join(native.Relation.scan(uri(case, "t")), keys,
                      [("right", column, alias) if relation == "t" else ("left", alias, alias)
                       for alias, relation, column in output_columns(case)])


def inner_reference(inputs):
    by_c = defaultdict(list)
    for row in inputs["t"]:
        if row["y"] is not None:
            by_c[row["y"]].append(row)
    for s in inputs["s"]:
        if s["y"] is not None:
            for t in by_c[s["y"]]:
                yield {"witness_b": s["x"], "witness_a": t["x"]}


def reference(case, inputs):
    def project(**rows):
        return {alias: rows[relation][column] for alias, relation, column in output_columns(case)}

    if case in EXISTENCE:
        s_domains, t_domains = defaultdict(set), defaultdict(set)
        for row in inputs["s"]:
            if row["x"] is not None and row["y"] is not None:
                s_domains[row["x"]].add(row["y"])
        for row in inputs["t"]:
            if row["x"] is not None and row["y"] is not None:
                t_domains[row["x"]].add(row["y"])
        for r in inputs["r"]:
            if r["x"] is not None and r["y"] is not None and s_domains[r["y"]] & t_domains[r["x"]]:
                yield project(r=r)
        return
    s_index, t_index = defaultdict(list), defaultdict(list)
    for row in inputs["s"]:
        if row["x"] is not None:
            s_index[row["x"]].append(row)
    for row in inputs.get("t", []):
        if row["x"] is not None and (case == "star_control" or row["y"] is not None):
            t_index[row["x"] if case == "star_control" else (row["x"], row["y"])].append(row)
    for r in inputs["r"]:
        candidates = s_index[r["x"] if case == "star_control" else r["y"]]
        if case in {"binary_semi", "binary_anti"}:
            if bool(candidates) == (case == "binary_semi"):
                yield project(r=r)
            continue
        for s in candidates:
            if case == "binary_control":
                yield project(r=r, s=s)
            else:
                for t in t_index[r["y"] if case == "star_control" else (r["x"], s["y"])]:
                    yield project(r=r, s=s, t=t)


def manifest():
    cells = []
    for case in CASES:
        inputs = sources(case)
        cells.append({"case": case, "target": case in TARGETS,
                      "sources": {name: {**digest_rows(rows), "fields": source_fields(case, name)}
                                  for name, rows in inputs.items()},
                      "expected": {**digest_rows(reference(case, inputs)), "schema": output_schema(case)},
                      "independent_inner": digest_rows(inner_reference(inputs)) if case in EXISTENCE else None})
    return {"schema": "shardloom.cyclic_existence_workload.v1", "grant_bytes": GRANT,
            "max_parallelism": 1, "cells": cells}
