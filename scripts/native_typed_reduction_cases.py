# SPDX-License-Identifier: Apache-2.0
"""Frozen exact oracles for public aggregate expressions and decimal state."""

from __future__ import annotations

import json
from decimal import Decimal


def run(context, output, guard, exercise, exercise_workflow, remember, denied,
        schema, native, raw):
    from shardloom.query import SqlWorkflow

    cases, sql_cases, failures = [], [], []

    def decimal(value, precision=38, scale=2):
        return f"decimal128({precision},{scale}):{value}"

    def add(name, frame, expected, columns, *, cells=None, typed=True):
        options = {"typed_orc": typed, "json_cells": tuple(columns if cells is None else cells)}
        cases.append((name, frame, expected, columns, options))

    measures = dict(total="sum(money)", mean="avg(money)", smallest="min(money)",
                    largest="max(money)", present="count(money)", unique="count(distinct money)")
    numeric = ["total", "mean", "smallest", "largest"]
    for source_name, source, composed in [
        ("native", context.read_vortex(native, schema=schema), True),
        ("declared-arrow", context.read_arrow_ipc(raw), True),
        ("direct-native", context.read_vortex(native, schema=schema), False),
    ]:
        prefix = f"typed-reductions-{source_name}"
        base = source.limit(4) if composed else source
        money = base.select("id", "CAST(id AS decimal128(12,2)) AS money",
                            "CASE WHEN id<4 THEN 'a' ELSE 'b' END AS cohort",
                            "CASE WHEN id<3 THEN 'x' ELSE 'y' END AS domain")
        add(prefix + "-scalar", money.agg(**measures), [{
            "total": decimal(1000), "mean": decimal(2_500_000, scale=6),
            "smallest": decimal(100, 12), "largest": decimal(400, 12),
            "present": 4, "unique": 4,
        }], list(measures), cells=numeric)
        add(prefix + "-scalar-empty", money.limit(0).agg(**measures), [{
            **dict.fromkeys(numeric), "present": 0, "unique": 0,
        }], list(measures), cells=numeric)
        computed = dict(total="sum(id+1)", mean="avg(id*2)", smallest="min(-id)",
                        largest="max(id+1)", rows="count(1)", absent="count(NULL)",
                        erased="count(coalesce(NULL,NULL))", distinct_null="count(distinct NULL)",
                        unique="count(distinct CASE WHEN id<3 THEN id-id ELSE NULL END)")
        add(prefix + "-computed", base.agg(**computed), [{
            "total": 14.0, "mean": 5.0, "smallest": -4, "largest": 5,
            "rows": 4, "absent": 0, "erased": 0, "distinct_null": 0, "unique": 1,
        }], list(computed), cells=(), typed=False)
        add(prefix + "-lazy", base.agg(total="sum(CASE WHEN id>0 THEN id ELSE 1/0 END)"),
            [{"total": 10.0}], ["total"], cells=(), typed=False)
        add(prefix + "-null-projection", base.select("NULL AS missing"),
            [{"missing": None}] * 4, ["missing"], cells=(), typed=False)
        nullable = money.select("cohort", "CASE WHEN id=4 THEN NULL ELSE money END AS money")
        grouped_expected = [
            {"cohort": "a", "total": decimal(600), "mean": decimal(2_000_000, scale=6), "present": 3},
            {"cohort": "b", "total": None, "mean": None, "present": 0},
        ]
        grouped = nullable.group_by("cohort").agg(total="sum(money)", mean="avg(money)", present="count(money)")
        add(prefix + "-groups", grouped, grouped_expected, ["cohort", "total", "mean", "present"], cells=("total", "mean"))
        add(prefix + "-groups-empty", nullable.limit(0).group_by("cohort").agg(total="sum(money)"),
            [], ["cohort", "total"], cells=("total",))
        add(prefix + "-joined", money.join(money, on="id").agg(total="sum(f.money+d.money)"),
            [{"total": decimal(2000)}], ["total"])
        statement = (f"SELECT q.id,SUM(q.money+q.money) AS total FROM ({money._relation_statement()}) AS q "
                     "GROUP BY q.id HAVING q.id>2 ORDER BY q.id")
        sql_cases.append((prefix + "-qualified-having", SqlWorkflow(
            statement, context.client, source_bindings=money._declared_sources()),
            [{"q.id": 3, "total": decimal(600)}, {"q.id": 4, "total": decimal(800)}],
            ["q.id", "total"], {"json_cells": ("total",)}))

        # Expectations use integer coefficients independently of runtime output.
        observations = [100, 200, 300, 400]
        for center in [False, True]:
            for aggregate in ["sum", "mean", "min", "max"]:
                expected = []
                for row in range(4):
                    start, end = (max(0, row-1), min(4, row+2)) if center else (max(0, row-2), row+1)
                    values = observations[start:end]
                    if aggregate == "mean":
                        numerator = sum(values) * 10_000
                        assert numerator % len(values) == 0
                        value, precision, scale = numerator // len(values), 38, 6
                    else:
                        value = {"sum": sum, "min": min, "max": max}[aggregate](values)
                        precision, scale = (38 if aggregate == "sum" else 12), 2
                    expected.append({"value": decimal(value, precision, scale)})
                frame = getattr(money.select("money").rolling(3, min_periods=1, center=center), aggregate)("money", alias="value")
                add(f"{prefix}-rolling-{aggregate}-{center}", frame, expected, ["value"])
                if center:
                    add(f"{prefix}-rolling-{aggregate}-limited", frame.limit(2), expected[:2], ["value"])

        shrinking = base.select(
            "CAST(CASE WHEN id=3 THEN 3 WHEN id=4 THEN 1 ELSE 0 END AS decimal128(8,0)) AS money"
        ).rolling(5, min_periods=1, center=True).mean("money", alias="value")
        add(prefix + "-rolling-eof-limited", shrinking.limit(3),
            [{"value": decimal(1_000_000, scale=6)}] * 3, ["value"])
        failures.append((prefix + "-rolling-eof-inexact", shrinking, "nonzero fractional digits"))

        for aggregate, precision, scale, values in [
            ("sum", 38, 2, [[300, 300, 600], [0, 400, 400], [300, 700, 1000]]),
            ("mean", 38, 6, [[1_500_000, 3_000_000, 2_000_000], [0, 4_000_000, 4_000_000], [1_500_000, 3_500_000, 2_500_000]]),
            ("min", 12, 2, [[100, 300, 100], [0, 400, 400], [100, 300, 100]]),
            ("max", 12, 2, [[200, 300, 300], [0, 400, 400], [200, 400, 400]]),
        ]:
            frame = money.pivot_table(index="cohort", columns="domain", values="money",
                                     aggfunc=aggregate, fill_value=Decimal("0.00"),
                                     margins=True, margins_name="total", dropna=False)
            columns = ["cohort", "pivot_x", "pivot_y", "pivot_total"]
            expected = [{"cohort": key, **{name: decimal(value, precision, scale)
                         for name, value in zip(columns[1:], row)}}
                        for key, row in zip(["a", "b", "total"], values)]
            add(f"{prefix}-pivot-{aggregate}", frame, expected, columns, cells=columns[1:])

        # The original typed fixture supplies a 38-digit coefficient. No Python
        # floating/decimal arithmetic is used to derive the wide-state oracle.
        maximum = 10**38 - 1
        positive = base.filter("id=2").select("-amount AS money")
        negative = base.filter("id=2").select("amount AS money")
        twice = positive.union_all(positive)
        add(prefix + "-wide-average", twice.agg(value="avg(money)"),
            [{"value": decimal(maximum, scale=6)}], ["value"])
        add(prefix + "-wide-cancellation", twice.union_all(negative).agg(value="sum(money)"),
            [{"value": decimal(maximum, scale=6)}], ["value"])
        failures.append((prefix + "-sum-overflow", twice.agg(value="sum(money)"), "precision overflow"))
        max_text = str(maximum)[:-6] + "." + str(maximum)[-6:]
        rolling_wide = base.limit(2).select(f"CAST('{max_text}' AS decimal128(38,6)) AS money")
        failures.append((prefix + "-rolling-overflow", rolling_wide.rolling(2, min_periods=2).sum("money"), "precision overflow"))
        repeating = base.limit(3).select(
            "CASE WHEN id=1 THEN CAST('0.01' AS decimal128(3,2)) ELSE CAST('0.00' AS decimal128(3,2)) END AS money")
        failures.append((prefix + "-inexact-average", repeating.agg(value="avg(money)"), "nonzero fractional digits"))
        failures.append((prefix + "-inexact-rolling", repeating.rolling(3, min_periods=3, center=True).mean("money"), "nonzero fractional digits"))
        failures.append((prefix + "-null-pivot", base.pivot_table(index="id", columns="payload", values="amount", aggfunc="sum"), "non-null"))

        if not composed:
            # Exercise bare native columns as well as the computed money stages:
            # direct strategy selection must admit the same decimal semantics.
            add(prefix + "-bare-aggregate", source.agg(
                total="sum(amount)", low="min(amount)", high="max(amount)", present="count(amount)"),
                [{"total": decimal(1_234_567 - maximum, scale=6),
                  "low": decimal(-maximum, scale=6), "high": decimal(1_234_567, scale=6),
                  "present": 3}], ["total", "low", "high", "present"], cells=("total", "low", "high"))
            add(prefix + "-bare-average", source.filter("id=1").agg(value="avg(amount)"),
                [{"value": decimal(1_234_567, scale=6)}], ["value"])
            pivot_columns = ["id", "pivot_binary", "pivot_binary_c3a9", "pivot_binary_00ff10"]
            pivot_expected = [
                {"id": 1, "pivot_binary": None, "pivot_binary_c3a9": None,
                 "pivot_binary_00ff10": decimal(1_234_567, scale=6)},
                {"id": 2, "pivot_binary": decimal(-maximum, scale=6),
                 "pivot_binary_c3a9": None, "pivot_binary_00ff10": None},
                {"id": 4, "pivot_binary": None, "pivot_binary_c3a9": decimal(0, scale=6),
                 "pivot_binary_00ff10": None},
            ]
            for aggregate in ("sum", "mean", "min", "max"):
                add(prefix + "-bare-rolling-" + aggregate,
                    getattr(source.rolling(1, min_periods=1), aggregate)("amount", alias="value"),
                    [{"value": decimal(value, scale=6)} for value in (1_234_567, -maximum, 0)], ["value"])
                add(prefix + "-bare-pivot-" + aggregate,
                    source.filter("id<>3").pivot_table(index="id", columns="payload", values="amount",
                                                      aggfunc=aggregate, dropna=False),
                    pivot_expected, pivot_columns, cells=pivot_columns[1:])

    # Boundary scales need a direct public SQL spelling in addition to the
    # composed, renamed decimal columns above.
    for scale, expression, total, mean in [
        (0, "CAST(id AS decimal128(38,0))", 10, 2_500_000),
        (6, "CAST(id AS decimal128(38,6))", 10_000_000, 2_500_000),
        (38, "CAST('0." + "0"*36 + "10' AS decimal128(38,38))", 40, 10),
    ]:
        sql_cases.append((f"typed-reductions-scale-{scale}", context.sql(
            f"SELECT SUM({expression}) AS total,AVG({expression}) AS mean FROM '{native}'"),
            [{"total": decimal(total, scale=scale), "mean": decimal(mean, scale=max(scale, 6))}],
            ["total", "mean"], {}))
    for name, statement, reason in [
        ("missing-empty", f"SELECT SUM(absent+1) FROM '{native}' LIMIT 0", "absent"),
        ("nested-empty", f"SELECT SUM(AVG(id)) FROM '{native}' LIMIT 0", None),
        ("sum-distinct", f"SELECT SUM(DISTINCT id) FROM '{native}'", "COUNT"),
    ]:
        failures.append(("typed-reductions-" + name, context.sql(statement), reason))

    oracle = output / "typed-reductions-expected.json"
    oracle.write_text(json.dumps({
        "cases": {name: {"columns": columns, "rows": expected}
                  for name, _, expected, columns, _ in cases + sql_cases},
        "denials": {name: reason for name, _, reason in failures},
    }, indent=2) + "\n")
    remember(oracle)
    for name, frame, expected, columns, options in cases:
        guard()
        exercise(name, frame, expected, columns, **options)
    for name, workflow, expected, columns, options in sql_cases:
        guard()
        exercise_workflow(name, workflow, expected, columns, **options)
    for name, workflow, reason in failures:
        guard()
        denied(name + "-collect", workflow.collect(check=False, memory_gb=1, max_parallelism=2), reason=reason)
        for extension in ("vortex", "parquet", "arrow_ipc", "avro", "json", "jsonl", "csv"):
            guard()
            path = output / f"{name}-denied.{extension}"
            denied(name + "-" + extension, getattr(workflow, "write_" + extension)(
                path, check=False, memory_gb=1, max_parallelism=2), path, reason)
