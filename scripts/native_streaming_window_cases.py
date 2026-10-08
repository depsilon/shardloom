# SPDX-License-Identifier: Apache-2.0
"""Complete streamed analytic functions, frames and composed public workflows."""

from __future__ import annotations

import json
from itertools import product

from shardloom.errors import ShardLoomCommandError
from native_streaming_input_cases import declaration
from native_window_frame_reference import fixture_rows, frame_rows

SCHEMA = {"n": "int64", "s": "utf8"}
ROWS = [{"n": n, "s": s} for n, s in [(3, "c"), (1, "a"), (1, "b"), (None, None), (2, "d")]]
MEASURES = (
    ("count_all", "COUNT(*)"), ("nonnull", "COUNT(value)"),
    ("distinct", "COUNT(DISTINCT value)"), ("total", "SUM(value)"),
    ("mean", "AVG(value)"), ("minimum", "MIN(value)"), ("maximum", "MAX(value)"),
    ("first", "FIRST_VALUE(value)"), ("last", "LAST_VALUE(value)"), ("nth", "NTH_VALUE(value,2)"),
)


def source(harness, rows=ROWS, schema=SCHEMA):
    trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}
    def produce():
        trace["opened"] += 1
        try:
            for batch in [[], rows[:2], [], rows[2:], []]:
                trace["batches"] += 1
                trace["rows"] += len(batch)
                yield batch
            trace["ended"] = True
        finally:
            trace["closed"] = True
    return harness.context.from_batches(produce, schema=schema, streaming=True), trace


def ranking():
    order = "ORDER BY n ASC NULLS LAST"
    frame = order + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    functions = [
        ("position", "ROW_NUMBER()", [4, 1, 2, 5, 3]),
        ("rank_value", "RANK()", [4, 1, 1, 5, 3]),
        ("dense", "DENSE_RANK()", [3, 1, 1, 4, 2]),
        ("percent", "PERCENT_RANK()", [0.75, 0.0, 0.0, 1.0, 0.5]),
        ("cumulative", "CUME_DIST()", [0.8, 0.4, 0.4, 1.0, 0.6]),
        ("tile", "NTILE(3)", [2, 1, 1, 3, 2]),
        ("previous", "LAG(s,1)", ["d", None, "a", "c", "b"]),
        ("following", "LEAD(s,1)", [None, "b", "d", None, "c"]),
    ]
    framed = [
        ("count_all", "COUNT(*)", [4, 1, 2, 5, 3]),
        ("nonnull", "COUNT(n)", [4, 1, 2, 4, 3]),
        ("distinct", "COUNT(DISTINCT n)", [3, 1, 1, 3, 2]),
        ("total", "SUM(n)", [7.0, 1.0, 2.0, 7.0, 4.0]),
        ("mean", "AVG(n)", [1.75, 1.0, 1.0, 1.75, 4.0 / 3.0]),
        ("minimum", "MIN(n)", [1, 1, 1, 1, 1]),
        ("maximum", "MAX(n)", [3, 1, 1, 3, 2]),
        ("first", "FIRST_VALUE(s)", ["a"] * 5),
        ("last", "LAST_VALUE(s)", ["c", "a", "b", None, "d"]),
        ("second", "NTH_VALUE(s,2)", ["b", None, "b", "b", "b"]),
    ]
    expressions = [f"{function} OVER ({order}) AS {name}" for name, function, _ in functions]
    expressions += [f"{function} OVER ({frame}) AS {name}" for name, function, _ in framed]
    expressions.append("ROW_NUMBER() OVER (ORDER BY n DESC NULLS FIRST) AS reverse_position")
    expected = [dict(row, **{name: values[index] for name, _, values in functions + framed},
                     reverse_position=[2, 4, 5, 1, 3][index]) for index, row in enumerate(ROWS)]
    return expressions, expected


def run(harness):
    policy = {"memory_gb": 1, "max_parallelism": 1}
    workspace = harness.output / "window-small-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    expressions, expected = ranking()
    cases = [("ranking-navigation", ROWS, SCHEMA, expressions, list(expected[0]), expected, 2)]
    schema = {"id": "int64", "cohort": "utf8", "priority": "int64", "value": "int64"}
    for unit, exclusion, descending, nulls_first in product(
        ("ROWS", "GROUPS", "RANGE"), ("NO OTHERS", "CURRENT ROW", "GROUP", "TIES"),
        (False, True), (False, True),
    ):
        direction, nulls = ("DESC" if descending else "ASC"), ("FIRST" if nulls_first else "LAST")
        name = f"{unit}-{exclusion}-{direction}-{nulls}".lower().replace(" ", "-")
        clause = (f"PARTITION BY cohort ORDER BY priority {direction} NULLS {nulls} "
                  f"{unit} BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE {exclusion}")
        functions = [f"{function} OVER ({clause}) AS {alias}" for alias, function in MEASURES]
        cases.append((name, fixture_rows(), schema, functions, ["id", *(alias for alias, _ in MEASURES)],
                      frame_rows(unit, exclusion, descending, nulls_first), 1))
    for name, rows in [("empty", []), ("singleton", ROWS[1:2]), ("all-null", [{"n": None, "s": None}])]:
        functions = ["COUNT(n) OVER () AS nonnull", "COUNT(DISTINCT s) OVER () AS distinct_values",
                     "MIN(s) OVER () AS minimum", "MAX(s) OVER () AS maximum"]
        values = [] if not rows else [dict(rows[0], nonnull=int(rows[0]["n"] is not None),
                                          distinct_values=int(rows[0]["s"] is not None),
                                          minimum=rows[0]["s"], maximum=rows[0]["s"])]
        cases.append((name, rows, SCHEMA, functions, ["n", "s", "nonnull", "distinct_values", "minimum", "maximum"], values, 1))
    harness.json(harness.logs / "window-small-oracles.json", {
        "cases": [{"name": name, "input": rows, "schema": schema, "expressions": functions,
                   "columns": columns, "expected": values, "groups": groups}
                  for name, rows, schema, functions, columns, values, groups in cases],
        "native_grant_bytes": 1 << 30, "spill": spill,
    })

    def completed(name, envelope, trace, spilled, stages=1, groups=None):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        harness.completed(name, envelope, batches=trace["batches"], rows=trace["rows"])
        assert envelope.field_int("native_input_window_rows_detached") == trace["rows"] * stages
        assert envelope.field_int("relational_ordered_window_stages") == (stages if spilled else 0)
        if spilled and groups is not None and trace["rows"]:
            assert envelope.field_int("relational_ordered_window_groups") == groups
        assert list(workspace.iterdir()) == []

    for spilled in (False, True):
        requested = dict(policy, spill=spill) if spilled else policy
        for name, rows, schema, functions, columns, values, groups in cases:
            for surface in ("dataframe", "sql"):
                label = f"window-{name}-{surface}-spill-{spilled}"
                def check(label=label, rows=rows, schema=schema, functions=functions, columns=columns,
                          values=values, groups=groups, surface=surface, spilled=spilled, requested=requested):
                    frame, trace = source(harness, rows, schema)
                    query = frame.window(*functions).select(*columns)
                    if surface == "sql":
                        iterator = harness.client.public_workflow_batches(
                            "sql", sql_statement=query._relation_statement(), batch_rows=2,
                            **dict(declaration(frame), **({"spill": spill} if spilled else {})))
                    else:
                        iterator = query.iter_batches(batch_rows=2, **requested)
                    assert trace["opened"] == 0
                    retained, actual, dtype = [], [], None
                    with iterator:
                        for batch in iterator:
                            assert trace["ended"] and iterator.report is None
                            assert len(batch.result_rows) <= 2
                            if dtype is None:
                                dtype = batch.result_schema
                            assert batch.result_schema == dtype
                            actual.extend(batch.result_rows)
                            retained.append(batch)
                        assert iterator.report is not None and iterator._process.poll() == 0
                        envelope = iterator.report.envelope
                    harness.values(label, actual, values)
                    assert [row for batch in retained for row in batch.result_rows] == values
                    completed(label, envelope, trace, spilled, groups=groups)
                harness.case(label, check)

        for destination in ("collect", "vortex"):
            name = f"window-composed-{destination}-spill-{spilled}"
            def composed(name=name, destination=destination, spilled=spilled, requested=requested):
                frame, trace = source(harness)
                ordinary = harness.context.from_rows([{"n": 1, "s": "one"}, {"n": 3, "s": "three"}], schema=SCHEMA)
                query = frame.window("ROW_NUMBER() OVER (ORDER BY n ASC NULLS LAST) AS pos")
                query = query.window("ROW_NUMBER() OVER (ORDER BY pos DESC) AS reverse_pos")
                query = query.join(ordinary, on="n", how="left").select("d.s AS label", "f.pos AS pos")
                query = query.group_by("label").agg(rows="count(*)", total="sum(pos)").sort("total").limit(2)
                values = [{"label": "one", "rows": 2, "total": 3.0}, {"label": "three", "rows": 1, "total": 4.0}]
                if destination == "collect":
                    result = query.collect(check=True, **requested)
                    harness.values(name, result.result_rows, values)
                else:
                    path = harness.output / (name + ".vortex")
                    result = query.write_vortex(path, **requested)
                    reopened = harness.context.read_vortex(path).collect(check=True, **policy)
                    harness.envelope(name + "-reopen", reopened.envelope)
                    harness.values(name + "-reopen", reopened.result_rows, values)
                completed(name, result.envelope, trace, spilled, stages=2)
            harness.case(name, composed)

        for mode in ("producer", "schema"):
            for zero in (False, True):
                name = f"window-late-{mode}-zero-{zero}-spill-{spilled}"
                def fail(name=name, mode=mode, zero=zero, requested=requested):
                    trace = {"pulled": 0, "closed": False}
                    def produce():
                        try:
                            trace["pulled"] += 1
                            yield ROWS[:2]
                            trace["pulled"] += 1
                            if mode == "producer":
                                raise RuntimeError("late window producer")
                            yield [{"wrong": 1}]
                        finally:
                            trace["closed"] = True
                    query = harness.context.from_batches(produce(), schema=SCHEMA, streaming=True).window(expressions[0])
                    if zero:
                        query = query.limit(0)
                    iterator = query.iter_batches(**requested)
                    try:
                        with iterator:
                            assert list(iterator) == []
                    except (RuntimeError, ValueError) as error:
                        assert not isinstance(error, ShardLoomCommandError), error
                        if mode == "producer":
                            assert str(error) == "late window producer"
                        else:
                            assert isinstance(error, ValueError)
                    else:
                        raise AssertionError("a window hid its producer failure")
                    assert trace == {"pulled": 2, "closed": True}
                    assert iterator.report is None and iterator._process.poll() is not None
                    assert list(workspace.iterdir()) == []
                harness.case(name, fail)

    def repeated():
        frame, trace = source(harness)
        query = frame.window(expressions[0])
        try:
            query.union_all(query).collect(check=True, **policy)
        except ShardLoomCommandError as error:
            harness.envelope("window-deny-repeated-source", error.envelope, success=False)
            assert "SL-NATIVE-BATCH" in json.dumps(error.envelope.raw)
        else:
            raise AssertionError("a repeated window source was admitted")
        assert trace["opened"] == 0
    harness.case("window-deny-repeated-source", repeated)
