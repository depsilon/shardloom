# SPDX-License-Identifier: Apache-2.0
"""Subquery position, dynamic outer schemas, nested demand and exact rich payloads."""

from __future__ import annotations

import shardloom as sl
from shardloom._result_schema import ResultType as T
from native_streaming_subquery_cases import LEFT, RIGHT, SCHEMA, POLICY, apply, oracle, complete_inputs
from native_streaming_unary_cases import frame
from native_streaming_unary_composition_cases import before, after
from native_streaming_dynamic_cases import ROWS as PIVOT_ROWS, SCHEMA as PIVOT_SCHEMA, pivot
from native_typed_input_fixtures import rich_fixture

FIELDS = (("n", T("int64", True)), ("s", T("utf8", True)))


def run(harness):
    stages = ("filter", "project", "sort", "limit", "aggregate", "window", "join", "set", "unary")
    for kind in ("in", "exists", "any-gt", "scalar-one", "parameterized-scalar", "guarded-scalar"):
        for position in ("outer", "inner", "after"):
            for stage in (stages if position != "after" else ("limit-0", "limit-2", *stages[1:3], *stages[4:])):
                name = f"subquery-composition-{kind}-{position}-{stage}"

                def check(name=name, kind=kind, position=position, stage=stage):
                    left, a = frame(harness, LEFT, SCHEMA, width=1)
                    right, b = frame(harness, RIGHT, SCHEMA, width=2)
                    left_rows, right_rows, traces = list(LEFT), list(RIGHT), [a, b]
                    if position != "after":
                        source, rows = (left, left_rows) if position == "outer" else (right, right_rows)
                        source, rows, extra, _ = before(harness, source, rows, FIELDS, stage)
                        traces.extend(extra)
                        if position == "outer":
                            left, left_rows = source, rows
                        else:
                            right, right_rows = source, rows
                    query = apply(left, right, kind, "sql")
                    expected = oracle(kind, left_rows, right_rows)
                    answer = T("int64", True) if "scalar" in kind else T("bool", kind != "exists")
                    fields = FIELDS + (("answer", answer),)
                    if position == "after":
                        query, expected, fields, extra, _ = after(harness, query, expected, fields, stage)
                        traces.extend(extra)
                    harness.json(harness.logs / (name + "-predeclared.json"), {"sql": query._relation_statement(), "expected": expected})
                    with query.iter_batches(batch_rows=2, **POLICY) as stream:
                        held, actual = [], []
                        for batch in stream:
                            assert stream.report is None
                            assert batch.result_schema == fields, (name, batch.result_schema, fields)
                            held.append(batch)
                            actual.extend(batch.result_rows)
                        report = stream.report
                        assert report is not None and stream._process.poll() == 0
                        assert stream._schema == fields
                    harness.values(name, actual, expected)
                    assert [row for batch in held for row in batch.result_rows] == expected
                    complete_inputs(harness, name, report.envelope, traces)
                    return {"traces": traces, "complete_rows": len(actual)}
                harness.case(name, check)
    dynamic(harness)
    nested(harness)
    rich(harness)
    files(harness)


def dynamic(harness):
    for kind in ("in", "scalar-one", "parameterized-scalar"):
        for shared in (False, True):
            name = f"subquery-dynamic-outer-{kind}-shared-{shared}"

            def check(name=name, kind=kind, shared=shared):
                source, a = frame(harness, PIVOT_ROWS, PIVOT_SCHEMA, width=1)
                left = pivot(source).select("entity AS n", "'x' AS s")
                left_rows = [dict(n=n, s="x") for n in (1, 2)]
                if shared:
                    right = source.select("entity AS n", "'x' AS s")
                    right_rows = [dict(n=row["entity"], s="x") for row in PIVOT_ROWS]
                    traces = [a]
                else:
                    right, b = frame(harness, RIGHT, SCHEMA)
                    right_rows, traces = RIGHT, [a, b]
                query, expected = apply(left, right, kind, "sql"), oracle(kind, left_rows, right_rows)
                harness.json(harness.logs / (name + "-predeclared.json"), expected)
                report = query.collect(check=True, **POLICY)
                harness.values(name, report.result_rows, expected)
                complete_inputs(harness, name, report.envelope, traces)
                assert report.envelope.field("relational_schema_binding") == "during_execution"
                assert report.envelope.field_int("relational_dynamic_schema_stages") == 1
                if shared:
                    assert report.envelope.field_int("native_input_replay_sources") == 1
                    assert report.envelope.field_int("native_input_replay_prefilled_sources") == 0
                return {"traces": traces}
            harness.case(name, check)


def nested(harness):
    for shared in (False, True):
        name = f"subquery-nested-static-shared-{shared}"

        def check(name=name, shared=shared):
            left, a = frame(harness, LEFT, SCHEMA, width=1)
            right, b = frame(harness, RIGHT, SCHEMA, width=1)
            if shared:
                deepest, traces, value = right, [a, b], 2
            else:
                deepest, c = frame(harness, [{"n": 9, "s": "deep"}], SCHEMA)
                traces, value = [a, b, c], 9
            inner = right.with_column("nested", sl.scalar_subquery(deepest.select("n").limit(1)))
            query = left.with_column("answer", sl.scalar_subquery(inner.select("nested").limit(1)))
            expected = [dict(row, answer=value) for row in LEFT]
            harness.json(harness.logs / (name + "-predeclared.json"), expected)
            report = query.collect(check=True, **POLICY)
            harness.values(name, report.result_rows, expected)
            complete_inputs(harness, name, report.envelope, traces)
            assert report.envelope.field_int("native_input_replay_prefilled_sources") == int(shared)
            return {"traces": traces}
        harness.case(name, check)


def rich(harness):
    schema, fields, rows, expected_rows = rich_fixture()
    for column, dtype in fields:
        for empty in (False, True):
            terminals = ("collect", "vortex") if column in ("samples", "detail", "empty_vector", "amount", "stamp") else ("collect",)
            for terminal in terminals:
                name = f"subquery-rich-scalar-{column}-empty-{empty}-{terminal}"

                def check(name=name, column=column, dtype=dtype, empty=empty, terminal=terminal):
                    left, a = frame(harness, LEFT, SCHEMA, width=1)
                    right, b = frame(harness, rows, schema, width=1)
                    inner = right.select(column).limit(0 if empty else 1)
                    query = left.select("n").with_column("answer", sl.scalar_subquery(inner))
                    expected = [{"n": row["n"], "answer": None if empty else expected_rows[0][column]} for row in LEFT]
                    fields = (("n", T("int64", True)), ("answer", dtype))
                    harness.json(harness.logs / (name + "-predeclared.json"), expected)
                    if terminal == "collect":
                        report = query.collect(check=True, **POLICY)
                        observed = report
                        artifact = None
                    else:
                        target = harness.output / (name + ".vortex")
                        report = query.write_vortex(target, **POLICY)
                        observed = harness.context.read_vortex(target).collect(check=True, **POLICY)
                        harness.envelope(name + "-reopened", observed.envelope)
                        artifact = harness.artifact(target)
                    assert observed.result_schema == fields, (column, observed.result_schema, fields)
                    harness.values(name, observed.result_rows, expected)
                    complete_inputs(harness, name, report.envelope, [a, b])
                    return {"traces": [a, b], "artifact": artifact}
                harness.case(name, check)


def files(harness):
    paths = []
    for label, rows in (("outer", LEFT), ("inner", RIGHT)):
        target = harness.output / ("subquery-file-" + label + ".vortex")
        paths.append(target)
        name = "subquery-file-prepare-" + label

        def prepare(name=name, rows=rows, target=target):
            report = harness.context.from_rows(rows, schema=SCHEMA).write_vortex(target, **POLICY)
            harness.envelope(name, report.envelope)
            return {"artifact": harness.artifact(target)}
        harness.case(name, prepare)
    for kind in ("in", "exists", "any-gt", "scalar-one", "parameterized-scalar"):
        for side in ("outer", "inner"):
            name = f"subquery-file-{kind}-{side}"

            def check(name=name, kind=kind, side=side):
                source, trace = frame(harness, RIGHT if side == "outer" else LEFT, SCHEMA)
                left = harness.context.read_vortex(paths[0]) if side == "outer" else source
                right = source if side == "outer" else harness.context.read_vortex(paths[1])
                report = apply(left, right, kind).collect(check=True, **POLICY)
                harness.values(name, report.result_rows, oracle(kind, LEFT, RIGHT))
                complete_inputs(harness, name, report.envelope, [trace])
                return {"trace": trace, "file": harness.artifact(paths[0 if side == "outer" else 1])}
            harness.case(name, check)
