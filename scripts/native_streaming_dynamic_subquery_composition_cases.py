# SPDX-License-Identifier: Apache-2.0
"""Dynamic predicate placement, nested declarations and mixed typed source lifetimes."""

from __future__ import annotations

import re

import shardloom as sl
from shardloom._result_schema import ResultType as T
from shardloom.query import LazyFrame
from native_streaming_dynamic_subquery_cases import FIELDS, apply, consume, fields, inner, oracle
from native_streaming_subquery_cases import LEFT, RIGHT, SCHEMA, POLICY, complete_inputs
from native_streaming_unary_cases import frame
from native_streaming_unary_composition_cases import before, after
from native_typed_input_fixtures import rich_fixture


def run(harness):
    stages = ("filter", "project", "sort", "limit", "aggregate", "window", "join", "set", "unary")
    for kind in ("in", "row-in", "exists", "any-gt"):
        for position in ("outer", "inner", "after"):
            for stage in (stages if position != "after" else ("limit-0", "limit-2", *stages[1:3], *stages[4:])):
                name = f"dynamic-subquery-composition-{kind}-{position}-{stage}"

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
                    query, expected, schema = apply(left, right, kind, "sql"), oracle(kind, left_rows, right_rows), fields(kind)
                    if position == "after":
                        query, expected, schema, extra, _ = after(harness, query, expected, schema, stage)
                        traces.extend(extra)
                    consume(harness, name, query, expected, schema, traces, terminal="batches")
                    return {"traces": traces, "complete_rows": len(expected)}
                harness.case(name, check)
    nested(harness)
    dynamic_outer(harness)
    files(harness)
    rich(harness)


def nested(harness):
    for mode in ("nested-shared", "nested-independent", "repeated-join", "repeated-set"):
        for kind in ("in", "exists", "all-ge"):
            name = f"dynamic-subquery-{mode}-{kind}"

            def check(name=name, mode=mode, kind=kind):
                query, expected, traces = nested_declaration(harness, mode, kind)
                # Each nested dynamic declaration retains its conservative SQL
                # metadata credit. Keep the 32-MiB refusal as a separate control.
                requested = dict(POLICY, memory_bytes=64 << 20) if mode.startswith("nested") else POLICY
                consume(harness, name, query, expected, fields(kind), traces, terminal="batches", requested=requested)
                return {"traces": traces, "complete_rows": len(expected), "grant": requested["memory_bytes"]}
            harness.case(name, check)
            if mode.startswith("nested"):
                denied_name = name + "-metadata-denied"

                def denied(name=denied_name, mode=mode, kind=kind):
                    query, expected, traces = nested_declaration(harness, mode, kind)
                    harness.json(harness.logs / (name + "-predeclared.json"), {
                        "sql": query._relation_statement(), "expected_if_admitted": expected,
                        "expected_outcome": "metadata_reservation_denied", "grant": POLICY["memory_bytes"]})
                    before = set(harness.output.iterdir())
                    try:
                        query.collect(check=True, **POLICY)
                    except sl.ShardLoomCommandError as error:
                        harness.envelope(name, error.envelope, success=False)
                        reason = "\n".join(item.reason or item.message for item in error.envelope.diagnostics)
                        match = re.search(r"memory reservation denied: requested=(\d+), reserved=(\d+), limit=(\d+)", reason)
                        assert match, reason
                        requested, reserved, limit = map(int, match.groups())
                        assert requested + reserved > limit == POLICY["memory_bytes"]
                    else:
                        raise AssertionError("nested SQL declarations bypassed their metadata grant")
                    assert set(harness.output.iterdir()) == before
                    assert all(trace["opened"] == 0 for trace in traces)
                    return {"traces": traces, "reservation_denied": True, "producer_demand": False,
                            "grant": limit, "requested": requested, "reserved": reserved}
                harness.case(denied_name, denied)


def nested_declaration(harness, mode, kind):
    left, a = frame(harness, LEFT, SCHEMA, width=1)
    right, b = frame(harness, RIGHT, SCHEMA, width=1)
    traces, rows = [a, b], list(RIGHT)
    if mode.startswith("nested"):
        if mode == "nested-independent":
            deepest, c = frame(harness, [dict(n=1, s="deep")], SCHEMA)
            traces.append(c)
        else:
            deepest = right
        right = right.with_column("keep", sl.exists_source(inner(deepest))).filter("keep").select("n", "s")
        rows = [row for row in rows if row["n"] is not None]
    elif mode == "repeated-join":
        relation = right._relation_statement()
        right = LazyFrame.from_sql(f"SELECT a.n AS n,a.s AS s FROM ({relation}) AS a CROSS JOIN ({relation}) AS b",
                            right.client, source_bindings=right._declared_sources())
        rows = [row for row in rows for _ in RIGHT]
    else:
        assert mode == "repeated-set"
        right, rows = right.union_all(right), rows * 2
    return apply(left, right, kind, "sql"), oracle(kind, LEFT, rows), traces


def dynamic_outer(harness):
    for shared in (False, True):
        for kind in ("in", "row-in", "exists", "all-ge"):
            name = f"dynamic-subquery-dynamic-outer-{kind}-shared-{shared}"

            def check(name=name, kind=kind, shared=shared):
                source, a = frame(harness, LEFT, SCHEMA, width=1)
                left = source.select("n AS entity", "'a' AS category", "1 AS amount").pivot_table(
                    index="entity", columns="category", values="amount", aggfunc="sum").select("entity AS n", "'x' AS s")
                if shared:
                    right, traces, rows = source, [a], LEFT
                else:
                    right, b = frame(harness, RIGHT, SCHEMA, width=1)
                    traces, rows = [a, b], RIGHT
                outer_rows = [dict(n=row["n"], s="x") for row in LEFT]
                expected = oracle(kind, outer_rows, rows)
                schema = (("n", T("int64", True)), ("s", T("utf8", False)), fields(kind)[-1])
                report = consume(harness, name, apply(left, right, kind), expected, schema, traces)
                assert report.envelope.field_int("relational_dynamic_schema_stages") == 5
                assert report.envelope.field_int("native_input_replay_prefilled_sources") == 0
                return {"traces": traces, "dynamic_outer_and_inner": True}
            harness.case(name, check)


def files(harness):
    paths = []
    for label, rows in (("outer", LEFT), ("inner", RIGHT)):
        target = harness.output / ("dynamic-subquery-file-" + label + ".vortex")
        paths.append(target)
        name = "dynamic-subquery-file-prepare-" + label

        def prepare(name=name, rows=rows, target=target):
            report = harness.context.from_rows(rows, schema=SCHEMA).write_vortex(target, **POLICY)
            harness.envelope(name, report.envelope)
            return {"artifact": harness.artifact(target)}
        harness.case(name, prepare)
    for kind in ("in", "exists", "all-le"):
        for side in ("outer", "inner", "inner-and-resident"):
            name = f"dynamic-subquery-file-{kind}-{side}"

            def check(name=name, kind=kind, side=side):
                source, trace = frame(harness, RIGHT if side == "outer" else LEFT, SCHEMA)
                left = harness.context.read_vortex(paths[0]) if side == "outer" else source
                right = source if side == "outer" else harness.context.read_vortex(paths[1])
                rows = RIGHT
                if side == "inner-and-resident":
                    right = right.union_all(harness.context.from_rows(RIGHT, schema=SCHEMA))
                    rows = RIGHT * 2
                consume(harness, name, apply(left, right, kind, "sql"), oracle(kind, LEFT, rows), fields(kind), [trace])
                return {"trace": trace, "file": harness.artifact(paths[0 if side == "outer" else 1])}
            harness.case(name, check)


def rich(harness):
    schema, base_fields, rows, expected_rows = rich_fixture()
    schema = dict(schema, n="int64", s="utf8")
    keys = ((1, "a"), (2, "b"), (3, "c"), (None, "z"))
    rows = [dict(row, n=value, s=text) for row, (value, text) in zip(rows, keys, strict=True)]
    expected_rows = [dict(row, n=value, s=text) for row, (value, text) in zip(expected_rows, keys, strict=True)]
    for kind in ("in", "row-not-in", "exists", "any-lt"):
        for terminal in ("collect", "vortex"):
            name = f"dynamic-subquery-rich-outer-{kind}-{terminal}"

            def check(name=name, kind=kind, terminal=terminal):
                left, a = frame(harness, rows, schema, width=1)
                right, b = frame(harness, RIGHT, SCHEMA, width=1)
                query, expected = apply(left, right, kind), oracle(kind, expected_rows, RIGHT)
                output_fields = base_fields + (("n", T("int64", True)), ("s", T("utf8", True)), fields(kind)[-1])
                if terminal == "collect":
                    consume(harness, name, query, expected, output_fields, [a, b])
                    artifact = None
                else:
                    harness.json(harness.logs / (name + "-predeclared.json"), expected)
                    target = harness.output / (name + ".vortex")
                    report = query.write_vortex(target, **POLICY)
                    observed = harness.context.read_vortex(target).collect(check=True, **POLICY)
                    harness.envelope(name + "-reopened", observed.envelope)
                    harness.values(name, observed.result_rows, expected)
                    assert observed.result_schema == output_fields
                    complete_inputs(harness, name, report.envelope, [a, b])
                    artifact = harness.artifact(target)
                return {"traces": [a, b], "artifact": artifact}
            harness.case(name, check)
