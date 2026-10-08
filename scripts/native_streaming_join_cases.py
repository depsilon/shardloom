# SPDX-License-Identifier: Apache-2.0
"""Complete mixed-source joins through the public DataFrame and SQL surfaces."""

from __future__ import annotations

import json

from shardloom.errors import ShardLoomCommandError
from shardloom.query import SqlWorkflow
from native_streaming_input_cases import declaration

SCHEMA = {"n": "int64", "s": "utf8"}
ORDINARY = [{"n": 1, "s": "r1"}, {"n": 3, "s": "r3"}, {"n": None, "s": "rn"}]
STREAM = [{"n": 1, "s": "a"}, {"n": 2, "s": "b"}, {"n": None, "s": "n"}, {"n": 1, "s": "c"}]
BATCHES = [[], STREAM[:2], [], STREAM[2:], []]
KINDS = ("inner", "left", "right", "full", "semi", "anti", "cross")


def oracle(left, right, kind):
    result, matched = [], set()
    for row in left:
        matches = [(index, other) for index, other in enumerate(right)
                   if kind == "cross" or (row["n"] is not None and row["n"] == other["n"])]
        if kind in ("semi", "anti"):
            if bool(matches) == (kind == "semi"):
                result.append({"left_value": row["s"]})
        else:
            for index, other in matches:
                result.append({"left_value": row["s"], "right_value": other["s"]})
                matched.add(index)
            if not matches and kind in ("left", "full"):
                result.append({"left_value": row["s"], "right_value": None})
    if kind in ("right", "full"):
        result.extend({"left_value": None, "right_value": row["s"]}
                      for index, row in enumerate(right) if index not in matched)
    return result


def joined(source, other, stream_right, kind):
    left, right = (other, source) if stream_right else (source, other)
    query = left.join(right, how=kind, **({} if kind == "cross" else {"on": "n"}))
    columns = ["f.s AS left_value"]
    if kind not in ("semi", "anti"):
        columns.append("d.s AS right_value")
    return query.select(*columns)


def frame(harness, batches=BATCHES, *, streaming=True):
    trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}
    def produce():
        trace["opened"] += 1
        try:
            for batch in batches:
                trace["batches"] += 1
                trace["rows"] += len(batch)
                yield batch
            trace["ended"] = True
        finally:
            trace["closed"] = True
    return harness.context.from_batches(produce, schema=SCHEMA, streaming=streaming), trace


def run(harness):
    policy = {"memory_gb": 1, "max_parallelism": 1}
    workspace = harness.output / "join-small-spill"
    workspace.mkdir()
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    native = harness.output / "join-ordinary.vortex"
    result = harness.context.from_rows(ORDINARY, schema=SCHEMA).write_vortex(native, **policy)
    harness.envelope("join-ordinary-fixture", result.envelope)
    harness.json(harness.logs / "join-small-oracles.json", {
        "schema": SCHEMA, "ordinary": ORDINARY, "stream_batches": BATCHES,
        "expected": {f"right-{side}-{kind}": oracle(ORDINARY if side else STREAM, STREAM if side else ORDINARY, kind)
                     for side in (False, True) for kind in KINDS},
        "native_grant_bytes": 1 << 30, "spill": spill,
    })

    def completed(name, envelope, trace, spilled, stream_right, stages=1):
        assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        harness.completed(name, envelope, batches=trace["batches"], rows=trace["rows"])
        assert envelope.field_int("relational_ordered_join_stages") == (stages if spilled else 0)
        assert envelope.field_int("native_input_join_build_rows_detached") == (4 if stream_right else 3) + (3 if stages == 2 else 0)
        assert envelope.field_int("native_input_join_build_batches_detached") > 0
        assert list(workspace.iterdir()) == []

    for spilled in (False, True):
        requested = dict(policy, spill=spill) if spilled else policy
        for stream_right in (False, True):
            for other_kind in ("file", "resident"):
                for kind in KINDS:
                    for surface in (("dataframe", "sql") if other_kind == "file" else ("dataframe",)):
                        name = f"join-{surface}-{other_kind}-{kind}-right-{stream_right}-spill-{spilled}"
                        def check(name=name, spilled=spilled, stream_right=stream_right,
                                  other_kind=other_kind, kind=kind, surface=surface, requested=requested):
                            source, trace = frame(harness)
                            other = (harness.context.read_vortex(native) if other_kind == "file"
                                     else harness.context.from_rows(ORDINARY, schema=SCHEMA))
                            expected = oracle(ORDINARY if stream_right else STREAM,
                                              STREAM if stream_right else ORDINARY, kind)
                            if surface == "sql":
                                left, right = (other, source) if stream_right else (source, other)
                                operator = {"semi": "LEFT SEMI", "anti": "LEFT ANTI"}.get(kind, kind.upper())
                                columns = "l.s AS left_value" + ("" if kind in ("semi", "anti") else ",r.s AS right_value")
                                statement = f"SELECT {columns} FROM '{left.source.uri}' AS l {operator} JOIN '{right.source.uri}' AS r"
                                if kind != "cross":
                                    statement += " ON l.n = r.n"
                                arguments = declaration(source)
                                if spilled:
                                    arguments["spill"] = spill
                                iterator = harness.client.public_workflow_batches("sql", sql_statement=statement,
                                                                                 batch_rows=2, **arguments)
                            else:
                                iterator = joined(source, other, stream_right, kind).iter_batches(batch_rows=2, **requested)
                            actual, retained, schema = [], [], None
                            assert trace["opened"] == 0
                            with iterator:
                                for batch in iterator:
                                    assert iterator.report is None and len(batch.result_rows) <= 2
                                    if stream_right:
                                        assert trace["ended"]
                                    if schema is None:
                                        schema = batch.result_schema
                                    assert batch.result_schema == schema
                                    actual.extend(batch.result_rows)
                                    retained.append(batch)
                                assert iterator.report is not None and iterator._process.poll() == 0
                                envelope = iterator.report.envelope
                            harness.values(name, actual, expected)
                            assert [row for batch in retained for row in batch.result_rows] == expected
                            completed(name, envelope, trace, spilled, stream_right)
                        harness.case(name, check)

            for destination in ("collect", "vortex"):
                name = f"join-composed-{destination}-right-{stream_right}-spill-{spilled}"
                def composed(name=name, stream_right=stream_right, spilled=spilled,
                             destination=destination, requested=requested):
                    source, trace = frame(harness)
                    other = harness.context.read_vortex(native)
                    left, right = (other, source) if stream_right else (source, other)
                    query = left.join(right, on="n", how="full").select("f.n AS n", "f.s AS label")
                    query = query.join(other, on="n", how="left").select("f.label AS cohort").group_by("cohort").agg(rows="count(*)")
                    query = SqlWorkflow(
                        f"SELECT cohort,rows FROM ({query._relation_statement()}) AS groups ORDER BY rows DESC,cohort ASC NULLS LAST LIMIT 3 OFFSET 1",
                        harness.client, source_bindings=query._declared_sources())
                    expected = ([{"cohort": None, "rows": 2}, {"cohort": "r3", "rows": 1}, {"cohort": "rn", "rows": 1}]
                                if stream_right else [{"cohort": value, "rows": 1} for value in ("a", "b", "c")])
                    if destination == "collect":
                        result = query.collect(check=True, **requested)
                        harness.values(name, result.result_rows, expected)
                    else:
                        path = harness.output / (name + ".vortex")
                        result = query.write_vortex(path, **requested)
                        reopened = harness.context.read_vortex(path).collect(check=True, **policy)
                        harness.envelope(name + "-reopen", reopened.envelope)
                        harness.values(name + "-reopen", reopened.result_rows, expected)
                    completed(name, result.envelope, trace, spilled, stream_right, stages=2)
                    assert result.envelope.field_int("relational_ordered_aggregate_stages") == int(spilled)
                harness.case(name, composed)

    for second_streaming in (False, True):
        name = f"join-deny-second-batch-streaming-{second_streaming}"
        def deny(name=name, second_streaming=second_streaming):
            first, first_trace = frame(harness)
            second, second_trace = frame(harness, streaming=second_streaming)
            try:
                joined(first, second, False, "inner").collect(check=True, **policy)
            except ShardLoomCommandError as error:
                harness.envelope(name, error.envelope, success=False)
                assert "batch" in json.dumps(error.envelope.raw).lower()
            else:
                raise AssertionError("multiple batch producers were admitted")
            assert first_trace["opened"] == second_trace["opened"] == 0
        harness.case(name, deny)

    for stream_right in (False, True):
        name = f"join-late-producer-zero-right-{stream_right}"
        def fail(name=name, stream_right=stream_right):
            trace = {"pulled": 0, "closed": False}
            def produce():
                try:
                    trace["pulled"] += 1
                    yield STREAM[:1]
                    trace["pulled"] += 1
                    raise RuntimeError("late zero-result join producer")
                finally:
                    trace["closed"] = True
            source = harness.context.from_batches(produce(), schema=SCHEMA, streaming=True)
            query = joined(source, harness.context.read_vortex(native), stream_right, "full").limit(0)
            iterator = query.iter_batches(**dict(policy, spill=spill))
            try:
                with iterator:
                    assert list(iterator) == []
            except RuntimeError as error:
                assert str(error) == "late zero-result join producer"
            else:
                raise AssertionError("zero limit hid producer failure")
            assert trace == {"pulled": 2, "closed": True}
            assert iterator.report is None and iterator._process.poll() is not None
            assert list(workspace.iterdir()) == []
        harness.case(name, fail)
