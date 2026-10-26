# SPDX-License-Identifier: Apache-2.0
"""Execution-time pivot discovery over finite public batch producers."""

from __future__ import annotations

import json
from decimal import Decimal

from shardloom.errors import ShardLoomCommandError
from shardloom.query import LazyFrame
from shardloom._result_schema import ResultType as Type
from native_streaming_protocol_cases import Peer
from native_report_evidence import require_native_allocation_peak

SCHEMA = {"entity": "int64", "category": "utf8", "amount": "int64"}
ROWS = [{"entity": 1, "category": "a", "amount": 3},
        {"entity": 1, "category": "b", "amount": 5},
        {"entity": 2, "category": "a", "amount": 7},
        {"entity": 2, "category": "a", "amount": 11}]
EXPECTED = [{"entity": 1, "pivot_a": 3.0, "pivot_b": 5.0},
            {"entity": 2, "pivot_a": 18.0, "pivot_b": None}]


def frame(harness, rows=ROWS, *, schema=SCHEMA):
    trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}
    def produce():
        trace["opened"] += 1
        try:
            for batch in ([], rows[:2], [], rows[2:]):
                trace["batches"] += 1
                trace["rows"] += len(batch)
                yield batch
            trace["ended"] = True
        finally:
            trace["closed"] = True
    return harness.context.from_batches(produce, schema=schema, streaming=True), trace


def pivot(source, aggregate="sum"):
    if aggregate == "first_unique":
        return source.pivot(index="entity", columns="category", values="amount")
    return source.pivot_table(index="entity", columns="category", values="amount", aggfunc=aggregate)


def run(harness):
    policy = {"memory_bytes": 32 << 20, "max_parallelism": 1}
    workspace = harness.output / "dynamic-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "dynamic-fixtures.json", {
        "rows": ROWS, "schema": SCHEMA, "expected_sum": EXPECTED, "policy": policy, "spill": spill,
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def completed(name, envelope, traces, stages=1):
        for trace in traces:
            assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        harness.completed(name, envelope, sources=len(traces),
            batches=sum(trace["batches"] for trace in traces), rows=sum(trace["rows"] for trace in traces))
        assert envelope.field("relational_schema_binding") == "during_execution"
        assert envelope.field_int("relational_dynamic_schema_stages") == stages
        require_native_allocation_peak(name, envelope, policy["memory_bytes"])
        clean()

    def consume(name, query, expected, traces, requested, stages=1, columns=None):
        assert all(trace["opened"] == 0 for trace in traces)
        with query.iter_batches(batch_rows=1, **requested) as iterator:
            actual, retained, schema = [], [], None
            for batch in iterator:
                assert all(trace["ended"] and trace["closed"] for trace in traces)
                assert iterator.report is None and len(batch.result_rows) <= 1
                if schema is None:
                    schema = batch.result_schema
                assert batch.result_schema == schema
                assert list(batch.result_columns) == (columns or list(expected[0]))
                actual.extend(batch.result_rows)
                retained.append(batch)
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
        harness.values(name, actual, expected)
        assert [row for batch in retained for row in batch.result_rows] == expected
        completed(name, envelope, traces, stages)

    for spilled in (False, True):
        requested = dict(policy, spill=spill) if spilled else policy
        for aggregate in ("sum", "count", "mean", "min", "max", "first_unique"):
            for surface in ("dataframe", "sql"):
                name = f"dynamic-{aggregate}-{surface}-spill-{spilled}"
                def check(name=name, aggregate=aggregate, surface=surface, requested=requested):
                    source, trace = frame(harness, ROWS[:3] if aggregate == "first_unique" else ROWS)
                    query = pivot(source, aggregate)
                    if surface == "sql":
                        query = LazyFrame.from_sql(query._relation_statement(), harness.client,
                                            source_bindings=query._declared_sources())
                    if aggregate == "count":
                        expected = [{"entity": 1, "pivot_a": 1, "pivot_b": 1},
                                    {"entity": 2, "pivot_a": 2, "pivot_b": None}]
                    else:
                        value = {"sum": 18, "mean": 9, "min": 7, "max": 11, "first_unique": 7}[aggregate]
                        expected = [EXPECTED[0], {"entity": 2, "pivot_a": value, "pivot_b": None}]
                    consume(name, query, expected, [trace], requested)
                harness.case(name, check)

        for shape in ("empty", "limit-before-zero", "limit-after-zero", "limit-before-one"):
            name = f"dynamic-{shape}-spill-{spilled}"
            def limited(name=name, shape=shape, requested=requested):
                source, trace = frame(harness, [] if shape == "empty" else ROWS)
                if shape.startswith("limit-before"):
                    source = source.limit(1 if shape.endswith("one") else 0)
                query = pivot(source)
                if shape == "limit-after-zero":
                    query = query.limit(0)
                expected = [{"entity": 1, "pivot_a": 3.0}] if shape == "limit-before-one" else []
                columns = (["entity", "pivot_a", "pivot_b"] if shape == "limit-after-zero" else
                           ["entity", "pivot_a"] if shape == "limit-before-one" else ["entity"])
                consume(name, query, expected, [trace], requested, columns=columns)
            harness.case(name, limited)

        for shape in ("project", "aggregate", "window", "successive", "two-sources", "write"):
            name = f"dynamic-composed-{shape}-spill-{spilled}"
            def composed(name=name, shape=shape, requested=requested):
                source, trace = frame(harness)
                query, traces, stages = pivot(source), [trace], 1
                expected = EXPECTED
                if shape == "project":
                    query = query.select("entity", "pivot_a + 1 AS adjusted").filter("adjusted > 4").sort("entity")
                    expected = [{"entity": 2, "adjusted": 19.0}]
                elif shape in ("aggregate", "window"):
                    prefix = ("SELECT SUM(pivot_a) AS total,COUNT(*) AS groups" if shape == "aggregate" else
                              "SELECT entity,ROW_NUMBER() OVER (ORDER BY pivot_a DESC) AS position")
                    query = LazyFrame.from_sql(f"{prefix} FROM ({query._relation_statement()}) AS p" +
                        (" ORDER BY entity" if shape == "window" else ""), harness.client,
                        source_bindings=query._declared_sources())
                    expected = ([{"total": 21.0, "groups": 2}] if shape == "aggregate" else
                                [{"entity": 1, "position": 2}, {"entity": 2, "position": 1}])
                elif shape == "successive":
                    query = pivot(query.select("entity", "'x' AS category", "pivot_a AS amount"))
                    expected = [{"entity": 1, "pivot_x": 3.0}, {"entity": 2, "pivot_x": 18.0}]
                    stages = 2
                elif shape == "two-sources":
                    other, other_trace = frame(harness, [{**row, "amount": row["amount"] * 2} for row in ROWS])
                    query = query.join(pivot(other), on="entity").select(
                        "f.entity AS entity", "f.pivot_a AS left_a", "d.pivot_a AS right_a")
                    traces.append(other_trace)
                    stages = 2
                    expected = [{"entity": 1, "left_a": 3.0, "right_a": 6.0},
                                {"entity": 2, "left_a": 18.0, "right_a": 36.0}]
                if shape == "write":
                    target = harness.output / (name + ".vortex")
                    result = query.write_vortex(target, **requested)
                    completed(name, result.envelope, traces)
                    reopened = harness.context.read_vortex(target).collect(check=True, **policy)
                    harness.envelope(name + "-reopened", reopened.envelope)
                    harness.values(name + "-reopened", reopened.result_rows, expected)
                else:
                    consume(name, query, expected, traces, requested, stages)
            harness.case(name, composed)

        for label, dtype, supplied, transported in (
            ("uint64", "uint64", [2**64 - 1, 0], [2**64 - 1, 0]),
            ("decimal", "decimal128(38,2)", [Decimal("999999999999999999999999999999999999.99"), Decimal("-0.01")],
             ["decimal128(38,2):99999999999999999999999999999999999999", "decimal128(38,2):-1"]),
            ("timestamp", "timestamp_micros", [-(2**63), 2**63 - 1], [-(2**63), 2**63 - 1]),
            ("binary", "binary", [b"\x00\xff", b""], ["00ff", ""]),
            ("list", {"type": "list", "item": "int64"}, [[None, 1], []], [[None, 1], []]),
        ):
            name = f"dynamic-typed-{label}-spill-{spilled}"
            def typed(name=name, label=label, dtype=dtype, supplied=supplied, transported=transported, requested=requested):
                rows = [{"entity": 1, "category": "a", "amount": supplied[0]},
                        {"entity": 2, "category": "b", "amount": supplied[1]}]
                source, trace = frame(harness, rows, schema=dict(SCHEMA, amount=dtype))
                query = pivot(source, "first_unique")
                expected = [{"entity": 1, "pivot_a": transported[0], "pivot_b": None},
                            {"entity": 2, "pivot_a": None, "pivot_b": transported[1]}]
                target = harness.output / (name + ".vortex")
                result = query.write_vortex(target, **requested)
                completed(name, result.envelope, [trace])
                reopened = harness.context.read_vortex(target).collect(check=True, **policy)
                harness.envelope(name + "-reopened", reopened.envelope)
                harness.values(name + "-reopened", reopened.result_rows, expected)
                value_type = (Type("decimal128", True, (38, 2)) if label == "decimal" else
                              Type("list", True, (Type("int64", True),)) if label == "list" else
                              Type("timestamp_micros" if label == "timestamp" else label, True))
                assert reopened.result_schema == (("entity", Type("int64", True)),
                                                  ("pivot_a", value_type), ("pivot_b", value_type))
            harness.case(name, typed)

    for failure in ("unobserved", "duplicate", "width", "producer"):
        name = "dynamic-failure-" + failure
        def failed(name=name, failure=failure):
            if failure == "producer":
                trace = {"opened": 0, "closed": False}
                def broken():
                    trace["opened"] += 1
                    try:
                        yield ROWS[:2]
                        raise RuntimeError("dynamic producer sentinel")
                    finally:
                        trace["closed"] = True
                source = harness.context.from_batches(broken, schema=SCHEMA, streaming=True)
            else:
                source, trace = frame(harness, ROWS if failure != "width" else [
                    {"entity": 1, "category": f"d{index:03d}", "amount": index} for index in range(128)])
            query = pivot(source, "first_unique" if failure == "duplicate" else "sum")
            if failure == "unobserved":
                query = query.select("pivot_missing")
            before = set(harness.output.iterdir())
            try:
                query.write_vortex(harness.output / (name + ".vortex"), spill=spill, **policy)
            except (ShardLoomCommandError, RuntimeError) as error:
                if failure == "producer":
                    assert str(error) == "dynamic producer sentinel"
                else:
                    assert isinstance(error, ShardLoomCommandError)
                    harness.envelope(name, error.envelope, success=False)
                    expected = {"unobserved": "not present", "duplicate": "multiple values", "width": "128 result columns"}[failure]
                    assert expected in json.dumps(error.envelope.raw)
                assert trace["opened"] == 1 and trace["closed"]
                if failure == "unobserved":
                    assert trace["ended"]
            else:
                raise AssertionError("failed dynamic discovery published a destination")
            assert set(harness.output.iterdir()) == before
            clean()
        harness.case(name, failed)

    for failure in ("cancel", "eof", "wrong-end"):
        name = "dynamic-protocol-" + failure
        def protocol(name=name, failure=failure):
            source, _ = frame(harness)
            query = pivot(source)
            workflow = dict(policy, input_uri=source.source.uri, input_format="memory", spill=spill,
                source_bindings={source.source.uri: {"input_format": "memory", "memory_input": dict(source.source.memory_input)}},
                sql_statement=query._relation_statement())
            before = set(harness.output.iterdir())
            with Peer(harness, name, write=True, workflow=workflow) as peer:
                peer.demand(0)
                peer.rows(0, [["1", "a", "3"]])
                peer.demand(1)
                if failure == "cancel":
                    peer.send({"kind": "cancel"})
                elif failure == "eof":
                    peer.process.stdin.close()
                else:
                    peer.send({"kind": "end", "uri": peer.uri, "index": 0})
                result = peer.failed()
            assert set(harness.output.iterdir()) == before
            clean()
            return result
        harness.case(name, protocol)
