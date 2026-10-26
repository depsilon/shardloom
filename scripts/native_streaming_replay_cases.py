# SPDX-License-Identifier: Apache-2.0
"""Once-per-execution input with full-schema native replay through public APIs."""

from __future__ import annotations

from shardloom.errors import ShardLoomCommandError
from shardloom.query import LazyFrame
from shardloom._result_schema import ResultType
from native_streaming_join_cases import KINDS, STREAM, frame, joined, oracle
from native_streaming_dynamic_cases import frame as dynamic_frame, pivot, ROWS as PIVOT_ROWS
from native_streaming_protocol_cases import Peer
from native_typed_input_fixtures import rich_fixture
from native_report_evidence import require_native_allocation_peak


def run(harness):
    policy = {"memory_bytes": 32 << 20, "max_parallelism": 1}
    workspace = harness.output / "replay-spill"
    workspace.mkdir()
    foreign = workspace / "foreign"
    foreign.write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "replay-fixtures.json", {
        "rows": STREAM, "pivot_rows": PIVOT_ROWS, "policy": policy, "spill": spill,
        "join_oracles": {kind: oracle(STREAM, STREAM, kind) for kind in KINDS},
    })

    def clean():
        assert foreign.read_bytes() == b"preserve" and list(workspace.iterdir()) == [foreign]

    def completed(name, envelope, traces, *, passes=1, replay_sources=1, replay_rows=None, spilled_sources=0):
        for trace in traces:
            assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        rows = sum(trace["rows"] for trace in traces)
        harness.completed(name, envelope, sources=len(traces),
            batches=sum(trace["batches"] for trace in traces), rows=rows)
        for key, value in {"sources": replay_sources, "passes": passes,
                           "rows": rows if replay_rows is None else replay_rows,
                           "rows_detached": rows if replay_sources else 0}.items():
            assert envelope.field_int("native_input_replay_" + key) == value, (key, envelope.raw)
        assert envelope.field_int("native_input_replay_spilled_sources") == spilled_sources
        require_native_allocation_peak(name, envelope, policy["memory_bytes"])
        clean()

    def consume(name, query, expected, traces, requested, **counts):
        assert all(trace["opened"] == 0 for trace in traces)
        retained, actual, schema = [], [], None
        with query.iter_batches(batch_rows=2, **requested) as iterator:
            for batch in iterator:
                assert all(trace["ended"] for trace in traces)
                assert iterator.report is None and len(batch.result_rows) <= 2
                if schema is None:
                    schema = batch.result_schema
                assert batch.result_schema == schema
                actual.extend(batch.result_rows)
                retained.append(batch)
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
        harness.values(name, actual, expected)
        assert [row for batch in retained for row in batch.result_rows] == expected
        completed(name, envelope, traces, **counts)

    for spilled in (False, True):
        requested = dict(policy, spill=spill) if spilled else policy
        for kind in KINDS:
            for surface in ("dataframe", "sql"):
                name = f"replay-self-{kind}-{surface}-spill-{spilled}"
                def check(name=name, kind=kind, surface=surface, requested=requested):
                    source, trace = frame(harness)
                    query = joined(source, source, False, kind)
                    if surface == "sql":
                        query = LazyFrame.from_sql(query._relation_statement(), harness.client,
                                            source_bindings=query._declared_sources())
                    consume(name, query, oracle(STREAM, STREAM, kind), [trace], requested)
                harness.case(name, check)

        for shape in ("empty", "zero", "filters", "three-scans", "window", "two-repeated"):
            name = f"replay-{shape}-spill-{spilled}"
            def composed(name=name, shape=shape, requested=requested):
                source, trace = frame(harness, [] if shape == "empty" else [[], STREAM[:2], [], STREAM[2:], []])
                traces, counts = [trace], {}
                query = joined(source, source, False, "inner")
                expected = oracle(STREAM, STREAM, "inner")
                if shape in ("empty", "zero"):
                    expected = []
                    if shape == "zero":
                        query = query.limit(0)
                elif shape == "filters":
                    query = source.filter("n >= 1").join(source.filter("n <= 1").select("n"), on="n") \
                        .select("f.s AS value")
                    expected = [{"value": value} for value in ("a", "a", "c", "c")]
                elif shape == "three-scans":
                    query = query.join(source, condition="f.left_value = d.s") \
                        .select("f.left_value AS value", "d.s AS third")
                    expected = [{"value": value, "third": value} for value in ("a", "a", "b", "c", "c")]
                    counts = {"passes": 2, "replay_rows": 8}
                elif shape == "window":
                    query = source.window("ROW_NUMBER() OVER (ORDER BY n ASC NULLS LAST) AS rn") \
                        .join(source, on="s").select("f.rn AS rn", "d.s AS value")
                    expected = [{"rn": rn, "value": value} for rn, value in ((1, "a"), (3, "b"), (4, "n"), (2, "c"))]
                elif shape == "two-repeated":
                    other, other_trace = frame(harness, [[{"n": 3, "s": "u"}, {"n": 4, "s": "v"}]])
                    query = query.join(joined(other, other, False, "inner"), how="cross") \
                        .select("f.left_value AS a", "d.right_value AS b")
                    expected = [{"a": a, "b": b} for a in ("a", "a", "b", "c", "c") for b in ("u", "v")]
                    traces.append(other_trace)
                    counts = {"passes": 2, "replay_sources": 2}
                consume(name, query, expected, traces, requested, **counts)
            harness.case(name, composed)

        for shape in ("two-pivots", "pivot-raw", "join-pivot", "one-pass-successive"):
            name = f"replay-dynamic-{shape}-spill-{spilled}"
            def dynamic(name=name, shape=shape, requested=requested):
                source, trace = dynamic_frame(harness)
                first = pivot(source)
                counts = {}
                if shape == "two-pivots":
                    query = first.join(first, on="entity").select("f.entity AS entity", "f.pivot_a AS a", "d.pivot_b AS b")
                    expected = [{"entity": 1, "a": 3.0, "b": 5.0}, {"entity": 2, "a": 18.0, "b": None}]
                elif shape == "pivot-raw":
                    query = first.join(source, on="entity").select("f.entity AS entity", "f.pivot_a AS a", "d.amount AS amount")
                    expected = [{"entity": row["entity"], "a": 3.0 if row["entity"] == 1 else 18.0, "amount": row["amount"]}
                                for row in PIVOT_ROWS]
                elif shape == "join-pivot":
                    query = pivot(source.join(source, on="entity").select(
                        "f.entity AS entity", "f.category AS category", "d.amount AS amount"))
                    expected = [{"entity": 1, "pivot_a": 8.0, "pivot_b": 8.0},
                                {"entity": 2, "pivot_a": 36.0, "pivot_b": None}]
                else:
                    query = pivot(first.select("entity", "'x' AS category", "pivot_a AS amount"))
                    expected = [{"entity": 1, "pivot_x": 3.0}, {"entity": 2, "pivot_x": 18.0}]
                    counts = {"passes": 0, "replay_sources": 0, "replay_rows": 0}
                consume(name, query, expected, [trace], requested, **counts)
            harness.case(name, dynamic)

        for destination in ("collect", "vortex"):
            name = f"replay-rich-{destination}-spill-{spilled}"
            def rich(name=name, destination=destination, requested=requested):
                schema, fields, rows, expected = rich_fixture()
                rows = [{"row_id": index, **row} for index, row in enumerate(rows)]
                expected = [{"row_id": index, **row} for index, row in enumerate(expected)]
                source, trace = dynamic_frame(harness, rows, schema={"row_id": "int64", **schema})
                query = source.join(source.select("row_id"), on="row_id").select(
                    "f.row_id AS row_id", *(f"f.{name} AS {name}" for name in schema))
                wanted_schema = (("row_id", ResultType("int64", True)), *fields)
                if destination == "collect":
                    result = query.collect(check=True, **requested)
                    harness.values(name, result.result_rows, expected)
                    assert result.result_schema == wanted_schema
                else:
                    target = harness.output / (name + ".vortex")
                    result = query.write_vortex(target, **requested)
                    reopened = harness.context.read_vortex(target).collect(check=True, **policy)
                    harness.envelope(name + "-reopened", reopened.envelope)
                    harness.values(name + "-reopened", reopened.result_rows, expected)
                    assert reopened.result_schema == wanted_schema
                completed(name, result.envelope, [trace])
            harness.case(name, rich)

    def rich_spooled():
        name = "replay-rich-actual-spill-write-reopen"
        schema, fields, rows, expected = rich_fixture()
        trace = {"opened": 0, "batches": 0, "rows": 0, "ended": False, "closed": False}
        def produce():
            trace["opened"] += 1
            try:
                for start in range(0, 1281, 41):
                    batch = [{"row_id": index, "padding": "p" * 4096, **rows[index % len(rows)]}
                             for index in range(start, min(start + 41, 1281))]
                    trace["batches"] += 1
                    trace["rows"] += len(batch)
                    yield batch
                trace["ended"] = True
            finally:
                trace["closed"] = True
        source = harness.context.from_batches(produce,
            schema={"row_id": "int64", "padding": "utf8", **schema}, streaming=True)
        query = source.join(source.select("row_id"), on="row_id").select(
            "f.row_id AS row_id", *(f"f.{column} AS {column}" for column in schema))
        target = harness.output / (name + ".vortex")
        result = query.write_vortex(target, spill=spill, **policy)
        completed(name, result.envelope, [trace], spilled_sources=1)
        assert result.envelope.field_int("relational_spill_runs_written") > 2
        assert result.envelope.field_int("relational_spill_merge_passes") > 0
        reopened = harness.context.read_vortex(target).collect(check=True, **policy)
        harness.envelope(name + "-reopened", reopened.envelope)
        harness.values(name, reopened.result_rows,
            [{"row_id": index, **expected[index % len(expected)]} for index in range(1281)])
        assert reopened.result_schema == (("row_id", ResultType("int64", True)), *fields)
    harness.case("replay-rich-actual-spill-write-reopen", rich_spooled)

    for failure in ("producer", "schema", "overflow"):
        for write in (False, True):
            name = f"replay-late-{failure}-write-{write}"
            def failed(name=name, failure=failure, write=write):
                trace = {"opened": 0, "closed": False}
                def produce():
                    trace["opened"] += 1
                    try:
                        yield [{"n": 1}]
                        if failure == "producer":
                            raise RuntimeError("replay producer sentinel")
                        yield [{"bad": 2}] if failure == "schema" else [{"n": 2**63 - 1}]
                    finally:
                        trace["closed"] = True
                source = harness.context.from_batches(produce, schema={"n": "int64"}, streaming=True)
                query = source.join(source.select("n"), on="n").select(
                    "f.n + 1 AS n" if failure == "overflow" else "f.n AS n")
                before = set(harness.output.iterdir())
                iterator = None
                try:
                    if write:
                        query.write_vortex(harness.output / (name + ".vortex"), spill=spill, **policy)
                    else:
                        iterator = query.iter_batches(batch_rows=1, spill=spill, **policy)
                        with iterator:
                            list(iterator)
                except (RuntimeError, ValueError, ShardLoomCommandError) as error:
                    if failure == "producer":
                        assert str(error) == "replay producer sentinel"
                    elif failure == "overflow":
                        assert isinstance(error, ShardLoomCommandError)
                        harness.envelope(name, error.envelope, success=False)
                    else:
                        assert isinstance(error, ValueError)
                else:
                    raise AssertionError("failed replay was certified")
                assert trace == {"opened": 1, "closed": True}
                if iterator is not None:
                    assert iterator.report is None and iterator._process.poll() is not None
                assert set(harness.output.iterdir()) == before
                clean()
            harness.case(name, failed)

    for failure in ("cancel", "eof", "wrong-end"):
        name = "replay-protocol-" + failure
        def protocol(name=name, failure=failure):
            source, _ = frame(harness)
            query = joined(source, source, False, "inner")
            workflow = dict(policy, input_uri=source.source.uri, input_format="memory", spill=spill,
                source_bindings={source.source.uri: {"input_format": "memory", "memory_input": dict(source.source.memory_input)}},
                sql_statement=query._relation_statement())
            before = set(harness.output.iterdir())
            with Peer(harness, name, write=True, workflow=workflow) as peer:
                peer.demand(0)
                peer.rows(0, [["1", "a"]])
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
