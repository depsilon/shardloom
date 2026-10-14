# SPDX-License-Identifier: Apache-2.0
"""Compatibility preparation and cancellation through the real batch transaction."""

from __future__ import annotations

import json

from native_streaming_protocol_cases import Peer

SCHEMA = {"entity": "int64", "category": "utf8", "amount": "float64"}
ROWS = [
    {"entity": 1, "category": "a", "amount": 2.0},
    {"entity": 1, "category": "b", "amount": 3.0},
    {"entity": 2, "category": "a", "amount": None},
    {"entity": None, "category": "a", "amount": 4.0},
]
FORMATS = ("vortex", "csv", "json", "jsonl", "parquet", "arrow_ipc", "avro", "orc")


def run(harness):
    context = harness.context
    policy = {"memory_bytes": 32 << 20, "max_parallelism": 1}
    sources = {}

    def consume(name, iterator, expected):
        with iterator:
            actual = [row for batch in iterator for row in batch.result_rows]
            assert iterator.report is not None and iterator._process.poll() == 0
            envelope = iterator.report.envelope
        harness.values(name, actual, expected)
        harness.envelope(name, envelope)
        assert envelope.field_int("execution_resource_declared_memory_bytes") == policy["memory_bytes"]
        return envelope

    for fmt in FORMATS:
        name = "preparation-" + fmt
        def check(fmt=fmt, name=name):
            path = harness.output / (name + (".arrow" if fmt == "arrow_ipc" else "." + fmt))
            created = getattr(context.from_rows(ROWS, schema=SCHEMA), "write_" + fmt)(path, **policy)
            harness.envelope(name + "-fixture", created.envelope)
            sources[fmt] = path
            reader = "read_json" if fmt == "jsonl" else "read_" + fmt
            kwargs = {"schema": SCHEMA} if fmt in ("csv", "json", "jsonl") else {}
            frame = getattr(context, reader)(path, **kwargs).select("entity", "category", "amount")
            frame = frame.order_by("entity", "category", nulls="last")
            for attempt in ("raw", "reuse"):
                envelope = consume(name + "-" + attempt, frame.iter_batches(batch_rows=2, **policy), ROWS)
                assert envelope.field_int("native_input_batch_sources") == 0
                assert envelope.field_int("relational_normalized_source_count") == int(fmt != "vortex")
        harness.case(name, check)

    path = sources["csv"]
    statement = f"SELECT * FROM PIVOT((SELECT entity,category,amount FROM '{path}' WHERE amount IS NOT NULL), " + \
        "'" + json.dumps({"index": "entity", "columns": "category", "values": "amount", "aggregate": "sum"}) + "') AS p ORDER BY entity NULLS LAST"
    workflow = dict(input_uri=str(path), input_format="csv", source_schema="entity:int64,category:utf8,amount:float64",
                    sql_statement=statement, **policy)
    def dynamic_pivot():
        consume("preparation-dynamic-pivot",
            harness.client.public_workflow_batches("sql", batch_rows=1, **workflow), [
                {"entity": 1, "pivot_a": 2.0, "pivot_b": 3.0},
                {"entity": None, "pivot_a": 4.0, "pivot_b": None},
            ])
    harness.case("preparation-dynamic-pivot", dynamic_pivot)

    for stream_right in (False, True):
        name = "preparation-mixed-join-right-" + str(stream_right)
        def mixed(name=name, stream_right=stream_right):
            trace = []
            def producer():
                trace.append("start")
                yield [{"entity": 1, "note": "matched"}, {"entity": 3, "note": "absent"}]
                trace.append("end")
            stream = context.from_batches(producer, schema={"entity": "int64", "note": "utf8"}, streaming=True)
            file = context.read_csv(path, schema=SCHEMA)
            left, right = (file, stream) if stream_right else (stream, file)
            note, category = ("d.note", "f.category") if stream_right else ("f.note", "d.category")
            query = left.join(right, on="entity", how="inner").select(
                note + " AS note", category + " AS category").order_by("category")
            expected = [{"note": "matched", "category": category} for category in ("a", "b")]
            envelope = consume(name, query.iter_batches(batch_rows=1, **policy), expected)
            harness.completed(name + "-input", envelope, batches=1, rows=2)
            assert trace == ["start", "end"]
            destination = harness.output / (name + ".vortex")
            written = query.write_vortex(destination, **policy)
            harness.completed(name + "-write", written.envelope, batches=1, rows=2)
            assert trace == ["start", "end", "start", "end"]
            reopened = context.read_vortex(destination).collect(check=True, **policy)
            harness.envelope(name + "-reopened", reopened.envelope)
            harness.values(name + "-reopened", reopened.result_rows, expected)
        harness.case(name, mixed)

    # Cancellation arrives immediately after declaration, before result delivery.
    # The original compatibility refusal must not mask the cancellation signal.
    for dynamic in (False, True):
        for failure in ("cancel", "eof"):
            name = f"preparation-{failure}-dynamic-{dynamic}"
            def cancelled(name=name, dynamic=dynamic, failure=failure):
                local = harness.output / name
                local.mkdir()
                source = local / "input.csv"
                source.write_bytes(path.read_bytes())
                sentinel = local / "foreign.vortex"
                sentinel.write_bytes(b"foreign destination must survive")
                args = dict(workflow, input_uri=str(source), sql_statement=(
                    statement.replace(str(path), str(source)) if dynamic else f"SELECT entity FROM '{source}'"))
                with Peer(harness, name, workflow=args) as peer:
                    if failure == "eof":
                        peer.process.stdin.close()
                    else:
                        peer.send({"kind": "cancel"})
                    result = peer.failed()
                    raw = json.dumps(peer.frames[-1]["value"])
                    assert "cancelled" in raw, raw
                assert sentinel.read_bytes() == b"foreign destination must survive"
                assert source.read_bytes() == path.read_bytes()
                # There is no successful publication or residual staging in this
                # pre-delivery cancellation. Directories may have been created.
                assert {item for item in local.rglob("*") if item.is_file()} == {source, sentinel}
                return result
            harness.case(name, cancelled)
