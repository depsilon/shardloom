# SPDX-License-Identifier: Apache-2.0
"""Independent finite producers through actual public SQL and native writes."""

from __future__ import annotations

from shardloom.query import LazyFrame
from native_streaming_join_cases import BATCHES, KINDS, ORDINARY, SCHEMA, STREAM, frame, joined, oracle
from native_streaming_protocol_cases import Peer


def run(harness):
    policy = {"memory_bytes": 32 << 20, "max_parallelism": 1}
    workspace = harness.output / "multiple-spill"
    workspace.mkdir()
    (workspace / "foreign").write_bytes(b"preserve")
    spill = {"workspace": str(workspace), "quota_bytes": 128 << 20, "buffer_bytes": 1 << 20}
    harness.json(harness.logs / "multiple-input-fixtures.json", {
        "schema": SCHEMA, "first_batches": BATCHES, "second_batches": [ORDINARY],
        "policy": policy, "spill": spill,
    })

    def completed(name, envelope, traces, streaming=(True, True)):
        for trace in traces:
            assert trace["opened"] == 1 and trace["ended"] and trace["closed"], trace
        selected = [trace for trace, admitted in zip(traces, streaming, strict=True) if admitted]
        harness.completed(name, envelope, sources=len(selected),
            batches=sum(trace["batches"] for trace in selected), rows=sum(trace["rows"] for trace in selected))
        assert envelope.field_int("execution_resource_declared_memory_bytes") == policy["memory_bytes"]
        assert (workspace / "foreign").read_bytes() == b"preserve"
        assert list(workspace.iterdir()) == [workspace / "foreign"]

    for spilled in (False, True):
        requested = dict(policy, spill=spill) if spilled else policy
        for side in (False, True):
            for kind in KINDS:
                name = f"multiple-join-{kind}-right-{side}-spill-{spilled}"
                def check(name=name, side=side, kind=kind, requested=requested):
                    first, first_trace = frame(harness)
                    second, second_trace = frame(harness, [ORDINARY])
                    query = joined(first, second, side, kind)
                    expected = oracle(ORDINARY if side else STREAM, STREAM if side else ORDINARY, kind)
                    with query.iter_batches(batch_rows=2, **requested) as iterator:
                        actual, retained = [], []
                        for batch in iterator:
                            assert iterator.report is None
                            actual.extend(batch.result_rows)
                            retained.append(batch)
                        assert iterator.report is not None and iterator._process.poll() == 0
                        envelope = iterator.report.envelope
                    harness.values(name, actual, expected)
                    assert [row for batch in retained for row in batch.result_rows] == expected
                    completed(name, envelope, (first_trace, second_trace))
                harness.case(name, check)

    for empty_first, empty_second in ((True, False), (False, True), (True, True)):
        name = f"multiple-empty-{empty_first}-{empty_second}"
        def empty(name=name, empty_first=empty_first, empty_second=empty_second):
            first, first_trace = frame(harness, [] if empty_first else BATCHES)
            second, second_trace = frame(harness, [] if empty_second else [ORDINARY])
            query = joined(first, second, False, "full")
            result = query.collect(check=True, **policy)
            harness.values(name, result.result_rows,
                oracle([] if empty_first else STREAM, [] if empty_second else ORDINARY, "full"))
            completed(name, result.envelope, (first_trace, second_trace))
        harness.case(name, empty)

    for resident in (False, True):
        name = f"multiple-sql-write-resident-{resident}"
        def sql_write(name=name, resident=resident):
            first, first_trace = frame(harness)
            second, second_trace = frame(harness, [ORDINARY], streaming=not resident)
            ordinary = harness.context.from_rows([{"n": 1, "label": "kept"}], schema={"n": "int64", "label": "utf8"})
            statement = ("SELECT q.left_value AS left_value,q.right_value AS right_value,c.label AS label FROM ("
                f"SELECT a.n AS join_key,a.s AS left_value,b.s AS right_value FROM '{first.source.uri}' AS a "
                f"JOIN '{second.source.uri}' AS b ON a.n=b.n) AS q "
                f"JOIN '{ordinary.source.uri}' AS c ON q.join_key=c.n ORDER BY left_value")
            bindings = first._declared_sources() + second._declared_sources() + ordinary._declared_sources()
            query = LazyFrame.from_sql(statement, harness.client, source_bindings=bindings)
            expected = [{"left_value": value, "right_value": "r1", "label": "kept"} for value in ("a", "c")]
            destination = harness.output / (name + ".vortex")
            written = query.write_vortex(destination, spill=spill, **policy)
            completed(name, written.envelope, (first_trace, second_trace), (True, not resident))
            reopened = harness.context.read_vortex(destination).collect(check=True, **policy)
            harness.envelope(name + "-reopened", reopened.envelope)
            harness.values(name + "-reopened", reopened.result_rows, expected)
        harness.case(name, sql_write)

    for fault_right in (False, True):
        for destination in ("zero", "write"):
            name = f"multiple-late-failure-right-{fault_right}-{destination}"
            def late_failure(name=name, fault_right=fault_right, destination=destination):
                fault = {"opened": 0, "closed": False, "failed": False}
                def broken():
                    fault["opened"] += 1
                    try:
                        yield STREAM[:2]
                        fault["failed"] = True
                        raise RuntimeError(name + " sentinel")
                    finally:
                        fault["closed"] = True
                first = harness.context.from_batches(broken, schema=SCHEMA, streaming=True)
                second, second_trace = frame(harness, [ORDINARY])
                query = joined(first, second, fault_right, "inner")
                before = set(harness.output.iterdir())
                try:
                    if destination == "zero":
                        query.limit(0).collect(check=True, spill=spill, **policy)
                    else:
                        query.write_vortex(harness.output / (name + ".vortex"), spill=spill, **policy)
                except RuntimeError as error:
                    assert str(error) == name + " sentinel"
                else:
                    raise AssertionError("late input failure was certified")
                assert fault == {"opened": 1, "closed": True, "failed": True}
                if second_trace["opened"]:
                    assert second_trace["closed"]
                assert set(harness.output.iterdir()) == before
                assert list(workspace.iterdir()) == [workspace / "foreign"]
            harness.case(name, late_failure)

    for failure in ("wrong-uri", "wrong-ordinal", "early-end", "cancel", "eof"):
        name = "multiple-protocol-" + failure
        def protocol(name=name, failure=failure):
            first, _ = frame(harness)
            second, _ = frame(harness, [ORDINARY])
            workflow = dict(policy, input_uri=first.source.uri, input_format="memory",
                source_bindings={source.source.uri: {"input_format": "memory", "memory_input": dict(source.source.memory_input)}
                                 for source in (first, second)},
                sql_statement=f"SELECT a.n FROM '{first.source.uri}' AS a JOIN '{second.source.uri}' AS b ON a.n=b.n")
            before = set(harness.output.iterdir())
            with Peer(harness, name, write=True, workflow=workflow) as peer:
                # The build side completes before probe demand. Its ordinal
                # must not carry into the second producer's first request.
                demand = peer.read()
                assert demand["kind"] == "input" and demand["uri"] == second.source.uri and demand["index"] == 0
                peer.send({"kind": "rows", "uri": second.source.uri, "index": 0, "rows": [["1", "r1"]]})
                demand = peer.read()
                assert demand["uri"] == second.source.uri and demand["index"] == 1
                peer.send({"kind": "end", "uri": second.source.uri, "index": 1})
                demand = peer.read()
                assert demand["kind"] == "input" and demand["uri"] == first.source.uri and demand["index"] == 0
                if failure == "cancel":
                    peer.send({"kind": "cancel"})
                elif failure == "eof":
                    peer.process.stdin.close()
                else:
                    peer.send({"kind": "end" if failure == "early-end" else "rows",
                        "uri": second.source.uri if failure == "wrong-uri" else first.source.uri,
                        "index": 1 if failure in ("wrong-ordinal", "early-end") else 0,
                        **({} if failure == "early-end" else {"rows": [["1", "a"]]})})
                result = peer.failed()
            assert set(harness.output.iterdir()) == before
            return result
        harness.case(name, protocol)
